// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package processing

import (
	"context"
	"fmt"
	"io"
	"sync"

	"os"
	"path/filepath"

	"github.com/google/medical_claims_tools/bulkfhir"
	"github.com/google/medical_claims_tools/gcs"
	log "github.com/google/medical_claims_tools/internal/logger"
	"github.com/google/medical_claims_tools/internal/metrics/aggregation"
	"github.com/google/medical_claims_tools/internal/metrics"

	cpb "github.com/google/fhir/go/proto/google/fhir/proto/r4/core/codes_go_proto"
)

const numWorkers = 10

var ndjsonChannelSizeCounter *metrics.Counter = metrics.NewCounter("ndjson-store-channel-size-counter", "The number of unread FHIR Resources that are waiting in the channel to be uploaded to GCS or saved locally as ndjson.", "1", aggregation.LastValueInGCPMaxValueInLocal)

type createFileFunc func(ctx context.Context, filename string) (io.WriteCloser, error)

type fileKey struct {
	resourceType cpb.ResourceTypeCode_Value
	sourceURL    string
}

type fileWrapper struct {
	*sync.Mutex
	w io.WriteCloser
}

type ndjsonSink struct {
	// mut is the mutex that must be held anytime ndjsonSink struct is modified.
	mut *sync.Mutex

	files      map[fileKey]*fileWrapper
	fileIndex  map[cpb.ResourceTypeCode_Value]int
	createFile createFileFunc

	resourceChan chan ResourceWrapper
	resourceWG   *sync.WaitGroup
}

// NewNDJSONSink creates a new Sink which writes resources to NDJSON files in
// the given directory. Resources are grouped by the URL they were retrieved
// from, with their file name containing the resource type and an incremented
// index to distinguish them.
//
// It is threadsafe to call Write on this Sink from multiple goroutines.
func NewNDJSONSink(ctx context.Context, directory string) (Sink, error) {
	if stat, err := os.Stat(directory); err != nil {
		return nil, fmt.Errorf("could not stat directory %q - %w", directory, err)
	} else if !stat.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", directory)
	}
	// This closure captures the `directory` parameter.
	createFile := func(ctx context.Context, filename string) (io.WriteCloser, error) {
		filename = filepath.Join(directory, filename)
		return os.Create(filename)
	}

	sink := &ndjsonSink{
		mut:          &sync.Mutex{},
		files:        map[fileKey]*fileWrapper{},
		fileIndex:    map[cpb.ResourceTypeCode_Value]int{},
		createFile:   createFile,
		resourceChan: make(chan ResourceWrapper, 100),
		resourceWG:   &sync.WaitGroup{},
	}

	for i := 0; i < numWorkers; i++ {
		go sink.writeWorker()
	}

	return sink, nil
}

// NewGCSNDJSONSink returns a Sink which writes NDJSON files to GCS. See
// NewNDJSONSink for additional documentation.
func NewGCSNDJSONSink(ctx context.Context, endpoint, bucket, directory string) (Sink, error) {
	return newGCSNDJSONSink(ctx, endpoint, bucket, directory)
}

// newGCSNDJSONSink returns the raw ndjsonSink, so that it can be embedded in
// gcsBasedFHIRStoreSink without a cast.
func newGCSNDJSONSink(ctx context.Context, endpoint, bucket, directory string) (*ndjsonSink, error) {
	gcsClient, err := gcs.NewClient(ctx, bucket, endpoint)
	if err != nil {
		return nil, err
	}

	// This closure captures the GCS client and the `directory` parameter.
	createFile := func(ctx context.Context, filename string) (io.WriteCloser, error) {
		return gcsClient.GetFileWriter(ctx, gcs.JoinPath(directory, filename)), nil
	}

	sink := &ndjsonSink{
		mut:          &sync.Mutex{},
		files:        map[fileKey]*fileWrapper{},
		fileIndex:    map[cpb.ResourceTypeCode_Value]int{},
		createFile:   createFile,
		resourceChan: make(chan ResourceWrapper, 100),
		resourceWG:   &sync.WaitGroup{},
	}

	for i := 0; i < numWorkers; i++ {
		go sink.writeWorker()
	}
	return sink, nil
}

func (ns *ndjsonSink) getWriter(ctx context.Context, resource ResourceWrapper) (*fileWrapper, error) {
	ns.mut.Lock()
	defer ns.mut.Unlock()
	key := fileKey{resource.Type(), resource.SourceURL()}
	if fw, ok := ns.files[key]; ok {
		return fw, nil
	}

	typeName, err := bulkfhir.ResourceTypeCodeToName(key.resourceType)
	if err != nil {
		return nil, err
	}

	idx := ns.fileIndex[key.resourceType]
	ns.fileIndex[resource.Type()] = idx + 1
	filename := fmt.Sprintf("%s_%d.ndjson", typeName, idx)

	w, err := ns.createFile(ctx, filename)
	if err != nil {
		return nil, err
	}
	ns.files[key] = &fileWrapper{
		Mutex: &sync.Mutex{},
		w:     w,
	}

	return ns.files[key], nil
}

// Write writes the resource to the ndjsonSink. For an ndjsonSink or gcsNDJSONSink, Write is
// non-blocking (other than writing to a channel), and may be called from multiple goroutines on
// a single sink instance.
func (ns *ndjsonSink) Write(ctx context.Context, resource ResourceWrapper) error {
	ns.resourceWG.Add(1)
	ns.resourceChan <- resource
	if err := ndjsonChannelSizeCounter.Record(ctx, int64(len(ns.resourceChan))); err != nil {
		return err
	}
	return nil
}

func (ns *ndjsonSink) writeResource(ctx context.Context, resource ResourceWrapper) error {
	fw, err := ns.getWriter(ctx, resource)
	if err != nil {
		return err
	}

	fw.Lock()
	defer fw.Unlock()

	json, err := resource.JSON()
	if err != nil {
		return err
	}
	_, err = fw.w.Write(append(json, byte('\n')))
	return err
}

func (ns *ndjsonSink) writeWorker() {
	for r := range ns.resourceChan {
		// TODO(b/277096473): plumb better context here.
		if err := ns.writeResource(context.Background(), r); err != nil {
			log.Errorf("error writing resource: %v", err)
		}
		ns.resourceWG.Done()
	}
}

func (ns *ndjsonSink) Finalize(ctx context.Context) error {
	close(ns.resourceChan)
	ns.resourceWG.Wait()

	ns.mut.Lock()
	defer ns.mut.Unlock()
	for _, fw := range ns.files {
		fw.Lock()
		defer fw.Unlock()
		if err := fw.w.Close(); err != nil {
			return err
		}
	}
	return nil
}
