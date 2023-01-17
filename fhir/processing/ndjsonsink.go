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

	"os"
	"path/filepath"

	"github.com/google/medical_claims_tools/bulkfhir"
	"github.com/google/medical_claims_tools/gcs"

	cpb "github.com/google/fhir/go/proto/google/fhir/proto/r4/core/codes_go_proto"
)

type createFileFunc func(ctx context.Context, filename string) (io.WriteCloser, error)

type fileKey struct {
	resourceType cpb.ResourceTypeCode_Value
	sourceURL    string
}

type ndjsonSink struct {
	files      map[fileKey]io.WriteCloser
	fileIndex  map[cpb.ResourceTypeCode_Value]int
	createFile createFileFunc
}

// NewNDJSONSink creates a new Sink which writes resources to NDJSON files in
// the given directory. Resources are grouped by the URL they were retrieved
// from, with their file name containing the resource type and an incremented
// index to distinguish them.
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

	return &ndjsonSink{
		files:      map[fileKey]io.WriteCloser{},
		fileIndex:  map[cpb.ResourceTypeCode_Value]int{},
		createFile: createFile,
	}, nil
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

	return &ndjsonSink{
		files:      map[fileKey]io.WriteCloser{},
		fileIndex:  map[cpb.ResourceTypeCode_Value]int{},
		createFile: createFile,
	}, nil
}

func (ns *ndjsonSink) getWriter(ctx context.Context, resource ResourceWrapper) (io.Writer, error) {
	key := fileKey{resource.Type(), resource.SourceURL()}
	if w, ok := ns.files[key]; ok {
		return w, nil
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
	ns.files[key] = w
	return w, nil
}

func (ns *ndjsonSink) Write(ctx context.Context, resource ResourceWrapper) error {
	w, err := ns.getWriter(ctx, resource)
	if err != nil {
		return err
	}
	json, err := resource.JSON()
	if err != nil {
		return err
	}
	_, err = w.Write(append(json, byte('\n')))
	return err
}

func (ns *ndjsonSink) Finalize(ctx context.Context) error {
	for _, wc := range ns.files {
		if err := wc.Close(); err != nil {
			return err
		}
	}
	return nil
}
