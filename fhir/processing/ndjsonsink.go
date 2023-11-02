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
	"time"

	"os"
	"path/filepath"

	"github.com/google/bulk_fhir_tools/gcs"
	log "github.com/google/bulk_fhir_tools/internal/logger"
	"github.com/google/bulk_fhir_tools/internal/metrics/aggregation"
	"github.com/google/bulk_fhir_tools/internal/metrics"

	cpb "github.com/google/fhir/go/proto/google/fhir/proto/r4/core/codes_go_proto"
)

const numWorkers = 10
const numResourcesPerShard = 1000 // this must be greater than zero.
const retryableWorkerErrLimit = 10

var (
	ndjsonChannelSizeCounter *metrics.Counter = metrics.NewCounter("ndjson-store-channel-size-counter", "The number of unread FHIR Resources that are waiting in the channel to be uploaded to GCS or saved locally as ndjson.", "1", aggregation.LastValueInGCPMaxValueInLocal)
	ndjsonSinkErrors         *metrics.Counter = metrics.NewCounter("ndjson-sink-errors", "The number of errors encountered in the GCS or local NDJSON write workers. Will have an ErrorType of FILE if it was a file operation related error, or will have ErrorType of JSON_MARSHAL if related to marshaling or fetching the FHIR JSON.", "1", aggregation.Count, "ErrorType")
)

const (
	errTypeFile        = "FILE"
	errTypeJSONMarshal = "JSON_MARSHAL"
)

// ErrWorkerError indicates one or more workers had fatal errors.
var ErrWorkerError = fmt.Errorf("at least one upload worker encountered errors, check the logs for details")

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
	// workerErrMut is the mutex that must be held anytime ndjsonSink workerError is modified.
	workerErrMut *sync.Mutex
	// Indicates if any of the workers had to return due to some kind of un-retryable error or
	// due to too many retries of some sort of retryable error. All errors should be logged by the
	// worker.
	workerErr bool

	createFile createFileFunc

	resourceChan     chan ResourceWrapper
	workerCompleteWG *sync.WaitGroup
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
		workerErrMut:     &sync.Mutex{},
		workerErr:        false,
		createFile:       createFile,
		resourceChan:     make(chan ResourceWrapper, 100),
		workerCompleteWG: &sync.WaitGroup{},
	}

	for i := 0; i < numWorkers; i++ {
		go sink.writeWorker(i)
		sink.workerCompleteWG.Add(1)
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
		workerErrMut:     &sync.Mutex{},
		workerErr:        false,
		createFile:       createFile,
		resourceChan:     make(chan ResourceWrapper, 100),
		workerCompleteWG: &sync.WaitGroup{},
	}

	for i := 0; i < numWorkers; i++ {
		go sink.writeWorker(i)
		sink.workerCompleteWG.Add(1)
	}
	return sink, nil
}

// Write writes the resource to the ndjsonSink. For an ndjsonSink or gcsNDJSONSink, Write is
// non-blocking (other than writing to a channel), and may be called from multiple goroutines on
// a single sink instance.
func (ns *ndjsonSink) Write(ctx context.Context, resource ResourceWrapper) error {
	// We need to check if the error flag is set. If it is, we should return errors and try to end
	// early as workers may not be consuming from the channel anymore.
	ns.workerErrMut.Lock()
	defer ns.workerErrMut.Unlock()
	if ns.workerErr {
		return ErrWorkerError
	}

	ns.resourceChan <- resource
	if err := ndjsonChannelSizeCounter.Record(ctx, int64(len(ns.resourceChan))); err != nil {
		return err
	}
	return nil
}

func (ns *ndjsonSink) writeWorker(workerID int) {
	var currFileShard io.WriteCloser = nil
	var err error
	itemsProcessed := 0
	retryableErrCount := 0

	for r := range ns.resourceChan {
		// Close the currFileShard and update it with a new file, if needed.
		if itemsProcessed%numResourcesPerShard == 0 || currFileShard == nil {
			if currFileShard != nil {
				if err := currFileShard.Close(); err != nil {
					log.Errorf("error closing file (ndjsonsink): %v", err)
					recordNDJSONSinkError(errTypeFile)
					time.Sleep(time.Second)
					ns.resourceChan <- r
					retryableErrCount++
					continue
				}
			}

			currFileShard, err = ns.createFile(context.Background(), fmt.Sprintf("fhir_data_%d_%d.ndjson", workerID, itemsProcessed/numResourcesPerShard))
			if err != nil {
				log.Errorf("error creating file (ndjsonsink): %v", err)
				recordNDJSONSinkError(errTypeFile)
				time.Sleep(time.Second)
				ns.resourceChan <- r
				retryableErrCount++
				continue
			}
		}

		json, err := r.JSON()
		if err != nil {
			// This is not a retryable error and we do not need to fail the pipeline for this either.
			// So we log an error, increment the error count, and continue processing other
			// resources.
			log.Errorf("unable to get JSON for resource (ndjsonsink), will SKIP resource and continue: %v", err)
			recordNDJSONSinkError(errTypeJSONMarshal)
			continue
		}
		_, err = currFileShard.Write(append(json, byte('\n')))
		if err != nil {
			log.Errorf("error writing FHIR resource to file (ndjsonsink): %v", err)
			recordNDJSONSinkError(errTypeFile)
			time.Sleep(time.Second)
			ns.resourceChan <- r
			retryableErrCount++
			continue
		}

		// If we've had too many retryable errors for this worker, we set the workerErr flag and return
		// which ends this worker.
		if retryableErrCount >= retryableWorkerErrLimit {
			log.Errorf("worker %d had too many retryable errors (%d), see logs for details", workerID, retryableWorkerErrLimit)
			ns.setWorkerErr()
			ns.workerCompleteWG.Done()
			return
		}

		itemsProcessed++
	}

	if currFileShard != nil {
		if err := currFileShard.Close(); err != nil {
			log.Errorf("error closing file (ndjsonsink): %v", err)
			// If the final file's close doesn't work, we set the error flag.
			ns.setWorkerErr()
			recordNDJSONSinkError(errTypeFile)
		}
	}
	ns.workerCompleteWG.Done()
}

func (ns *ndjsonSink) setWorkerErr() {
	ns.workerErrMut.Lock()
	ns.workerErr = true
	ns.workerErrMut.Unlock()
}

func (ns *ndjsonSink) Finalize(ctx context.Context) error {
	close(ns.resourceChan)
	ns.workerCompleteWG.Wait()

	ns.workerErrMut.Lock()
	defer ns.workerErrMut.Unlock()
	if ns.workerErr {
		return ErrWorkerError
	}

	return nil
}

func recordNDJSONSinkError(errType string) {
	if err := ndjsonSinkErrors.Record(context.Background(), 1, errType); err != nil {
		log.Errorf("error recording ndjsonSinkErrors metric: %v", err)
	}
}
