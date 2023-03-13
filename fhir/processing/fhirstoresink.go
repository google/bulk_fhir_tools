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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/medical_claims_tools/bulkfhir"
	"github.com/google/medical_claims_tools/fhir"
	"github.com/google/medical_claims_tools/fhirstore"
	log "github.com/google/medical_claims_tools/internal/logger"
)

// ErrUploadFailures is returned (wrapped) when uploads to FHIR Store have
// failed. It is primarily used to detect this specific failure in tests.
var ErrUploadFailures = errors.New("non-zero FHIR store upload errors")

// defaultBatchSize is the deafult batch size for FHIR store uploads in batch
// mode.
const defaultBatchSize = 5

// directFHIRStoreSink implements the processing.Sink interface to upload
// resources directly to FHIR store, either individually or batched.
type directFHIRStoreSink struct {
	fhirStoreCfg *fhirstore.Config

	// batchUpload indicates if fhirJSONs should be uploaded to FHIR store in
	// batches using executeBundle in batch mode.
	batchUpload bool
	batchSize   int

	fhirJSONs  chan string
	maxWorkers int
	wg         *sync.WaitGroup

	uploadErrorOccurred  atomic.Bool
	noFailOnUploadErrors bool
	errorFileOutputPath  string

	errNDJSONFileMut sync.Mutex
	errorNDJSONFile  *os.File
}

func (dfss *directFHIRStoreSink) init(ctx context.Context) {
	dfss.fhirJSONs = make(chan string, 100)
	dfss.wg = &sync.WaitGroup{}

	for i := 0; i < dfss.maxWorkers; i++ {
		if dfss.batchUpload {
			go dfss.uploadBatchWorker(ctx)
		} else {
			go dfss.uploadWorker(ctx)
		}
	}
}

// Write is Sink.Write. The provided resource is written to FHIR Store.
func (dfss *directFHIRStoreSink) Write(ctx context.Context, resource ResourceWrapper) error {
	json, err := resource.JSON()
	if err != nil {
		return err
	}
	dfss.wg.Add(1)
	dfss.fhirJSONs <- string(json)
	return nil
}

// Finalize is Sink.Finalize. This waits for all resources to be written to FHIR
// Store before returning. It may return an error if there was an issue writing
// resources (if NoFailOnUploadErrors was set when the sink was created), or if
// there was an issue closing the error file (if ErrorFileOutputPath was set
// when the sink was created).
func (dfss *directFHIRStoreSink) Finalize(ctx context.Context) error {
	close(dfss.fhirJSONs)
	dfss.wg.Wait()
	if dfss.errorNDJSONFile != nil {
		if err := dfss.errorNDJSONFile.Close(); err != nil {
			return err
		}
	}
	if dfss.uploadErrorOccurred.Load() {
		if dfss.noFailOnUploadErrors {
			log.Warningf("%v", ErrUploadFailures)
		} else {
			return fmt.Errorf("%w", ErrUploadFailures)
		}
	}
	return nil
}

func (dfss *directFHIRStoreSink) uploadWorker(ctx context.Context) {
	c, err := fhirstore.NewClient(ctx, dfss.fhirStoreCfg)
	if err != nil {
		log.Fatalf("error initializing FHIR store client: %v", err)
	}

	for fhirJSON := range dfss.fhirJSONs {
		err := c.UploadResource([]byte(fhirJSON))
		if err != nil {
			// TODO(b/211490544): consider adding an auto-retrying mechanism in the
			// future.
			log.Errorf("error uploading resource: %v", err)
			dfss.uploadErrorOccurred.Store(true)
			dfss.writeError(fhirJSON, err)
		}
		dfss.wg.Done()
	}
}

func (dfss *directFHIRStoreSink) uploadBatchWorker(ctx context.Context) {
	c, err := fhirstore.NewClient(ctx, dfss.fhirStoreCfg)
	if err != nil {
		log.Fatalf("error initializing FHIR store client: %v", err)
	}

	fhirBatchBuffer := make([][]byte, dfss.batchSize)
	lastChannelReadOK := true
	for lastChannelReadOK {
		var fhirJSON string
		numBufferItemsPopulated := 0
		// Attempt to populate the fhirBatchBuffer. Note that this could populate
		// from 0 up to dfss.batchSize elements before lastChannelReadOK is false.
		for i := 0; i < dfss.batchSize; i++ {
			fhirJSON, lastChannelReadOK = <-dfss.fhirJSONs
			if !lastChannelReadOK {
				break
			}
			fhirBatchBuffer[i] = []byte(fhirJSON)
			numBufferItemsPopulated++
		}

		if numBufferItemsPopulated == 0 {
			break
		}

		fhirBatch := fhirBatchBuffer[0:numBufferItemsPopulated]

		// Upload batch
		if err := c.UploadBatch(fhirBatch); err != nil {

			log.Errorf("error uploading batch: %v", err)
			dfss.uploadErrorOccurred.Store(true)
			// TODO(b/225916126): in the future, try to unpack the error and only
			// write out the resources within the bundle that failed. For now, we
			// write out all resources in the bundle to be safe.
			for _, errResource := range fhirBatch {
				dfss.writeError(string(errResource), err)
			}
		}

		for j := 0; j < numBufferItemsPopulated; j++ {
			dfss.wg.Done()
		}
	}
}

func (dfss *directFHIRStoreSink) writeError(fhirJSON string, err error) {
	if dfss.errorNDJSONFile != nil {
		data, jsonErr := json.Marshal(errorNDJSONLine{Err: err.Error(), FHIRResource: fhirJSON})
		if jsonErr != nil {
			log.Errorf("error marshaling data to write to error file: %v", jsonErr)
			return
		}
		dfss.errNDJSONFileMut.Lock()
		defer dfss.errNDJSONFileMut.Unlock()
		dfss.errorNDJSONFile.Write(data)
		dfss.errorNDJSONFile.Write([]byte("\n"))
	}
}

type errorNDJSONLine struct {
	Err          string `json:"err"`
	FHIRResource string `json:"fhir_resource"`
}

// gcsBasedFHIRStoreSink wraps an ndjsonSink which writes files to GCS, and then
// triggers the FHIR Store import process ([0]) when Finalize is called.
//
// [0]: https://cloud.google.com/healthcare-api/docs/reference/rest/v1/projects.locations.datasets.fhirStores/import
type gcsBasedFHIRStoreSink struct {
	// ndjsonSink is lazily initialised so that we can retrieve the transaction
	// time from transactionTime. The context stored here is used *only* for
	// initialising ndjsonSink, as if the sink were initialized when the Sink was
	// created.
	ndjsonSink    *ndjsonSink
	ndjsonSinkCtx context.Context

	fhirStoreClient *fhirstore.Client

	transactionTime *bulkfhir.TransactionTime

	gcsEndpoint         string
	gcsBucket           string
	gcsImportJobTimeout time.Duration
	gcsImportJobPeriod  time.Duration

	noFailOnUploadErrors bool
}

func (gbfss *gcsBasedFHIRStoreSink) Write(ctx context.Context, resource ResourceWrapper) error {
	if gbfss.ndjsonSink == nil {
		transactionTime, err := gbfss.transactionTime.Get()
		if err != nil {
			return err
		}
		// Use the stored context from NewFHIRStoreSink, in case ctx is cancelled
		// before subsequent Write calls.
		gbfss.ndjsonSink, err = newGCSNDJSONSink(gbfss.ndjsonSinkCtx, gbfss.gcsEndpoint, gbfss.gcsBucket, fhir.ToFHIRInstant(transactionTime))
		if err != nil {
			return err
		}
	}
	return gbfss.ndjsonSink.Write(ctx, resource)
}

func (gbfss *gcsBasedFHIRStoreSink) Finalize(ctx context.Context) error {
	if gbfss.ndjsonSink == nil {
		// Write was never called; nothing to do here.
		return nil
	}

	if err := gbfss.ndjsonSink.Finalize(ctx); err != nil {
		return fmt.Errorf("failed to close GCS files: %w", err)
	}

	transactionTime, err := gbfss.transactionTime.Get()
	if err != nil {
		// This should never happen; by the time we're calling this, the
		// TransactionTime should have been populated.
		return err
	}
	gcsURI := fmt.Sprintf("gs://%s/%s/**", gbfss.gcsBucket, fhir.ToFHIRInstant(transactionTime))

	log.Infof("Starting the import job from GCS location where FHIR data was saved: %s", gcsURI)
	opName, err := gbfss.fhirStoreClient.ImportFromGCS(gcsURI)

	if err != nil {
		return fmt.Errorf("failed to start import job: %w", err)
	}

	isDone := false
	deadline := time.Now().Add(gbfss.gcsImportJobTimeout)
	for !isDone && time.Now().Before(deadline) {
		time.Sleep(gbfss.gcsImportJobPeriod)
		log.Infof("GCS Import Job still pending...")

		isDone, err = gbfss.fhirStoreClient.CheckGCSImportStatus(opName)
		if err != nil {
			log.Errorf("Error reported from the GCS FHIR store Import Job: %s", err)
			if !gbfss.noFailOnUploadErrors {
				return fmt.Errorf("error from the GCS FHIR Store Import Job: %w", err)
			}
			break
		}
	}

	if !isDone && !gbfss.noFailOnUploadErrors {
		return errors.New("fhir store import via GCS timed out")
	}
	log.Infof("FHIR Store import is complete!")
	return nil
}

// FHIRStoreSinkConfig defines the configuration passed to NewFHIRStoreSink.
type FHIRStoreSinkConfig struct {
	FHIRStoreConfig      *fhirstore.Config
	NoFailOnUploadErrors bool

	// If true, the sink will write NDJSON files to GCS, and use the FHIR Store
	// import functionality to read those files into the FHIR Store.
	UseGCSUpload bool

	// Parameters for direct upload
	BatchUpload         bool
	BatchSize           int
	MaxWorkers          int
	ErrorFileOutputPath string

	// Parameters for GCS-based upload
	GCSEndpoint         string
	GCSBucket           string
	GCSImportJobTimeout time.Duration
	GCSImportJobPeriod  time.Duration
	TransactionTime     *bulkfhir.TransactionTime
}

func newGCSBasedFHIRStoreSink(ctx context.Context, cfg *FHIRStoreSinkConfig) (Sink, error) {
	fhirStoreClient, err := fhirstore.NewClient(ctx, cfg.FHIRStoreConfig)
	if err != nil {
		return nil, err
	}
	return &gcsBasedFHIRStoreSink{
		// Used only for deferred initialisation of the ndjsonSink
		ndjsonSinkCtx:        ctx,
		fhirStoreClient:      fhirStoreClient,
		transactionTime:      cfg.TransactionTime,
		gcsEndpoint:          cfg.GCSEndpoint,
		gcsBucket:            cfg.GCSBucket,
		gcsImportJobTimeout:  cfg.GCSImportJobTimeout,
		gcsImportJobPeriod:   cfg.GCSImportJobPeriod,
		noFailOnUploadErrors: cfg.NoFailOnUploadErrors,
	}, nil
}

// newDirectFHIRStoreSink initializes and returns a directFHIRStoreSink.
func newDirectFHIRStoreSink(ctx context.Context, cfg *FHIRStoreSinkConfig) (Sink, error) {
	batchSize := defaultBatchSize
	if cfg.BatchSize != 0 {
		batchSize = cfg.BatchSize
	}

	dfss := &directFHIRStoreSink{
		fhirStoreCfg:         cfg.FHIRStoreConfig,
		maxWorkers:           cfg.MaxWorkers,
		noFailOnUploadErrors: cfg.NoFailOnUploadErrors,
		errorFileOutputPath:  cfg.ErrorFileOutputPath,
		batchUpload:          cfg.BatchUpload,
		batchSize:            batchSize,
	}

	if cfg.ErrorFileOutputPath != "" {
		f, err := os.OpenFile(path.Join(cfg.ErrorFileOutputPath, "resourcesWithErrors.ndjson"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, err
		}
		dfss.errorNDJSONFile = f
	}

	dfss.init(ctx)

	return dfss, nil
}

// NewFHIRStoreSink creates a new Sink which writes resources to FHIR Store,
// either directly or via GCS.
func NewFHIRStoreSink(ctx context.Context, cfg *FHIRStoreSinkConfig) (Sink, error) {
	if cfg.UseGCSUpload {
		return newGCSBasedFHIRStoreSink(ctx, cfg)
	}
	return newDirectFHIRStoreSink(ctx, cfg)
}
