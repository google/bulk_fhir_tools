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
	"errors"
	"fmt"
	"time"

	log "github.com/golang/glog"
	"github.com/google/medical_claims_tools/bulkfhir"
	"github.com/google/medical_claims_tools/fhir"
	"github.com/google/medical_claims_tools/fhirstore"
	"github.com/google/medical_claims_tools/internal/counter"
)

// ErrUploadFailures is returned (wrapped) when uploads to FHIR Store have
// failed. It is primarily used to detect this specific failure in tests.
var ErrUploadFailures = errors.New("non-zero FHIR store upload errors")

// directFHIRStoreSink is a thin shim around fhirstore.Uploader to translate the
// Sink interface. Once bulk_fhir_fetch is migrated to use the processing
// library, fhirstore.Uploader will be updated to directly implement Sink.
//
// TODO(b/254648498): migrate fhirstore.Uploader to fulfil the Sink interface.
type directFHIRStoreSink struct {
	uploader             *fhirstore.Uploader
	errorCounter         *counter.Counter
	noFailOnUploadErrors bool
}

// Write is Sink.Write. This calls through to the underlying Uploader's Upload
// method.
func (dfss *directFHIRStoreSink) Write(ctx context.Context, resource ResourceWrapper) error {
	json, err := resource.JSON()
	if err != nil {
		return err
	}
	dfss.uploader.Upload(json)
	return nil
}

// Finalize is Sink.Finalize. This calls DoneUploading and Wait on the
// underlying uploader.
func (dfss *directFHIRStoreSink) Finalize(ctx context.Context) error {
	dfss.uploader.DoneUploading()
	if err := dfss.uploader.Wait(); err != nil {
		return err
	}
	if errCnt := dfss.errorCounter.CloseAndGetCount(); errCnt > 0 {
		if dfss.noFailOnUploadErrors {
			log.Warningf("%v: %d", ErrUploadFailures, errCnt)
		} else {
			return fmt.Errorf("%w: %d", ErrUploadFailures, errCnt)
		}
	}
	return nil
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

	fhirStoreClient       *fhirstore.Client
	fhirStoreGCPProject   string
	fhirStoreGCPLocation  string
	fhirStoreGCPDatasetID string
	fhirStoreID           string

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
		gbfss.ndjsonSink, err = newGCSNDJSONSink(gbfss.ndjsonSinkCtx, gbfss.gcsEndpoint, gbfss.gcsBucket, fhir.ToFHIRInstant(transactionTime), "" /* filePrefix */)
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
	opName, err := gbfss.fhirStoreClient.ImportFromGCS(
		gcsURI,
		gbfss.fhirStoreGCPProject,
		gbfss.fhirStoreGCPLocation,
		gbfss.fhirStoreGCPDatasetID,
		gbfss.fhirStoreID)

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
	FHIRStoreEndpoint    string
	FHIRStoreID          string
	FHIRProjectID        string
	FHIRLocation         string
	FHIRDatasetID        string
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

// NewFHIRStoreSink creates a new Sink which writes resources to FHIR Store,
// either directly or via GCS.
func NewFHIRStoreSink(ctx context.Context, cfg *FHIRStoreSinkConfig) (Sink, error) {
	if cfg.UseGCSUpload {
		fhirStoreClient, err := fhirstore.NewClient(ctx, cfg.FHIRStoreEndpoint)
		if err != nil {
			return nil, err
		}
		return &gcsBasedFHIRStoreSink{
			// Used only for deferred initialisation of the ndjsonSink
			ndjsonSinkCtx:         ctx,
			fhirStoreClient:       fhirStoreClient,
			fhirStoreID:           cfg.FHIRStoreID,
			fhirStoreGCPProject:   cfg.FHIRProjectID,
			fhirStoreGCPLocation:  cfg.FHIRLocation,
			fhirStoreGCPDatasetID: cfg.FHIRDatasetID,
			transactionTime:       cfg.TransactionTime,
			gcsEndpoint:           cfg.GCSEndpoint,
			gcsBucket:             cfg.GCSBucket,
			gcsImportJobTimeout:   cfg.GCSImportJobTimeout,
			gcsImportJobPeriod:    cfg.GCSImportJobPeriod,
			noFailOnUploadErrors:  cfg.NoFailOnUploadErrors,
		}, nil
	}

	errorCounter := counter.New()
	uploader, err := fhirstore.NewUploader(ctx, fhirstore.UploaderConfig{
		FHIRStoreEndpoint:   cfg.FHIRStoreEndpoint,
		FHIRStoreID:         cfg.FHIRStoreID,
		FHIRProjectID:       cfg.FHIRProjectID,
		FHIRLocation:        cfg.FHIRLocation,
		FHIRDatasetID:       cfg.FHIRDatasetID,
		MaxWorkers:          cfg.MaxWorkers,
		ErrorCounter:        errorCounter,
		ErrorFileOutputPath: cfg.ErrorFileOutputPath,
		BatchUpload:         cfg.BatchUpload,
		BatchSize:           cfg.BatchSize,
	})
	if err != nil {
		return nil, err
	}
	return &directFHIRStoreSink{
		uploader:             uploader,
		errorCounter:         errorCounter,
		noFailOnUploadErrors: cfg.NoFailOnUploadErrors,
	}, nil
}
