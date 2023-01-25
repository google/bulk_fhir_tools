// Copyright 2021 Google LLC
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

// fetch is an example program that uses the bulkfhir API client library to retrieve
// resources from a bulk FHIR API like BCDA.
package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"flag"
	log "github.com/golang/glog"
	"github.com/google/medical_claims_tools/bcda"
	"github.com/google/medical_claims_tools/bulkfhir"
	"github.com/google/medical_claims_tools/fetcher"
	"github.com/google/medical_claims_tools/fhir/processing"
	"github.com/google/medical_claims_tools/fhirstore"
	"github.com/google/medical_claims_tools/gcs"
)

// TODO(b/244579147): consider a yml config to represent configuration inputs
// to the bulk_fhir_fetch program.
var (
	clientID     = flag.String("client_id", "", "API client ID (required)")
	clientSecret = flag.String("client_secret", "", "API client secret (required)")
	outputPrefix = flag.String("output_prefix", "", "DEPRECATED: use output_dir instead.")
	outputDir    = flag.String("output_dir", "", "Data output directory. If unset, no file output will be written. This can also be a GCS path in the form of gs://bucket/folder_path. At least one bucket and folder must be specified. Do not add a file prefix, only specify the folder path.")
	rectify      = flag.Bool("rectify", false, "This indicates that this program should attempt to rectify BCDA FHIR so that it is valid R4 FHIR. This is needed for FHIR store upload.")

	enableFHIRStore             = flag.Bool("enable_fhir_store", false, "If true, this enables write to GCP FHIR store. If true, all other fhir_store_* flags and the rectify flag must be set.")
	maxFHIRStoreUploadWorkers   = flag.Int("max_fhir_store_upload_workers", 10, "The max number of concurrent FHIR store upload workers.")
	fhirStoreGCPProject         = flag.String("fhir_store_gcp_project", "", "The GCP project for the FHIR store to upload to.")
	fhirStoreGCPLocation        = flag.String("fhir_store_gcp_location", "", "The GCP location of the FHIR Store.")
	fhirStoreGCPDatasetID       = flag.String("fhir_store_gcp_dataset_id", "", "The dataset ID for the FHIR Store.")
	fhirStoreID                 = flag.String("fhir_store_id", "", "The FHIR Store ID.")
	fhirStoreUploadErrorFileDir = flag.String("fhir_store_upload_error_file_dir", "", "An optional path to a directory where an upload errors file should be written. This file will contain the FHIR NDJSON and error information of FHIR resources that fail to upload to FHIR store.")
	fhirStoreEnableBatchUpload  = flag.Bool("fhir_store_enable_batch_upload", false, "If true, uploads FHIR resources to FHIR Store in batch bundles.")
	fhirStoreBatchUploadSize    = flag.Int("fhir_store_batch_upload_size", 0, "If set, this is the batch size used to upload FHIR batch bundles to FHIR store. If this flag is not set and fhir_store_enable_batch_upload is true, a default batch size is used.")

	fhirStoreEnableGCSBasedUpload = flag.Bool("fhir_store_enable_gcs_based_upload", false, "If true, writes NDJSONs from the FHIR server to GCS, and then triggers a batch FHIR store import job from the GCS location. fhir_store_gcs_based_upload_bucket must also be set.")
	fhirStoreGCSBasedUploadBucket = flag.String("fhir_store_gcs_based_upload_bucket", "", "If fhir_store_enable_gcs_based_upload is set, this must be provided to indicate the GCS bucket to write NDJSONs to.")

	bcdaServerURL               = flag.String("bcda_server_url", "https://sandbox.bcda.cms.gov", "The BCDA server to communicate with. By deafult this is https://sandbox.bcda.cms.gov")
	enableGeneralizedBulkImport = flag.Bool("enable_generalized_bulk_import", false, "Indicates if the generalized (non-BCDA) bulk fhir flags should be used to configure the fetch. If true, fhir_server_base_url, fhir_auth_url, and fhir_auth_scopes must be set. This overrides any of the bcda-specific flags.")
	baseServerURL               = flag.String("fhir_server_base_url", "", "The full bulk FHIR server base URL to communicate with. For example, https://sandbox.bcda.cms.gov/api/v2")
	authURL                     = flag.String("fhir_auth_url", "", "The full authentication or \"token\" URL to use for authenticating with the FHIR server. For example, https://sandbox.bcda.cms.gov/auth/token")
	fhirAuthScopes              = flag.String("fhir_auth_scopes", "", "A comma seperated list of auth scopes that should be requested when getting an auth token.")

	since                = flag.String("since", "", "The optional timestamp after which data should be fetched for. If not specified, fetches all available data. This should be a FHIR instant in the form of YYYY-MM-DDThh:mm:ss.sss+zz:zz.")
	sinceFile            = flag.String("since_file", "", "Optional. If specified, the fetch program will read the latest since timestamp in this file to use when fetching data from the FHIR API. DO NOT run simultaneous fetch programs with the same since file. Once the fetch is completed successfully, fetch will write the FHIR API transaction timestamp for this fetch operation to the end of the file specified here, to be used in the subsequent run (to only fetch new data since the last successful run). The first time fetch is run with this flag set, it will fetch all data. If the file is of the form `gs://<GCS Bucket Name>/<Since File Name>` it will attempt to write the since file to the GCS bucket and file specified.")
	noFailOnUploadErrors = flag.Bool("no_fail_on_upload_errors", false, "If true, fetch will not fail on FHIR store upload errors, and will continue (and write out updates to since_file) as normal.")
	pendingJobURL        = flag.String("pending_job_url", "", "(For debug/manual use). If set, skip creating a new FHIR export job on the bulk fhir server. Instead, bulk_fhir_fetch will download and process the data from the existing pending job url provided by this flag. bulk_fhir_fetch will wait until the provided job id is complete before proceeding.")
)

var (
	errInvalidSince            = errors.New("invalid since timestamp")
	errMustRectifyForFHIRStore = errors.New("for now, rectify must be enabled for FHIR store upload")
	errMustSpecifyGCSBucket    = errors.New("if fhir_store_enable_gcs_based_upload=true, fhir_store_gcs_based_upload_bucket must be set")
)

const (
	// gcsImportJobPeriod indicates how often the program should check the FHIR
	// store GCS import job.
	gcsImportJobPeriod = 5 * time.Second
	// gcsImportJobTimeout indicates the maximum time that should be spent
	// checking on the FHIR Store GCS import job.
	gcsImportJobTimeout = 6 * time.Hour
)

func main() {
	flag.Parse()
	if err := mainWrapper(buildMainWrapperConfig()); err != nil {
		log.Exit(err)
	}
}

// mainWrapper allows for easier testing of the main function.
func mainWrapper(cfg mainWrapperConfig) error {
	ctx := context.Background()

	if cfg.clientID == "" || cfg.clientSecret == "" {
		return errors.New("both clientID and clientSecret flags must be non-empty")
	}

	if cfg.enableFHIRStore && (cfg.fhirStoreGCPProject == "" ||
		cfg.fhirStoreGCPLocation == "" ||
		cfg.fhirStoreGCPDatasetID == "" ||
		cfg.fhirStoreID == "") {
		return errors.New("if enable_fhir_store is true, all other FHIR store related flags must be set")
	}

	if cfg.enableFHIRStore && !cfg.rectify {
		return errMustRectifyForFHIRStore
	}

	if cfg.fhirStoreEnableGCSBasedUpload && cfg.fhirStoreGCSBasedUploadBucket == "" {
		return errMustSpecifyGCSBucket
	}

	if cfg.outputPrefix != "" {
		errStr := "outputPrefix is deprecated, please use outputDir instead"
		log.Error(errStr)
		return errors.New(errStr)
	}

	if cfg.outputDir == "" && !cfg.enableFHIRStore {
		log.Warningln("outputDir is not set and neither is enableFHIRStore: BCDA fetch will not produce any output.")
	}

	cl, err := getBulkFHIRClient(cfg)
	if err != nil {
		return fmt.Errorf("Error making bulkfhir client: %v", err)
	}
	defer func() {
		if err := cl.Close(); err != nil {
			log.Errorf("error closing the bulkfhir client: %v", err)
		}
	}()

	ttStore, err := getTransactionTimeStore(ctx, cfg)
	if err != nil {
		return err
	}

	transactionTime := bulkfhir.NewTransactionTime()

	var processors []processing.Processor
	if cfg.rectify {
		processors = append(processors, processing.NewBCDARectifyProcessor())
	}

	var sinks []processing.Sink
	if cfg.outputDir != "" {
		if strings.HasPrefix(cfg.outputDir, "gs://") {
			bucket, relativePath, err := gcs.PathComponents(cfg.outputDir)
			if err != nil {
				return err
			}
			gcsSink, err := processing.NewGCSNDJSONSink(ctx, cfg.gcsEndpoint, bucket, relativePath)
			if err != nil {
				return fmt.Errorf("error making GCS output sink: %v", err)
			}
			sinks = append(sinks, gcsSink)
		} else {
			// Add a local directory NDJSON sink.
			ndjsonSink, err := processing.NewNDJSONSink(ctx, cfg.outputDir)
			if err != nil {
				return fmt.Errorf("error making ndjson sink: %v", err)
			}
			sinks = append(sinks, ndjsonSink)
		}
	}

	if cfg.enableFHIRStore {
		log.Infof("Data will also be uploaded to FHIR store based on provided parameters.")
		fhirStoreSink, err := processing.NewFHIRStoreSink(ctx, &processing.FHIRStoreSinkConfig{
			FHIRStoreConfig: &fhirstore.Config{
				CloudHealthcareEndpoint: cfg.fhirStoreEndpoint,
				FHIRStoreID: cfg.fhirStoreID,
				ProjectID: cfg.fhirStoreGCPProject,
				DatasetID:   cfg.fhirStoreGCPDatasetID,
				Location:    cfg.fhirStoreGCPLocation,
			},
			NoFailOnUploadErrors: cfg.noFailOnUploadErrors,

			UseGCSUpload: cfg.fhirStoreEnableGCSBasedUpload,

			BatchUpload:         cfg.fhirStoreEnableBatchUpload,
			BatchSize:           cfg.fhirStoreBatchUploadSize,
			MaxWorkers:          cfg.maxFHIRStoreUploadWorkers,
			ErrorFileOutputPath: cfg.fhirStoreUploadErrorFileDir,

			GCSEndpoint:         cfg.gcsEndpoint,
			GCSBucket:           cfg.fhirStoreGCSBasedUploadBucket,
			GCSImportJobTimeout: gcsImportJobTimeout,
			GCSImportJobPeriod:  gcsImportJobPeriod,
			TransactionTime:     transactionTime,
		})
		if err != nil {
			return fmt.Errorf("error making FHIR Store sink: %v", err)
		}
		sinks = append(sinks, fhirStoreSink)
	}

	pipeline, err := processing.NewPipeline(processors, sinks)
	if err != nil {
		return fmt.Errorf("error making output pipeline: %v", err)
	}

	f := &fetcher.Fetcher{
		Client:               cl,
		Pipeline:             pipeline,
		TransactionTimeStore: ttStore,
		TransactionTime:      transactionTime,
		JobURL:               cfg.pendingJobURL,
		ResourceTypes:        bcda.ResourceTypes,
		ExportGroup:          bulkfhir.ExportGroupAll,
	}
	return f.Run(ctx)
}

// getBulkFHIRClient builds and returns the right kind of bulk fhir client to
// use, based on the mainWrapperConfig. If generalized FHIR flags are set,
// those are used, otherwise the bcda specific flags are used to make a
// traiditonal BCDA client. Eventually BCDA specific logic will be deprecated.
func getBulkFHIRClient(cfg mainWrapperConfig) (*bulkfhir.Client, error) {
	if cfg.useGeneralizedBulkImport {
		authenticator, err := bulkfhir.NewHTTPBasicOAuthAuthenticator(cfg.clientID, cfg.clientSecret, cfg.authURL, &bulkfhir.HTTPBasicOAuthOptions{Scopes: cfg.fhirAuthScopes})
		if err != nil {
			return nil, err
		}
		return bulkfhir.NewClient(cfg.baseServerURL, authenticator)
	}
	return bcda.NewClient(cfg.bcdaServerURL, bcda.V2, cfg.clientID, cfg.clientSecret)
}

func getTransactionTimeStore(ctx context.Context, cfg mainWrapperConfig) (bulkfhir.TransactionTimeStore, error) {
	if cfg.since != "" && cfg.sinceFile != "" {
		return nil, errors.New("only one of since or since_file flags may be set (cannot set both)")
	}

	if cfg.since != "" {
		store, err := bulkfhir.NewInMemoryTransactionTimeStore(cfg.since)
		if err != nil {
			// We match the text of errInvalidSince in tests; fmt.Errorf does not
			// allow using multiple %w verbs.
			return nil, fmt.Errorf("%v: %w", errInvalidSince, err)
		}
		return store, nil
	}

	if strings.HasPrefix(cfg.sinceFile, "gs://") {
		return bulkfhir.NewGCSTransactionTimeStore(ctx, cfg.gcsEndpoint, cfg.sinceFile)
	}

	if cfg.sinceFile != "" {
		return bulkfhir.NewLocalFileTransactionTimeStore(cfg.sinceFile), nil
	}

	return bulkfhir.NewInMemoryTransactionTimeStore("")
}

func getGCSOutputSink(ctx context.Context, gcsEndpoint, gcsPathPrefix string) (processing.Sink, error) {
	bucket, relativePath, err := gcs.PathComponents(gcsPathPrefix)
	if err != nil {
		return nil, err
	}

	return processing.NewGCSNDJSONSink(ctx, gcsEndpoint, bucket, relativePath)
}

// mainWrapperConfig holds non-flag (for now) config variables for the
// mainWrapper function. This is largely to assist in better testing without
// having to change global variables.
// TODO(b/213587622): it may be possible to safely refactor flags into this
// struct in the future.
type mainWrapperConfig struct {
	fhirStoreEndpoint string
	gcsEndpoint       string

	// Fields that originate from flags:
	clientID                      string
	clientSecret                  string
	outputPrefix                  string
	outputDir                     string
	rectify                       bool
	enableFHIRStore               bool
	maxFHIRStoreUploadWorkers     int
	fhirStoreGCPProject           string
	fhirStoreGCPLocation          string
	fhirStoreGCPDatasetID         string
	fhirStoreID                   string
	fhirStoreUploadErrorFileDir   string
	fhirStoreEnableBatchUpload    bool
	fhirStoreBatchUploadSize      int
	fhirStoreEnableGCSBasedUpload bool
	fhirStoreGCSBasedUploadBucket string
	bcdaServerURL                 string
	useGeneralizedBulkImport      bool
	baseServerURL                 string
	authURL                       string
	fhirAuthScopes                []string
	since                         string
	sinceFile                     string
	noFailOnUploadErrors          bool
	pendingJobURL                 string
}

func buildMainWrapperConfig() mainWrapperConfig {
	return mainWrapperConfig{
		fhirStoreEndpoint: fhirstore.DefaultHealthcareEndpoint,
		gcsEndpoint:       gcs.DefaultCloudStorageEndpoint,

		clientID:     *clientID,
		clientSecret: *clientSecret,
		outputPrefix: *outputPrefix,
		outputDir:    *outputDir,
		rectify:      *rectify,

		enableFHIRStore:             *enableFHIRStore,
		maxFHIRStoreUploadWorkers:   *maxFHIRStoreUploadWorkers,
		fhirStoreGCPProject:         *fhirStoreGCPProject,
		fhirStoreGCPLocation:        *fhirStoreGCPLocation,
		fhirStoreGCPDatasetID:       *fhirStoreGCPDatasetID,
		fhirStoreID:                 *fhirStoreID,
		fhirStoreUploadErrorFileDir: *fhirStoreUploadErrorFileDir,
		fhirStoreEnableBatchUpload:  *fhirStoreEnableBatchUpload,
		fhirStoreBatchUploadSize:    *fhirStoreBatchUploadSize,

		fhirStoreEnableGCSBasedUpload: *fhirStoreEnableGCSBasedUpload,
		fhirStoreGCSBasedUploadBucket: *fhirStoreGCSBasedUploadBucket,

		bcdaServerURL:            *bcdaServerURL,
		useGeneralizedBulkImport: *enableGeneralizedBulkImport,
		baseServerURL:            *baseServerURL,
		authURL:                  *authURL,
		fhirAuthScopes:           strings.Split(*fhirAuthScopes, ","),
		since:                    *since,
		sinceFile:                *sinceFile,
		noFailOnUploadErrors:     *noFailOnUploadErrors,
		pendingJobURL:            *pendingJobURL,
	}
}
