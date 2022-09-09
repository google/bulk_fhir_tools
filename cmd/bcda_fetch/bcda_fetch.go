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

// fetch is an example program that uses the bcda API client library to retrieve
// resources from the BCDA API.
package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"flag"
	log "github.com/golang/glog"
	"github.com/google/medical_claims_tools/bcda"
	"github.com/google/medical_claims_tools/bulkfhir"
	"github.com/google/medical_claims_tools/fhir"
	"github.com/google/medical_claims_tools/fhirstore"
	"github.com/google/medical_claims_tools/gcs"
	"github.com/google/medical_claims_tools/internal/counter"
)

// TODO(b/244579147): consider a yml config to represent configuration inputs
// to the bcda_fetch program.
var (
	clientID     = flag.String("client_id", "", "BCDA API client ID (required)")
	clientSecret = flag.String("client_secret", "", "BCDA API client secret (required)")
	outputPrefix = flag.String("output_prefix", "", "Data output prefix. If unset, no file output will be written.")
	useV2        = flag.Bool("use_v2", false, "This indicates if the BCDA V2 API should be used, which returns R4 mapped data.")
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

	serverURL            = flag.String("bcda_server_url", "https://sandbox.bcda.cms.gov", "The BCDA server to communicate with. By deafult this is https://sandbox.bcda.cms.gov")
	since                = flag.String("since", "", "The optional timestamp after which data should be fetched for. If not specified, fetches all available data. This should be a FHIR instant in the form of YYYY-MM-DDThh:mm:ss.sss+zz:zz.")
	sinceFile            = flag.String("since_file", "", "Optional. If specified, the fetch program will read the latest since timestamp in this file to use when fetching data from BCDA. DO NOT run simultaneous fetch programs with the same since file. Once the fetch is completed successfully, fetch will write the BCDA transaction timestamp for this fetch operation to the end of the file specified here, to be used in the subsequent run (to only fetch new data since the last successful run). The first time fetch is run with this flag set, it will fetch all data.")
	noFailOnUploadErrors = flag.Bool("no_fail_on_upload_errors", false, "If true, fetch will not fail on FHIR store upload errors, and will continue (and write out updates to since_file) as normal.")
	bcdaJobID            = flag.String("bcda_job_id", "", "DEPRECATED in favor of bcda_job_url.")
	bcdaJobURL           = flag.String("bcda_job_url", "", "If set, skip calling the BCD API to create a new data export job. Instead, bcda_fetch will download and process the data from the BCDA job url provided by this flag. bcda_fetch will wait until the provided job id is complete before proceeding.")
)

var (
	errInvalidSince            = errors.New("invalid since timestamp")
	errUploadFailures          = errors.New("fhir store upload failures")
	errMustRectifyForFHIRStore = errors.New("for now, rectify must be enabled for FHIR store upload")
	errBCDAJobIDDeprecated     = errors.New("bcda_job_id flag is deprecated in favor of the bcda_job_url flag")
	errMustSpecifyGCSBucket    = errors.New("if fhir_store_enable_gcs_based_upload=true, fhir_store_gcs_based_upload_bucket must be set")
)

const (
	// jobStatusPeriod indicates how often we check a pending job status.
	jobStatusPeriod = 5 * time.Second
	// jobStatusTimeout indicates the maximum time that should be spend checking on
	// a pending JobStatus.
	jobStatusTimeout = 6 * time.Hour
	// gcsImportJobPeriod indicates how often the program should check the FHIR
	// store GCS import job.
	gcsImportJobPeriod = 5 * time.Second
	// gcsImportJobTimeout indicates the maximum time that should be spent
	// checking on the FHIR Store GCS import job.
	gcsImportJobTimeout = 6 * time.Hour
	// maxTokenSize represents the maximum newline delimited token size in bytes
	// expected when parsing FHIR NDJSON.
	maxTokenSize = 500 * 1024
	// initialBufferSize indicates the initial buffer size in bytes to use when
	// parsing a FHIR NDJSON token.
	initialBufferSize = 5 * 1024
)

func main() {
	flag.Parse()
	if err := mainWrapper(buildMainWrapperConfig()); err != nil {
		log.Fatal(err)
	}
}

// mainWrapper allows for easier testing of the main function.
func mainWrapper(cfg mainWrapperConfig) error {
	// Init counters
	fhirStoreUploadErrorCounter := counter.New()

	if cfg.clientID == "" || cfg.clientSecret == "" {
		return errors.New("both clientID and clientSecret flags must be non-empty")
	}

	if cfg.enableFHIRStore && (cfg.fhirStoreGCPProject == "" ||
		cfg.fhirStoreGCPLocation == "" ||
		cfg.fhirStoreGCPDatasetID == "" ||
		cfg.fhirStoreID == "") {
		return errors.New("if enable_fhir_store is true, all other FHIR store related flags must be set")
	}

	if cfg.bcdaJobID != "" {
		return errBCDAJobIDDeprecated
	}

	if cfg.enableFHIRStore && !cfg.rectify {
		return errMustRectifyForFHIRStore
	}

	if cfg.fhirStoreEnableGCSBasedUpload && cfg.fhirStoreGCSBasedUploadBucket == "" {
		return errMustSpecifyGCSBucket
	}

	if cfg.outputPrefix == "" && !cfg.enableFHIRStore {
		log.Warningln("outputPrefix is not set and neither is enableFHIRStore: BCDA fetch will not produce any output.")
	}

	apiVersion := bcda.V1
	if cfg.useV2 {
		apiVersion = bcda.V2
	}

	cl, err := bcda.NewClient(cfg.serverURL, apiVersion, cfg.clientID, cfg.clientSecret)
	if err != nil {
		return fmt.Errorf("NewClient(%v, %v) error: %v", cfg.serverURL, apiVersion, err)
	}

	_, err = cl.Authenticate()
	if err != nil {
		return fmt.Errorf("Error authenticating with API: %v", err)
	}

	parsedSince, err := getSince(cfg.since, cfg.sinceFile)
	if err != nil {
		return err
	}

	jobURL := cfg.bcdaJobURL
	if jobURL == "" {
		jobURL, err = cl.StartBulkDataExport(bulkfhir.AllResourceTypes, parsedSince, bulkfhir.ExportGroupAll)
		if err != nil {
			return fmt.Errorf("unable to StartBulkDataExport: %v", err)
		}
		log.Infof("Started BCDA job: %s\n", jobURL)
	}

	var monitorResult *bulkfhir.MonitorResult
	for monitorResult = range cl.MonitorJobStatus(jobURL, jobStatusPeriod, jobStatusTimeout) {
		if monitorResult.Error != nil {
			log.Errorf("error while checking the jobStatus: %v", err)
		}
		if !monitorResult.Status.IsComplete {
			log.Infof("BCDA Export job pending, progress: %d\n", monitorResult.Status.PercentComplete)
		}
	}

	jobStatus := monitorResult.Status
	if !jobStatus.IsComplete {
		return fmt.Errorf("BCDA Job did not finish before the timeout of %v", jobStatusTimeout)
	}

	log.Infof("BCDA Job Finished. Transaction Time: %v", fhir.ToFHIRInstant(jobStatus.TransactionTime))
	log.Infof("Begin BCDA data download and write out to disk.")
	if cfg.enableFHIRStore {
		log.Infof("Data will also be uploaded to FHIR store based on provided parmaters.")
	}

	for r, urls := range jobStatus.ResultURLs {
		for i, url := range urls {
			filename := fmt.Sprintf("%s_%d.ndjson", r, i)

			r, err := getDataOrExit(cl, url, cfg.clientID, cfg.clientSecret)
			if err != nil {
				return err
			}
			defer r.Close()
			if cfg.rectify {
				rectifyAndWrite(r, filename, cfg, jobStatus.TransactionTime, fhirStoreUploadErrorCounter)
			} else {
				writeData(r, cfg.outputPrefix, filename)
			}
		}
	}

	// After files are written, trigger FHIR Store GCS import if needed.
	if cfg.fhirStoreEnableGCSBasedUpload && cfg.enableFHIRStore {
		fc, err := fhirstore.NewClient(context.Background(), cfg.fhirStoreEndpoint)
		if err != nil {
			return err
		}

		gcsURI := fmt.Sprintf("gs://%s/%s/**", cfg.fhirStoreGCSBasedUploadBucket, fhir.ToFHIRInstant(jobStatus.TransactionTime))
		log.Infof("Starting the import job from GCS location where FHIR data was saved: %s", gcsURI)
		opName, err := fc.ImportFromGCS(
			gcsURI,
			cfg.fhirStoreGCPProject,
			cfg.fhirStoreGCPLocation,
			cfg.fhirStoreGCPDatasetID,
			cfg.fhirStoreID)

		if err != nil {
			return err
		}

		isDone := false
		deadline := time.Now().Add(gcsImportJobTimeout)
		for !isDone && time.Now().Before(deadline) {
			time.Sleep(gcsImportJobPeriod)
			log.Infof("GCS Import Job still pending...")

			isDone, err = fc.CheckGCSImportStatus(opName)
			if err != nil {
				log.Errorf("Error reported from the GCS FHIR store Import Job: %s", err)
				if !cfg.noFailOnUploadErrors {
					return fmt.Errorf("error from the GCS FHIR Store Import Job: %w", err)
				}
				break
			}
		}

		if !isDone {
			return errors.New("fhir store import via GCS timed out")
		}
		log.Infof("FHIR Store import is complete!")
	}

	// Check failure counters.
	if errCnt := fhirStoreUploadErrorCounter.CloseAndGetCount(); errCnt > 0 {
		log.Warningf("non-zero FHIR Store Upload Errors: %d", errCnt)
		if !cfg.noFailOnUploadErrors {
			return fmt.Errorf("non-zero FHIR store upload errors (check logs for details): %d %w", errCnt, errUploadFailures)
		}
	}

	// Write out since file time. This should only be written if there are no
	// errors during the fetch process.
	if cfg.sinceFile != "" {
		f, err := os.OpenFile(cfg.sinceFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		defer f.Close()
		f.Write([]byte(fhir.ToFHIRInstant(jobStatus.TransactionTime) + "\n"))
	}

	log.Info("bcda_fetch complete.")
	return nil
}

func getDataOrExit(cl *bulkfhir.Client, url, clientID, clientSecret string) (io.ReadCloser, error) {
	r, err := cl.GetData(url)
	numRetries := 0
	// Retry both unauthorized and other retryable errors by re-authenticating,
	// as sometimes they appear to be related.
	for (errors.Is(err, bulkfhir.ErrorUnauthorized) || errors.Is(err, bulkfhir.ErrorRetryableHTTPStatus)) && numRetries < 5 {
		time.Sleep(2 * time.Second)
		log.Infof("Got retryable error from BCDA. Re-authenticating and trying again.")
		if _, err := cl.Authenticate(); err != nil {
			return nil, fmt.Errorf("Error authenticating with API: %w", err)
		}
		r, err = cl.GetData(url)
		numRetries++
	}

	if err != nil {
		return nil, fmt.Errorf("Unable to GetData(%s) %w", url, err)
	}

	return r, nil
}

func writeData(r io.Reader, outputPrefix, filename string) {
	f, err := os.OpenFile(fmt.Sprintf("%s_%s", outputPrefix, filename), os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		log.Exitf("Unable to create output file. Error: %v", err)
	}
	defer f.Close()

	_, err = io.Copy(f, r)
	if err != nil {
		log.Exitf("Unable to copy data to output file. Error: %v", err)
	}
}

// rectifyAndWrite is responsible for reading FHIR resources from the NDJSON
// reader provided, rectifying them, optionally writing them to disk and/or GCS,
// and uploading them directly to GCS if appropiate. This is done "all at once"
// for each FHIR resource read from the reader.
func rectifyAndWrite(r io.Reader, filename string, cfg mainWrapperConfig, since time.Time, fhirStoreUploadErrorCounter *counter.Counter) {
	filesWriter := getFilesWriter(filename, cfg, since)
	defer func() {
		if err := filesWriter.Close(); err != nil {
			log.Errorf("error when closing the file writers: %v", err)
		}
	}()

	uploader, err := fhirstore.NewUploader(fhirstore.UploaderConfig{
		FHIRStoreEndpoint:   cfg.fhirStoreEndpoint,
		FHIRStoreID:         cfg.fhirStoreID,
		FHIRProjectID:       cfg.fhirStoreGCPProject,
		FHIRLocation:        cfg.fhirStoreGCPLocation,
		FHIRDatasetID:       cfg.fhirStoreGCPDatasetID,
		MaxWorkers:          cfg.maxFHIRStoreUploadWorkers,
		ErrorCounter:        fhirStoreUploadErrorCounter,
		ErrorFileOutputPath: cfg.fhirStoreUploadErrorFileDir,
		BatchUpload:         cfg.fhirStoreEnableBatchUpload,
		BatchSize:           cfg.fhirStoreBatchUploadSize,
	})

	if err != nil {
		log.Exitf("unable to init uploader: %v", err)
	}

	// indicates if the FHIRStore Uploader should be used. It shouldn't be used
	// if using GCS based upload, as that is handled by triggering a sepearte
	// batch import job.
	shouldUseFHIRStoreUploader := cfg.enableFHIRStore && !cfg.fhirStoreEnableGCSBasedUpload

	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, initialBufferSize), maxTokenSize)
	for s.Scan() {
		fhirOut, err := fhir.RectifyBCDA(s.Bytes())
		if err != nil {
			log.Warningf("WARN: issue during rectification: %v, proceeding without rectification for this resource.", err)
			// Override fhirOut to be the original un-rectified json.
			fhirOut = s.Bytes()
		}

		// Add this FHIR resource to the queue to be written to FHIR Store via the
		// Uploader API.
		if shouldUseFHIRStoreUploader {
			uploader.Upload(fhirOut)
		}

		if filesWriter != nil {
			// We write with a newline because this is a newline delimited JSON file.
			if _, err := filesWriter.Write(append(fhirOut, byte('\n'))); err != nil {
				log.Exitf("issue during file write: %v", err)
			}
		}
	}

	// Since there is only one sender goroutine (this one), it should be safe to
	// indicate we're done sending here, after we have finished sending all the
	// work in the for loop above.
	if shouldUseFHIRStoreUploader {
		uploader.DoneUploading()
		if err := uploader.Wait(); err != nil {
			log.Warningf("error closing the uploader: %v", err)
		}
	}
}

func getFilesWriter(filename string, cfg mainWrapperConfig, since time.Time) io.WriteCloser {
	multiFilesWriter := &multiWriteCloser{}
	if cfg.outputPrefix != "" {
		w, err := os.OpenFile(fmt.Sprintf("%s_%s", cfg.outputPrefix, filename), os.O_RDWR|os.O_CREATE, 0755)
		multiFilesWriter.Add(w)
		if err != nil {
			log.Exitf("Unable to create output file: %v", err)
		}
	}

	if cfg.fhirStoreEnableGCSBasedUpload {
		gcsClient, err := gcs.NewClient(cfg.fhirStoreGCSBasedUploadBucket, cfg.gcsEndpoint)
		if err != nil {
			log.Exitf("unable to create gcs client")
		}

		gcsWriter := gcsClient.GetFileWriter(filename, since)
		multiFilesWriter.Add(gcsWriter)
	}

	return multiFilesWriter
}

func getSince(since, sinceFile string) (time.Time, error) {
	parsedSince := time.Time{}
	if since != "" && sinceFile != "" {
		return parsedSince, errors.New("only one of since or since_file flags may be set (cannot set both)")
	}

	var err error
	if since != "" {
		parsedSince, err = fhir.ParseFHIRInstant(since)
		if err != nil {
			return parsedSince, fmt.Errorf("invalid since timestamp provided (%s), should be in form YYYY-MM-DDThh:mm:ss.sss+zz:zz %w", since, errInvalidSince)
		}
	}

	if sinceFile != "" {
		if info, err := os.Stat(sinceFile); errors.Is(err, os.ErrNotExist) || (err == nil && info.Size() == 0) {
			// There is no since information, since this is the first time fetch is being
			// called on a new file or an empty file--so we return an empty time to fetch
			// all data the first time.
			return time.Time{}, nil
		}

		f, err := os.Open(sinceFile)
		if err != nil {
			return parsedSince, err
		}
		defer f.Close()

		// TODO(b/213614828): if we expect extremely large since files in the future,
		// we can make this more efficient by reading the file backwards (at the
		// expense of more complex code).
		s := bufio.NewScanner(f)
		lastLine := ""
		for s.Scan() {
			lastLine = s.Text()
		} // Scan through all lines of the file
		parsedSince, err = fhir.ParseFHIRInstant(lastLine)
		if err != nil {
			return parsedSince, fmt.Errorf("invalid since timestamp provided (%s), should be in form YYYY-MM-DDThh:mm:ss.sss+zz:zz %w", lastLine, errInvalidSince)
		}
	}

	return parsedSince, nil
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
	useV2                         bool
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
	serverURL                     string
	since                         string
	sinceFile                     string
	noFailOnUploadErrors          bool
	bcdaJobID                     string
	bcdaJobURL                    string
}

func buildMainWrapperConfig() mainWrapperConfig {
	return mainWrapperConfig{
		fhirStoreEndpoint: fhirstore.DefaultHealthcareEndpoint,
		gcsEndpoint:       gcs.DefaultCloudStorageEndpoint,

		clientID:     *clientID,
		clientSecret: *clientSecret,
		outputPrefix: *outputPrefix,
		useV2:        *useV2,
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

		serverURL:            *serverURL,
		since:                *since,
		sinceFile:            *sinceFile,
		noFailOnUploadErrors: *noFailOnUploadErrors,
		bcdaJobID:            *bcdaJobID,
		bcdaJobURL:           *bcdaJobURL,
	}
}

// multiWriteCloser wraps multiple WriteClosers into a single one.
type multiWriteCloser struct {
	writerClosers []io.WriteCloser
}

// Add a new WriteCloser to the multiWriteCloser.
func (m *multiWriteCloser) Add(wc io.WriteCloser) {
	m.writerClosers = append(m.writerClosers, wc)
}

// Write writes the specified bytes to every WriteCloser within this
// multiWriteCloser
func (m *multiWriteCloser) Write(p []byte) (n int, err error) {
	for _, w := range m.writerClosers {
		n, err = w.Write(p)
		if err != nil {
			return n, err
		}
		if n != len(p) {
			return n, io.ErrShortWrite
		}
	}
	return len(p), nil
}

// Close closes every WriteCloser within this multiWriteCloser. If multiple
// have errors on close, the errors are combined into a single error and
// returned.
func (m *multiWriteCloser) Close() error {
	errStrings := make([]string, 0, len(m.writerClosers))
	for _, w := range m.writerClosers {
		if err := w.Close(); err != nil {
			errStrings = append(errStrings, err.Error())
		}
	}
	return errors.New(strings.Join(errStrings, ","))
}
