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
	"cloud.google.com/go/storage"
	"github.com/google/medical_claims_tools/bcda"
	"github.com/google/medical_claims_tools/bulkfhir"
	"github.com/google/medical_claims_tools/fhir"
	"github.com/google/medical_claims_tools/fhirstore"
	"github.com/google/medical_claims_tools/gcs"
	"github.com/google/medical_claims_tools/internal/counter"
	"github.com/google/medical_claims_tools/internal/iohelpers"
)

// TODO(b/244579147): consider a yml config to represent configuration inputs
// to the bulk_fhir_fetch program.
var (
	clientID     = flag.String("client_id", "", "BCDA API client ID (required)")
	clientSecret = flag.String("client_secret", "", "BCDA API client secret (required)")
	outputPrefix = flag.String("output_prefix", "", "Data output prefix. If unset, no file output will be written.")
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
	sinceFile            = flag.String("since_file", "", "Optional. If specified, the fetch program will read the latest since timestamp in this file to use when fetching data from BCDA. DO NOT run simultaneous fetch programs with the same since file. Once the fetch is completed successfully, fetch will write the BCDA transaction timestamp for this fetch operation to the end of the file specified here, to be used in the subsequent run (to only fetch new data since the last successful run). The first time fetch is run with this flag set, it will fetch all data. If the file is of the form `gs://<GCS Bucket Name>/<Since File Name>` it will attempt to write the since file to the GCS bucket and file specified.")
	noFailOnUploadErrors = flag.Bool("no_fail_on_upload_errors", false, "If true, fetch will not fail on FHIR store upload errors, and will continue (and write out updates to since_file) as normal.")
	pendingJobURL        = flag.String("pending_job_url", "", "(For debug/manual use). If set, skip creating a new FHIR export job on the bulk fhir server. Instead, bulk_fhir_fetch will download and process the data from the existing pending job url provided by this flag. bulk_fhir_fetch will wait until the provided job id is complete before proceeding.")
)

var (
	errInvalidSince            = errors.New("invalid since timestamp")
	errUploadFailures          = errors.New("fhir store upload failures")
	errMustRectifyForFHIRStore = errors.New("for now, rectify must be enabled for FHIR store upload")
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

	if cfg.enableFHIRStore && !cfg.rectify {
		return errMustRectifyForFHIRStore
	}

	if cfg.fhirStoreEnableGCSBasedUpload && cfg.fhirStoreGCSBasedUploadBucket == "" {
		return errMustSpecifyGCSBucket
	}

	if cfg.outputPrefix == "" && !cfg.enableFHIRStore {
		log.Warningln("outputPrefix is not set and neither is enableFHIRStore: BCDA fetch will not produce any output.")
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

	parsedSince, err := getSince(cfg.since, cfg.sinceFile, cfg.gcsEndpoint)
	if err != nil {
		return err
	}

	jobURL := cfg.pendingJobURL
	if jobURL == "" {
		jobURL, err = cl.StartBulkDataExport(bcda.ResourceTypes, parsedSince, bulkfhir.ExportGroupAll)
		if err != nil {
			return fmt.Errorf("unable to StartBulkDataExport: %v", err)
		}
		log.Infof("Started BCDA job: %s\n", jobURL)
	}

	var monitorResult *bulkfhir.MonitorResult
	for monitorResult = range cl.MonitorJobStatus(jobURL, jobStatusPeriod, jobStatusTimeout) {
		if monitorResult.Error != nil {
			log.Errorf("error while checking the jobStatus: %v", monitorResult.Error)
		}
		if !monitorResult.Status.IsComplete {
			if monitorResult.Status.PercentComplete >= 0 {
				log.Infof("BCDA Export job pending, progress: %d", monitorResult.Status.PercentComplete)
			} else {
				log.Info("BCDA Export job pending, progress unknown")
			}
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
		resourceName, err := bulkfhir.ResourceTypeCodeToName(r)
		if err != nil {
			return err
		}

		for i, url := range urls {
			filename := fmt.Sprintf("%s_%d.ndjson", resourceName, i)

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
		// Write since file to GCS if needed.
		if strings.HasPrefix(cfg.sinceFile, "gs://") {
			bucketName, filePath := getBucketAndFilePathFromSince(cfg.sinceFile)
			gcsClient, err := gcs.NewClient(bucketName, cfg.gcsEndpoint)
			if err != nil {
				return err
			}
			sinceFileWriter := gcsClient.GetFileWriter(filePath)
			sinceFileWriter.Write([]byte(fhir.ToFHIRInstant(jobStatus.TransactionTime)))
			if err = sinceFileWriter.Close(); err != nil {
				return err
			}
		} else {
			f, err := os.OpenFile(cfg.sinceFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				return err
			}
			defer func() {
				if err := f.Close(); err != nil {
					log.Errorf("error when closing the file writer: %v", err)
				}
			}()

			f.Write([]byte(fhir.ToFHIRInstant(jobStatus.TransactionTime) + "\n"))
		}
	}

	log.Info("bulk_fhir_fetch complete.")
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
		if err := cl.Authenticate(); err != nil {
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
	multiFilesWriter := &iohelpers.MultiWriteCloser{}
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

		gcsWriter := gcsClient.GetFHIRFileWriter(filename, since)
		multiFilesWriter.Add(gcsWriter)
	}

	return multiFilesWriter
}

func getBucketAndFilePathFromSince(sinceFile string) (bucketName, filePath string) {
	rawFilePath := strings.TrimPrefix(sinceFile, "gs://")
	splitPaths := strings.SplitN(rawFilePath, "/", 2)
	bucketName, filePath = splitPaths[0], splitPaths[1]
	return
}

func getSinceFromGCS(sinceFile, gcsEndpoint string) (time.Time, error) {
	bucketName, remainingPath := getBucketAndFilePathFromSince(sinceFile)
	gcsClient, err := gcs.NewClient(bucketName, gcsEndpoint)
	if err != nil {
		return time.Time{}, fmt.Errorf("Error getting GCS client: (%s)", err)
	}
	reader, err := gcsClient.GetFileReader(context.Background(), remainingPath)
	if err == storage.ErrObjectNotExist {
		// If that GCS file has not been created, assume that this is the first time it has been
		// called and return an empty time to fetch all data.
		return time.Time{}, nil
	} else if err != nil {
		return time.Time{}, fmt.Errorf("Error getting GCS reader: (%s)", err)
	}

	b, err := io.ReadAll(reader)
	if err != nil {
		return time.Time{}, fmt.Errorf("Error reading GCS reader: (%s)", err)
	}
	parsedSince, err := fhir.ParseFHIRInstant(string(b))
	if err != nil {
		return parsedSince, fmt.Errorf("Error parsing since file from GCS (%s)", err)
	}
	return parsedSince, nil

}

func getSince(since, sinceFile, gcsEndpoint string) (time.Time, error) {
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

		if strings.HasPrefix(sinceFile, "gs://") {
			return getSinceFromGCS(sinceFile, gcsEndpoint)
		}

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
