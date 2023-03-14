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

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strings"
	"sync"
	"testing"

	cpb "github.com/google/fhir/go/proto/google/fhir/proto/r4/core/codes_go_proto"
	"github.com/google/medical_claims_tools/gcs"
	"github.com/google/medical_claims_tools/internal/metrics"
	"github.com/google/medical_claims_tools/internal/testhelpers"

	"flag"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/medical_claims_tools/bulkfhir"
	"github.com/google/medical_claims_tools/fetcher"
	"github.com/google/medical_claims_tools/fhir/processing"
	"github.com/google/medical_claims_tools/fhirstore"
)

func TestMainWrapper(t *testing.T) {
	cases := []struct {
		name                              string
		rectify                           bool
		enableFHIRStore                   bool
		since                             string // empty string indicates no value provided.
		enableFHIRStoreUploadErrorFileDir bool

		sinceFileContent []byte // empty string indicates no value provided.
		// sinceFileLatestTimestamp is the timestamp expected to be sent to the
		// BCDA server.
		sinceFileLatestTimestamp string
		// sinceFileExpectedContent is the expected sinceFile content after mainWrapper
		// completes.
		sinceFileExpectedContent []byte
		// fhirStoreFailures causes the test fhir store server to return errors if
		// set to true.
		fhirStoreFailures          bool
		noFailOnUploadErrors       bool
		setPendingJobURL           bool
		fhirStoreEnableBatchUpload bool
		// unsetOutputDir sets the outputDir to empty string if true.
		unsetOutputDir bool
		// disableFHIRStoreUploadChecks will disable the portion of the test that
		// checks for FHIR store uploads, even if fhir store uploads is set to true.
		// This is needed for only some subtests.
		disableFHIRStoreUploadChecks bool
		wantError                    error
	}{
		{
			name:            "RectifyEnabledWithoutFHIRStoreBCDAV2",
			rectify:         true,
			enableFHIRStore: false,
		},
		{
			name:            "RectifyEnabledWithFHIRStoreBCDAV2",
			rectify:         true,
			enableFHIRStore: true,
		},
		{
			name:            "RectifyDisabledWithoutFHIRStoreBCDAV2",
			rectify:         false,
			enableFHIRStore: false,
		},

		// Currently, rectify must be enabled to upload to FHIR store.
		{
			name:                         "RectifyDisabledWithFHIRStoreBCDAV2",
			rectify:                      false,
			enableFHIRStore:              true,
			disableFHIRStoreUploadChecks: true,
			wantError:                    errMustRectifyForFHIRStore,
		},
		// For the Since and SinceFile test cases, BCDA Version is the primary
		// parameter to vary. The others are set to useful values and not varied.
		{
			name:            "SinceProvidedWithRectifyWithFHIRStoreBCDAV2",
			rectify:         true,
			enableFHIRStore: true,
			since:           "2006-01-02T15:04:05.000-07:00",
		},
		{
			name:      "InvalidSince",
			rectify:   true,
			since:     "2006-01-02",
			wantError: errInvalidSince,
		},
		{
			name:                     "SinceFileProvidedWithBCDAV2",
			rectify:                  true,
			enableFHIRStore:          true,
			sinceFileContent:         []byte("2013-12-09T11:00:00.123+00:00\n2015-12-09T11:00:00.123+00:00\n"),
			sinceFileLatestTimestamp: "2015-12-09T11:00:00.123+00:00",
			sinceFileExpectedContent: []byte("2013-12-09T11:00:00.123+00:00\n2015-12-09T11:00:00.123+00:00\n2020-12-09T11:00:00.123+00:00\n"),
		},
		{
			name:                     "SinceFileEmptyProvidedWithBCDAV2",
			rectify:                  true,
			enableFHIRStore:          true,
			sinceFileContent:         []byte(""),
			sinceFileExpectedContent: []byte("2020-12-09T11:00:00.123+00:00\n"),
		},
		{
			name:             "InvalidSinceFileInstant",
			rectify:          true,
			sinceFileContent: []byte("2006-01-02\n"),
			wantError:        fetcher.ErrInvalidTransactionTime,
		},
		// Test FHIR Store upload failures.
		{
			name:              "FHIRStoreUploadFailuresBCDAV2",
			rectify:           true,
			enableFHIRStore:   true,
			fhirStoreFailures: true,
			wantError:         processing.ErrUploadFailures,
		},
		{
			name:                 "FHIRStoreUploadFailuresWithNoFailFlagBCDAV2",
			rectify:              true,
			enableFHIRStore:      true,
			fhirStoreFailures:    true,
			noFailOnUploadErrors: true,
			wantError:            nil,
		},
		// Test FHIR Store upload errors file output.
		{
			name:                              "ErrorFileWithFHIRStoreUploadFailuresBCDAV2",
			rectify:                           true,
			enableFHIRStore:                   true,
			fhirStoreFailures:                 true,
			enableFHIRStoreUploadErrorFileDir: true,
			wantError:                         processing.ErrUploadFailures,
		},
		{
			name:                              "ErrorFileWithSuccessfulUploadBCDAV2",
			rectify:                           true,
			enableFHIRStore:                   true,
			enableFHIRStoreUploadErrorFileDir: true,
		},
		// Only testing cases with FHIR Store enabled for setting outputDir to ""
		{
			name:            "EmptyoutputDirWithRectifyEnabledWithFHIRStoreBCDAV2",
			rectify:         true,
			enableFHIRStore: true,
			unsetOutputDir:  true,
		},
		// Batch upload tests cases
		{
			name:                       "BatchUploadWithBCDAV2",
			enableFHIRStore:            true,
			fhirStoreEnableBatchUpload: true,
			rectify:                    true,
		},
		// TODO(b/226375559): see if we can generate some of the test cases above
		// instead of having to spell them out explicitly.
	}
	t.Parallel()
	metrics.InitNoOp()
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Declare test data:
			file1URLSuffix := "/data/10.ndjson"
			file1Data := []byte(`{"resourceType":"Patient","id":"PatientID"}`)
			file2URLSuffix := "/data/20.ndjson"
			file2Data := []byte(`{"resourceType": "Coverage", "id": "CoverageID", "contract": [{"reference": "Coverage/part-a-contract1"}]}`)
			file2DataRectified := []byte(`{"resourceType": "Coverage", "id": "CoverageID", "contract": [{"reference": "Contract/part-a-contract1"}]}`)
			file3URLSuffix := "/data/30.ndjson"
			file3Data := []byte(`{` +
				`"resourceType":"ExplanationOfBenefit","id":"EOBID",` +
				`"patient":{"reference":"PatientID"},` +
				`"use":"claim",` +
				`"status":"active",` +
				`"type":{"coding":[{"code":"71","display":"type","system":"https://bluebutton.cms.gov/resources/variables/nch_clm_type_cd"}]},` +
				`"insurer":{"reference": "insurer-id"},` +
				`"provider":{"reference":"provider-id"},` +
				`"outcome":"complete",` +
				`"insurance":[{"coverage":{"reference":"coverage"},"focal":true}],` +
				`"created":"2021-07-30T10:57:34+01:00"}`)

			exportEndpoint := "/api/v2/Patient/$export"
			expectedJobURLSuffix := "/jobs/1234"
			serverTransactionTime := "2020-12-09T11:00:00.123+00:00"

			// Setup BCDA test servers:

			// A seperate resource server is needed during testing, so that we can send
			// the jobsEndpoint response in the bcdaServer that includes a URL for the
			// bcdaResourceServer in it.
			bcdaResourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				switch req.URL.Path {
				case file1URLSuffix:
					w.Write(file1Data)
				case file2URLSuffix:
					w.Write(file2Data)
				case file3URLSuffix:
					w.Write(file3Data)
				default:
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer bcdaResourceServer.Close()

			var exportEndpointCalled mutexCounter

			jobStatusURL := ""

			bcdaServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				switch req.URL.Path {
				case "/auth/token":
					w.Write([]byte(`{"access_token": "token", "expires_in": 1200}`))
				case exportEndpoint:
					exportEndpointCalled.Increment()
					if tc.since != "" || tc.sinceFileLatestTimestamp != "" {
						expectedSince := tc.since
						if tc.since == "" {
							expectedSince = tc.sinceFileLatestTimestamp
						}
						// Since provided, so let's check it was sent to the server:
						if got := req.URL.Query()["_since"][0]; got != expectedSince {
							t.Errorf("got unexpected _since value, got %v, want: %v", got, tc.since)
							w.WriteHeader(http.StatusBadRequest)
							return
						}
					}
					w.Header()["Content-Location"] = []string{jobStatusURL}
					w.WriteHeader(http.StatusAccepted)
				case expectedJobURLSuffix:
					w.Write([]byte(fmt.Sprintf("{\"output\": [{\"type\": \"Patient\", \"url\": \"%s/data/10.ndjson\"}, {\"type\": \"Coverage\", \"url\": \"%s/data/20.ndjson\"}, {\"type\": \"ExplanationOfBenefit\", \"url\": \"%s/data/30.ndjson\"}], \"transactionTime\": \"%s\"}", bcdaResourceServer.URL, bcdaResourceServer.URL, bcdaResourceServer.URL, serverTransactionTime)))
				default:
					t.Errorf("unexpected request to bcda server: %s", req.URL.Path)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer bcdaServer.Close()

			// Set jobStatusURL after we have a bcdaServer base url:
			jobStatusURL = bcdaServer.URL + expectedJobURLSuffix

			// FHIR Store values (always set, but enable_fhir_store is set based on the
			// test case).
			gcpProject := "project"
			gcpLocation := "location"
			gcpDatasetID := "dataset"
			gcpFHIRStoreID := "fhirID"

			outputDir := t.TempDir()

			// Set mainWrapperConfig for this test case. In practice, values are
			// populated in mainWrapperConfig from flags. Setting the config struct
			// instead of the flags in tests enables parallelization with significant
			// performance improvement. A seperate test below tests that setting flags
			// properly populates mainWrapperConfig.
			cfg := mainWrapperConfig{
				clientID:                   "id",
				clientSecret:               "secret",
				outputDir:                  outputDir,
				bcdaServerURL:              bcdaServer.URL,
				fhirStoreGCPProject:        gcpProject,
				fhirStoreGCPLocation:       gcpLocation,
				fhirStoreGCPDatasetID:      gcpDatasetID,
				fhirStoreID:                gcpFHIRStoreID,
				fhirStoreEnableBatchUpload: tc.fhirStoreEnableBatchUpload,
				enableFHIRStore:            tc.enableFHIRStore,
				rectify:                    tc.rectify,
				since:                      tc.since,
				noFailOnUploadErrors:       tc.noFailOnUploadErrors,
				maxFHIRStoreUploadWorkers:  10,
			}

			if tc.setPendingJobURL {
				cfg.pendingJobURL = bcdaServer.URL + expectedJobURLSuffix
			}

			if tc.enableFHIRStoreUploadErrorFileDir {
				cfg.fhirStoreUploadErrorFileDir = t.TempDir()
			}

			var sinceTmpFile *os.File
			var err error
			if len(tc.sinceFileContent) != 0 {
				sinceTmpFile, err = ioutil.TempFile(t.TempDir(), "since_file.txt")
				if err != nil {
					t.Fatalf("unable to initialize since_file.txt: %v", err)
				}
				_, err = sinceTmpFile.Write(tc.sinceFileContent)
				if err != nil {
					sinceTmpFile.Close()
					t.Fatalf("unable to initialize since_file.txt: %v", err)
				}
				sinceTmpFile.Close()
				cfg.sinceFile = sinceTmpFile.Name()
			}

			var fhirStoreTests []testhelpers.FHIRStoreTestResource
			if tc.enableFHIRStore {
				fhirStoreTests = []testhelpers.FHIRStoreTestResource{
					{
						ResourceID:       "PatientID",
						ResourceTypeCode: cpb.ResourceTypeCode_PATIENT,
						Data:             file1Data,
					},
					{
						ResourceID:       "EOBID",
						ResourceTypeCode: cpb.ResourceTypeCode_EXPLANATION_OF_BENEFIT,
						Data:             file3Data,
					},
				}

				if tc.rectify {
					fhirStoreTests = append(fhirStoreTests, testhelpers.FHIRStoreTestResource{
						ResourceID:       "CoverageID",
						ResourceTypeCode: cpb.ResourceTypeCode_COVERAGE,
						Data:             file2DataRectified,
					})
				} else {
					fhirStoreTests = append(fhirStoreTests, testhelpers.FHIRStoreTestResource{
						ResourceID:       "CoverageID",
						ResourceTypeCode: cpb.ResourceTypeCode_COVERAGE,
						Data:             file2Data,
					})
				}

				if tc.fhirStoreFailures {
					cfg.fhirStoreEndpoint = serverAlwaysFails(t)
				} else {
					if !tc.disableFHIRStoreUploadChecks && !tc.fhirStoreEnableBatchUpload {
						cfg.fhirStoreEndpoint = testhelpers.FHIRStoreServer(t, fhirStoreTests, gcpProject, gcpLocation, gcpDatasetID, gcpFHIRStoreID)
					}
					if !tc.disableFHIRStoreUploadChecks && tc.fhirStoreEnableBatchUpload {
						expectedResources := [][]byte{file1Data, file2DataRectified, file3Data}
						// Note that differing batch upload sizes are tested more
						// extensively in the TestMainWrapper_BatchUploadSize test below.
						expectedBatchSize := 1 // this is because we have one uploader per data file, and per data file there's only one resource.
						cfg.fhirStoreEndpoint = testhelpers.FHIRStoreServerBatch(t, expectedResources, expectedBatchSize, gcpProject, gcpLocation, gcpDatasetID, gcpFHIRStoreID)
					}
				}
			}

			// Run mainWrapper:
			if err := mainWrapper(cfg); !(errors.Is(err, tc.wantError) || strings.Contains(err.Error(), tc.wantError.Error())) {
				t.Errorf("mainWrapper(%v) want error %v; got %v", cfg, tc.wantError, err)
			}

			// Check upload errors file if necessary:
			if tc.enableFHIRStoreUploadErrorFileDir {
				var wantErrors []testhelpers.ErrorNDJSONLine
				if tc.fhirStoreFailures {
					for _, ft := range fhirStoreTests {
						wantErrors = append(wantErrors, testhelpers.ErrorNDJSONLine{Err: "error from API server: status 500 500 Internal Server Error:  error was received from the Healthcare API server", FHIRResource: string(ft.Data)})
					}
				}
				testhelpers.CheckErrorNDJSONFile(t, cfg.fhirStoreUploadErrorFileDir, wantErrors)
			}

			// Checks that should only run if wantError is nil.
			if tc.wantError == nil {
				// Check NDJSON outputs:
				expectedFileSuffixToData := map[string][]byte{
					"ExplanationOfBenefit_0.ndjson": file3Data,
					"Coverage_0.ndjson":             file2Data,
					"Patient_0.ndjson":              file1Data}

				if tc.rectify {
					// Replace expected data with the rectified version of resource:
					expectedFileSuffixToData["Coverage_0.ndjson"] = file2DataRectified
				}

				if tc.setPendingJobURL {
					if got := exportEndpointCalled.Value(); got > 0 {
						t.Errorf("mainWrapper: when pendingJobURL is provided, did not expect any calls to BCDA export API. got: %v, want: %v", got, 0)
					}
				}

				if !tc.unsetOutputDir {
					for fileSuffix, wantData := range expectedFileSuffixToData {
						fullPath := path.Join(outputDir, fileSuffix)
						r, err := os.Open(fullPath)
						if err != nil {
							t.Errorf("unable to open file %s: %s", fullPath, err)
						}
						defer r.Close()
						gotData, err := io.ReadAll(r)
						if err != nil {
							t.Errorf("error reading file %s: %v", fullPath, err)
						}
						if !cmp.Equal(testhelpers.NormalizeJSON(t, gotData), testhelpers.NormalizeJSON(t, wantData)) {
							t.Errorf("mainWrapper unexpected ndjson output for file %s. got: %s, want: %s", fullPath, gotData, wantData)
						}
					}
				}

				// Check sinceFile if necessary:
				if len(tc.sinceFileContent) != 0 {
					f, err := os.Open(sinceTmpFile.Name())
					if err != nil {
						t.Errorf("unable to open sinceTmpFile: %v", err)
					}
					defer f.Close()
					fileData, err := io.ReadAll(f)
					if err != nil {
						t.Errorf("unable to read sinceTmpFile: %v", err)
					}

					if !cmp.Equal(fileData, tc.sinceFileExpectedContent) {
						t.Errorf("sinceFile unexpected content. got: %v, want: %v", fileData, tc.sinceFileExpectedContent)
					}
				}
			}

		})

	}
}

func TestMainWrapper_FirstTimeSinceFile(t *testing.T) {
	t.Parallel()
	metrics.InitNoOp()
	// Declare test data:
	file1Data := []byte(`{"resourceType":"Patient","id":"PatientID"}`)
	exportEndpoint := "/api/v2/Patient/$export"
	jobsEndpoint := "/api/v2/jobs/1234"
	serverTransactionTime := "2020-12-09T11:00:00.123+00:00"

	// Setup BCDA test servers:

	// A seperate resource server is needed during testing, so that we can send
	// the jobsEndpoint response in the bcdaServer that includes a URL for the
	// bcdaResourceServer in it.
	bcdaResourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Write(file1Data)
	}))
	defer bcdaResourceServer.Close()

	jobStatusURL := ""

	bcdaServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/auth/token":
			w.Write([]byte(`{"access_token": "token", "expires_in": 1200}`))
		case exportEndpoint:
			// Check that since is empty
			if got := len(req.URL.Query()["_since"]); got != 0 {
				t.Errorf("got unexpected _since URL param length. got: %v, want: %v", got, 0)
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			w.Header()["Content-Location"] = []string{jobStatusURL}

			w.WriteHeader(http.StatusAccepted)
		case jobsEndpoint:
			w.Write([]byte(fmt.Sprintf("{\"output\": [{\"type\": \"Patient\", \"url\": \"%s/data/10.ndjson\"}, {\"type\": \"Coverage\", \"url\": \"%s/data/20.ndjson\"}, {\"type\": \"ExplanationOfBenefit\", \"url\": \"%s/data/30.ndjson\"}], \"transactionTime\": \"%s\"}", bcdaResourceServer.URL, bcdaResourceServer.URL, bcdaResourceServer.URL, serverTransactionTime)))
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer bcdaServer.Close()

	jobStatusURL = bcdaServer.URL + jobsEndpoint

	// Set flags for this test case:
	outputDir := t.TempDir()
	sinceFilePath := path.Join(t.TempDir(), "since_file.txt")
	// Set mainWrapperConfig for this test case. In practice, values are
	// populated in mainWrapperConfig from flags. Setting the config struct
	// instead of the flags in tests enables parallelization with significant
	// performance improvement. A seperate test below tests that setting flags
	// properly populates mainWrapperConfig.
	cfg := mainWrapperConfig{
		clientID:                  "id",
		clientSecret:              "secret",
		outputDir:                 outputDir,
		bcdaServerURL:             bcdaServer.URL,
		sinceFile:                 sinceFilePath,
		maxFHIRStoreUploadWorkers: 10,
	}

	// Run mainWrapper:
	if err := mainWrapper(cfg); err != nil {
		t.Errorf("mainWrapper(%v) error: %v", cfg, err)
	}

	// Check that since file was created with the proper first entry.
	expctedSinceFileContent := []byte("2020-12-09T11:00:00.123+00:00\n")
	f, err := os.Open(sinceFilePath)
	if err != nil {
		t.Errorf("unable to open sinceTmpFile: %v", err)
	}
	defer f.Close()
	fileData, err := io.ReadAll(f)
	if err != nil {
		t.Errorf("unable to read sinceTmpFile: %v", err)
	}

	if !cmp.Equal(fileData, expctedSinceFileContent) {
		t.Errorf("sinceFile unexpected content. got: %v, want: %v", fileData, expctedSinceFileContent)
	}

}

func TestMainWrapper_GetJobStatusAuthRetry(t *testing.T) {
	// This tests that if JobStatus returns unauthorized, mainWrapper attempts to
	// re-authorize and try again.
	t.Parallel()
	metrics.InitNoOp()
	// Declare test data:
	file1Data := []byte(`{"resourceType":"Patient","id":"PatientID"}`)
	exportEndpoint := "/api/v2/Patient/$export"
	jobsEndpoint := "/api/v2/jobs/1234"
	serverTransactionTime := "2020-12-09T11:00:00.123+00:00"

	// Setup BCDA test servers:

	// A seperate resource server is needed during testing, so that we can send
	// the jobsEndpoint response in the bcdaServer that includes a URL for the
	// bcdaResourceServer in it.
	bcdaResourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Write(file1Data)
	}))
	defer bcdaResourceServer.Close()

	var authCalled mutexCounter
	var jobsCalled mutexCounter

	jobStatusURL := ""

	bcdaServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/auth/token":
			authCalled.Increment()
			w.Write([]byte(`{"access_token": "token", "expires_in": 1200}`))
		case exportEndpoint:

			w.Header()["Content-Location"] = []string{jobStatusURL}

			w.WriteHeader(http.StatusAccepted)
		case jobsEndpoint:
			jobsCalled.Increment()
			if authCalled.Value() < 2 {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}

			w.Write([]byte(fmt.Sprintf("{\"output\": [{\"type\": \"Patient\", \"url\": \"%s/data/10.ndjson\"}, {\"type\": \"Coverage\", \"url\": \"%s/data/20.ndjson\"}, {\"type\": \"ExplanationOfBenefit\", \"url\": \"%s/data/30.ndjson\"}], \"transactionTime\": \"%s\"}", bcdaResourceServer.URL, bcdaResourceServer.URL, bcdaResourceServer.URL, serverTransactionTime)))
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer bcdaServer.Close()

	jobStatusURL = bcdaServer.URL + jobsEndpoint

	// Set flags for this test case:
	outputDir := t.TempDir()
	// Set mainWrapperConfig for this test case. In practice, values are
	// populated in mainWrapperConfig from flags. Setting the config struct
	// instead of the flags in tests enables parallelization with significant
	// performance improvement. A seperate test below tests that setting flags
	// properly populates mainWrapperConfig.
	cfg := mainWrapperConfig{
		clientID:                  "id",
		clientSecret:              "secret",
		outputDir:                 outputDir,
		bcdaServerURL:             bcdaServer.URL,
		maxFHIRStoreUploadWorkers: 10,
	}

	// Run mainWrapper:
	if err := mainWrapper(cfg); err != nil {
		t.Errorf("mainWrapper(%v) error: %v", cfg, err)
	}

	if got := authCalled.Value(); got != 2 {
		t.Errorf("mainWrapper: expected auth to be called exactly twice, got: %d, want: %d", got, 2)
	}
	if got := jobsCalled.Value(); got != 2 {
		t.Errorf("mainWrapper: expected jobStatus to be called exactly twice, got: %d, want: %d", got, 2)
	}
}

func TestMainWrapper_GetDataRetry(t *testing.T) {
	// This tests that if GetData returns unauthorized or not found, mainWrapper
	// attempts to re-authorize and try again at least 5 times.
	cases := []struct {
		name               string
		httpErrorToRetrun  int
		numRetriesBeforeOK int
		wantError          error
	}{
		{
			name:               "BCDAV2",
			httpErrorToRetrun:  http.StatusUnauthorized,
			numRetriesBeforeOK: 5,
		},
		{
			name:               "BCDAV2TooManyRetries",
			httpErrorToRetrun:  http.StatusUnauthorized,
			numRetriesBeforeOK: 6,
			wantError:          bulkfhir.ErrorUnauthorized,
		},
		{
			name:               "BCDAV2With404",
			httpErrorToRetrun:  http.StatusNotFound,
			numRetriesBeforeOK: 5,
		},
		{
			name:               "BCDAV2TooManyRetriesWith404",
			httpErrorToRetrun:  http.StatusNotFound,
			numRetriesBeforeOK: 6,
			wantError:          bulkfhir.ErrorRetryableHTTPStatus,
		},
	}
	t.Parallel()
	metrics.InitNoOp()
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Declare test data:
			file1Data := []byte(`{"resourceType":"Patient","id":"PatientID"}`)
			exportEndpoint := "/api/v2/Patient/$export"
			jobURLSuffix := "/api/v2/jobs/1234"
			serverTransactionTime := "2020-12-09T11:00:00.123+00:00"

			var authCalled mutexCounter
			var getDataCalled mutexCounter

			// Setup BCDA test servers:

			// A seperate resource server is needed during testing, so that we can send
			// the jobsEndpoint response in the bcdaServer that includes a URL for the
			// bcdaResourceServer in it.
			bcdaResourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				getDataCalled.Increment()
				if authCalled.Value() < tc.numRetriesBeforeOK+1 { // plus 1 because auth always called once at client init.
					w.WriteHeader(tc.httpErrorToRetrun)
					return
				}
				w.Write(file1Data)
			}))
			defer bcdaResourceServer.Close()

			jobStatusURL := ""

			bcdaServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				switch req.URL.Path {
				case "/auth/token":
					authCalled.Increment()
					w.Write([]byte(`{"access_token": "token", "expires_in": 1200}`))
				case exportEndpoint:

					w.Header()["Content-Location"] = []string{jobStatusURL}

					w.WriteHeader(http.StatusAccepted)
				case jobURLSuffix:
					w.Write([]byte(fmt.Sprintf("{\"output\": [{\"type\": \"Patient\", \"url\": \"%s/data/10.ndjson\"}], \"transactionTime\": \"%s\"}", bcdaResourceServer.URL, serverTransactionTime)))
				default:
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer bcdaServer.Close()

			jobStatusURL = bcdaServer.URL + jobURLSuffix

			// Set flags for this test case:
			outputDir := t.TempDir()
			// Set mainWrapperConfig for this test case. In practice, values are
			// populated in mainWrapperConfig from flags. Setting the config struct
			// instead of the flags in tests enables parallelization with significant
			// performance improvement. A seperate test below tests that setting flags
			// properly populates mainWrapperConfig.
			cfg := mainWrapperConfig{
				clientID:                  "id",
				clientSecret:              "secret",
				outputDir:                 outputDir,
				bcdaServerURL:             bcdaServer.URL,
				maxFHIRStoreUploadWorkers: 10,
			}

			// Run mainWrapper:
			if err := mainWrapper(cfg); !errors.Is(err, tc.wantError) {
				t.Errorf("mainWrapper(%v) unexpected error. got: %v, want: %v", cfg, err, tc.wantError)
			}
			if tc.wantError == nil {
				wantCalls := tc.numRetriesBeforeOK + 1
				if got := authCalled.Value(); got != wantCalls {
					t.Errorf("mainWrapper: expected auth to be called exactly %d, got: %d, want: %d", wantCalls, got, wantCalls)
				}
				if got := getDataCalled.Value(); got != wantCalls {
					t.Errorf("mainWrapper: expected getDataCalled to be called exactly %d, got: %d, want: %d", wantCalls, got, wantCalls)
				}
			}
		})
	}
}

func TestMainWrapper_BatchUploadSize(t *testing.T) {
	// This test more comprehensively checks setting different batch sizes in
	// bulk_fhir_fetch.
	cases := []struct {
		name            string
		batchUploadSize int
	}{
		{
			name:            "BCDAV2WithBatchSize2",
			batchUploadSize: 2,
		},
		{
			name:            "BCDAV2WithBatchSize3",
			batchUploadSize: 3,
		},
	}

	t.Parallel()
	metrics.InitNoOp()
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// File 1 contains 3 Patient resources.
			patient1 := `{"resourceType":"Patient","id":"PatientID1"}`
			patient2 := `{"resourceType":"Patient","id":"PatientID2"}`
			patient3 := `{"resourceType":"Patient","id":"PatientID3"}`
			file1Data := []byte(patient1 + "\n" + patient2 + "\n" + patient3)
			exportEndpoint := "/api/v2/Patient/$export"
			jobStatusURLSuffix := "/api/v2/jobs/1234"
			serverTransactionTime := "2020-12-09T11:00:00.123+00:00"

			// Setup BCDA test servers:

			jobStatusURL := ""

			// A seperate resource server is needed during testing, so that we can send
			// the jobsEndpoint response in the bcdaServer that includes a URL for the
			// bcdaResourceServer in it.
			bcdaResourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				w.Write(file1Data)
			}))
			defer bcdaResourceServer.Close()

			bcdaServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				switch req.URL.Path {
				case "/auth/token":
					w.Write([]byte(`{"access_token": "token", "expires_in": 1200}`))
				case exportEndpoint:
					// Check that since is empty
					if got := len(req.URL.Query()["_since"]); got != 0 {
						t.Errorf("got unexpected _since URL param length. got: %v, want: %v", got, 0)
						w.WriteHeader(http.StatusBadRequest)
						return
					}

					w.Header()["Content-Location"] = []string{jobStatusURL}

					w.WriteHeader(http.StatusAccepted)
				case jobStatusURLSuffix:
					w.Write([]byte(fmt.Sprintf("{\"output\": [{\"type\": \"Patient\", \"url\": \"%s/data/10.ndjson\"}], \"transactionTime\": \"%s\"}", bcdaResourceServer.URL, serverTransactionTime)))
				default:
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer bcdaServer.Close()

			jobStatusURL = bcdaServer.URL + jobStatusURLSuffix

			// Set minimal flags for this test case:
			outputDir := t.TempDir()
			gcpProject := "project"
			gcpLocation := "location"
			gcpDatasetID := "dataset"
			gcpFHIRStoreID := "fhirID"

			// Set mainWrapperConfig for this test case. In practice, values are
			// populated in mainWrapperConfig from flags. Setting the config struct
			// instead of the flags in tests enables parallelization with significant
			// performance improvement. A seperate test below tests that setting flags
			// properly populates mainWrapperConfig.
			cfg := mainWrapperConfig{
				clientID:                   "id",
				clientSecret:               "secret",
				outputDir:                  outputDir,
				bcdaServerURL:              bcdaServer.URL,
				fhirStoreGCPProject:        gcpProject,
				fhirStoreGCPLocation:       gcpLocation,
				fhirStoreGCPDatasetID:      gcpDatasetID,
				fhirStoreID:                gcpFHIRStoreID,
				fhirStoreEnableBatchUpload: true,
				fhirStoreBatchUploadSize:   tc.batchUploadSize,
				enableFHIRStore:            true,
				rectify:                    true,
				maxFHIRStoreUploadWorkers:  1,
			}

			expectedResources := [][]byte{[]byte(patient1), []byte(patient2), []byte(patient3)}
			cfg.fhirStoreEndpoint = testhelpers.FHIRStoreServerBatch(t, expectedResources, tc.batchUploadSize, gcpProject, gcpLocation, gcpDatasetID, gcpFHIRStoreID)

			// Run mainWrapper:
			if err := mainWrapper(cfg); err != nil {
				t.Errorf("mainWrapper(%v) error: %v", cfg, err)
			}

		})
	}
}

func TestMainWrapper_GCSBasedUpload(t *testing.T) {
	t.Parallel()
	metrics.InitNoOp()
	patient1 := `{"resourceType":"Patient","id":"PatientID1"}`
	file1Data := []byte(patient1)
	exportEndpoint := "/api/v2/Patient/$export"
	jobStatusURLSuffix := "/api/v2/jobs/1234"
	serverTransactionTime := "2020-12-09T11:00:00.123+00:00"

	bucketName := "bucket"

	// Set minimal flags for this test case:
	outputDir := t.TempDir()
	gcpProject := "project"
	gcpLocation := "location"
	gcpDatasetID := "dataset"
	gcpFHIRStoreID := "fhirID"

	// Setup BCDA test servers:

	// A seperate resource server is needed during testing, so that we can send
	// the jobsEndpoint response in the bcdaServer that includes a URL for the
	// bcdaResourceServer in it.
	bcdaResourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Write(file1Data)
	}))
	defer bcdaResourceServer.Close()

	jobStatusURL := ""
	bcdaServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/auth/token":
			w.Write([]byte(`{"access_token": "token", "expires_in": 1200}`))
		case exportEndpoint:
			w.Header()["Content-Location"] = []string{jobStatusURL}
			w.WriteHeader(http.StatusAccepted)
		case jobStatusURLSuffix:
			w.Write([]byte(fmt.Sprintf("{\"output\": [{\"type\": \"Patient\", \"url\": \"%s/data/10.ndjson\"}], \"transactionTime\": \"%s\"}", bcdaResourceServer.URL, serverTransactionTime)))
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer bcdaServer.Close()

	jobStatusURL = bcdaServer.URL + jobStatusURLSuffix

	gcsServer := testhelpers.NewGCSServer(t)

	importCalled := false
	statusCalled := false
	expectedImportRequest := gcsImportRequest{
		ContentStructure: "RESOURCE",
		GCSSource: gcsSource{
			URI: "gs://bucket/2020-12-09T11:00:00.123+00:00/**",
		},
	}
	expectedImportPath := fmt.Sprintf("/v1/projects/%s/locations/%s/datasets/%s/fhirStores/%s:import?alt=json&prettyPrint=false", gcpProject, gcpLocation, gcpDatasetID, gcpFHIRStoreID)
	opName := fmt.Sprintf("projects/%s/locations/%s/datasets/%s/operations/OPNAME", gcpProject, gcpLocation, gcpDatasetID)
	expectedStatusPath := "/v1/" + opName + "?alt=json&prettyPrint=false"
	fhirStoreServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.String() {
		case expectedImportPath:
			bodyData, err := io.ReadAll(req.Body)
			if err != nil {
				t.Errorf("fhir server unexpected error when reading body: %v", err)
			}
			var importReq gcsImportRequest
			if err := json.Unmarshal(bodyData, &importReq); err != nil {
				t.Errorf("error unmarshalling request body in fhir server: %v", err)
			}
			if !cmp.Equal(importReq, expectedImportRequest) {
				t.Errorf("FHIR store test server received unexpected gcsURI. got: %v, want: %v", importReq, expectedImportRequest)
			}
			importCalled = true
			w.Write([]byte(fmt.Sprintf("{\"name\": \"%s\"}", opName)))
			return
		case expectedStatusPath:
			statusCalled = true
			w.Write([]byte(`{"done": true}`))
			return
		default:
			t.Errorf("fhir server got unexpected URL. got: %v, want: %s or %s", req.URL.String(), expectedImportPath, expectedStatusPath)
		}
	}))

	// Set mainWrapperConfig for this test case. In practice, values are
	// populated in mainWrapperConfig from flags. Setting the config struct
	// instead of the flags in tests enables parallelization with significant
	// performance improvement. A seperate test below tests that setting flags
	// properly populates mainWrapperConfig.
	cfg := mainWrapperConfig{
		gcsEndpoint:                   gcsServer.URL(),
		fhirStoreEndpoint:             fhirStoreServer.URL,
		clientID:                      "id",
		clientSecret:                  "secret",
		outputDir:                     outputDir,
		bcdaServerURL:                 bcdaServer.URL,
		fhirStoreGCPProject:           gcpProject,
		fhirStoreGCPLocation:          gcpLocation,
		fhirStoreGCPDatasetID:         gcpDatasetID,
		fhirStoreID:                   gcpFHIRStoreID,
		fhirStoreEnableGCSBasedUpload: true,
		fhirStoreGCSBasedUploadBucket: bucketName,
		enableFHIRStore:               true,
		rectify:                       true,
	}
	// Run mainWrapper:
	if err := mainWrapper(cfg); err != nil {
		t.Errorf("mainWrapper(%v) error: %v", cfg, err)
	}

	objName := serverTransactionTime + "/Patient_0.ndjson"
	obj, ok := gcsServer.GetObject(bucketName, objName)
	if !ok {
		t.Fatalf("gs://%s/%s not found", bucketName, objName)
	}
	if !bytes.Contains(obj.Data, []byte(patient1)) {
		t.Errorf("gcs server unexpected data: got: %s, want: %s", obj.Data, patient1)
	}

	if !importCalled {
		t.Errorf("mainWrapper(%v) expected FHIR Store import to be called, but was not", cfg)
	}
	if !statusCalled {
		t.Errorf("mainWrapper(%v) expected FHIR Store import operation status to be called, but was not", cfg)
	}

	// Check that files were also written to disk under outputDir
	fullPath := path.Join(outputDir, "Patient_0.ndjson")
	r, err := os.Open(fullPath)
	if err != nil {
		t.Errorf("unable to open file %s: %s", fullPath, err)
	}
	defer r.Close()
	gotData, err := io.ReadAll(r)
	if err != nil {
		t.Errorf("error reading file %s: %v", fullPath, err)
	}
	if !cmp.Equal(testhelpers.NormalizeJSON(t, gotData), testhelpers.NormalizeJSON(t, file1Data)) {
		t.Errorf("mainWrapper unexpected ndjson output for file %s. got: %s, want: %s", fullPath, gotData, file1Data)
	}
}

func TestMainWrapper_GCSBasedUpload_InvalidCfg(t *testing.T) {
	metrics.InitNoOp()
	cfg := mainWrapperConfig{
		clientID:                      "id",
		clientSecret:                  "secret",
		fhirStoreEnableGCSBasedUpload: true,
		// fhirStoreGCSBasedUploadBucket not specified.
	}
	// Run mainWrapper:
	if err := mainWrapper(cfg); err != errMustSpecifyGCSBucket {
		t.Errorf("mainWrapper(%v) unexpected error. got: %v, want: %v", cfg, err, errMustSpecifyGCSBucket)
	}
}

func TestMainWrapper_GeneralizedImport(t *testing.T) {
	t.Parallel()
	metrics.InitNoOp()
	patient1 := `{"resourceType":"Patient","id":"PatientID1"}`
	file1Data := []byte(patient1)

	baseURLSuffix := "/api/v20"

	exportEndpoint := "/api/v20/Patient/$export"
	jobStatusURLSuffix := "/api/v20/jobs/1234"
	serverTransactionTime := "2020-12-09T11:00:00.123+00:00"

	scopes := []string{"a", "b", "c"}

	// Setup bulk fhir test servers:
	jobStatusURL := ""

	// A seperate resource server is needed during testing, so that we can send
	// the jobsEndpoint response in the bulkFHIRServer that includes a URL for the
	// bulkFHIRResourceServer in it.
	bulkFHIRResourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Write(file1Data)
	}))
	defer bulkFHIRResourceServer.Close()

	bulkFHIRServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/auth/token":
			if err := req.ParseForm(); err != nil {
				t.Errorf("Authenticate was sent a body that could not be parsed as a form: %s", err)
			}
			if got := len(req.Form["scope"]); got != 1 {
				t.Errorf("Authenticate was sent invalid number of scope values. got: %v, want: %v", got, 1)
			}
			splitScopes := strings.Split(req.Form["scope"][0], " ")
			if diff := cmp.Diff(splitScopes, scopes, cmpopts.SortSlices(func(a, b string) bool { return a > b })); diff != "" {
				t.Errorf("Authenticate got invalid scopes. diff: %s", diff)
			}
			w.Write([]byte(`{"access_token": "token", "expires_in": 1200}`))
		case exportEndpoint:
			// Check that since is empty
			if got := len(req.URL.Query()["_since"]); got != 0 {
				t.Errorf("got unexpected _since URL param length. got: %v, want: %v", got, 0)
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			w.Header()["Content-Location"] = []string{jobStatusURL}

			w.WriteHeader(http.StatusAccepted)
		case jobStatusURLSuffix:
			w.Write([]byte(fmt.Sprintf("{\"output\": [{\"type\": \"Patient\", \"url\": \"%s/data/10.ndjson\"}], \"transactionTime\": \"%s\"}", bulkFHIRResourceServer.URL, serverTransactionTime)))
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer bulkFHIRServer.Close()
	jobStatusURL = bulkFHIRServer.URL + jobStatusURLSuffix

	bulkFHIRBaseURL := bulkFHIRServer.URL + baseURLSuffix
	authURL := bulkFHIRServer.URL + "/auth/token"

	outputDir := t.TempDir()

	// Set mainWrapperConfig for this test case. In practice, values are
	// populated in mainWrapperConfig from flags. Setting the config struct
	// instead of the flags in tests enables parallelization with significant
	// performance improvement. A seperate test below tests that setting flags
	// properly populates mainWrapperConfig.
	cfg := mainWrapperConfig{
		clientID:                 "id",
		clientSecret:             "secret",
		outputDir:                outputDir,
		useGeneralizedBulkImport: true,
		baseServerURL:            bulkFHIRBaseURL,
		authURL:                  authURL,
		fhirAuthScopes:           scopes,
		rectify:                  true,
	}

	// Run mainWrapper:
	if err := mainWrapper(cfg); err != nil {
		t.Errorf("mainWrapper(%v) error: %v", cfg, err)
	}

	// Check that files were also written to disk under outputDir
	fullPath := path.Join(outputDir, "Patient_0.ndjson")
	r, err := os.Open(fullPath)
	if err != nil {
		t.Errorf("unable to open file %s: %s", fullPath, err)
	}
	defer r.Close()
	gotData, err := io.ReadAll(r)
	if err != nil {
		t.Errorf("error reading file %s: %v", fullPath, err)
	}
	if !cmp.Equal(testhelpers.NormalizeJSON(t, gotData), testhelpers.NormalizeJSON(t, file1Data)) {
		t.Errorf("mainWrapper unexpected ndjson output for file %s. got: %s, want: %s", fullPath, gotData, file1Data)
	}

}

func TestMainWrapper_GCSBasedSince(t *testing.T) {
	t.Parallel()
	metrics.InitNoOp()
	patient1 := `{"resourceType":"Patient","id":"PatientID1"}`
	file1Data := []byte(patient1)
	exportEndpoint := "/api/v2/Patient/$export"
	jobStatusURLSuffix := "/api/v2/jobs/1234"
	serverTransactionTime := "2020-12-09T11:00:00.123+00:00"

	// Set minimal flags for this test case:
	outputDir := t.TempDir()
	sinceFile := "gs://sinceBucket/sinceFile"
	since := "2006-01-02T15:04:05.000-07:00"

	// Setup BCDA test servers:

	// A seperate resource server is needed during testing, so that we can send
	// the jobsEndpoint response in the bcdaServer that includes a URL for the
	// bcdaResourceServer in it.
	bcdaResourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Write(file1Data)
	}))
	defer bcdaResourceServer.Close()

	jobStatusURL := ""
	bcdaServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/auth/token":
			w.Write([]byte(`{"access_token": "token", "expires_in": 1200}`))
		case exportEndpoint:
			w.Header()["Content-Location"] = []string{jobStatusURL}
			w.WriteHeader(http.StatusAccepted)
		case jobStatusURLSuffix:
			w.Write([]byte(fmt.Sprintf("{\"output\": [{\"type\": \"Patient\", \"url\": \"%s/data/10.ndjson\"}], \"transactionTime\": \"%s\"}", bcdaResourceServer.URL, serverTransactionTime)))
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer bcdaServer.Close()

	jobStatusURL = bcdaServer.URL + jobStatusURLSuffix

	gcsServer := testhelpers.NewGCSServer(t)
	gcsServer.AddObject("sinceBucket", "sinceFile", testhelpers.GCSObjectEntry{
		Data: []byte(since),
	})

	// Set mainWrapperConfig for this test case. In practice, values are
	// populated in mainWrapperConfig from flags. Setting the config struct
	// instead of the flags in tests enables parallelization with significant
	// performance improvement. A seperate test below tests that setting flags
	// properly populates mainWrapperConfig.
	cfg := mainWrapperConfig{
		gcsEndpoint:   gcsServer.URL(),
		clientID:      "id",
		clientSecret:  "secret",
		outputDir:     outputDir,
		bcdaServerURL: bcdaServer.URL,
		rectify:       true,
		sinceFile:     sinceFile,
	}
	// Run mainWrapper:
	if err := mainWrapper(cfg); err != nil {
		t.Errorf("mainWrapper(%v) error: %v", cfg, err)
	}

	obj, ok := gcsServer.GetObject("sinceBucket", "sinceFile")
	if !ok {
		t.Errorf("gs://sinceBucket/sinceFile not found")
	}
	if !bytes.Contains(obj.Data, []byte(serverTransactionTime)) {
		t.Errorf("gcs server unexpected data in since file: got: %s, want: %s", obj.Data, []byte(serverTransactionTime))
	}

	// Check that files were also written to disk under outputDir
	fullPath := path.Join(outputDir, "Patient_0.ndjson")
	r, err := os.Open(fullPath)
	if err != nil {
		t.Errorf("unable to open file %s: %s", fullPath, err)
	}
	defer r.Close()
	gotData, err := io.ReadAll(r)
	if err != nil {
		t.Errorf("error reading file %s: %v", fullPath, err)
	}
	if !cmp.Equal(testhelpers.NormalizeJSON(t, gotData), testhelpers.NormalizeJSON(t, file1Data)) {
		t.Errorf("mainWrapper unexpected ndjson output for file %s. got: %s, want: %s", fullPath, gotData, file1Data)
	}

}

func TestMainWrapper_GCSoutputDir(t *testing.T) {
	cases := []struct {
		name                        string
		outputDir                   string
		expectedGCSItemBucket       string
		expectedGCSItemRelativePath string
	}{
		{
			name:                        "TrailingSlash",
			outputDir:                   "gs://fhirBucket/patients/",
			expectedGCSItemBucket:       "fhirBucket",
			expectedGCSItemRelativePath: "patients/Patient_0.ndjson",
		},
		{
			name:                        "NoTrailingSlash",
			outputDir:                   "gs://fhirBucket/patients",
			expectedGCSItemBucket:       "fhirBucket",
			expectedGCSItemRelativePath: "patients/Patient_0.ndjson",
		},
	}
	t.Parallel()
	metrics.InitNoOp()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			patient1 := `{"resourceType":"Patient","id":"PatientID1"}`
			file1Data := []byte(patient1)
			exportEndpoint := "/api/v2/Patient/$export"
			jobStatusURLSuffix := "/api/v2/jobs/1234"
			serverTransactionTime := "2020-12-09T11:00:00.123+00:00"

			// Setup BCDA test servers:

			// A seperate resource server is needed during testing, so that we can send
			// the jobsEndpoint response in the bcdaServer that includes a URL for the
			// bcdaResourceServer in it.
			bcdaResourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				w.Write(file1Data)
			}))
			defer bcdaResourceServer.Close()

			jobStatusURL := ""
			bcdaServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				switch req.URL.Path {
				case "/auth/token":
					w.Write([]byte(`{"access_token": "token", "expires_in": 1200}`))
				case exportEndpoint:
					w.Header()["Content-Location"] = []string{jobStatusURL}
					w.WriteHeader(http.StatusAccepted)
				case jobStatusURLSuffix:
					w.Write([]byte(fmt.Sprintf("{\"output\": [{\"type\": \"Patient\", \"url\": \"%s/data/10.ndjson\"}], \"transactionTime\": \"%s\"}", bcdaResourceServer.URL, serverTransactionTime)))
				default:
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer bcdaServer.Close()

			jobStatusURL = bcdaServer.URL + jobStatusURLSuffix

			gcsServer := testhelpers.NewGCSServer(t)

			// Set mainWrapperConfig for this test case. In practice, values are
			// populated in mainWrapperConfig from flags. Setting the config struct
			// instead of the flags in tests enables parallelization with significant
			// performance improvement. A seperate test below tests that setting flags
			// properly populates mainWrapperConfig.
			cfg := mainWrapperConfig{
				gcsEndpoint:   gcsServer.URL(),
				clientID:      "id",
				clientSecret:  "secret",
				outputDir:     tc.outputDir,
				bcdaServerURL: bcdaServer.URL,
				rectify:       true,
			}
			// Run mainWrapper:
			if err := mainWrapper(cfg); err != nil {
				t.Errorf("mainWrapper(%v) error: %v", cfg, err)
			}

			obj, ok := gcsServer.GetObject(tc.expectedGCSItemBucket, tc.expectedGCSItemRelativePath)
			if !ok {
				t.Errorf("gs://%s/%s not found", tc.expectedGCSItemBucket, tc.expectedGCSItemRelativePath)
			}
			if !bytes.Contains(obj.Data, file1Data) {
				t.Errorf("gcs server unexpected data in prefix_Patient_0 file: got: %s, want: %s", obj.Data, file1Data)
			}
		})
	}
}

func TestMainWrapper_GroupID(t *testing.T) {
	cases := []struct {
		name    string
		groupID string
	}{
		{
			name:    "NonEmptyGroupID",
			groupID: "mygroup",
		},
		{
			name:    "EmptyGroupID",
			groupID: "",
		},
	}
	t.Parallel()
	metrics.InitNoOp()
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			metrics.InitNoOp()
			patient1 := `{"resourceType":"Patient","id":"PatientID1"}`
			file1Data := []byte(patient1)

			baseURLSuffix := "/api/v20"
			exportEndpoint := "/api/v20/Patient/$export"
			if tc.groupID != "" {
				exportEndpoint = fmt.Sprintf("/api/v20/Group/%s/$export", tc.groupID)
			}
			jobStatusURLSuffix := "/api/v20/jobs/1234"
			serverTransactionTime := "2020-12-09T11:00:00.123+00:00"

			scopes := []string{"a", "b", "c"}

			// Setup bulk fhir test servers:
			jobStatusURL := ""

			// A separate resource server is needed during testing, so that we can send
			// the jobsEndpoint response in the bulkFHIRServer that includes a URL for the
			// bulkFHIRResourceServer in it.
			bulkFHIRResourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				w.Write(file1Data)
			}))
			defer bulkFHIRResourceServer.Close()

			bulkFHIRServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				switch req.URL.Path {
				case "/auth/token":
					w.Write([]byte(`{"access_token": "token", "expires_in": 1200}`))
				case exportEndpoint:
					w.Header()["Content-Location"] = []string{jobStatusURL}
					w.WriteHeader(http.StatusAccepted)
				case jobStatusURLSuffix:
					w.Write([]byte(fmt.Sprintf("{\"output\": [{\"type\": \"Patient\", \"url\": \"%s/data/10.ndjson\"}], \"transactionTime\": \"%s\"}", bulkFHIRResourceServer.URL, serverTransactionTime)))
				default:
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer bulkFHIRServer.Close()
			jobStatusURL = bulkFHIRServer.URL + jobStatusURLSuffix

			bulkFHIRBaseURL := bulkFHIRServer.URL + baseURLSuffix
			authURL := bulkFHIRServer.URL + "/auth/token"

			outputDir := t.TempDir()

			// Set mainWrapperConfig for this test case. In practice, values are
			// populated in mainWrapperConfig from flags. Setting the config struct
			// instead of the flags in tests enables parallelization with significant
			// performance improvement. A separate test below tests that setting flags
			// properly populates mainWrapperConfig.
			cfg := mainWrapperConfig{
				clientID:                 "id",
				clientSecret:             "secret",
				outputDir:                outputDir,
				useGeneralizedBulkImport: true,
				baseServerURL:            bulkFHIRBaseURL,
				authURL:                  authURL,
				fhirAuthScopes:           scopes,
				rectify:                  true,
				groupID:                  tc.groupID,
			}

			// Run mainWrapper:
			if err := mainWrapper(cfg); err != nil {
				t.Errorf("mainWrapper(%v) error: %v", cfg, err)
			}

			// Check that files were also written to disk under outputDir
			fullPath := path.Join(outputDir, "Patient_0.ndjson")
			r, err := os.Open(fullPath)
			if err != nil {
				t.Errorf("unable to open file %s: %s", fullPath, err)
			}
			defer r.Close()
			gotData, err := io.ReadAll(r)
			if err != nil {
				t.Errorf("error reading file %s: %v", fullPath, err)
			}
			if !cmp.Equal(testhelpers.NormalizeJSON(t, gotData), testhelpers.NormalizeJSON(t, file1Data)) {
				t.Errorf("mainWrapper unexpected ndjson output for file %s. got: %s, want: %s", fullPath, gotData, file1Data)
			}
		})
	}
}

func TestBuildMainWrapperConfig(t *testing.T) {
	// Set every flag, and see that it is built into mainWrapper correctly.
	defer SaveFlags().Restore()
	flag.Set("client_id", "clientID")
	flag.Set("client_secret", "clientSecret")
	flag.Set("output_prefix", "outputPrefix")
	flag.Set("output_dir", "outputDir")
	flag.Set("rectify", "true")
	flag.Set("enable_fhir_store", "true")
	flag.Set("max_fhir_store_upload_workers", "99")
	flag.Set("fhir_store_enable_batch_upload", "true")
	flag.Set("fhir_store_batch_upload_size", "10")
	flag.Set("fhir_store_gcp_project", "project")
	flag.Set("fhir_store_gcp_location", "location")
	flag.Set("fhir_store_gcp_dataset_id", "dataset")
	flag.Set("fhir_store_id", "id")
	flag.Set("fhir_store_upload_error_file_dir", "uploadDir")
	flag.Set("fhir_store_enable_batch_upload", "true")
	flag.Set("fhir_store_batch_upload_size", "10")
	flag.Set("fhir_store_enable_gcs_based_upload", "true")
	flag.Set("fhir_store_gcs_based_upload_bucket", "my-bucket")
	flag.Set("bcda_server_url", "url")
	flag.Set("enable_generalized_bulk_import", "true")
	flag.Set("fhir_server_base_url", "url")
	flag.Set("fhir_auth_url", "url")
	flag.Set("fhir_auth_scopes", "scope1,scope2")
	flag.Set("since", "12345")
	flag.Set("since_file", "sinceFile")
	flag.Set("no_fail_on_upload_errors", "true")
	flag.Set("pending_job_url", "jobURL")

	expectedCfg := mainWrapperConfig{
		fhirStoreEndpoint:             fhirstore.DefaultHealthcareEndpoint,
		gcsEndpoint:                   gcs.DefaultCloudStorageEndpoint,
		clientID:                      "clientID",
		clientSecret:                  "clientSecret",
		outputPrefix:                  "outputPrefix",
		outputDir:                     "outputDir",
		rectify:                       true,
		enableFHIRStore:               true,
		maxFHIRStoreUploadWorkers:     99,
		fhirStoreGCPProject:           "project",
		fhirStoreGCPLocation:          "location",
		fhirStoreGCPDatasetID:         "dataset",
		fhirStoreID:                   "id",
		fhirStoreUploadErrorFileDir:   "uploadDir",
		fhirStoreEnableBatchUpload:    true,
		fhirStoreBatchUploadSize:      10,
		fhirStoreEnableGCSBasedUpload: true,
		fhirStoreGCSBasedUploadBucket: "my-bucket",
		bcdaServerURL:                 "url",
		useGeneralizedBulkImport:      true,
		baseServerURL:                 "url",
		authURL:                       "url",
		fhirAuthScopes:                []string{"scope1", "scope2"},
		since:                         "12345",
		sinceFile:                     "sinceFile",
		noFailOnUploadErrors:          true,
		pendingJobURL:                 "jobURL",
	}

	if diff := cmp.Diff(buildMainWrapperConfig(), expectedCfg, cmp.AllowUnexported(mainWrapperConfig{})); diff != "" {
		t.Errorf("buildMainWrapperConfig unexpected diff: %s", diff)
	}

}

// serverAlwaysFails returns a server that always fails with a 500 error code.
func serverAlwaysFails(t *testing.T) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(500)
	}))
	t.Cleanup(server.Close)
	return server.URL
}

type mutexCounter struct {
	m sync.Mutex
	i int
}

func (mc *mutexCounter) Increment() {
	mc.m.Lock()
	defer mc.m.Unlock()
	mc.i++
}

func (mc *mutexCounter) Value() int {
	mc.m.Lock()
	defer mc.m.Unlock()
	return mc.i
}

// SaveFlags returns a Stash that captures the current value of all non-hidden flags.
func SaveFlags() *Stash {
	s := Stash{
		flags: make(map[string]string, flag.NFlag()),
	}

	flag.VisitAll(func(f *flag.Flag) {
		s.flags[f.Name] = f.Value.String()
	})

	return &s
}

// Stash holds flag values so that they can be restored at the end of a test.
type Stash struct {
	flags map[string]string
}

// Restore sets all non-hidden flags to the values they had when the Stash was created.
func (s *Stash) Restore() {
	flag.VisitAll(func(f *flag.Flag) {
		prevVal, ok := s.flags[f.Name]
		if !ok {
			return
		}
		newVal := f.Value.String()
		// Setting a flag to its current value can trigger tsan if another thread uses the flag.
		if prevVal != newVal {
			flag.Set(f.Name, prevVal)
		}
	})
}

type gcsSource struct {
	URI string `json:"uri"`
}

type gcsImportRequest struct {
	ContentStructure string    `json:"contentStructure"`
	GCSSource        gcsSource `json:"gcsSource"`
}
