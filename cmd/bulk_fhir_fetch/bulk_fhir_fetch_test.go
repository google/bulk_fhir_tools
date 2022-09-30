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
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"testing"

	"github.com/google/medical_claims_tools/gcs"
	"github.com/google/medical_claims_tools/internal/testhelpers"

	"flag"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/medical_claims_tools/bcda"
	"github.com/google/medical_claims_tools/bulkfhir"
	"github.com/google/medical_claims_tools/fhirstore"
)

func TestMainWrapper(t *testing.T) {
	cases := []struct {
		name                              string
		rectify                           bool
		enableFHIRStore                   bool
		apiVersion                        bcda.Version
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
		bcdaJobURLSuffix           string
		fhirStoreEnableBatchUpload bool
		// unsetOutputPrefix sets the outputPrefix to empty string if true.
		unsetOutputPrefix bool
		// disableFHIRStoreUploadChecks will disable the portion of the test that
		// checks for FHIR store uploads, even if fhir store uploads is set to true.
		// This is needed for only some subtests.
		disableFHIRStoreUploadChecks bool
		wantError                    error
	}{
		{
			name:            "RectifyEnabledWithoutFHIRStoreBCDAV1",
			rectify:         true,
			enableFHIRStore: false,
			apiVersion:      bcda.V1,
		},
		{
			name:            "RectifyEnabledWithoutFHIRStoreBCDAV2",
			rectify:         true,
			enableFHIRStore: false,
			apiVersion:      bcda.V2,
		},
		{
			name:            "RectifyEnabledWithFHIRStoreBCDAV1",
			rectify:         true,
			enableFHIRStore: true,
			apiVersion:      bcda.V1,
		},
		{
			name:            "RectifyEnabledWithFHIRStoreBCDAV2",
			rectify:         true,
			enableFHIRStore: true,
			apiVersion:      bcda.V2,
		},
		{
			name:            "RectifyDisabledWithoutFHIRStoreBCDAV1",
			rectify:         false,
			enableFHIRStore: false,
			apiVersion:      bcda.V1,
		},
		{
			name:            "RectifyDisabledWithoutFHIRStoreBCDAV2",
			rectify:         false,
			enableFHIRStore: false,
			apiVersion:      bcda.V2,
		},

		// Currently, rectify must be enabled to upload to FHIR store.
		{
			name:                         "RectifyDisabledWithFHIRStoreBCDAV1",
			rectify:                      false,
			enableFHIRStore:              true,
			apiVersion:                   bcda.V1,
			disableFHIRStoreUploadChecks: true,
			wantError:                    errMustRectifyForFHIRStore,
		},
		{
			name:                         "RectifyDisabledWithFHIRStoreBCDAV2",
			rectify:                      false,
			enableFHIRStore:              true,
			apiVersion:                   bcda.V2,
			disableFHIRStoreUploadChecks: true,
			wantError:                    errMustRectifyForFHIRStore,
		},
		// For the Since and SinceFile test cases, BCDA Version is the primary
		// parameter to vary. The others are set to useful values and not varied.
		{
			name:            "SinceProvidedWithRectifyWithFHIRStoreBCDAV2",
			rectify:         true,
			enableFHIRStore: true,
			apiVersion:      bcda.V2,
			since:           "2006-01-02T15:04:05.000-07:00",
		},
		{
			name:            "SinceProvidedWithRectifyWithFHIRStoreBCDAV1",
			rectify:         true,
			enableFHIRStore: true,
			apiVersion:      bcda.V1,
			since:           "2006-01-02T15:04:05.000-07:00",
		},
		{
			name:       "InvalidSince",
			rectify:    true,
			apiVersion: bcda.V1,
			since:      "2006-01-02",
			wantError:  errInvalidSince,
		},
		{
			name:                     "SinceFileProvidedWithBCDAV2",
			rectify:                  true,
			enableFHIRStore:          true,
			apiVersion:               bcda.V2,
			sinceFileContent:         []byte("2013-12-09T11:00:00.123+00:00\n2015-12-09T11:00:00.123+00:00\n"),
			sinceFileLatestTimestamp: "2015-12-09T11:00:00.123+00:00",
			sinceFileExpectedContent: []byte("2013-12-09T11:00:00.123+00:00\n2015-12-09T11:00:00.123+00:00\n2020-12-09T11:00:00.123+00:00\n"),
		},
		{
			name:                     "SinceFileProvidedWithBCDAV1",
			rectify:                  true,
			enableFHIRStore:          true,
			apiVersion:               bcda.V1,
			sinceFileContent:         []byte("2013-12-09T11:00:00.123+00:00\n2015-12-09T11:00:00.123+00:00\n"),
			sinceFileLatestTimestamp: "2015-12-09T11:00:00.123+00:00",
			sinceFileExpectedContent: []byte("2013-12-09T11:00:00.123+00:00\n2015-12-09T11:00:00.123+00:00\n2020-12-09T11:00:00.123+00:00\n"),
		},
		{
			name:                     "SinceFileEmptyProvidedWithBCDAV2",
			rectify:                  true,
			enableFHIRStore:          true,
			apiVersion:               bcda.V2,
			sinceFileContent:         []byte(""),
			sinceFileExpectedContent: []byte("2020-12-09T11:00:00.123+00:00\n"),
		},
		{
			name:                     "SinceFileEmptyProvidedWithBCDAV1",
			rectify:                  true,
			enableFHIRStore:          true,
			apiVersion:               bcda.V1,
			sinceFileContent:         []byte(""),
			sinceFileExpectedContent: []byte("2020-12-09T11:00:00.123+00:00\n"),
		},
		{
			name:             "InvalidSinceFileInstant",
			rectify:          true,
			apiVersion:       bcda.V1,
			sinceFileContent: []byte("2006-01-02\n"),
			wantError:        errInvalidSince,
		},
		// Test FHIR Store upload failures.
		{
			name:              "FHIRStoreUploadFailuresBCDAV1",
			rectify:           true,
			enableFHIRStore:   true,
			apiVersion:        bcda.V1,
			fhirStoreFailures: true,
			wantError:         errUploadFailures,
		},
		{
			name:              "FHIRStoreUploadFailuresBCDAV2",
			rectify:           true,
			enableFHIRStore:   true,
			apiVersion:        bcda.V2,
			fhirStoreFailures: true,
			wantError:         errUploadFailures,
		},
		{
			name:                 "FHIRStoreUploadFailuresWithNoFailFlagBCDAV1",
			rectify:              true,
			enableFHIRStore:      true,
			apiVersion:           bcda.V1,
			fhirStoreFailures:    true,
			noFailOnUploadErrors: true,
			wantError:            nil,
		},
		{
			name:                 "FHIRStoreUploadFailuresWithNoFailFlagBCDAV2",
			rectify:              true,
			enableFHIRStore:      true,
			apiVersion:           bcda.V2,
			fhirStoreFailures:    true,
			noFailOnUploadErrors: true,
			wantError:            nil,
		},
		// Test FHIR Store upload errors file output.
		{
			name:                              "ErrorFileWithFHIRStoreUploadFailuresBCDAV1",
			rectify:                           true,
			enableFHIRStore:                   true,
			apiVersion:                        bcda.V1,
			fhirStoreFailures:                 true,
			enableFHIRStoreUploadErrorFileDir: true,
			wantError:                         errUploadFailures,
		},
		{
			name:                              "ErrorFileWithFHIRStoreUploadFailuresBCDAV2",
			rectify:                           true,
			enableFHIRStore:                   true,
			apiVersion:                        bcda.V2,
			fhirStoreFailures:                 true,
			enableFHIRStoreUploadErrorFileDir: true,
			wantError:                         errUploadFailures,
		},
		{
			name:                              "ErrorFileWithSuccessfulUploadBCDAV1",
			rectify:                           true,
			enableFHIRStore:                   true,
			apiVersion:                        bcda.V1,
			enableFHIRStoreUploadErrorFileDir: true,
		},
		{
			name:                              "ErrorFileWithSuccessfulUploadBCDAV2",
			rectify:                           true,
			enableFHIRStore:                   true,
			apiVersion:                        bcda.V2,
			enableFHIRStoreUploadErrorFileDir: true,
		},
		{
			name:             "SinceProvidedWithRectifyWithFHIRStoreBCDAV2WithBCDAJobId",
			rectify:          true,
			enableFHIRStore:  true,
			apiVersion:       bcda.V2,
			since:            "2006-01-02T15:04:05.000-07:00",
			bcdaJobURLSuffix: "/jobs/999",
		},
		{
			name:             "SinceProvidedWithRectifyWithFHIRStoreBCDAV1WithBCDAJobId",
			rectify:          true,
			enableFHIRStore:  true,
			apiVersion:       bcda.V1,
			since:            "2006-01-02T15:04:05.000-07:00",
			bcdaJobURLSuffix: "/jobs/999",
		},
		{
			name:                     "SinceFileProvidedWithBCDAV2WithBCDAJobId",
			rectify:                  true,
			enableFHIRStore:          true,
			apiVersion:               bcda.V2,
			sinceFileContent:         []byte("2013-12-09T11:00:00.123+00:00\n2015-12-09T11:00:00.123+00:00\n"),
			sinceFileLatestTimestamp: "2015-12-09T11:00:00.123+00:00",
			sinceFileExpectedContent: []byte("2013-12-09T11:00:00.123+00:00\n2015-12-09T11:00:00.123+00:00\n2020-12-09T11:00:00.123+00:00\n"),
			bcdaJobURLSuffix:         "/jobs/999",
		},
		{
			name:                     "SinceFileProvidedWithBCDAV1WithBCDAJobId",
			rectify:                  true,
			enableFHIRStore:          true,
			apiVersion:               bcda.V1,
			sinceFileContent:         []byte("2013-12-09T11:00:00.123+00:00\n2015-12-09T11:00:00.123+00:00\n"),
			sinceFileLatestTimestamp: "2015-12-09T11:00:00.123+00:00",
			sinceFileExpectedContent: []byte("2013-12-09T11:00:00.123+00:00\n2015-12-09T11:00:00.123+00:00\n2020-12-09T11:00:00.123+00:00\n"),
			bcdaJobURLSuffix:         "/jobs/999",
		},
		{
			name:                     "SinceFileEmptyProvidedWithBCDAV2WithBCDAJobId",
			rectify:                  true,
			enableFHIRStore:          true,
			apiVersion:               bcda.V2,
			sinceFileContent:         []byte(""),
			sinceFileExpectedContent: []byte("2020-12-09T11:00:00.123+00:00\n"),
			bcdaJobURLSuffix:         "/jobs/999",
		},
		// Only testing cases with FHIR Store enabled for setting outputPrefix to ""
		{
			name:              "EmptyOutputPrefixWithRectifyEnabledWithFHIRStoreBCDAV1",
			rectify:           true,
			enableFHIRStore:   true,
			apiVersion:        bcda.V1,
			unsetOutputPrefix: true,
		},
		{
			name:              "EmptyOutputPrefixWithRectifyEnabledWithFHIRStoreBCDAV2",
			rectify:           true,
			enableFHIRStore:   true,
			apiVersion:        bcda.V2,
			unsetOutputPrefix: true,
		},
		{
			name:              "EmptyOutputPrefixWithSinceProvidedWithRectifyWithFHIRStoreBCDAV2WithBCDAJobId",
			rectify:           true,
			enableFHIRStore:   true,
			apiVersion:        bcda.V2,
			since:             "2006-01-02T15:04:05.000-07:00",
			bcdaJobURLSuffix:  "/jobs/999",
			unsetOutputPrefix: true,
		},
		{
			name:              "EmptyOutputPrefixWithSinceProvidedWithRectifyWithFHIRStoreBCDAV1WithBCDAJobId",
			rectify:           true,
			enableFHIRStore:   true,
			apiVersion:        bcda.V1,
			since:             "2006-01-02T15:04:05.000-07:00",
			bcdaJobURLSuffix:  "/jobs/999",
			unsetOutputPrefix: true,
		},
		{
			name:                     "EmptyOutputPrefixWithSinceFileProvidedWithBCDAV2WithBCDAJobIdWithFHIRStore",
			rectify:                  true,
			enableFHIRStore:          true,
			apiVersion:               bcda.V2,
			sinceFileContent:         []byte("2013-12-09T11:00:00.123+00:00\n2015-12-09T11:00:00.123+00:00\n"),
			sinceFileLatestTimestamp: "2015-12-09T11:00:00.123+00:00",
			sinceFileExpectedContent: []byte("2013-12-09T11:00:00.123+00:00\n2015-12-09T11:00:00.123+00:00\n2020-12-09T11:00:00.123+00:00\n"),
			bcdaJobURLSuffix:         "/jobs/999",
			unsetOutputPrefix:        true,
		},
		{
			name:                     "EmptyOutputPrefixWithSinceFileProvidedWithBCDAV1WithBCDAJobIdWithFHIRStore",
			rectify:                  true,
			enableFHIRStore:          true,
			apiVersion:               bcda.V1,
			sinceFileContent:         []byte("2013-12-09T11:00:00.123+00:00\n2015-12-09T11:00:00.123+00:00\n"),
			sinceFileLatestTimestamp: "2015-12-09T11:00:00.123+00:00",
			sinceFileExpectedContent: []byte("2013-12-09T11:00:00.123+00:00\n2015-12-09T11:00:00.123+00:00\n2020-12-09T11:00:00.123+00:00\n"),
			bcdaJobURLSuffix:         "/jobs/999",
			unsetOutputPrefix:        true,
		},
		// Batch upload tests cases
		{
			name:                       "BatchUploadWithBCDAV1",
			enableFHIRStore:            true,
			fhirStoreEnableBatchUpload: true,
			apiVersion:                 bcda.V1,
			rectify:                    true,
		},
		{
			name:                       "BatchUploadWithBCDAV2",
			enableFHIRStore:            true,
			fhirStoreEnableBatchUpload: true,
			apiVersion:                 bcda.V2,
			rectify:                    true,
		},
		// TODO(b/226375559): see if we can generate some of the test cases above
		// instead of having to spell them out explicitly.
		// TODO(b/213365276): test that bcda V1 with rectify = true results in an
		// error.
	}
	t.Parallel()
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
			file3Data := []byte(`{"resourceType": "ExplanationOfBenefit", "id": "EOBID", "provider":{"reference": "123"}, "insurance": [{"coverage": {"reference": "coverage"}, "focal": false}], "item": [{"sequence": 1, "productOrService": {"text": "test"}}]}`)
			if tc.apiVersion == bcda.V2 {
				// The valid R4 EOB, all other resources used in this test work across both
				// STU3 and R4.
				file3Data = []byte(`{` +
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
			}

			exportEndpoint := "/api/v1/Group/all/$export"
			expectedJobURLSuffix := "/jobs/1234"
			if tc.bcdaJobURLSuffix != "" {
				expectedJobURLSuffix = tc.bcdaJobURLSuffix
			}
			if tc.apiVersion == bcda.V2 {
				exportEndpoint = "/api/v2/Group/all/$export"
			}
			serverTransactionTime := "2020-12-09T11:00:00.123+00:00"

			// Setup BCDA test servers:

			// A seperate resource server is needed during testing, so that we can send
			// the jobsEndpoint response in the bcdaServer that includes a URL for the
			// bcdaResourceServer in it.
			bcdaResourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				switch req.URL.Path {
				case file1URLSuffix:
					w.WriteHeader(http.StatusOK)
					w.Write(file1Data)
				case file2URLSuffix:
					w.WriteHeader(http.StatusOK)
					w.Write(file2Data)
				case file3URLSuffix:
					w.WriteHeader(http.StatusOK)
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
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(`{"access_token": "token"}`))
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
					w.WriteHeader(http.StatusOK)
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

			outputPrefix := t.TempDir()

			// Set mainWrapperConfig for this test case. In practice, values are
			// populated in mainWrapperConfig from flags. Setting the config struct
			// instead of the flags in tests enables parallelization with significant
			// performance improvement. A seperate test below tests that setting flags
			// properly populates mainWrapperConfig.
			cfg := mainWrapperConfig{
				clientID:                   "id",
				clientSecret:               "secret",
				outputPrefix:               outputPrefix,
				bcdaServerURL:              bcdaServer.URL,
				fhirStoreGCPProject:        gcpProject,
				fhirStoreGCPLocation:       gcpLocation,
				fhirStoreGCPDatasetID:      gcpDatasetID,
				fhirStoreID:                gcpFHIRStoreID,
				bcdaJobURL:                 bcdaServer.URL + expectedJobURLSuffix,
				fhirStoreEnableBatchUpload: tc.fhirStoreEnableBatchUpload,
				enableFHIRStore:            tc.enableFHIRStore,
				rectify:                    tc.rectify,
				since:                      tc.since,
				noFailOnUploadErrors:       tc.noFailOnUploadErrors,
				maxFHIRStoreUploadWorkers:  10,
			}

			if tc.apiVersion == bcda.V2 {
				cfg.useV2 = true
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
						ResourceID:   "PatientID",
						ResourceType: "Patient",
						Data:         file1Data,
					},
					{
						ResourceID:   "EOBID",
						ResourceType: "ExplanationOfBenefit",
						Data:         file3Data,
					},
				}

				if tc.rectify {
					fhirStoreTests = append(fhirStoreTests, testhelpers.FHIRStoreTestResource{
						ResourceID:   "CoverageID",
						ResourceType: "Coverage",
						Data:         file2DataRectified,
					})
				} else {
					fhirStoreTests = append(fhirStoreTests, testhelpers.FHIRStoreTestResource{
						ResourceID:   "CoverageID",
						ResourceType: "Coverage",
						Data:         file2Data,
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
			if err := mainWrapper(cfg); !errors.Is(err, tc.wantError) {
				t.Errorf("mainWrapper(%v) error: %v", cfg, err)
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
					"_ExplanationOfBenefit_0.ndjson": file3Data,
					"_Coverage_0.ndjson":             file2Data,
					"_Patient_0.ndjson":              file1Data}

				if tc.rectify {
					// Replace expected data with the rectified version of resource:
					expectedFileSuffixToData["_Coverage_0.ndjson"] = file2DataRectified
				}

				if tc.bcdaJobURLSuffix != "" {
					if got := exportEndpointCalled.Value(); got > 0 {
						t.Errorf("mainWrapper: when bcdaJobID is provided, did not expect any calls to BCDA export API. got: %v, want: %v", got, 0)
					}
				}

				if !tc.unsetOutputPrefix {
					for fileSuffix, wantData := range expectedFileSuffixToData {
						fullPath := outputPrefix + fileSuffix
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
	cases := []struct {
		name       string
		apiVersion bcda.Version
	}{
		{
			name:       "BCDAV1",
			apiVersion: bcda.V1,
		},
		{
			name:       "BCDAV2",
			apiVersion: bcda.V2,
		},
	}
	t.Parallel()
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Declare test data:
			file1Data := []byte(`{"resourceType":"Patient","id":"PatientID"}`)
			exportEndpoint := "/api/v1/Group/all/$export"
			jobsEndpoint := "/api/v1/jobs/1234"
			if tc.apiVersion == bcda.V2 {
				exportEndpoint = "/api/v2/Group/all/$export"
				jobsEndpoint = "/api/v2/jobs/1234"
			}
			serverTransactionTime := "2020-12-09T11:00:00.123+00:00"

			// Setup BCDA test servers:

			// A seperate resource server is needed during testing, so that we can send
			// the jobsEndpoint response in the bcdaServer that includes a URL for the
			// bcdaResourceServer in it.
			bcdaResourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write(file1Data)
			}))
			defer bcdaResourceServer.Close()

			jobStatusURL := ""

			bcdaServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				switch req.URL.Path {
				case "/auth/token":
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(`{"access_token": "token"}`))
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
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(fmt.Sprintf("{\"output\": [{\"type\": \"Patient\", \"url\": \"%s/data/10.ndjson\"}, {\"type\": \"Coverage\", \"url\": \"%s/data/20.ndjson\"}, {\"type\": \"ExplanationOfBenefit\", \"url\": \"%s/data/30.ndjson\"}], \"transactionTime\": \"%s\"}", bcdaResourceServer.URL, bcdaResourceServer.URL, bcdaResourceServer.URL, serverTransactionTime)))
				default:
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer bcdaServer.Close()

			jobStatusURL = bcdaServer.URL + jobsEndpoint

			// Set flags for this test case:
			outputPrefix := t.TempDir()
			sinceFilePath := path.Join(t.TempDir(), "since_file.txt")
			// Set mainWrapperConfig for this test case. In practice, values are
			// populated in mainWrapperConfig from flags. Setting the config struct
			// instead of the flags in tests enables parallelization with significant
			// performance improvement. A seperate test below tests that setting flags
			// properly populates mainWrapperConfig.
			cfg := mainWrapperConfig{
				clientID:                  "id",
				clientSecret:              "secret",
				outputPrefix:              outputPrefix,
				bcdaServerURL:             bcdaServer.URL,
				sinceFile:                 sinceFilePath,
				maxFHIRStoreUploadWorkers: 10,
			}

			if tc.apiVersion == bcda.V2 {
				cfg.useV2 = true
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
		})
	}
}

func TestMainWrapper_GetJobStatusAuthRetry(t *testing.T) {
	// This tests that if JobStatus returns unauthorized, mainWrapper attempts to
	// re-authorize and try again.
	cases := []struct {
		name       string
		apiVersion bcda.Version
	}{
		{
			name:       "BCDAV1",
			apiVersion: bcda.V1,
		},
		{
			name:       "BCDAV2",
			apiVersion: bcda.V2,
		},
	}

	t.Parallel()
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Declare test data:
			file1Data := []byte(`{"resourceType":"Patient","id":"PatientID"}`)
			exportEndpoint := "/api/v1/Group/all/$export"
			jobsEndpoint := "/api/v1/jobs/1234"
			if tc.apiVersion == bcda.V2 {
				exportEndpoint = "/api/v2/Group/all/$export"
				jobsEndpoint = "/api/v2/jobs/1234"
			}
			serverTransactionTime := "2020-12-09T11:00:00.123+00:00"

			// Setup BCDA test servers:

			// A seperate resource server is needed during testing, so that we can send
			// the jobsEndpoint response in the bcdaServer that includes a URL for the
			// bcdaResourceServer in it.
			bcdaResourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				w.WriteHeader(http.StatusOK)
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
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(`{"access_token": "token"}`))
				case exportEndpoint:

					w.Header()["Content-Location"] = []string{jobStatusURL}

					w.WriteHeader(http.StatusAccepted)
				case jobsEndpoint:
					jobsCalled.Increment()
					if authCalled.Value() < 2 {
						w.WriteHeader(http.StatusUnauthorized)
						return
					}

					w.WriteHeader(http.StatusOK)
					w.Write([]byte(fmt.Sprintf("{\"output\": [{\"type\": \"Patient\", \"url\": \"%s/data/10.ndjson\"}, {\"type\": \"Coverage\", \"url\": \"%s/data/20.ndjson\"}, {\"type\": \"ExplanationOfBenefit\", \"url\": \"%s/data/30.ndjson\"}], \"transactionTime\": \"%s\"}", bcdaResourceServer.URL, bcdaResourceServer.URL, bcdaResourceServer.URL, serverTransactionTime)))
				default:
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer bcdaServer.Close()

			jobStatusURL = bcdaServer.URL + jobsEndpoint

			// Set flags for this test case:
			outputPrefix := t.TempDir()
			// Set mainWrapperConfig for this test case. In practice, values are
			// populated in mainWrapperConfig from flags. Setting the config struct
			// instead of the flags in tests enables parallelization with significant
			// performance improvement. A seperate test below tests that setting flags
			// properly populates mainWrapperConfig.
			cfg := mainWrapperConfig{
				clientID:                  "id",
				clientSecret:              "secret",
				outputPrefix:              outputPrefix,
				bcdaServerURL:             bcdaServer.URL,
				maxFHIRStoreUploadWorkers: 10,
			}

			if tc.apiVersion == bcda.V2 {
				cfg.useV2 = true
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
		})
	}
}

func TestMainWrapper_GetDataRetry(t *testing.T) {
	// This tests that if GetData returns unauthorized or not found, mainWrapper
	// attempts to re-authorize and try again at least 5 times.
	cases := []struct {
		name               string
		apiVersion         bcda.Version
		httpErrorToRetrun  int
		numRetriesBeforeOK int
		wantError          error
	}{
		{
			name:               "BCDAV1",
			apiVersion:         bcda.V1,
			httpErrorToRetrun:  http.StatusUnauthorized,
			numRetriesBeforeOK: 5,
		},
		{
			name:               "BCDAV2",
			apiVersion:         bcda.V2,
			httpErrorToRetrun:  http.StatusUnauthorized,
			numRetriesBeforeOK: 5,
		},
		{
			name:               "BCDAV1TooManyRetries",
			apiVersion:         bcda.V1,
			httpErrorToRetrun:  http.StatusUnauthorized,
			numRetriesBeforeOK: 6,
			wantError:          bulkfhir.ErrorUnauthorized,
		},
		{
			name:               "BCDAV2TooManyRetries",
			apiVersion:         bcda.V2,
			httpErrorToRetrun:  http.StatusUnauthorized,
			numRetriesBeforeOK: 6,
			wantError:          bulkfhir.ErrorUnauthorized,
		},
		{
			name:               "BCDAV1With404",
			apiVersion:         bcda.V1,
			httpErrorToRetrun:  http.StatusNotFound,
			numRetriesBeforeOK: 5,
		},
		{
			name:               "BCDAV2With404",
			apiVersion:         bcda.V2,
			httpErrorToRetrun:  http.StatusNotFound,
			numRetriesBeforeOK: 5,
		},
		{
			name:               "BCDAV1TooManyRetriesWith404",
			apiVersion:         bcda.V1,
			httpErrorToRetrun:  http.StatusNotFound,
			numRetriesBeforeOK: 6,
			wantError:          bulkfhir.ErrorRetryableHTTPStatus,
		},
		{
			name:               "BCDAV2TooManyRetriesWith404",
			apiVersion:         bcda.V2,
			httpErrorToRetrun:  http.StatusNotFound,
			numRetriesBeforeOK: 6,
			wantError:          bulkfhir.ErrorRetryableHTTPStatus,
		},
	}
	t.Parallel()
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Declare test data:
			file1Data := []byte(`{"resourceType":"Patient","id":"PatientID"}`)
			exportEndpoint := "/api/v1/Group/all/$export"
			jobURLSuffix := "/api/v1/jobs/1234"
			if tc.apiVersion == bcda.V2 {
				exportEndpoint = "/api/v2/Group/all/$export"
				jobURLSuffix = "/api/v2/jobs/1234"
			}
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
				w.WriteHeader(http.StatusOK)
				w.Write(file1Data)
			}))
			defer bcdaResourceServer.Close()

			jobStatusURL := ""

			bcdaServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				switch req.URL.Path {
				case "/auth/token":
					authCalled.Increment()
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(`{"access_token": "token"}`))
				case exportEndpoint:

					w.Header()["Content-Location"] = []string{jobStatusURL}

					w.WriteHeader(http.StatusAccepted)
				case jobURLSuffix:
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(fmt.Sprintf("{\"output\": [{\"type\": \"Patient\", \"url\": \"%s/data/10.ndjson\"}], \"transactionTime\": \"%s\"}", bcdaResourceServer.URL, serverTransactionTime)))
				default:
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer bcdaServer.Close()

			jobStatusURL = bcdaServer.URL + jobURLSuffix

			// Set flags for this test case:
			outputPrefix := t.TempDir()
			// Set mainWrapperConfig for this test case. In practice, values are
			// populated in mainWrapperConfig from flags. Setting the config struct
			// instead of the flags in tests enables parallelization with significant
			// performance improvement. A seperate test below tests that setting flags
			// properly populates mainWrapperConfig.
			cfg := mainWrapperConfig{
				clientID:                  "id",
				clientSecret:              "secret",
				outputPrefix:              outputPrefix,
				bcdaServerURL:             bcdaServer.URL,
				maxFHIRStoreUploadWorkers: 10,
			}

			if tc.apiVersion == bcda.V2 {
				cfg.useV2 = true
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

func TestMainWrapper_WithDeprecatedJobIDFlag(t *testing.T) {
	t.Parallel()
	cfg := mainWrapperConfig{
		clientID:     "ID",
		clientSecret: "secret",
		bcdaJobID:    "1234",
	}
	if err := mainWrapper(cfg); !errors.Is(err, errBCDAJobIDDeprecated) {
		t.Errorf("mainWrapper unexpected error. got: %v, want: %v", err, errBCDAJobIDDeprecated)
	}
}

func TestMainWrapper_BatchUploadSize(t *testing.T) {
	// This test more comprehensively checks setting different batch sizes in
	// bulk_fhir_fetch.
	cases := []struct {
		name            string
		apiVersion      bcda.Version
		batchUploadSize int
	}{
		{
			name:            "BCDAV1WithBatchSize2",
			apiVersion:      bcda.V1,
			batchUploadSize: 2,
		},
		{
			name:            "BCDAV2WithBatchSize2",
			apiVersion:      bcda.V2,
			batchUploadSize: 2,
		},
		{
			name:            "BCDAV1WithBatchSize3",
			apiVersion:      bcda.V1,
			batchUploadSize: 3,
		},
		{
			name:            "BCDAV2WithBatchSize3",
			apiVersion:      bcda.V2,
			batchUploadSize: 3,
		},
	}

	t.Parallel()
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// File 1 contains 3 Patient resources.
			patient1 := `{"resourceType":"Patient","id":"PatientID1"}`
			patient2 := `{"resourceType":"Patient","id":"PatientID2"}`
			patient3 := `{"resourceType":"Patient","id":"PatientID3"}`
			file1Data := []byte(patient1 + "\n" + patient2 + "\n" + patient3)
			exportEndpoint := "/api/v1/Group/all/$export"
			jobStatusURLSuffix := "/api/v1/jobs/1234"
			if tc.apiVersion == bcda.V2 {
				exportEndpoint = "/api/v2/Group/all/$export"
				jobStatusURLSuffix = "/api/v2/jobs/1234"
			}
			serverTransactionTime := "2020-12-09T11:00:00.123+00:00"

			// Setup BCDA test servers:

			jobStatusURL := ""

			// A seperate resource server is needed during testing, so that we can send
			// the jobsEndpoint response in the bcdaServer that includes a URL for the
			// bcdaResourceServer in it.
			bcdaResourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write(file1Data)
			}))
			defer bcdaResourceServer.Close()

			bcdaServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				switch req.URL.Path {
				case "/auth/token":
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(`{"access_token": "token"}`))
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
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(fmt.Sprintf("{\"output\": [{\"type\": \"Patient\", \"url\": \"%s/data/10.ndjson\"}], \"transactionTime\": \"%s\"}", bcdaResourceServer.URL, serverTransactionTime)))
				default:
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer bcdaServer.Close()

			jobStatusURL = bcdaServer.URL + jobStatusURLSuffix

			// Set minimal flags for this test case:
			outputPrefix := t.TempDir()
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
				outputPrefix:               outputPrefix,
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

			if tc.apiVersion == bcda.V2 {
				cfg.useV2 = true
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
	patient1 := `{"resourceType":"Patient","id":"PatientID1"}`
	file1Data := []byte(patient1)
	exportEndpoint := "/api/v2/Group/all/$export"
	jobStatusURLSuffix := "/api/v2/jobs/1234"
	serverTransactionTime := "2020-12-09T11:00:00.123+00:00"

	bucketName := "bucket"

	// Set minimal flags for this test case:
	outputPrefix := t.TempDir()
	gcpProject := "project"
	gcpLocation := "location"
	gcpDatasetID := "dataset"
	gcpFHIRStoreID := "fhirID"

	// Setup BCDA test servers:

	// A seperate resource server is needed during testing, so that we can send
	// the jobsEndpoint response in the bcdaServer that includes a URL for the
	// bcdaResourceServer in it.
	bcdaResourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(file1Data)
	}))
	defer bcdaResourceServer.Close()

	jobStatusURL := ""
	bcdaServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/auth/token":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"access_token": "token"}`))
		case exportEndpoint:
			w.Header()["Content-Location"] = []string{jobStatusURL}
			w.WriteHeader(http.StatusAccepted)
		case jobStatusURLSuffix:
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(fmt.Sprintf("{\"output\": [{\"type\": \"Patient\", \"url\": \"%s/data/10.ndjson\"}], \"transactionTime\": \"%s\"}", bcdaResourceServer.URL, serverTransactionTime)))
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer bcdaServer.Close()

	jobStatusURL = bcdaServer.URL + jobStatusURLSuffix

	gcsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		fileWritePath := ("/upload/storage/v1/b/" +
			bucketName + "/o?alt=json&name=" +
			url.QueryEscape(serverTransactionTime+"/"+"Patient_0.ndjson") +
			"&prettyPrint=false&projection=full&uploadType=multipart")

		if req.URL.String() != fileWritePath {
			t.Errorf("gcs server got unexpected request. got: %v, want: %v", req.URL.String(), fileWritePath)
		}
		data, err := io.ReadAll(req.Body)
		if err != nil {
			t.Errorf("gcs server error reading body.")
		}
		if !bytes.Contains(data, file1Data) {
			t.Errorf("gcs server unexpected data: got: %s, want: %s", data, file1Data)
		}
	}))

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
		gcsEndpoint:                   gcsServer.URL,
		fhirStoreEndpoint:             fhirStoreServer.URL,
		clientID:                      "id",
		clientSecret:                  "secret",
		outputPrefix:                  outputPrefix,
		bcdaServerURL:                 bcdaServer.URL,
		fhirStoreGCPProject:           gcpProject,
		fhirStoreGCPLocation:          gcpLocation,
		fhirStoreGCPDatasetID:         gcpDatasetID,
		fhirStoreID:                   gcpFHIRStoreID,
		fhirStoreEnableGCSBasedUpload: true,
		fhirStoreGCSBasedUploadBucket: bucketName,
		enableFHIRStore:               true,
		rectify:                       true,
		useV2:                         true,
	}
	// Run mainWrapper:
	if err := mainWrapper(cfg); err != nil {
		t.Errorf("mainWrapper(%v) error: %v", cfg, err)
	}

	if !importCalled {
		t.Errorf("mainWrapper(%v) expected FHIR Store import to be called, but was not", cfg)
	}
	if !statusCalled {
		t.Errorf("mainWrapper(%v) expected FHIR Store import operation status to be called, but was not", cfg)
	}

	// Check that files were also written to disk under outputPrefix
	fullPath := outputPrefix + "_Patient_0.ndjson"
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
	patient1 := `{"resourceType":"Patient","id":"PatientID1"}`
	file1Data := []byte(patient1)

	baseURLSuffix := "/api/v20"

	exportEndpoint := "/api/v20/Group/all/$export"
	jobStatusURLSuffix := "/api/v20/jobs/1234"
	serverTransactionTime := "2020-12-09T11:00:00.123+00:00"

	scopes := []string{"a", "b", "c"}

	// Setup bulk fhir test servers:
	jobStatusURL := ""

	// A seperate resource server is needed during testing, so that we can send
	// the jobsEndpoint response in the bulkFHIRServer that includes a URL for the
	// bulkFHIRResourceServer in it.
	bulkFHIRResourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
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
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"access_token": "token"}`))
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
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(fmt.Sprintf("{\"output\": [{\"type\": \"Patient\", \"url\": \"%s/data/10.ndjson\"}], \"transactionTime\": \"%s\"}", bulkFHIRResourceServer.URL, serverTransactionTime)))
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer bulkFHIRServer.Close()
	jobStatusURL = bulkFHIRServer.URL + jobStatusURLSuffix

	bulkFHIRBaseURL := bulkFHIRServer.URL + baseURLSuffix
	authURL := bulkFHIRServer.URL + "/auth/token"

	outputPrefix := t.TempDir()

	// Set mainWrapperConfig for this test case. In practice, values are
	// populated in mainWrapperConfig from flags. Setting the config struct
	// instead of the flags in tests enables parallelization with significant
	// performance improvement. A seperate test below tests that setting flags
	// properly populates mainWrapperConfig.
	cfg := mainWrapperConfig{
		clientID:                 "id",
		clientSecret:             "secret",
		outputPrefix:             outputPrefix,
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

	// Check that files were also written to disk under outputPrefix
	fullPath := outputPrefix + "_Patient_0.ndjson"
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
	patient1 := `{"resourceType":"Patient","id":"PatientID1"}`
	file1Data := []byte(patient1)
	exportEndpoint := "/api/v2/Group/all/$export"
	jobStatusURLSuffix := "/api/v2/jobs/1234"
	serverTransactionTime := "2020-12-09T11:00:00.123+00:00"

	// Set minimal flags for this test case:
	outputPrefix := t.TempDir()
	sinceFile := "gs://sinceBucket/sinceFile"
	since := "2006-01-02T15:04:05.000-07:00"

	// Setup BCDA test servers:

	// A seperate resource server is needed during testing, so that we can send
	// the jobsEndpoint response in the bcdaServer that includes a URL for the
	// bcdaResourceServer in it.
	bcdaResourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(file1Data)
	}))
	defer bcdaResourceServer.Close()

	jobStatusURL := ""
	bcdaServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/auth/token":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"access_token": "token"}`))
		case exportEndpoint:
			w.Header()["Content-Location"] = []string{jobStatusURL}
			w.WriteHeader(http.StatusAccepted)
		case jobStatusURLSuffix:
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(fmt.Sprintf("{\"output\": [{\"type\": \"Patient\", \"url\": \"%s/data/10.ndjson\"}], \"transactionTime\": \"%s\"}", bcdaResourceServer.URL, serverTransactionTime)))
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer bcdaServer.Close()

	jobStatusURL = bcdaServer.URL + jobStatusURLSuffix

	requestsSince := false
	uploadsNewSince := false
	gcsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {

		requestSinceURL := "/sinceBucket/sinceFile"
		uploadSinceURL := "/upload/storage/v1/b/sinceBucket/o"

		switch req.URL.Path {
		case requestSinceURL:
			requestsSince = true
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(since))
		case uploadSinceURL:
			uploadsNewSince = true
			data, err := io.ReadAll(req.Body)
			if err != nil {
				t.Errorf("gcs server error reading body.")
			}
			if !bytes.Contains(data, []byte(serverTransactionTime)) {
				t.Errorf(
					"gcs server unexpected new since data; got: %s, want: %s", data, serverTransactionTime)
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("{}"))
		default:
			w.WriteHeader(http.StatusBadRequest)
		}

	}))

	// Set mainWrapperConfig for this test case. In practice, values are
	// populated in mainWrapperConfig from flags. Setting the config struct
	// instead of the flags in tests enables parallelization with significant
	// performance improvement. A seperate test below tests that setting flags
	// properly populates mainWrapperConfig.
	cfg := mainWrapperConfig{
		gcsEndpoint:   gcsServer.URL,
		clientID:      "id",
		clientSecret:  "secret",
		outputPrefix:  outputPrefix,
		bcdaServerURL: bcdaServer.URL,
		rectify:       true,
		useV2:         true,
		sinceFile:     sinceFile,
	}
	// Run mainWrapper:
	if err := mainWrapper(cfg); err != nil {
		t.Errorf("mainWrapper(%v) error: %v", cfg, err)
	}

	if !requestsSince {
		t.Error("Expected bcda_fetch to request since file from GCS. But it was not requested.")
	}
	if !uploadsNewSince {
		t.Error("Expected bcda_fetch to upload new since file to GCS. But it was not uploaded.")
	}

	// Check that files were also written to disk under outputPrefix
	fullPath := outputPrefix + "_Patient_0.ndjson"
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

func TestBuildMainWrapperConfig(t *testing.T) {
	// Set every flag, and see that it is built into mainWrapper correctly.
	defer SaveFlags().Restore()
	flag.Set("client_id", "clientID")
	flag.Set("client_secret", "clientSecret")
	flag.Set("output_prefix", "outputPrefix")
	flag.Set("use_v2", "true")
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
	flag.Set("bcda_job_url", "jobURL")

	expectedCfg := mainWrapperConfig{
		fhirStoreEndpoint:             fhirstore.DefaultHealthcareEndpoint,
		gcsEndpoint:                   gcs.DefaultCloudStorageEndpoint,
		clientID:                      "clientID",
		clientSecret:                  "clientSecret",
		outputPrefix:                  "outputPrefix",
		useV2:                         true,
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
		bcdaJobURL:                    "jobURL",
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