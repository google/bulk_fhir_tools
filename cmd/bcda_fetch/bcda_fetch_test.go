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
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"sync"
	"testing"

	"github.com/google/medical_claims_tools/internal/testhelpers"

	"flag"
	"github.com/google/go-cmp/cmp"
	"github.com/google/medical_claims_tools/bcda"
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
		fhirStoreFailures    bool
		noFailOnUploadErrors bool
		bcdaJobID            string
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
			name:            "SinceProvidedWithRectifyWithFHIRStoreBCDAV2WithBCDAJobId",
			rectify:         true,
			enableFHIRStore: true,
			apiVersion:      bcda.V2,
			since:           "2006-01-02T15:04:05.000-07:00",
			bcdaJobID:       "999",
		},
		{
			name:            "SinceProvidedWithRectifyWithFHIRStoreBCDAV1WithBCDAJobId",
			rectify:         true,
			enableFHIRStore: true,
			apiVersion:      bcda.V1,
			since:           "2006-01-02T15:04:05.000-07:00",
			bcdaJobID:       "999",
		},
		{
			name:                     "SinceFileProvidedWithBCDAV2WithBCDAJobId",
			rectify:                  true,
			enableFHIRStore:          true,
			apiVersion:               bcda.V2,
			sinceFileContent:         []byte("2013-12-09T11:00:00.123+00:00\n2015-12-09T11:00:00.123+00:00\n"),
			sinceFileLatestTimestamp: "2015-12-09T11:00:00.123+00:00",
			sinceFileExpectedContent: []byte("2013-12-09T11:00:00.123+00:00\n2015-12-09T11:00:00.123+00:00\n2020-12-09T11:00:00.123+00:00\n"),
			bcdaJobID:                "999",
		},
		{
			name:                     "SinceFileProvidedWithBCDAV1WithBCDAJobId",
			rectify:                  true,
			enableFHIRStore:          true,
			apiVersion:               bcda.V1,
			sinceFileContent:         []byte("2013-12-09T11:00:00.123+00:00\n2015-12-09T11:00:00.123+00:00\n"),
			sinceFileLatestTimestamp: "2015-12-09T11:00:00.123+00:00",
			sinceFileExpectedContent: []byte("2013-12-09T11:00:00.123+00:00\n2015-12-09T11:00:00.123+00:00\n2020-12-09T11:00:00.123+00:00\n"),
			bcdaJobID:                "999",
		},
		{
			name:                     "SinceFileEmptyProvidedWithBCDAV2WithBCDAJobId",
			rectify:                  true,
			enableFHIRStore:          true,
			apiVersion:               bcda.V2,
			sinceFileContent:         []byte(""),
			sinceFileExpectedContent: []byte("2020-12-09T11:00:00.123+00:00\n"),
			bcdaJobID:                "999",
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
			bcdaJobID:         "999",
			unsetOutputPrefix: true,
		},
		{
			name:              "EmptyOutputPrefixWithSinceProvidedWithRectifyWithFHIRStoreBCDAV1WithBCDAJobId",
			rectify:           true,
			enableFHIRStore:   true,
			apiVersion:        bcda.V1,
			since:             "2006-01-02T15:04:05.000-07:00",
			bcdaJobID:         "999",
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
			bcdaJobID:                "999",
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
			bcdaJobID:                "999",
			unsetOutputPrefix:        true,
		},
		// TODO(b/226375559): see if we can generate some of the test cases above
		// instead of having to spell them out explicitly.
		// TODO(b/213365276): test that bcda V1 with rectify = true results in an
		// error.
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
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
			expectedJobID := "1234"
			if tc.bcdaJobID != "" {
				expectedJobID = tc.bcdaJobID
			}
			jobsEndpoint := fmt.Sprintf("/api/v1/jobs/%s", expectedJobID)
			if tc.apiVersion == bcda.V2 {
				exportEndpoint = "/api/v2/Group/all/$export"
				jobsEndpoint = fmt.Sprintf("/api/v2/jobs/%s", expectedJobID)
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
					w.Header()["Content-Location"] = []string{"some/info/1234"}
					w.WriteHeader(http.StatusAccepted)
				case jobsEndpoint:
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(fmt.Sprintf("{\"output\": [{\"type\": \"Patient\", \"url\": \"%s/data/10.ndjson\"}, {\"type\": \"Coverage\", \"url\": \"%s/data/20.ndjson\"}, {\"type\": \"ExplanationOfBenefit\", \"url\": \"%s/data/30.ndjson\"}], \"transactionTime\": \"%s\"}", bcdaResourceServer.URL, bcdaResourceServer.URL, bcdaResourceServer.URL, serverTransactionTime)))
				default:
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer bcdaServer.Close()

			// FHIR Store values (always set, but enable_fhir_store is set based on the
			// test case).
			gcpProject := "project"
			gcpLocation := "location"
			gcpDatasetID := "dataset"
			gcpFHIRStoreID := "fhirID"

			// Set flags for this test case:
			outputPrefix := t.TempDir()

			defer SaveFlags().Restore()
			flag.Set("client_id", "id")
			flag.Set("client_secret", "secret")
			if !tc.unsetOutputPrefix {
				flag.Set("output_prefix", outputPrefix)
			}
			flag.Set("bcda_server_url", bcdaServer.URL)

			flag.Set("fhir_store_gcp_project", gcpProject)
			flag.Set("fhir_store_gcp_location", gcpLocation)
			flag.Set("fhir_store_gcp_dataset_id", gcpDatasetID)
			flag.Set("fhir_store_id", gcpFHIRStoreID)
			flag.Set("bcda_job_id", tc.bcdaJobID)

			if tc.enableFHIRStore {
				flag.Set("enable_fhir_store", "true")
			}

			if tc.apiVersion == bcda.V2 {
				flag.Set("use_v2", "true")
			}
			if tc.rectify {
				flag.Set("rectify", "true")
			}
			if tc.since != "" {
				flag.Set("since", tc.since)
			}
			if tc.noFailOnUploadErrors {
				flag.Set("no_fail_on_upload_errors", "true")
			}

			fhirStoreUploadErrorFileDir := ""
			if tc.enableFHIRStoreUploadErrorFileDir {
				fhirStoreUploadErrorFileDir = t.TempDir()
				flag.Set("fhir_store_upload_error_file_dir", fhirStoreUploadErrorFileDir)
			}

			sinceTmpFile, err := ioutil.TempFile(t.TempDir(), "since_file.txt")
			if err != nil {
				t.Fatalf("unable to initialize since_file.txt: %v", err)
			}
			if len(tc.sinceFileContent) != 0 {
				_, err := sinceTmpFile.Write(tc.sinceFileContent)
				if err != nil {
					sinceTmpFile.Close()
					t.Fatalf("unable to initialize since_file.txt: %v", err)
				}
				sinceTmpFile.Close()
				flag.Set("since_file", sinceTmpFile.Name())
			}

			fhirStoreEndpoint := ""
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
					fhirStoreEndpoint = serverAlwaysFails(t)
				} else {
					if !tc.disableFHIRStoreUploadChecks {
						fhirStoreEndpoint = testhelpers.FHIRStoreServer(t, fhirStoreTests, gcpProject, gcpLocation, gcpDatasetID, gcpFHIRStoreID)
					}
				}
			}

			// Run mainWrapper:
			cfg := mainWrapperConfig{fhirStoreEndpoint: fhirStoreEndpoint}
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
				testhelpers.CheckErrorNDJSONFile(t, fhirStoreUploadErrorFileDir, wantErrors)
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

				if tc.bcdaJobID != "" {
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

	for _, tc := range cases {
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
				w.Header()["Content-Location"] = []string{"some/info/1234"}
				w.WriteHeader(http.StatusAccepted)
			case jobsEndpoint:
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(fmt.Sprintf("{\"output\": [{\"type\": \"Patient\", \"url\": \"%s/data/10.ndjson\"}, {\"type\": \"Coverage\", \"url\": \"%s/data/20.ndjson\"}, {\"type\": \"ExplanationOfBenefit\", \"url\": \"%s/data/30.ndjson\"}], \"transactionTime\": \"%s\"}", bcdaResourceServer.URL, bcdaResourceServer.URL, bcdaResourceServer.URL, serverTransactionTime)))
			default:
				w.WriteHeader(http.StatusBadRequest)
			}
		}))
		defer bcdaServer.Close()

		// Set flags for this test case:
		outputPrefix := t.TempDir()
		defer SaveFlags().Restore()
		flag.Set("client_id", "id")
		flag.Set("client_secret", "secret")
		flag.Set("output_prefix", outputPrefix)
		flag.Set("bcda_server_url", bcdaServer.URL)

		if tc.apiVersion == bcda.V2 {
			flag.Set("use_v2", "true")
		}
		sinceFilePath := path.Join(t.TempDir(), "since_file.txt")
		flag.Set("since_file", sinceFilePath)

		// Run mainWrapper:
		cfg := mainWrapperConfig{fhirStoreEndpoint: ""}
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

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
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

			bcdaServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				switch req.URL.Path {
				case "/auth/token":
					authCalled.Increment()
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(`{"access_token": "token"}`))
				case exportEndpoint:
					w.Header()["Content-Location"] = []string{"some/info/1234"}
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

			// Set flags for this test case:
			outputPrefix := t.TempDir()
			defer SaveFlags().Restore()
			flag.Set("client_id", "id")
			flag.Set("client_secret", "secret")
			flag.Set("output_prefix", outputPrefix)
			flag.Set("bcda_server_url", bcdaServer.URL)

			if tc.apiVersion == bcda.V2 {
				flag.Set("use_v2", "true")
			}

			// Run mainWrapper:
			cfg := mainWrapperConfig{fhirStoreEndpoint: ""}
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

func TestMainWrapper_GetDataAuthRetry(t *testing.T) {
	// This tests that if GetData returns unauthorized, mainWrapper attempts to
	// re-authorize and try again at least 5 times.
	cases := []struct {
		name               string
		apiVersion         bcda.Version
		numRetriesBeforeOK int
		wantError          error
	}{
		{
			name:               "BCDAV1",
			apiVersion:         bcda.V1,
			numRetriesBeforeOK: 5,
		},
		{
			name:               "BCDAV2",
			apiVersion:         bcda.V2,
			numRetriesBeforeOK: 5,
		},
		{
			name:               "BCDAV1TooManyRetries",
			apiVersion:         bcda.V1,
			numRetriesBeforeOK: 6,
			wantError:          bcda.ErrorUnauthorized,
		},
		{
			name:               "BCDAV2TooManyRetries",
			apiVersion:         bcda.V2,
			numRetriesBeforeOK: 6,
			wantError:          bcda.ErrorUnauthorized,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Declare test data:
			file1Data := []byte(`{"resourceType":"Patient","id":"PatientID"}`)
			exportEndpoint := "/api/v1/Group/all/$export"
			jobsEndpoint := "/api/v1/jobs/1234"
			if tc.apiVersion == bcda.V2 {
				exportEndpoint = "/api/v2/Group/all/$export"
				jobsEndpoint = "/api/v2/jobs/1234"
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
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				w.WriteHeader(http.StatusOK)
				w.Write(file1Data)
			}))
			defer bcdaResourceServer.Close()

			bcdaServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				switch req.URL.Path {
				case "/auth/token":
					authCalled.Increment()
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(`{"access_token": "token"}`))
				case exportEndpoint:
					w.Header()["Content-Location"] = []string{"some/info/1234"}
					w.WriteHeader(http.StatusAccepted)
				case jobsEndpoint:
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(fmt.Sprintf("{\"output\": [{\"type\": \"Patient\", \"url\": \"%s/data/10.ndjson\"}], \"transactionTime\": \"%s\"}", bcdaResourceServer.URL, serverTransactionTime)))
				default:
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer bcdaServer.Close()

			// Set flags for this test case:
			outputPrefix := t.TempDir()
			defer SaveFlags().Restore()
			flag.Set("client_id", "id")
			flag.Set("client_secret", "secret")
			flag.Set("output_prefix", outputPrefix)
			flag.Set("bcda_server_url", bcdaServer.URL)

			if tc.apiVersion == bcda.V2 {
				flag.Set("use_v2", "true")
			}

			// Run mainWrapper:
			cfg := mainWrapperConfig{fhirStoreEndpoint: ""}
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
