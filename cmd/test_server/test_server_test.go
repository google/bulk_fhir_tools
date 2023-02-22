// Copyright 2023 Google LLC
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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	cpb "github.com/google/fhir/go/proto/google/fhir/proto/r4/core/codes_go_proto"
	"github.com/google/go-cmp/cmp"
	"github.com/google/medical_claims_tools/bulkfhir"
)

// This includes some basic __sanity__ tests of the test_server.
// We will work on configuring some integration testing of bulk_fhir_fetch
// against the test server, which may be more comprehensive.

func TestTimestampConversion(t *testing.T) {
	t.Parallel()

	now := time.Now()
	for _, tc := range []struct {
		description, input, want string
		fn                       func(string) (string, error)
	}{
		{
			description: "filepath to fhir",
			input:       "20230217T131545Z",
			want:        "2023-02-17T13:15:45.000+00:00",
			fn:          filepathTimestampToFHIRTimestamp,
		},
		{
			description: "fhir to filepath, utc",
			input:       "2023-02-17T13:15:45Z",
			want:        "20230217T131545Z",
			fn:          fhirTimestampToFilepathTimestamp,
		},
		{
			description: "fhir to filepath, with zone",
			input:       "2023-02-17T13:15:45+07:00",
			want:        "20230217T061545Z",
			fn:          fhirTimestampToFilepathTimestamp,
		},
		{
			description: "fhir to filepath, fractional seconds utc",
			input:       "2023-02-17T13:15:45.123Z",
			want:        "20230217T131545Z",
			fn:          fhirTimestampToFilepathTimestamp,
		},
		{
			description: "fhir to filepath, fractional seconds with zone",
			input:       "2023-02-17T13:15:45.123+07:00",
			want:        "20230217T061545Z",
			fn:          fhirTimestampToFilepathTimestamp,
		},
		{
			description: "round trip of arbitrary time",
			input:       now.Format(filepathTimestampFormat),
			want:        now.Format(filepathTimestampFormat),
			fn: func(input string) (string, error) {
				fhir, err := filepathTimestampToFHIRTimestamp(input)
				if err != nil {
					return "", err
				}
				return fhirTimestampToFilepathTimestamp(fhir)
			},
		},
	} {
		t.Run(tc.description, func(t *testing.T) {
			got, err := tc.fn(tc.input)
			if err != nil {
				t.Error(err)
			} else if got != tc.want {
				t.Errorf("fn(%q) = %q; want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestTestServer_ValidExport(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name             string
		dataDir          string
		expectedFileData []byte
	}{
		{
			name:             "With custom data",
			dataDir:          t.TempDir(),
			expectedFileData: []byte(`{"resourceType":"Patient","id":"PatientIDTest"}`),
		},
		{
			name:             "With default data",
			dataDir:          "",
			expectedFileData: defaultFileData,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			file1Name := "Patient_0.ndjson"
			file1Data := []byte(`{"resourceType":"Patient","id":"PatientIDTest"}`)

			clientID := "clientID"
			clientSecret := "clientSecret"
			groupName := "all"
			dirName := "20180917T175311Z"

			if tc.dataDir != "" {
				if err := os.MkdirAll(filepath.Join(tc.dataDir, groupName, dirName), 0755); err != nil {
					t.Fatalf("Unable to create test directory: %v", err)
				}
				if err := os.WriteFile(filepath.Join(tc.dataDir, groupName, dirName, file1Name), file1Data, 0644); err != nil {
					t.Fatalf("Unable to write test setup data: %v", err)
				}
			}

			baseURL := runTestServer(t, tc.dataDir, clientID, clientSecret)

			auth, err := bulkfhir.NewHTTPBasicOAuthAuthenticator(clientID, clientSecret, fmt.Sprintf("%s/token", baseURL), nil)
			if err != nil {
				t.Fatalf("Error creating HTTPBasicOAuth authenticator: %v", err)
			}
			c, err := bulkfhir.NewClient(baseURL, auth)
			if err != nil {
				t.Fatalf("Error creating bulkfhir client: %v", err)
			}

			// Start export:
			jobURL, err := c.StartBulkDataExport([]cpb.ResourceTypeCode_Value{
				cpb.ResourceTypeCode_PATIENT}, time.Time{}, groupName)
			if err != nil {
				t.Fatalf("Error starting bulk fhir export: %v", err)
			}

			// Check job status:
			var result *bulkfhir.MonitorResult
			for result = range c.MonitorJobStatus(jobURL, time.Second, 5*time.Second) {
				if result.Error != nil {
					t.Fatalf("Error in checking job status: %v", result.Error)
				}
			}

			if len(result.Status.ResultURLs) != 1 {
				t.Fatalf("unexpected number of result resources. got: %v, want: %v", len(result.Status.ResultURLs), 1)
			}
			if len(result.Status.ResultURLs[cpb.ResourceTypeCode_PATIENT]) != 1 {
				t.Fatalf("unexpected number of Patient URLs. got: %v, want: %v", len(result.Status.ResultURLs[cpb.ResourceTypeCode_PATIENT]), 1)
			}

			// Download data:
			d, err := c.GetData(result.Status.ResultURLs[cpb.ResourceTypeCode_PATIENT][0])
			if err != nil {
				t.Fatalf("Error getting data: %v", err)
			}
			defer d.Close()

			gotData, err := io.ReadAll(d)
			if err != nil {
				t.Fatalf("Error downloading data: %v", err)
			}

			if !cmp.Equal(gotData, tc.expectedFileData) {
				t.Errorf("Unexpected Patient data. got: %v, want: %v", gotData, tc.expectedFileData)
			}
		})
	}
}

func TestTestServer_Hello(t *testing.T) {
	t.Parallel()
	serverURL := runTestServer(t, t.TempDir(), "", "")
	t.Logf(serverURL)
	resp, err := http.Get(serverURL + "/hello")
	if err != nil {
		t.Errorf("unexpected error when making request: %v", err)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("unexpeced error when reading response body: %v", err)
	}

	if !cmp.Equal(data, []byte("Hello there!")) {
		t.Errorf("unexpected response body. got: %v, want: %v", data, []byte("Hello there!"))
	}
}

func runTestServer(t *testing.T, dataDir, validClientID, validClientSecret string) string {
	testServer := &server{
		dataDir:           dataDir,
		jobs:              map[string]*exportJob{},
		validClientID:     validClientID,
		validClientSecret: validClientSecret,
		jobDelay:          2 * time.Second,
		retryAfter:        1,
	}
	h := testServer.buildHandler()
	server := httptest.NewServer(h)
	testServer.baseURL = server.URL
	t.Logf("base url: %v", testServer.baseURL)
	t.Cleanup(server.Close)
	return testServer.baseURL
}
