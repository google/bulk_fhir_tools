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
	"runtime"
	"testing"
	"time"

	cpb "github.com/google/fhir/go/proto/google/fhir/proto/r4/core/codes_go_proto"
	"github.com/google/go-cmp/cmp"
	"github.com/google/medical_claims_tools/bulkfhir"
	"github.com/google/medical_claims_tools/fhir"
)

// This includes some basic __sanity__ tests of the test_server.
// We will work on configuring some integration testing of bulk_fhir_fetch
// against the test server, which may be more comprehensive.

func TestTestServer_ValidExport(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		// Skip this test on windows, since the file path conventions of the
		// test server don't yet work on windows.
		// TODO(b/268241366): add windows support to the test bulk fhir server.
		return
	}

	cases := []struct {
		name             string
		dataDir          string
		expectedFileData []byte
	}{
		{
			name:             "WithDataDir",
			dataDir:          t.TempDir(),
			expectedFileData: []byte(`{"resourceType":"Patient","id":"PatientIDTest"}`),
		},
		{
			name:             "WithoutDataDir",
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
			transactionTime := "2018-09-17T17:53:11.476Z"

			if tc.dataDir != "" {
				if err := os.MkdirAll(filepath.Join(tc.dataDir, groupName, transactionTime), 0755); err != nil {
					t.Fatalf("Unable to create test directory: %v", err)
				}
				if err := os.WriteFile(filepath.Join(tc.dataDir, groupName, transactionTime, file1Name), file1Data, 0644); err != nil {
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

			nativeTransactionTime, err := fhir.ParseFHIRInstant(transactionTime)
			if err != nil {
				t.Fatalf("Error parsing transaction time: %v", err)
			}

			// Start export:
			jobURL, err := c.StartBulkDataExport([]cpb.ResourceTypeCode_Value{
				cpb.ResourceTypeCode_PATIENT}, nativeTransactionTime, groupName)
			if err != nil {
				t.Errorf("Error starting bulk fhir export: %v", err)
			}

			// Check job status:
			var result *bulkfhir.MonitorResult
			for result = range c.MonitorJobStatus(jobURL, time.Second, time.Minute) {
				if result.Error != nil {
					t.Errorf("Error in checking job status: %v", result.Error)
				}
			}

			if len(result.Status.ResultURLs) != 1 {
				t.Errorf("unexpected number of result resources. got: %v, want: %v", len(result.Status.ResultURLs), 1)
			}
			if len(result.Status.ResultURLs[cpb.ResourceTypeCode_PATIENT]) != 1 {
				t.Errorf("unexpected number of Patient URLs. got: %v, want: %v", len(result.Status.ResultURLs[cpb.ResourceTypeCode_PATIENT]), 1)
			}

			// Download data:
			d, err := c.GetData(result.Status.ResultURLs[cpb.ResourceTypeCode_PATIENT][0])
			if err != nil {
				t.Errorf("Error getting data: %v", err)
			}

			gotData, err := io.ReadAll(d)
			if err != nil {
				t.Errorf("Error downloading data: %v", err)
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
	}
	h := testServer.buildHandler()
	server := httptest.NewServer(h)
	testServer.baseURL = server.URL
	t.Logf("base url: %v", testServer.baseURL)
	t.Cleanup(server.Close)
	return testServer.baseURL
}
