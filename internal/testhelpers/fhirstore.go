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

// Package testhelpers provides common testing helpers and utilities that are used
// across packages in this project. Only non-trivial helper logic used in
// multiple places in this library should be added here.
package testhelpers

import (
	"bufio"
	"encoding/json"
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

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	cpb "github.com/google/fhir/go/proto/google/fhir/proto/r4/core/codes_go_proto"
)

// FHIRStoreTestResource represents a test FHIR resource to be uploaded to
// FHIR store.
type FHIRStoreTestResource struct {
	ResourceID       string
	ResourceTypeCode cpb.ResourceTypeCode_Value
	Data             []byte
}

// FHIRStoreServer creates a test FHIR store server that expects the provided
// expectedResources uploaded as single resources. If it receives valid upload
// requests that do not include elements from expectedResources, it will call
// t.Errorf with an error. If not all of the resources in expectedResources are
// uploaded by the end of the test errors are thrown. The test server's URL is
// returned by this function, and is auto-closed at the end of the test.
func FHIRStoreServer(t *testing.T, expectedResources []FHIRStoreTestResource, projectID, location, datasetID, fhirStoreID string) string {
	t.Helper()
	var expectedResourceWasUploadedMutex sync.Mutex
	expectedResourceWasUploaded := make([]bool, len(expectedResources))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		expectedResource, expectedResourceIdx := validateURLAndMatchResource(t, req.URL.String(), expectedResources, projectID, location, datasetID, fhirStoreID)
		if expectedResource == nil {
			t.Errorf("FHIR Store Test server received an unexpected request at url: %s", req.URL.String())
			w.WriteHeader(500)
			return
		}
		if req.Method != http.MethodPut {
			t.Errorf("FHIR Store test server unexpected HTTP method. got: %v, want: %v", req.Method, http.MethodPut)
		}

		bodyContent, err := ioutil.ReadAll(req.Body)
		if err != nil {
			t.Errorf("FHIR Store test server error reading body content for URL: %s", req.URL.String())
		}
		if !cmp.Equal(NormalizeJSON(t, bodyContent), NormalizeJSON(t, expectedResource.Data)) {
			t.Errorf("FHIR store test server received unexpected body content. got: %s, want: %s", bodyContent, expectedResource.Data)
		}

		// Update the corresponding index in expectedResourceWasUploaded slice.
		expectedResourceWasUploadedMutex.Lock()
		expectedResourceWasUploaded[expectedResourceIdx] = true
		expectedResourceWasUploadedMutex.Unlock()

		w.WriteHeader(200) // Send OK status code.
	}))

	t.Cleanup(func() {
		server.Close()
		for idx, val := range expectedResourceWasUploaded {
			if !val {
				t.Errorf("FHIR store test server error. Expected resource was not uploaded. got: nil, want: %v", expectedResources[idx])
			}
		}
	})
	return server.URL
}

// FHIRStoreServerBatch sets up a test FHIR Store Server for batch executeBundle
// requests. It ensures proper executeBundle requests are sent, and that the
// bundles are in batch mode and contain the expectedFHIRResources.
// It is okay to upload the full set of expectedFHIRResources over multiple
// executeBundle batch calls to this server. At the end of the test, the
// teardown of this server checks and ensures that all expectedFHIRResources
// were uploaded at least once.
func FHIRStoreServerBatch(t *testing.T, expectedFHIRResources [][]byte, expectedFullBatchSize int, projectID, location, datasetID, fhirStoreID string) string {
	t.Helper()
	var expectedResourceWasUploadedMutex sync.Mutex
	expectedResourceWasUploaded := make([]bool, len(expectedFHIRResources))

	var oneBatchWithExpectedSizeMutex sync.Mutex
	oneBatchWithExpectedSize := false

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		bundlePath := fmt.Sprintf("/v1/projects/%s/locations/%s/datasets/%s/fhirStores/%s/fhir?", projectID, location, datasetID, fhirStoreID)
		if req.URL.String() != bundlePath {
			t.Errorf("FHIR store test server got call to unexpected URL. got: %v, want: %v", req.URL.String(), bundlePath)
		}
		if req.Method != http.MethodPost {
			t.Errorf("FHIR Store test server unexpected HTTP method. got: %v, want: %v", req.Method, http.MethodPost)
		}

		data, err := io.ReadAll(req.Body)
		if err != nil {
			t.Fatalf("unable to read executeBundle request body")
		}
		var gotBundle fhirBundle
		err = json.Unmarshal(data, &gotBundle)
		if err != nil {
			t.Fatalf("unable to unmarshal executeBundle request body")
		}
		if gotBundle.Type != "batch" {
			t.Errorf("unexpected bundle type, got: %v, want: batch", gotBundle.Type)
		}
		if len(gotBundle.Entry) == expectedFullBatchSize {
			oneBatchWithExpectedSizeMutex.Lock()
			oneBatchWithExpectedSize = true
			oneBatchWithExpectedSizeMutex.Unlock()
		}

		var response bundleResponses
		for _, gotEntry := range gotBundle.Entry {
			gotResource := []byte(gotEntry.Resource)
			expectedResourceIdx, ok := getIndexOf(t, gotResource, expectedFHIRResources)
			if !ok {
				t.Errorf("server received unexpected FHIR resource: %s", gotResource)
			}

			// Check the entry.request is correctly formed.
			resourceType, resourceID, err := getResourceTypeAndID(gotEntry.Resource)
			if err != nil {
				t.Errorf("unable to get resourceType and resourceID for entry in bundle: %v", err)
			}

			if gotEntry.Request.Method != "PUT" {
				t.Errorf("unexpected entry.request.method. got: %v, want: PUT", gotEntry.Request.Method)
			}

			wantURL := fmt.Sprintf("%s/%s", resourceType, resourceID)
			if gotEntry.Request.URL != wantURL {
				t.Errorf("unexpected entry.request.url. got: %v, want: %v", gotEntry.Request.URL, wantURL)
			}

			// Update the corresponding index in expectedResourceWasUploaded slice.
			expectedResourceWasUploadedMutex.Lock()
			expectedResourceWasUploaded[expectedResourceIdx] = true
			expectedResourceWasUploadedMutex.Unlock()

			r := bundleResponse{}
			r.Response.Status = "201 Created"
			response.Entry = append(response.Entry, r)
		}

		body, err := json.Marshal(response)
		if err != nil {
			t.Errorf("error marshalling the bundle response: %v", err)
		}

		w.WriteHeader(200)
		w.Write(body)
	}))

	t.Cleanup(func() {
		server.Close()
		for idx, val := range expectedResourceWasUploaded {
			if !val {
				t.Errorf("FHIR store test server error. Expected resource was not uploaded. got: nil, want: %v", expectedFHIRResources[idx])
			}
		}
		if !oneBatchWithExpectedSize {
			t.Errorf("expected at least one batch with expected size: %v", expectedFullBatchSize)
		}
	})
	return server.URL
}

// CheckErrorNDJSONFile is a helper to test that the contents of the error
// ndjson file written by fhirstore.Uploader are correct.
func CheckErrorNDJSONFile(t *testing.T, dir string, wantErrors []ErrorNDJSONLine) {
	t.Helper()
	f, err := os.Open(path.Join(dir, "resourcesWithErrors.ndjson"))
	if err != nil {
		t.Errorf("unable to open resourcesWithErrors error file: %v", err)
	}
	defer f.Close()

	var gotErrors []ErrorNDJSONLine
	s := bufio.NewScanner(f)
	for s.Scan() {
		var e ErrorNDJSONLine
		if err := json.Unmarshal(s.Bytes(), &e); err != nil {
			t.Errorf("error unmarshaling data in error file: %v", err)
		}
		gotErrors = append(gotErrors, e)
	}

	normalizedGotErrors := normalizeErrorNDJSONLines(t, gotErrors)
	normalizedWantErrors := normalizeErrorNDJSONLines(t, wantErrors)

	sortFunc := func(a, b ErrorNDJSONLine) bool { return a.FHIRResource < b.FHIRResource }
	if diff := cmp.Diff(normalizedGotErrors, normalizedWantErrors, cmpopts.SortSlices(sortFunc)); diff != "" {
		t.Errorf("unexpected resourcesWithErrors data. diff: %v", diff)
	}
}

func normalizeErrorNDJSONLines(t *testing.T, in []ErrorNDJSONLine) []ErrorNDJSONLine {
	normalized := make([]ErrorNDJSONLine, len(in))
	for i, e := range in {
		normalized[i] = ErrorNDJSONLine{Err: e.Err, FHIRResource: NormalizeJSONString(t, e.FHIRResource)}
	}
	return normalized
}

// ErrorNDJSONLine represents one line of an error NDJSON file produced by
// fhirstore.Uploader.
type ErrorNDJSONLine struct {
	Err          string `json:"err"`
	FHIRResource string `json:"fhir_resource"`
}

func validateURLAndMatchResource(t *testing.T, callURL string, expectedResources []FHIRStoreTestResource, projectID, location, datasetID, fhirStoreID string) (*FHIRStoreTestResource, int) {
	for idx, r := range expectedResources {
		// bulkfhir.ResourceTypeCodeToName would cause a dependency cycle, so we
		// convert from CONST_CASE to PascalCase manually, which should be correct
		// in most cases - good enough for tests.
		resourceTypeParts := []string{}
		for _, part := range strings.Split(strings.ToLower(r.ResourceTypeCode.String()), "_") {
			resourceTypeParts = append(resourceTypeParts, strings.Title(part))
		}
		resourceType := strings.Join(resourceTypeParts, "")
		expectedPath := fmt.Sprintf("/v1/projects/%s/locations/%s/datasets/%s/fhirStores/%s/fhir/%s/%s?", projectID, location, datasetID, fhirStoreID, resourceType, r.ResourceID)
		if callURL == expectedPath {
			return &r, idx
		}
	}
	return nil, 0
}

func getIndexOf(t *testing.T, fhirResource []byte, fhirResources [][]byte) (int, bool) {
	for idx, r := range fhirResources {
		if cmp.Equal(NormalizeJSON(t, fhirResource), NormalizeJSON(t, r)) {
			return idx, true
		}
	}
	return 0, false
}

type bundleResponse struct {
	Response struct {
		Status  string          `json:"status"`
		Outcome json.RawMessage `json:"outcome"`
	} `json:"response"`
}

type bundleResponses struct {
	Entry []bundleResponse `json:"entry"`
}

type fhirBundle struct {
	ResourceType string  `json:"resourceType"`
	Type         string  `json:"type"`
	Entry        []entry `json:"entry"`
}

type request struct {
	Method string `json:"method"`
	URL    string `json:"url"`
}

type entry struct {
	Resource json.RawMessage `json:"resource"`
	Request  request         `json:"request"`
}

type resourceData struct {
	ResourceID   string `json:"id"`
	ResourceType string `json:"resourceType"`
}

func getResourceTypeAndID(fhirJSON []byte) (resourceType, resourceID string, err error) {
	var data resourceData
	err = json.Unmarshal(fhirJSON, &data)
	if err != nil {
		return "", "", err
	}
	return data.ResourceType, data.ResourceID, err
}
