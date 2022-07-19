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
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

// FHIRStoreTestResource represents a test FHIR resource to be uploaded to
// FHIR store.
type FHIRStoreTestResource struct {
	ResourceID   string
	ResourceType string
	Data         []byte
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
		expectedResource, expectedResourceIdx := validateURLAndMatchResource(req.URL.String(), expectedResources, projectID, location, datasetID, fhirStoreID)
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
func FHIRStoreServerBatch(t *testing.T, expectedFHIRResources [][]byte, projectID, location, datasetID, fhirStoreID string) string {
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
		for i, gotEntry := range gotBundle.Entry {
			if !cmp.Equal([]byte(gotEntry.Resource), expectedFHIRResources[i]) {
				t.Errorf("unexpected entry in uploaded bundle. got: %s, want: %s", gotEntry.Resource, expectedFHIRResources[i])
			}
		}
	}))

	t.Cleanup(server.Close)
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

func validateURLAndMatchResource(callURL string, expectedResources []FHIRStoreTestResource, projectID, location, datasetID, fhirStoreID string) (*FHIRStoreTestResource, int) {
	for idx, r := range expectedResources {
		expectedPath := fmt.Sprintf("/v1/projects/%s/locations/%s/datasets/%s/fhirStores/%s/fhir/%s/%s?", projectID, location, datasetID, fhirStoreID, r.ResourceType, r.ResourceID)
		if callURL == expectedPath {
			return &r, idx
		}
	}
	return nil, 0
}

type fhirBundle struct {
	ResourceType string  `json:"resourceType"`
	Type         string  `json:"type"`
	Entry        []entry `json:"entry"`
}

type entry struct {
	Resource json.RawMessage `json:"resource"`
}
