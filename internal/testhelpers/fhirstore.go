// Package testhelpers provides common testing helpers and utilities that are used
// across packages in this project. Only non-trivial helper logic used in
// multiple places in this library should be added here.
package testhelpers

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// FHIRStoreTestResource represents a test FHIR resource to be uploaded to
// FHIR store.
type FHIRStoreTestResource struct {
	ResourceID   string
	ResourceType string
	Data         []byte
}

// FHIRStoreServer creates a test FHIR store server that expects the provided
// expectedResources. If it receives valid upload requests that do not include
// elements from expectedResources, it will call t.Errorf with an error. There
// is also a simple check to see that the number of valid upload calls with
// expectedResources is the same size as the expectedResources slice. Note that
// at the moment, this does not mean that Upload was called for every expected
// resource, because only the count is checked at the moment. The test server's
// URL is returned.
// TODO(b/227361268): go beyond checking valid upload counts, and check that
// each resource in expectedResources was actually uploaded.
func FHIRStoreServer(t *testing.T, expectedResources []FHIRStoreTestResource, projectID, location, datasetID, fhirStoreID string) string {
	t.Helper()
	var validResourcesUploaded mutexCounter
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		expectedResource := validateURLAndMatchResource(req.URL.String(), expectedResources, projectID, location, datasetID, fhirStoreID)
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
		validResourcesUploaded.Increment()
		w.WriteHeader(200) // Send OK status code.
	}))

	t.Cleanup(func() {
		server.Close()
		if got := validResourcesUploaded.Value(); got != len(expectedResources) {
			t.Errorf("FHIR Store Test server did not receive expected number of valid uploads. got: %v, want: %v", got, len(expectedResources))
		}
	})
	return server.URL
}

func validateURLAndMatchResource(callURL string, expectedResources []FHIRStoreTestResource, projectID, location, datasetID, fhirStoreID string) *FHIRStoreTestResource {
	for _, r := range expectedResources {
		expectedPath := fmt.Sprintf("/v1/projects/%s/locations/%s/datasets/%s/fhirStores/%s/fhir/%s/%s?", projectID, location, datasetID, fhirStoreID, r.ResourceType, r.ResourceID)
		if callURL == expectedPath {
			return &r
		}
	}
	return nil
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
