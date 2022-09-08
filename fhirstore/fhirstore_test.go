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

package fhirstore_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/medical_claims_tools/fhirstore"
	"github.com/google/medical_claims_tools/internal/testhelpers"
)

func TestUploadResource(t *testing.T) {
	resourceType := "Patient"
	resourceID := "resourceID"
	inputJSON := []byte(fmt.Sprintf("{\"id\": \"%s\", \"resourceType\": \"%s\"}", resourceID, resourceType))
	projectID := "projectID"
	location := "us-east1"
	datasetID := "datasetID"
	fhirStoreID := "fhirstoreID"

	t.Run("ValidResponse", func(t *testing.T) {
		uploadResourceWithParams := fmt.Sprintf("UploadResource(%s, %s, %s, %s, %s)", inputJSON, projectID, location, datasetID, fhirStoreID)
		serverURL := testhelpers.FHIRStoreServer(
			t,
			[]testhelpers.FHIRStoreTestResource{{ResourceID: resourceID, ResourceType: resourceType, Data: inputJSON}},
			projectID,
			location,
			datasetID,
			fhirStoreID)

		c, err := fhirstore.NewClient(context.Background(), serverURL)
		if err != nil {
			t.Errorf(uploadResourceWithParams+" encountered an unexpected error when creating the FHIR store client: %v", err)
		}
		if err := c.UploadResource(inputJSON, projectID, location, datasetID, fhirStoreID); err != nil {
			t.Errorf(uploadResourceWithParams+" encountered an unexpected error: %v", err)
		}
	})

	t.Run("ErrorResponse", func(t *testing.T) {
		uploadResourceWithParams := fmt.Sprintf("UploadResource(%s, %s, %s, %s, %s)", inputJSON, projectID, location, datasetID, fhirStoreID)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(500)
		}))
		defer server.Close()

		c, err := fhirstore.NewClient(context.Background(), server.URL)
		if err != nil {
			t.Errorf(uploadResourceWithParams+" encountered an unexpected error when creating the FHIR store client: %v", err)
		}
		if err := c.UploadResource(inputJSON, projectID, location, datasetID, fhirStoreID); !errors.Is(err, fhirstore.ErrorAPIServer) {
			t.Errorf(uploadResourceWithParams+" unexpected error. got: %v, want: %v", err, fhirstore.ErrorAPIServer)
		}
	})
}

func TestUploadBatch(t *testing.T) {
	inputJSONs := [][]byte{
		[]byte("{\"id\":\"1\",\"resourceType\":\"Patient\"}"),
		[]byte("{\"id\":\"2\",\"resourceType\":\"ExplanationOfBenefit\"}"),
	}
	projectID := "projectID"
	location := "us-east1"
	datasetID := "datasetID"
	fhirStoreID := "fhirstoreID"

	uploadBatchWithParams := fmt.Sprintf("UploadBatch(%s, %s, %s, %s, %s)", inputJSONs, projectID, location, datasetID, fhirStoreID)

	t.Run("ValidResponse", func(t *testing.T) {
		// FHIRStoreServerBatch will check that the uploaded bundle matches resources
		// in inputJSONs.
		expectedFullBatchSize := 2
		serverURL := testhelpers.FHIRStoreServerBatch(t, inputJSONs, expectedFullBatchSize, projectID, location, datasetID, fhirStoreID)

		c, err := fhirstore.NewClient(context.Background(), serverURL)
		if err != nil {
			t.Errorf(uploadBatchWithParams+" encountered an unexpected error when creating the FHIR store client: %v", err)
		}
		if err := c.UploadBatch(inputJSONs, projectID, location, datasetID, fhirStoreID); err != nil {
			t.Errorf(uploadBatchWithParams+" encountered an unexpected error: %v", err)
		}
	})

	t.Run("ErrorResponse", func(t *testing.T) {
		errBody := []byte("error")
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(500)
			w.Write(errBody)
		}))
		defer server.Close()

		c, err := fhirstore.NewClient(context.Background(), server.URL)
		if err != nil {
			t.Errorf(uploadBatchWithParams+" encountered an unexpected error when creating the FHIR store client: %v", err)
		}

		uploadErr := c.UploadBatch(inputJSONs, projectID, location, datasetID, fhirStoreID)

		checkInternalServerBundleError(t, uploadErr, errBody)
	})
}

func TestUploadBundle(t *testing.T) {
	inputBundle := []byte(`{"id":"1","resourceType":"bundle","type":"transaction","entry":[{"resource": {"id":"pat","resourceType":"Patient"}}]}`)
	projectID := "projectID"
	location := "us-east1"
	datasetID := "datasetID"
	fhirStoreID := "fhirstoreID"

	uploadBundleWithParams := fmt.Sprintf("UploadBatch(%s, %s, %s, %s, %s)", inputBundle, projectID, location, datasetID, fhirStoreID)

	t.Run("ValidResponse", func(t *testing.T) {
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
			if !cmp.Equal(data, inputBundle) {
				t.Errorf("unexpected executeBundle request body, got: %v, want: %v", data, inputBundle)
			}
		}))
		defer server.Close()

		c, err := fhirstore.NewClient(context.Background(), server.URL)
		if err != nil {
			t.Errorf(uploadBundleWithParams+" encountered an unexpected error when creating the FHIR store client: %v", err)
		}
		if err := c.UploadBundle(inputBundle, projectID, location, datasetID, fhirStoreID); err != nil {
			t.Errorf(uploadBundleWithParams+" encountered an unexpected error: %v", err)
		}
	})

	t.Run("ErrorResponse", func(t *testing.T) {
		errBody := []byte("error")
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(500)
			w.Write(errBody)
		}))
		defer server.Close()

		c, err := fhirstore.NewClient(context.Background(), server.URL)
		if err != nil {
			t.Errorf(uploadBundleWithParams+" encountered an unexpected error when creating the FHIR store client: %v", err)
		}

		uploadErr := c.UploadBundle(inputBundle, projectID, location, datasetID, fhirStoreID)
		if !errors.Is(uploadErr, fhirstore.ErrorAPIServer) {
			t.Errorf(uploadBundleWithParams+" unexpected error. got: %v, want: %v", err, fhirstore.ErrorAPIServer)
		}

		checkInternalServerBundleError(t, uploadErr, errBody)
	})
}

func TestImportFromGCS(t *testing.T) {
	projectID := "projectID"
	location := "us-east1"
	datasetID := "datasetID"
	fhirStoreID := "fhirstoreID"
	expectedOPName := fmt.Sprintf("projects/%s/locations/%s/datasets/%s/operations/OPNAME", projectID, location, datasetID)
	gcsURI := "gs://bucket/dir/**.ndjson"
	expectedImportRequest := gcsImportRequest{
		ContentStructure: "RESOURCE",
		GCSSource: gcsSource{
			URI: gcsURI,
		},
	}

	t.Run("ValidResponse", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			expectedPath := fmt.Sprintf("/v1/projects/%s/locations/%s/datasets/%s/fhirStores/%s:import?alt=json&prettyPrint=false", projectID, location, datasetID, fhirStoreID)
			if req.URL.String() != expectedPath {
				t.Errorf("FHIR store test server got call to unexpected URL. got: %v, want: %v", req.URL.String(), expectedPath)
			}

			bodyData, err := io.ReadAll(req.Body)
			if err != nil {
				t.Errorf("unexpected error reading request body in fhir server: %v", err)
			}
			var importReq gcsImportRequest
			if err := json.Unmarshal(bodyData, &importReq); err != nil {
				t.Errorf("error unmarshalling request body in fhir server: %v", err)
			}
			if !cmp.Equal(importReq, expectedImportRequest) {
				t.Errorf("FHIR store test server received unexpected gcsURI. got: %v, want: %v", importReq, expectedImportRequest)
			}

			w.Write([]byte(fmt.Sprintf("{\"name\": \"%s\"}", expectedOPName)))
		}))

		c, err := fhirstore.NewClient(context.Background(), server.URL)
		if err != nil {
			t.Errorf("encountered an unexpected error when creating the FHIR store client: %v", err)
		}
		opName, err := c.ImportFromGCS(gcsURI, projectID, location, datasetID, fhirStoreID)
		if err != nil {
			t.Errorf("ImportFromGCS unexpected error: %v", err)
		}
		if opName != expectedOPName {
			t.Errorf("ImportFromGCS unexpected opname, got: %v, want: %v", opName, expectedOPName)
		}
	})

	t.Run("ErrorResponse", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(500)
		}))
		defer server.Close()

		c, err := fhirstore.NewClient(context.Background(), server.URL)
		if err != nil {
			t.Errorf("encountered an unexpected error when creating the FHIR store client: %v", err)
		}
		_, err = c.ImportFromGCS(gcsURI, projectID, location, datasetID, fhirStoreID)
		if err == nil {
			t.Errorf("expected non-nil error from ImportFromGCS")
		}
	})

}

func TestCheckGCSImportStatus(t *testing.T) {
	expectedOPName := "projects/project/locations/location/datasets/dataset/operations/OPNAME"

	cases := []struct {
		name     string
		isDone   bool
		hasError bool
	}{
		{
			name:   "ValidIsDone",
			isDone: true,
		},
		{
			name:   "ValidNotDone",
			isDone: false,
		},
		{
			name:     "WithError",
			hasError: true,
		},
	}

	for _, tc := range cases {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if req.URL.String() != "/v1/"+expectedOPName+"?alt=json&prettyPrint=false" {
				t.Errorf("unexpected request. got: %v, want: %v", req.URL.String(), expectedOPName)
			}

			if tc.hasError {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			if tc.isDone {
				w.Write([]byte(`{"done": true}`))
				return
			}
			w.Write([]byte(`{"done": false}`))
		}))

		c, err := fhirstore.NewClient(context.Background(), server.URL)
		if err != nil {
			t.Errorf("encountered an unexpected error when creating the FHIR store client: %v", err)
		}

		isDone, err := c.CheckGCSImportStatus(expectedOPName)
		if !tc.hasError && err != nil {
			t.Errorf("CheckGCSImportStatus: unexpected err: %v", err)
		}
		if tc.hasError && err == nil {
			t.Errorf("CheckGCSImportStatus: expected error but got nil")
		}

		if isDone != tc.isDone {
			t.Errorf("CheckGCSImportStatus: unexpected isDone value. got: %v, want: %v", isDone, tc.isDone)
		}
	}

}

// checkInternalServerBundleError checks that the provided errorToCheck is a
// *fhirstore.BundleError and that the BundleError matches is a 500 Internal
// Server error with the provided body. It also checks that the errors.Is
// resolves the error as a fhirstore.ErrorAPIServer.
func checkInternalServerBundleError(t *testing.T, errorToCheck error, errBody []byte) {
	if !errors.Is(errorToCheck, fhirstore.ErrorAPIServer) {
		t.Errorf("unexpected error. got: %v, want: %v", errorToCheck, fhirstore.ErrorAPIServer)
	}

	expectedBundleError := &fhirstore.BundleError{
		ResponseStatusCode: 500,
		ResponseBytes:      errBody,
		ResponseStatusText: "500 Internal Server Error",
	}

	bundleError, ok := errorToCheck.(*fhirstore.BundleError)
	if !ok {
		t.Errorf("expected error to be returned to be a *BundleError")
	}

	if diff := cmp.Diff(expectedBundleError, bundleError); diff != "" {
		t.Errorf("unexpected bundleError with diff: %v", diff)
	}
}

type fhirBundle struct {
	ResourceType string  `json:"resourceType"`
	Type         string  `json:"type"`
	Entry        []entry `json:"entry"`
}

type entry struct {
	Resource json.RawMessage `json:"resource"`
}

type gcsSource struct {
	URI string `json:"uri"`
}

type gcsImportRequest struct {
	ContentStructure string    `json:"contentStructure"`
	GCSSource        gcsSource `json:"gcsSource"`
}
