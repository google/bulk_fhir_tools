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
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

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
