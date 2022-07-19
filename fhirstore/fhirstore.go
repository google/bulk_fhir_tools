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

// Package fhirstore contains utilities for interacting with GCP's FHIR store.
package fhirstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	healthcare "google.golang.org/api/healthcare/v1"
	"google.golang.org/api/option"
)

// DefaultHealthcareEndpoint represents the default cloud healthcare API
// endpoint. This should be passed to UploadResource, unless in a test
// environment.
const DefaultHealthcareEndpoint = "https://healthcare.googleapis.com/"

// ErrorAPIServer indicates that an error was received from the Healthcare API
// server.
var ErrorAPIServer = errors.New("error was received from the Healthcare API server")

// Client represents a FHIR store client that can be used to interact with GCP's
// FHIR store. Do not use this directly, call NewFHIRStoreClient to create a
// new one.
type Client struct {
	service *healthcare.Service
}

// NewClient initializes and returns a new FHIR store client.
func NewClient(ctx context.Context, healthcareEndpoint string) (*Client, error) {
	var service *healthcare.Service
	var err error
	if healthcareEndpoint == DefaultHealthcareEndpoint {
		service, err = healthcare.NewService(ctx, option.WithEndpoint(healthcareEndpoint))
	} else {
		// When not using the default GCP Healthcare endpoint, we provide an empty
		// http.Client. This case is generally used in the test, so that the
		// healthcare.Service doesn't complain about not being able to find
		// credentials in the test environment.
		// TODO(b/211028663): we should try to find a better way to handle this
		// case, perhaps we can set fake default creds in the test setup.
		service, err = healthcare.NewService(ctx, option.WithHTTPClient(&http.Client{}), option.WithEndpoint(healthcareEndpoint))
	}
	if err != nil {
		return nil, err
	}

	return &Client{service: service}, nil
}

// UploadResource uploads the provided FHIR Resource to the GCP FHIR Store
// specified by projectID, location, datasetID, and fhirStoreID.
func (f *Client) UploadResource(fhirJSON []byte, projectID, location, datasetID, fhirStoreID string) error {
	fhirService := f.service.Projects.Locations.Datasets.FhirStores.Fhir

	resourceType, resourceID, err := getResourceTypeAndID(fhirJSON)
	if err != nil {
		return err
	}
	name := fmt.Sprintf("projects/%s/locations/%s/datasets/%s/fhirStores/%s/fhir/%s/%s", projectID, location, datasetID, fhirStoreID, resourceType, resourceID)

	call := fhirService.Update(name, bytes.NewReader(fhirJSON))
	call.Header().Set("Content-Type", "application/fhir+json;charset=utf-8")

	resp, err := call.Do()
	if err != nil {
		return fmt.Errorf("error executing Healthcare API call: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode > 299 {
		respBytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("could not read response: %v", err)
		}
		return fmt.Errorf("error from API server: status %d %s: %s %w", resp.StatusCode, resp.Status, respBytes, ErrorAPIServer)
	}
	return nil
}

// UploadBatch uploads the provided group of FHIR resources to the GCP FHIR
// store specified, and does so in "batch" mode assuming each FHIR resource is
// independent. The error returned may be an instance of BundleError,
// which provides additional structured information on the error.
func (f *Client) UploadBatch(fhirJSONs [][]byte, projectID, location, datasetID, fhirStoreID string) error {
	bundle := makeFHIRBundle(fhirJSONs, false)
	bundleJSON, err := json.Marshal(bundle)
	if err != nil {
		return err
	}
	return f.UploadBundle(bundleJSON, projectID, location, datasetID, fhirStoreID)
}

// UploadBundle uploads the provided json serialized FHIR Bundle to the GCP
// FHIR store specified. The error returned may be an instance of BundleError,
// which provides additional structured information on the error.
func (f *Client) UploadBundle(fhirBundleJSON []byte, projectID, location, datasetID, fhirStoreID string) error {
	fhirService := f.service.Projects.Locations.Datasets.FhirStores.Fhir
	parent := fmt.Sprintf("projects/%s/locations/%s/datasets/%s/fhirStores/%s", projectID, location, datasetID, fhirStoreID)

	call := fhirService.ExecuteBundle(parent, bytes.NewReader(fhirBundleJSON))
	call.Header().Set("Content-Type", "application/fhir+json;charset=utf-8")
	resp, err := call.Do()
	if err != nil {
		return fmt.Errorf("error executing Healthcare API call (ExecuteBundle): %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode > 299 {
		respBytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("could not read response: %v", err)
		}
		return &BundleError{ResponseStatusCode: resp.StatusCode, ResponseStatusText: resp.Status, ResponseBytes: respBytes}
	}

	return nil
}

// BundleError represents an error returned from GCP FHIR Store when attempting
// to upload a FHIR bundle. BundleError holds some structured error information
// that may be of interest to the error consumer, including the error response
// bytes (that may indicate more details on what particular resources in the
// bundle had errors).
// TODO(b/225916126): try to figure out if we can detect the format of error in
// ResponseBytes and unpack that in a structured way for consumers.
type BundleError struct {
	ResponseStatusCode int
	ResponseStatusText string
	ResponseBytes      []byte
}

// Error returns a string version of error information.
func (b *BundleError) Error() string {
	return fmt.Sprintf("error from API server: status %d %s: %s", b.ResponseStatusCode, b.ResponseStatusText, b.ResponseBytes)
}

// Is returns true if this error should be considered equivalent to the target
// error (and makes this work smoothly with errors.Is calls)
func (b *BundleError) Is(target error) bool {
	return target == ErrorAPIServer
}

type fhirBundle struct {
	ResourceType string  `json:"resourceType"`
	Type         string  `json:"type"`
	Entry        []entry `json:"entry"`
}

type entry struct {
	Resource json.RawMessage `json:"resource"`
}

func makeFHIRBundle(fhirJSONs [][]byte, isTransaction bool) *fhirBundle {
	bundleType := "batch"
	if isTransaction {
		bundleType = "transaction"
	}

	bundle := fhirBundle{
		ResourceType: "Bundle",
		Type:         bundleType,
	}

	bundle.Entry = make([]entry, len(fhirJSONs))
	for i, fhirJSON := range fhirJSONs {
		bundle.Entry[i].Resource = fhirJSON
	}

	return &bundle
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
