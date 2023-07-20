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
	"strconv"

	healthcare "google.golang.org/api/healthcare/v1"
	"google.golang.org/api/option"
	log "github.com/google/medical_claims_tools/internal/logger"
	"github.com/google/medical_claims_tools/internal/metrics/aggregation"
	"github.com/google/medical_claims_tools/internal/metrics"
)

var fhirStoreUploadCounter *metrics.Counter = metrics.NewCounter("fhir-store-upload-counter", "Count of uploads to FHIR Store by FHIR Resource Type and HTTP Status.", "1", aggregation.Count, "FHIRResourceType", "HTTPStatus")
var fhirStoreBatchUploadCounter *metrics.Counter = metrics.NewCounter("fhir-store-batch-upload-counter", "Count of FHIR Bundles uploaded to FHIR Store by HTTP Status. Even if the bundle succeeds FHIR resources in the bundle may fail. See fhir-store-batch-upload-resource-counter for status of individual FHIR resources.", "1", aggregation.Count, "HTTPStatus")
var fhirStoreBatchUploadResourceCounter *metrics.Counter = metrics.NewCounter("fhir-store-batch-upload-resource-counter", "Unpacks the FHIR Bundles Response and counts the individiual FHIR Resources uploaded to FHIR Store by HTTP Status.", "1", aggregation.Count, "HTTPStatus")

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
	cfg     *Config
}

// Config represents a FHIR Store configuration. It is passed to NewClient, but can also be used
// elsewhere to hold and represent FHIR Store configuration concepts.
type Config struct {
	// CloudHealthcareEndpoint is the base cloud healthcare API endpoint to be used for accessing this
	// FHIR store. For example, "https://healthcare.googleapis.com/".
	CloudHealthcareEndpoint string
	// ProjectID is the GCP project the FHIR Store belongs to.
	ProjectID string
	// Location is the GCP location the FHIR Store was created in.
	Location string
	// DatasetID is the GCP dataset this FHIR store is part of.
	DatasetID string
	// FHIRStoreID is the FHIR store identifier.
	FHIRStoreID string
}

// NewClient initializes and returns a new FHIR store client.
func NewClient(ctx context.Context, cfg *Config) (*Client, error) {
	var service *healthcare.Service
	var err error
	if cfg.CloudHealthcareEndpoint == DefaultHealthcareEndpoint {
		service, err = healthcare.NewService(ctx, option.WithEndpoint(cfg.CloudHealthcareEndpoint))
	} else {
		// When not using the default GCP Healthcare endpoint, we provide an empty
		// http.Client. This case is generally used in the test, so that the
		// healthcare.Service doesn't complain about not being able to find
		// credentials in the test environment.
		// TODO(b/211028663): we should try to find a better way to handle this
		// case, perhaps we can set fake default creds in the test setup.
		service, err = healthcare.NewService(ctx, option.WithHTTPClient(&http.Client{}), option.WithEndpoint(cfg.CloudHealthcareEndpoint))
	}
	if err != nil {
		return nil, err
	}

	return &Client{service: service, cfg: cfg}, nil
}

// UploadResource uploads the provided FHIR Resource to the GCP FHIR Store
// specified by projectID, location, datasetID, and fhirStoreID.
func (c *Client) UploadResource(fhirJSON []byte) error {
	fhirService := c.service.Projects.Locations.Datasets.FhirStores.Fhir

	resourceType, resourceID, err := getResourceTypeAndID(fhirJSON)
	if err != nil {
		return err
	}
	name := fmt.Sprintf("projects/%s/locations/%s/datasets/%s/fhirStores/%s/fhir/%s/%s", c.cfg.ProjectID, c.cfg.Location, c.cfg.DatasetID, c.cfg.FHIRStoreID, resourceType, resourceID)

	call := fhirService.Update(name, bytes.NewReader(fhirJSON))
	call.Header().Set("Content-Type", "application/fhir+json;charset=utf-8")

	resp, err := call.Do()
	if err != nil {
		return fmt.Errorf("error executing Healthcare API call: %v", err)
	}
	defer resp.Body.Close()

	if err := fhirStoreUploadCounter.Record(context.Background(), 1, resourceType, http.StatusText(resp.StatusCode)); err != nil {
		return err
	}

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
func (c *Client) UploadBatch(fhirJSONs [][]byte) error {
	bundle, err := makeFHIRBundle(fhirJSONs, false)
	if err != nil {
		return err
	}
	bundleJSON, err := json.Marshal(bundle)
	if err != nil {
		return err
	}
	return c.UploadBundle(bundleJSON)
}

// UploadBundle uploads the provided json serialized FHIR Bundle to the GCP
// FHIR store specified. The error returned may be an instance of BundleError,
// which provides additional structured information on the error.
func (c *Client) UploadBundle(fhirBundleJSON []byte) error {
	fhirService := c.service.Projects.Locations.Datasets.FhirStores.Fhir
	parent := fmt.Sprintf("projects/%s/locations/%s/datasets/%s/fhirStores/%s", c.cfg.ProjectID, c.cfg.Location, c.cfg.DatasetID, c.cfg.FHIRStoreID)

	call := fhirService.ExecuteBundle(parent, bytes.NewReader(fhirBundleJSON))
	call.Header().Set("Content-Type", "application/fhir+json;charset=utf-8")
	resp, err := call.Do()
	if err != nil {
		return fmt.Errorf("error executing Healthcare API call (ExecuteBundle): %v", err)
	}
	defer resp.Body.Close()

	if err := fhirStoreBatchUploadCounter.Record(context.Background(), 1, http.StatusText(resp.StatusCode)); err != nil {
		return err
	}

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("could not read response: %v", err)
	}

	var resps BundleResponses
	err = json.Unmarshal(respBytes, &resps)
	if err != nil {
		return fmt.Errorf("could not unmarshal response: %v", err)
	}

	errInsideBundle := false
	for _, r := range resps.Entry {
		if err := fhirStoreBatchUploadResourceCounter.Record(context.Background(), 1, r.Response.Status); err != nil {
			return err
		}

		// According to the FHIR spec Response.status shall start with a 3 digit HTTP code
		// (https://build.fhir.org/bundle-definitions.html#Bundle.entry.response.status)
		scode, err := strconv.Atoi(r.Response.Status[:3])
		if err != nil {
			return err
		}
		if scode > 299 {
			errInsideBundle = true
			log.Errorf("error uploading fhir resource in bundle: %s", r.Response.Outcome)
		}
	}

	if resp.StatusCode > 299 || errInsideBundle {
		return &BundleError{ResponseStatusCode: resp.StatusCode, ResponseStatusText: resp.Status, ResponseBytes: respBytes}
	}

	return nil
}

// BundleResponse holds a single FHIR Bundle response from the fhirService.ExecuteBundle call.
type BundleResponse struct {
	Response struct {
		Status  string `json:"status"`
		Outcome struct {
			Issue json.RawMessage `json:"issue"`
		} `json:"outcome,omitempty"`
	} `json:"response"`
}

// BundleResponses holds the FHIR Bundle responses from the fhirService.ExecuteBundle call.
type BundleResponses struct {
	Entry []BundleResponse `json:"entry"`
}

// BundleError represents an error returned from GCP FHIR Store when attempting
// to upload a FHIR bundle. The Bundle may succeed even if FHIR resources inside
// the bundle failed to upload. In that case ResponseStatusCode and
// ResponseStatusText hold the status of the bundle while ResponseBytes may have
// details on the individual resources.
type BundleError struct {
	// ResponseStatusCode and ResponseStatusText hold the status for the bundle.
	// Within the bundle individual FHIR resources may have still failed to
	// upload.
	ResponseStatusCode int
	ResponseStatusText string
	ResponseBytes      []byte
}

// Error returns a string version of error information.
func (b *BundleError) Error() string {
	return fmt.Sprintf("error from API server, StatusCode: %d StatusText: %s Response: %s", b.ResponseStatusCode, b.ResponseStatusText, b.ResponseBytes)
}

// Is returns true if this error should be considered equivalent to the target
// error (and makes this work smoothly with errors.Is calls)
func (b *BundleError) Is(target error) bool {
	return target == ErrorAPIServer
}

// ImportFromGCS triggers a long-running FHIR store import job from a
// GCS location. Note wildcards can be used in the gcsURI, for example,
// gs://BUCKET/DIRECTORY/**.ndjson imports all files with .ndjson extension
// in DIRECTORY and its subdirectories.
//
// This function returns the GCP long running op name, which can be passed
// to CheckGCSImportStatus to check the status of the long running import
// operation.
func (c *Client) ImportFromGCS(gcsURI string) (string, error) {
	storesService := c.service.Projects.Locations.Datasets.FhirStores
	name := fmt.Sprintf("projects/%s/locations/%s/datasets/%s/fhirStores/%s", c.cfg.ProjectID, c.cfg.Location, c.cfg.DatasetID, c.cfg.FHIRStoreID)

	req := &healthcare.ImportResourcesRequest{
		ContentStructure: "RESOURCE",
		GcsSource: &healthcare.GoogleCloudHealthcareV1FhirGcsSource{
			Uri: gcsURI,
		},
	}

	op, err := storesService.Import(name, req).Do()
	if err != nil {
		return "", fmt.Errorf("error kicking off the GCS to FHIR store import job: %v", err)
	}

	return op.Name, nil
}

// CheckGCSImportStatus will check the long running GCS to FHIR store import
// job specified by opName, and return whether it is complete or not along with
// a possible error.
func (c *Client) CheckGCSImportStatus(opName string) (isDone bool, err error) {
	operationsService := c.service.Projects.Locations.Datasets.Operations
	op, err := operationsService.Get(opName).Do()
	if err != nil {
		return false, fmt.Errorf("error in operationsService.Get(%q): %v", opName, err)
	}
	return op.Done, nil
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

func makeFHIRBundle(fhirJSONs [][]byte, isTransaction bool) (*fhirBundle, error) {
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
		resourceType, resourceID, err := getResourceTypeAndID(fhirJSON)
		if err != nil {
			return nil, err
		}
		bundle.Entry[i].Request = request{
			URL:    fmt.Sprintf("%s/%s", resourceType, resourceID),
			Method: "PUT",
		}
	}

	return &bundle, nil
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
