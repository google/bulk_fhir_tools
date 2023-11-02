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

package processing_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/bulk_fhir_tools/bulkfhir"
	"github.com/google/bulk_fhir_tools/fhir/processing"
	"github.com/google/bulk_fhir_tools/fhirstore"
	"github.com/google/bulk_fhir_tools/internal/testhelpers"

	cpb "github.com/google/fhir/go/proto/google/fhir/proto/r4/core/codes_go_proto"
)

type gcsSource struct {
	URI string `json:"uri"`
}

type gcsImportRequest struct {
	ContentStructure string    `json:"contentStructure"`
	GCSSource        gcsSource `json:"gcsSource"`
}

func TestDirectFHIRStoreSink(t *testing.T) {
	cases := []struct {
		name                string
		setFHIRErrorFileDir bool
	}{
		{name: "WithErrorFileDir", setFHIRErrorFileDir: true},
		{name: "NoErrorFileDir", setFHIRErrorFileDir: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resources := []testhelpers.FHIRStoreTestResource{
				{
					ResourceID:       "PatientID",
					ResourceTypeCode: cpb.ResourceTypeCode_PATIENT,
					Data:             []byte(`{"resourceType":"Patient","id":"PatientID"}`),
				},
				{
					ResourceID:       "PatientID2",
					ResourceTypeCode: cpb.ResourceTypeCode_PATIENT,
					Data:             []byte(`{"resourceType":"Patient","id":"PatientID2"}`),
				},
				{
					ResourceID:       "PatientID3",
					ResourceTypeCode: cpb.ResourceTypeCode_PATIENT,
					Data:             []byte(`{"resourceType":"Patient","id":"PatientID3"}`),
				},
				{
					ResourceID:       "CoverageID",
					ResourceTypeCode: cpb.ResourceTypeCode_COVERAGE,
					Data:             []byte(`{"resourceType": "Coverage", "id": "CoverageID", "contract": [{"reference": "Contract/part-a-contract1"}]}`),
				},
				{
					ResourceID:       "CoverageID2",
					ResourceTypeCode: cpb.ResourceTypeCode_COVERAGE,
					Data:             []byte(`{"resourceType": "Coverage", "id": "CoverageID2", "contract": [{"reference": "Contract/part-a-contract1"}]}`),
				},
			}

			fhirStoreProjectID := "test"
			fhirStoreLocation := "loc"
			fhirStoreDatasetID := "dataset"
			fhirStoreID := "fhirstore"

			testServerURL := testhelpers.FHIRStoreServer(t, resources, fhirStoreProjectID, fhirStoreLocation, fhirStoreDatasetID, fhirStoreID)

			numWorkers := 2
			outputPrefix := ""
			if tc.setFHIRErrorFileDir {
				outputPrefix = t.TempDir()
			}
			ctx := context.Background()
			sink, err := processing.NewFHIRStoreSink(ctx, &processing.FHIRStoreSinkConfig{
				FHIRStoreConfig: &fhirstore.Config{
					CloudHealthcareEndpoint: testServerURL,
					ProjectID:               fhirStoreProjectID,
					Location:                fhirStoreLocation,
					DatasetID:               fhirStoreDatasetID,
					FHIRStoreID:             fhirStoreID,
				},
				MaxWorkers:          numWorkers,
				ErrorFileOutputPath: outputPrefix,
			})
			if err != nil {
				t.Fatalf("NewFHIRStoreSink unexpected error: %v", err)
			}
			p, err := processing.NewPipeline(nil, []processing.Sink{sink})
			if err != nil {
				t.Fatalf("failed to create pipeline: %v", err)
			}

			for _, r := range resources {
				if err := p.Process(ctx, r.ResourceTypeCode, r.ResourceTypeCode.String(), r.Data); err != nil {
					t.Fatalf("pipeline.Process() returned unexpected error: %v", err)
				}
			}
			if err := p.Finalize(ctx); err != nil {
				t.Fatalf("pipeline.Finalize() returned unexpected error: %v", err)
			}

			// At the end of the test, testhelpers.FHIRStoreServer will automatically
			// ensure that all resources in the resources slice were uploaded to the
			// server.
		})
	}
}

func TestDirectFHIRStoreSink_Batch(t *testing.T) {
	cases := []struct {
		name                  string
		batchSize             int
		expectedFullBatchSize int
		setFHIRErrorFileDir   bool
	}{
		{name: "WithErrorFileDir", setFHIRErrorFileDir: true, batchSize: 2, expectedFullBatchSize: 2},
		{name: "NoErrorFileDir", setFHIRErrorFileDir: false, batchSize: 2, expectedFullBatchSize: 2},
		// Test different batch sizes, without multiplexing over
		// setFHIRErrorFileDir.
		{name: "WithBatchSize1", batchSize: 1, expectedFullBatchSize: 1},
		{name: "WithBatchSize2", batchSize: 2, expectedFullBatchSize: 2},
		{name: "WithBatchSize3", batchSize: 3, expectedFullBatchSize: 3},
		{name: "WithBatchSize4", batchSize: 4, expectedFullBatchSize: 4},
		{name: "WithBatchSize5", batchSize: 5, expectedFullBatchSize: 4},
	}

	resources := []testhelpers.FHIRStoreTestResource{
		{
			ResourceID:       "1",
			ResourceTypeCode: cpb.ResourceTypeCode_PATIENT,
			Data:             []byte(`{"id":"1","resourceType":"Patient"}`),
		},
		{
			ResourceID:       "2",
			ResourceTypeCode: cpb.ResourceTypeCode_EXPLANATION_OF_BENEFIT,
			Data:             []byte(`{"id":"2","resourceType":"ExplanationOfBenefit"}`),
		},
		{
			ResourceID:       "3",
			ResourceTypeCode: cpb.ResourceTypeCode_EXPLANATION_OF_BENEFIT,
			Data:             []byte(`{"id":"3","resourceType":"ExplanationOfBenefit"}`),
		},
		{
			ResourceID:       "4",
			ResourceTypeCode: cpb.ResourceTypeCode_EXPLANATION_OF_BENEFIT,
			Data:             []byte(`{"id":"4","resourceType":"ExplanationOfBenefit"}`),
		},
	}

	inputJSONs := [][]byte{}
	for _, r := range resources {
		inputJSONs = append(inputJSONs, r.Data)
	}
	projectID := "projectID"
	location := "us-east1"
	datasetID := "datasetID"
	fhirStoreID := "fhirstoreID"

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			serverURL := testhelpers.FHIRStoreServerBatch(t, inputJSONs, tc.expectedFullBatchSize, projectID, location, datasetID, fhirStoreID)
			numWorkers := 1
			outputPrefix := ""
			if tc.setFHIRErrorFileDir {
				outputPrefix = t.TempDir()
			}
			ctx := context.Background()
			sink, err := processing.NewFHIRStoreSink(ctx, &processing.FHIRStoreSinkConfig{
				FHIRStoreConfig: &fhirstore.Config{
					CloudHealthcareEndpoint: serverURL,
					ProjectID:               projectID,
					Location:                location,
					DatasetID:               datasetID,
					FHIRStoreID:             fhirStoreID,
				},
				MaxWorkers:          numWorkers,
				ErrorFileOutputPath: outputPrefix,
				BatchUpload:         true,
				BatchSize:           tc.batchSize,
			})
			if err != nil {
				t.Fatalf("NewFHIRStoreSink unexpected error: %v", err)
			}
			p, err := processing.NewPipeline(nil, []processing.Sink{sink})
			if err != nil {
				t.Fatalf("failed to create pipeline: %v", err)
			}

			for _, r := range resources {
				if err := p.Process(ctx, r.ResourceTypeCode, r.ResourceTypeCode.String(), r.Data); err != nil {
					t.Fatalf("pipeline.Process() returned unexpected error: %v", err)
				}
			}
			if err := p.Finalize(ctx); err != nil {
				t.Fatalf("pipeline.Finalize() returned unexpected error: %v", err)
			}
			// At the end of the test, testhelpers.FHIRStoreServerBatch will
			// automatically ensure that all resources in the resources slice were
			// uploaded to the server.
		})
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

func TestDirectFHIRStoreSink_BatchDefaultBatchSize(t *testing.T) {
	// This tests that if FHIRStoreSinkConfig.BatchSize = 0, then a default batchSize
	// of 5 is used.

	resources := []testhelpers.FHIRStoreTestResource{
		{
			ResourceID:       "1",
			ResourceTypeCode: cpb.ResourceTypeCode_PATIENT,
			Data:             []byte(`{"id":"1","resourceType":"Patient"}`),
		},
		{
			ResourceID:       "2",
			ResourceTypeCode: cpb.ResourceTypeCode_EXPLANATION_OF_BENEFIT,
			Data:             []byte(`{"id":"2","resourceType":"ExplanationOfBenefit"}`),
		},
		{
			ResourceID:       "3",
			ResourceTypeCode: cpb.ResourceTypeCode_EXPLANATION_OF_BENEFIT,
			Data:             []byte(`{"id":"3","resourceType":"ExplanationOfBenefit"}`),
		},
		{
			ResourceID:       "4",
			ResourceTypeCode: cpb.ResourceTypeCode_EXPLANATION_OF_BENEFIT,
			Data:             []byte(`{"id":"4","resourceType":"ExplanationOfBenefit"}`),
		},
		{
			ResourceID:       "5",
			ResourceTypeCode: cpb.ResourceTypeCode_EXPLANATION_OF_BENEFIT,
			Data:             []byte(`{"id":"5","resourceType":"ExplanationOfBenefit"}`),
		},
	}

	projectID := "projectID"
	location := "us-east1"
	datasetID := "datasetID"
	fhirStoreID := "fhirstoreID"
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
		if len(gotBundle.Entry) != 5 {
			t.Errorf("unexpected batch size. got: %v, want: %v", len(gotBundle.Entry), 5)
		}

		w.WriteHeader(200)
		w.Write([]byte(`{
			"entry": [
				{
					"response": {
						"status": "201 Created"
					}
				},
				{
					"response": {
						"status": "201 Created"
					}
				},
				{
					"response": {
						"status": "201 Created"
					}
				},
				{
					"response": {
						"status": "201 Created"
					}
				},
				{
					"response": {
						"status": "201 Created"
					}
				}
			]
		}`))
	}))
	numWorkers := 1

	ctx := context.Background()
	sink, err := processing.NewFHIRStoreSink(ctx, &processing.FHIRStoreSinkConfig{
		FHIRStoreConfig: &fhirstore.Config{
			CloudHealthcareEndpoint: server.URL,
			ProjectID:               projectID,
			Location:                location,
			DatasetID:               datasetID,
			FHIRStoreID:             fhirStoreID,
		},
		MaxWorkers:  numWorkers,
		BatchUpload: true,
		BatchSize:   0, // BatchSize of 0 in the config should lead to a default of 5 in NewFHIRStoreSink
	})
	if err != nil {
		t.Fatalf("NewFHIRStoreSink unexpected error: %v", err)
	}
	p, err := processing.NewPipeline(nil, []processing.Sink{sink})
	if err != nil {
		t.Fatalf("failed to create pipeline: %v", err)
	}

	for _, r := range resources {
		if err := p.Process(ctx, r.ResourceTypeCode, r.ResourceTypeCode.String(), r.Data); err != nil {
			t.Fatalf("pipeline.Process() returned unexpected error: %v", err)
		}
	}
	if err := p.Finalize(ctx); err != nil {
		t.Fatalf("pipeline.Finalize() returned unexpected error: %v", err)
	}
}

func TestDirectFHIRStoreSink_BatchErrors(t *testing.T) {
	cases := []struct {
		name                string
		batchSize           int
		setFHIRErrorFileDir bool
	}{
		{name: "WithErrorFileDir", setFHIRErrorFileDir: true, batchSize: 2},
		{name: "NoErrorFileDir", setFHIRErrorFileDir: false, batchSize: 2},
	}

	resources := []testhelpers.FHIRStoreTestResource{
		{
			ResourceID:       "1",
			ResourceTypeCode: cpb.ResourceTypeCode_PATIENT,
			Data:             []byte(`{"id":"1","resourceType":"Patient"}`),
		},
		{
			ResourceID:       "2",
			ResourceTypeCode: cpb.ResourceTypeCode_EXPLANATION_OF_BENEFIT,
			Data:             []byte(`{"id":"2","resourceType":"ExplanationOfBenefit"}`),
		},
		{
			ResourceID:       "3",
			ResourceTypeCode: cpb.ResourceTypeCode_EXPLANATION_OF_BENEFIT,
			Data:             []byte(`{"id":"3","resourceType":"ExplanationOfBenefit"}`),
		},
		{
			ResourceID:       "4",
			ResourceTypeCode: cpb.ResourceTypeCode_EXPLANATION_OF_BENEFIT,
			Data:             []byte(`{"id":"4","resourceType":"ExplanationOfBenefit"}`),
		},
	}

	projectID := "projectID"
	location := "us-east1"
	datasetID := "datasetID"
	fhirStoreID := "fhirstoreID"

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			numWorkers := 2
			outputPrefix := ""
			if tc.setFHIRErrorFileDir {
				outputPrefix = t.TempDir()
			}

			body := []byte(`{
				"entry": [
					{
						"response": {
							"status": "201 Created"
						}
					},
					{
						"response": {
							"status": "201 Created"
						}
					}
				]
			}`)
			testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				w.WriteHeader(500)
				w.Write([]byte(body))
			}))
			defer testServer.Close()

			ctx := context.Background()
			sink, err := processing.NewFHIRStoreSink(context.Background(), &processing.FHIRStoreSinkConfig{
				FHIRStoreConfig: &fhirstore.Config{
					CloudHealthcareEndpoint: testServer.URL,
					ProjectID:               projectID,
					Location:                location,
					DatasetID:               datasetID,
					FHIRStoreID:             fhirStoreID,
				},
				MaxWorkers:           numWorkers,
				ErrorFileOutputPath:  outputPrefix,
				BatchUpload:          true,
				BatchSize:            tc.batchSize,
				NoFailOnUploadErrors: true,
			})
			if err != nil {
				t.Fatalf("NewFHIRStoreSink unexpected error: %v", err)
			}
			p, err := processing.NewPipeline(nil, []processing.Sink{sink})
			if err != nil {
				t.Fatalf("failed to create pipeline: %v", err)
			}

			for _, r := range resources {
				if err := p.Process(ctx, r.ResourceTypeCode, r.ResourceTypeCode.String(), r.Data); err != nil {
					t.Fatalf("pipeline.Process() returned unexpected error: %v", err)
				}
			}
			if err := p.Finalize(ctx); err != nil {
				t.Fatalf("pipeline.Finalize() returned unexpected error: %v", err)
			}

			if tc.setFHIRErrorFileDir {
				expectedErrors := make([]testhelpers.ErrorNDJSONLine, len(resources))
				for i, r := range resources {
					expectedError := fhirstore.BundleError{
						ResponseStatusCode: 500,
						ResponseStatusText: "500 Internal Server Error",
						ResponseBytes:      body,
					}
					expectedErrors[i] = testhelpers.ErrorNDJSONLine{Err: expectedError.Error(), FHIRResource: string(r.Data)}
				}
				testhelpers.CheckErrorNDJSONFile(t, outputPrefix, expectedErrors)
			}
		})
	}
}

func TestDirectFHIRStoreSink_Errors(t *testing.T) {
	cases := []struct {
		name            string
		setOutputPrefix bool
	}{
		{name: "WithOutputPrefix", setOutputPrefix: true},
		{name: "NoOutputPrefix", setOutputPrefix: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resources := []testhelpers.FHIRStoreTestResource{
				{
					ResourceID:       "PatientID",
					ResourceTypeCode: cpb.ResourceTypeCode_PATIENT,
					Data:             []byte(`{"resourceType":"Patient","id":"PatientID"}`),
				},
				{
					ResourceID:       "PatientID1",
					ResourceTypeCode: cpb.ResourceTypeCode_PATIENT,
					Data:             []byte(`{"resourceType":"Patient","id":"PatientID1"}`),
				},
			}

			fhirStoreProjectID := "test"
			fhirStoreLocation := "loc"
			fhirStoreDatasetID := "dataset"
			fhirStoreID := "fhirstore"

			testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				w.WriteHeader(500)
			}))
			defer testServer.Close()

			numWorkers := 2
			outputPrefix := ""
			if tc.setOutputPrefix {
				outputPrefix = t.TempDir()
			}
			ctx := context.Background()
			sink, err := processing.NewFHIRStoreSink(context.Background(), &processing.FHIRStoreSinkConfig{
				FHIRStoreConfig: &fhirstore.Config{
					CloudHealthcareEndpoint: testServer.URL,
					ProjectID:               fhirStoreProjectID,
					Location:                fhirStoreLocation,
					DatasetID:               fhirStoreDatasetID,
					FHIRStoreID:             fhirStoreID,
				},
				MaxWorkers:           numWorkers,
				ErrorFileOutputPath:  outputPrefix,
				NoFailOnUploadErrors: true,
			})
			if err != nil {
				t.Fatalf("NewFHIRStoreSink unexpected error: %v", err)
			}
			p, err := processing.NewPipeline(nil, []processing.Sink{sink})
			if err != nil {
				t.Fatalf("failed to create pipeline: %v", err)
			}

			for _, r := range resources {
				if err := p.Process(ctx, r.ResourceTypeCode, r.ResourceTypeCode.String(), r.Data); err != nil {
					t.Fatalf("pipeline.Process() returned unexpected error: %v", err)
				}
			}
			if err := p.Finalize(ctx); err != nil {
				t.Fatalf("pipeline.Finalize() returned unexpected error: %v", err)
			}

			if tc.setOutputPrefix {
				expectedErrors := make([]testhelpers.ErrorNDJSONLine, len(resources))
				for i, r := range resources {
					expectedErrors[i] = testhelpers.ErrorNDJSONLine{Err: "error from API server: status 500 500 Internal Server Error:  error was received from the Healthcare API server", FHIRResource: string(r.Data)}
				}
				testhelpers.CheckErrorNDJSONFile(t, outputPrefix, expectedErrors)
			}
		})
	}
}

func TestGCSBasedFHIRStoreSink(t *testing.T) {
	ctx := context.Background()

	patient1 := []byte(`{"resourceType":"Patient","id":"PatientID1"}`)
	transactionTime := time.Date(2020, 12, 9, 11, 0, 0, 123000000, time.UTC)

	bucketName := "bucket"
	gcpProject := "project"
	gcpLocation := "location"
	gcpDatasetID := "dataset"
	gcpFHIRStoreID := "fhirID"

	gcsServer := testhelpers.NewGCSServer(t)

	importCalled := false
	statusCalled := false
	expectedImportRequest := gcsImportRequest{
		ContentStructure: "RESOURCE",
		GCSSource: gcsSource{
			URI: "gs://bucket/2020-12-09T11:00:00.123+00:00/**",
		},
	}
	expectedImportPath := fmt.Sprintf("/v1/projects/%s/locations/%s/datasets/%s/fhirStores/%s:import?alt=json&prettyPrint=false", gcpProject, gcpLocation, gcpDatasetID, gcpFHIRStoreID)
	opName := fmt.Sprintf("projects/%s/locations/%s/datasets/%s/operations/OPNAME", gcpProject, gcpLocation, gcpDatasetID)
	expectedStatusPath := "/v1/" + opName + "?alt=json&prettyPrint=false"
	fhirStoreServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.String() {
		case expectedImportPath:
			bodyData, err := io.ReadAll(req.Body)
			if err != nil {
				t.Errorf("fhir server unexpected error when reading body: %v", err)
			}
			var importReq gcsImportRequest
			if err := json.Unmarshal(bodyData, &importReq); err != nil {
				t.Errorf("error unmarshalling request body in fhir server: %v", err)
			}
			if !cmp.Equal(importReq, expectedImportRequest) {
				t.Errorf("FHIR store test server received unexpected import request. got: %v, want: %v", importReq, expectedImportRequest)
			}
			importCalled = true
			w.Write([]byte(fmt.Sprintf("{\"name\": \"%s\"}", opName)))
			return
		case expectedStatusPath:
			statusCalled = true
			w.Write([]byte(`{"done": true}`))
			return
		default:
			t.Errorf("fhir server got unexpected URL. got: %v, want: %s or %s", req.URL.String(), expectedImportPath, expectedStatusPath)
		}
	}))

	tt := bulkfhir.NewTransactionTime()
	tt.Set(transactionTime)

	sink, err := processing.NewFHIRStoreSink(ctx, &processing.FHIRStoreSinkConfig{
		FHIRStoreConfig: &fhirstore.Config{
			CloudHealthcareEndpoint: fhirStoreServer.URL,
			ProjectID:               gcpProject,
			Location:                gcpLocation,
			DatasetID:               gcpDatasetID,
			FHIRStoreID:             gcpFHIRStoreID,
		},

		UseGCSUpload: true,

		GCSEndpoint:         gcsServer.URL(),
		GCSBucket:           bucketName,
		GCSImportJobTimeout: 5 * time.Second,
		GCSImportJobPeriod:  time.Second,
		TransactionTime:     tt,
	})
	if err != nil {
		t.Fatalf("failed to create sink: %v", err)
	}
	p, err := processing.NewPipeline(nil, []processing.Sink{sink})
	if err != nil {
		t.Fatalf("failed to create pipeline: %v", err)
	}

	if err := p.Process(ctx, cpb.ResourceTypeCode_PATIENT, "url1", patient1); err != nil {
		t.Fatalf("pipeline.Process() returned unexpected error: %v", err)
	}
	if err := p.Finalize(ctx); err != nil {
		t.Fatalf("pipeline.Finalize() returned unexpected error: %v", err)
	}

	wantData := [][]byte{testhelpers.NormalizeJSON(t, patient1)}
	gotData := testhelpers.ReadAllGCSFHIRJSON(t, gcsServer, true)

	if !cmp.Equal(gotData, wantData, cmpopts.SortSlices(func(a, b []byte) bool { return string(a) < string(b) })) {
		t.Errorf("unexpected data in GCS file shards. got: %s, want: %s", gotData, wantData)
	}

	wantTransactionTimeStr := "2020-12-09T11:00:00.123+00:00"
	// Check that all ndjsons were written to a directory with the transaction time:
	gcsPaths := gcsServer.GetAllPaths()
	wantPrefix := fmt.Sprintf("gs://%s/%s", bucketName, wantTransactionTimeStr)
	for _, p := range gcsPaths {
		if strings.HasSuffix(p, ".ndjson") && !strings.HasPrefix(p, wantPrefix) {
			t.Errorf("fhirstoresink: ndjson file shard written to wrong directory, got path: %v, want directory: %v", p, wantPrefix)
		}
	}

	if !importCalled {
		t.Error("expected FHIR Store import to be called")
	}
	if !statusCalled {
		t.Error("expected FHIR Store import operation status to be called")
	}
}
