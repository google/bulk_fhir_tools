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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/medical_claims_tools/fhir/processing"
	"github.com/google/medical_claims_tools/internal/testhelpers"

	cpb "github.com/google/fhir/go/proto/google/fhir/proto/r4/core/codes_go_proto"
)

type gcsSource struct {
	URI string `json:"uri"`
}

type gcsImportRequest struct {
	ContentStructure string    `json:"contentStructure"`
	GCSSource        gcsSource `json:"gcsSource"`
}

// Note: this is a really basic test given directFHIRStoreSink is a thin shim.
// Comprehensive testing is done on the underlying uploader.
//
// TODO(b/254648498): remove this test once fhirstore.Uploader fulfils the Sink interface.
func TestDirectFHIRStoreSink(t *testing.T) {
	ctx := context.Background()

	patient1 := []byte(`{"resourceType":"Patient","id":"PatientID1"}`)
	gcpProject := "project"
	gcpLocation := "location"
	gcpDatasetID := "dataset"
	gcpFHIRStoreID := "fhirID"

	expectedResources := []testhelpers.FHIRStoreTestResource{
		{
			ResourceID:   "PatientID1",
			ResourceType: "Patient",
			Data:         patient1,
		},
	}

	fhirStoreEndpoint := testhelpers.FHIRStoreServer(t, expectedResources, gcpProject, gcpLocation, gcpDatasetID, gcpFHIRStoreID)

	sink, err := processing.NewFHIRStoreSink(ctx, &processing.FHIRStoreSinkConfig{
		FHIRStoreEndpoint: fhirStoreEndpoint,
		FHIRStoreID:       gcpFHIRStoreID,
		FHIRProjectID:     gcpProject,
		FHIRLocation:      gcpLocation,
		FHIRDatasetID:     gcpDatasetID,
		MaxWorkers:        10,
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
}

func TestGCSBasedFHIRStoreSink(t *testing.T) {
	ctx := context.Background()

	patient1 := []byte(`{"resourceType":"Patient","id":"PatientID1"}`)
	transactionTimeStr := "2020-12-09T11:00:00.123+00:00"
	transactionTime := time.Date(2020, 12, 9, 11, 0, 0, 123000000, time.UTC)

	bucketName := "bucket"
	gcpProject := "project"
	gcpLocation := "location"
	gcpDatasetID := "dataset"
	gcpFHIRStoreID := "fhirID"

	gcsWriteCalled := false

	gcsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		fileWritePath := ("/upload/storage/v1/b/" +
			bucketName + "/o?alt=json&name=" +
			url.QueryEscape(transactionTimeStr+"/"+"Patient_0.ndjson") +
			"&prettyPrint=false&projection=full&uploadType=multipart")

		if req.URL.String() != fileWritePath {
			t.Errorf("gcs server got unexpected request. got: %v, want: %v", req.URL.String(), fileWritePath)
		}
		data, err := io.ReadAll(req.Body)
		if err != nil {
			t.Errorf("gcs server error reading body.")
		}
		if !bytes.Contains(data, patient1) {
			t.Errorf("gcs server unexpected data: got: %s, want: %s", data, patient1)
		}
		gcsWriteCalled = true
		// We have to write a response, else closing the file returns an EOF error,
		// but the response doesn't actually need to contain anything.
		w.Write([]byte(`{}`))
	}))
	defer gcsServer.Close()

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

	sink, err := processing.NewFHIRStoreSink(ctx, &processing.FHIRStoreSinkConfig{
		FHIRStoreEndpoint: fhirStoreServer.URL,
		FHIRStoreID:       gcpFHIRStoreID,
		FHIRProjectID:     gcpProject,
		FHIRLocation:      gcpLocation,
		FHIRDatasetID:     gcpDatasetID,

		UseGCSUpload: true,

		GCSEndpoint:         gcsServer.URL,
		GCSBucket:           bucketName,
		GCSImportJobTimeout: 5 * time.Second,
		GCSImportJobPeriod:  time.Second,
		TransactionTime:     transactionTime,
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

	if !gcsWriteCalled {
		t.Error("expected data to be written to GCS")
	}

	if !importCalled {
		t.Error("expected FHIR Store import to be called")
	}
	if !statusCalled {
		t.Error("expected FHIR Store import operation status to be called")
	}
}
