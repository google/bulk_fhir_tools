package fhirstore_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/medical_claims_tools/fhirstore"
	"github.com/google/medical_claims_tools/internal/counter"
	"github.com/google/medical_claims_tools/internal/testhelpers"
)

func TestUploader(t *testing.T) {
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
					ResourceID:   "PatientID",
					ResourceType: "Patient",
					Data:         []byte(`{"resourceType":"Patient","id":"PatientID"}`),
				},
				{
					ResourceID:   "PatientID2",
					ResourceType: "Patient",
					Data:         []byte(`{"resourceType":"Patient","id":"PatientID2"}`),
				},
				{
					ResourceID:   "PatientID3",
					ResourceType: "Patient",
					Data:         []byte(`{"resourceType":"Patient","id":"PatientID3"}`),
				},
				{
					ResourceID:   "CoverageID",
					ResourceType: "Coverage",
					Data:         []byte(`{"resourceType": "Coverage", "id": "CoverageID", "contract": [{"reference": "Contract/part-a-contract1"}]}`),
				},
				{
					ResourceID:   "CoverageID2",
					ResourceType: "Coverage",
					Data:         []byte(`{"resourceType": "Coverage", "id": "CoverageID2", "contract": [{"reference": "Contract/part-a-contract1"}]}`),
				},
			}

			fhirStoreProjectID := "test"
			fhirStoreLocation := "loc"
			fhirStoreDatasetID := "dataset"
			fhirStoreID := "fhirstore"

			testServerURL := testhelpers.FHIRStoreServer(t, resources, fhirStoreProjectID, fhirStoreLocation, fhirStoreDatasetID, fhirStoreID)

			errCounter := counter.New()
			numWorkers := 2
			outputPrefix := ""
			if tc.setFHIRErrorFileDir {
				outputPrefix = t.TempDir()
			}
			u, err := fhirstore.NewUploader(fhirstore.UploaderConfig{
				FHIRStoreEndpoint:   testServerURL,
				FHIRProjectID:       fhirStoreProjectID,
				FHIRLocation:        fhirStoreLocation,
				FHIRDatasetID:       fhirStoreDatasetID,
				FHIRStoreID:         fhirStoreID,
				MaxWorkers:          numWorkers,
				ErrorCounter:        errCounter,
				ErrorFileOutputPath: outputPrefix,
			})
			if err != nil {
				t.Fatalf("NewUploader unexpected error: %v", err)
			}

			for _, r := range resources {
				u.Upload(r.Data)
			}
			u.DoneUploading()
			if err := u.Wait(); err != nil {
				t.Errorf("Uploader.Wait returned unexpected error: %v", err)
			}

			if gotErrs := errCounter.CloseAndGetCount(); gotErrs != 0 {
				t.Errorf("Uploader unexpected error count. got: %v, want: 0", gotErrs)
			}
			// At the end of the test, testhelpers.FHIRStoreServer will automatically
			// ensure that all resources in the resources slice were uploaded to the
			// server.
		})
	}
}

func TestUploader_Batch(t *testing.T) {
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

	inputJSONs := [][]byte{
		[]byte(`{"id":"1","resourceType":"Patient"}`),
		[]byte(`{"id":"2","resourceType":"ExplanationOfBenefit"}`),
		[]byte(`{"id":"3","resourceType":"ExplanationOfBenefit"}`),
		[]byte(`{"id":"4","resourceType":"ExplanationOfBenefit"}`),
	}
	projectID := "projectID"
	location := "us-east1"
	datasetID := "datasetID"
	fhirStoreID := "fhirstoreID"

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			serverURL := testhelpers.FHIRStoreServerBatch(t, inputJSONs, tc.expectedFullBatchSize, projectID, location, datasetID, fhirStoreID)
			errCounter := counter.New()
			numWorkers := 2
			outputPrefix := ""
			if tc.setFHIRErrorFileDir {
				outputPrefix = t.TempDir()
			}
			u, err := fhirstore.NewUploader(fhirstore.UploaderConfig{
				FHIRStoreEndpoint:   serverURL,
				FHIRProjectID:       projectID,
				FHIRLocation:        location,
				FHIRDatasetID:       datasetID,
				FHIRStoreID:         fhirStoreID,
				MaxWorkers:          numWorkers,
				ErrorCounter:        errCounter,
				ErrorFileOutputPath: outputPrefix,
				BatchUpload:         true,
				BatchSize:           tc.batchSize,
			})
			if err != nil {
				t.Fatalf("NewUploader unexpected error: %v", err)
			}

			for _, j := range inputJSONs {
				u.Upload(j)
			}
			u.DoneUploading()
			if err := u.Wait(); err != nil {
				t.Errorf("Uploader.Wait returned unexpected error: %v", err)
			}

			if gotErrs := errCounter.CloseAndGetCount(); gotErrs != 0 {
				t.Errorf("Uploader unexpected error count. got: %v, want: 0", gotErrs)
			}
			// At the end of the test, testhelpers.FHIRStoreServerBatch will
			// automatically ensure that all resources in the resources slice were
			// uploaded to the server.
		})
	}
}

func TestUploader_BatchDefaultBatchSize(t *testing.T) {
	// This tests that if UploaderConfig.BatchSize = 0, then a default batchSize
	// of 5 is used.
	inputJSONs := [][]byte{
		[]byte(`{"id":"1","resourceType":"Patient"}`),
		[]byte(`{"id":"2","resourceType":"ExplanationOfBenefit"}`),
		[]byte(`{"id":"3","resourceType":"ExplanationOfBenefit"}`),
		[]byte(`{"id":"4","resourceType":"ExplanationOfBenefit"}`),
		[]byte(`{"id":"5","resourceType":"ExplanationOfBenefit"}`),
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
	}))
	errCounter := counter.New()
	numWorkers := 1

	u, err := fhirstore.NewUploader(fhirstore.UploaderConfig{
		FHIRStoreEndpoint: server.URL,
		FHIRProjectID:     projectID,
		FHIRLocation:      location,
		FHIRDatasetID:     datasetID,
		FHIRStoreID:       fhirStoreID,
		MaxWorkers:        numWorkers,
		ErrorCounter:      errCounter,
		BatchUpload:       true,
		BatchSize:         0, // BatchSize of 0 in the config should lead to a default of 5 in NewUploader
	})
	if err != nil {
		t.Fatalf("NewUploader unexpected error: %v", err)
	}

	for _, j := range inputJSONs {
		u.Upload(j)
	}
	u.DoneUploading()
	if err := u.Wait(); err != nil {
		t.Errorf("Uploader.Wait returned unexpected error: %v", err)
	}

	if gotErrs := errCounter.CloseAndGetCount(); gotErrs != 0 {
		t.Errorf("Uploader unexpected error count. got: %v, want: 0", gotErrs)
	}
	// At the end of the test, testhelpers.FHIRStoreServerBatch will
	// automatically ensure that all resources in the resources slice were
	// uploaded to the server.

}

func TestUploader_BatchErrors(t *testing.T) {
	cases := []struct {
		name                string
		batchSize           int
		setFHIRErrorFileDir bool
	}{
		{name: "WithErrorFileDir", setFHIRErrorFileDir: true, batchSize: 2},
		{name: "NoErrorFileDir", setFHIRErrorFileDir: false, batchSize: 2},
	}

	inputJSONs := [][]byte{
		[]byte(`{"id":"1","resourceType":"Patient"}`),
		[]byte(`{"id":"2","resourceType":"ExplanationOfBenefit"}`),
		[]byte(`{"id":"3","resourceType":"ExplanationOfBenefit"}`),
		[]byte(`{"id":"4","resourceType":"ExplanationOfBenefit"}`),
	}
	projectID := "projectID"
	location := "us-east1"
	datasetID := "datasetID"
	fhirStoreID := "fhirstoreID"

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			errCounter := counter.New()
			numWorkers := 2
			outputPrefix := ""
			if tc.setFHIRErrorFileDir {
				outputPrefix = t.TempDir()
			}

			testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				w.WriteHeader(500)
			}))
			defer testServer.Close()

			u, err := fhirstore.NewUploader(fhirstore.UploaderConfig{
				FHIRStoreEndpoint:   testServer.URL,
				FHIRProjectID:       projectID,
				FHIRLocation:        location,
				FHIRDatasetID:       datasetID,
				FHIRStoreID:         fhirStoreID,
				MaxWorkers:          numWorkers,
				ErrorCounter:        errCounter,
				ErrorFileOutputPath: outputPrefix,
				BatchUpload:         true,
				BatchSize:           tc.batchSize,
			})
			if err != nil {
				t.Fatalf("NewUploader unexpected error: %v", err)
			}

			for _, j := range inputJSONs {
				u.Upload(j)
			}
			u.DoneUploading()
			if err := u.Wait(); err != nil {
				t.Errorf("Uploader.Wait returned unexpected error: %v", err)
			}

			if gotErrs := errCounter.CloseAndGetCount(); gotErrs != len(inputJSONs) {
				t.Errorf("Uploader unexpected error count. got: %v, want: %v", gotErrs, len(inputJSONs))
			}

			expectedErrors := make([]testhelpers.ErrorNDJSONLine, len(inputJSONs))
			for i, j := range inputJSONs {
				expectedErrors[i] = testhelpers.ErrorNDJSONLine{Err: "error from API server: status 500 500 Internal Server Error: ", FHIRResource: string(j)}
			}

			if tc.setFHIRErrorFileDir {
				testhelpers.CheckErrorNDJSONFile(t, outputPrefix, expectedErrors)
			}
		})
	}
}

func TestUploader_Errors(t *testing.T) {
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
					ResourceID:   "PatientID",
					ResourceType: "Patient",
					Data:         []byte(`{"resourceType":"Patient","id":"PatientID"}`),
				},
				{
					ResourceID:   "PatientID1",
					ResourceType: "Patient",
					Data:         []byte(`{"resourceType":"Patient","id":"PatientID1"}`),
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

			errCounter := counter.New()
			numWorkers := 2
			outputPrefix := ""
			if tc.setOutputPrefix {
				outputPrefix = t.TempDir()
			}
			u, err := fhirstore.NewUploader(fhirstore.UploaderConfig{
				FHIRStoreEndpoint:   testServer.URL,
				FHIRProjectID:       fhirStoreProjectID,
				FHIRLocation:        fhirStoreLocation,
				FHIRDatasetID:       fhirStoreDatasetID,
				FHIRStoreID:         fhirStoreID,
				MaxWorkers:          numWorkers,
				ErrorCounter:        errCounter,
				ErrorFileOutputPath: outputPrefix,
			})
			if err != nil {
				t.Fatalf("NewUploader unexpected error: %v", err)
			}

			for _, r := range resources {
				u.Upload(r.Data)
			}
			u.DoneUploading()
			if err := u.Wait(); err != nil {
				t.Errorf("Uploader.Wait returned unexpected error: %v", err)
			}

			if gotErrs := errCounter.CloseAndGetCount(); gotErrs != len(resources) {
				t.Errorf("Uploader unexpected error count. got: %v, want: %v", gotErrs, len(resources))
			}

			expectedErrors := make([]testhelpers.ErrorNDJSONLine, len(resources))
			for i, r := range resources {
				expectedErrors[i] = testhelpers.ErrorNDJSONLine{Err: "error from API server: status 500 500 Internal Server Error:  error was received from the Healthcare API server", FHIRResource: string(r.Data)}
			}

			if tc.setOutputPrefix {
				testhelpers.CheckErrorNDJSONFile(t, outputPrefix, expectedErrors)
			}
		})
	}
}
