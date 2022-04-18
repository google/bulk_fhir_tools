package fhirstore_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/medical_claims_tools/fhirstore"
	"github.com/google/medical_claims_tools/internal/counter"
	"github.com/google/medical_claims_tools/internal/testhelpers"
)

func TestUploader(t *testing.T) {
	cases := []struct {
		name            string
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
			u, err := fhirstore.NewUploader(testServerURL, fhirStoreProjectID, fhirStoreLocation, fhirStoreDatasetID, fhirStoreID, numWorkers, errCounter, outputPrefix)
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
			u, err := fhirstore.NewUploader(testServer.URL, fhirStoreProjectID, fhirStoreLocation, fhirStoreDatasetID, fhirStoreID, numWorkers, errCounter, outputPrefix)
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
