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
	u := fhirstore.NewUploader(testServerURL, fhirStoreProjectID, fhirStoreLocation, fhirStoreDatasetID, fhirStoreID, numWorkers, errCounter)

	for _, r := range resources {
		u.Upload(r.Data)
	}
	u.DoneUploading()
	u.Wait()

	if gotErrs := errCounter.CloseAndGetCount(); gotErrs != 0 {
		t.Errorf("Uploader unexpected error count. got: %v, want: 0", gotErrs)
	}
	// At the end of the test, testhelpers.FHIRStoreServer will automatically
	// ensure that all resources in the resources slice were uploaded to the
	// server.
}

func TestUploader_Errors(t *testing.T) {
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
	u := fhirstore.NewUploader(testServer.URL, fhirStoreProjectID, fhirStoreLocation, fhirStoreDatasetID, fhirStoreID, 2, errCounter)

	for _, r := range resources {
		u.Upload(r.Data)
	}
	u.DoneUploading()
	u.Wait()

	if gotErrs := errCounter.CloseAndGetCount(); gotErrs != len(resources) {
		t.Errorf("Uploader unexpected error count. got: %v, want: %v", gotErrs, len(resources))
	}
}
