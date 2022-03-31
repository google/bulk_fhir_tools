package fhirstore

import (
	"context"
	"sync"

	log "github.com/golang/glog"
	"github.com/google/medical_claims_tools/internal/counter"
)

// Uploader is a convenience wrapper for concurrent upload to FHIR store.
type Uploader struct {
	fhirStoreEndpoint  string
	fhirStoreProjectID string
	fhirStoreLocation  string
	fhirStoreDatasetID string
	fhirStoreID        string

	errorCounter *counter.Counter

	fhirJSONs  chan string
	maxWorkers int
	wg         *sync.WaitGroup
}

// NewUploader initializes and returns an Uploader.
func NewUploader(fhirStoreEndpoint, projectID, location, datasetID, fhirStoreID string, maxWorkers int, errorCounter *counter.Counter) *Uploader {
	return &Uploader{
		fhirStoreEndpoint:  fhirStoreEndpoint,
		fhirStoreProjectID: projectID,
		fhirStoreLocation:  location,
		fhirStoreDatasetID: datasetID,
		fhirStoreID:        fhirStoreID,
		errorCounter:       errorCounter,
		maxWorkers:         maxWorkers}
}

func (u *Uploader) init() {
	u.fhirJSONs = make(chan string, 100)
	u.wg = &sync.WaitGroup{}

	for i := 0; i < u.maxWorkers; i++ {
		go u.uploadWorker()
	}
}

// Upload uploads the provided FHIR JSON to FHIR store.
func (u *Uploader) Upload(fhirJSON []byte) {
	if u.fhirJSONs == nil {
		// This is the first upload call, so let's initialize.
		u.init()
	}
	u.wg.Add(1)
	u.fhirJSONs <- string(fhirJSON)
}

// Wait waits for all pending uploads to finish, and then returns.
func (u *Uploader) Wait() {
	u.wg.Wait()
}

// DoneUploading must be called when the caller is done sending items to upload to
// this uploader.
func (u *Uploader) DoneUploading() {
	close(u.fhirJSONs)
}

func (u *Uploader) uploadWorker() {
	c, err := NewClient(context.Background(), u.fhirStoreEndpoint)
	if err != nil {
		log.Fatalf("error initializing FHIR store client: %v", err)
	}

	for fhirJSON := range u.fhirJSONs {
		err := c.UploadResource([]byte(fhirJSON), u.fhirStoreProjectID, u.fhirStoreLocation, u.fhirStoreDatasetID, u.fhirStoreID)
		if err != nil {
			// TODO(b/211490544): consider adding an auto-retrying mechanism in the
			// future.
			log.Errorf("error uploading resource: %v", err)
			if u.errorCounter != nil {
				u.errorCounter.Increment()
			}
		}
		u.wg.Done()
	}
}
