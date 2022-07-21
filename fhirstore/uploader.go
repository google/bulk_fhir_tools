package fhirstore

import (
	"context"
	"encoding/json"
	"os"
	"path"
	"sync"

	log "github.com/golang/glog"
	"github.com/google/medical_claims_tools/internal/counter"
)

// defaultBatchSize is the deafult batch size for FHIR store uploads in batch
// mode.
const defaultBatchSize = 5

// Uploader is a convenience wrapper for concurrent upload to FHIR store.
type Uploader struct {
	fhirStoreEndpoint  string
	fhirStoreProjectID string
	fhirStoreLocation  string
	fhirStoreDatasetID string
	fhirStoreID        string

	// batchUpload indicates if fhirJSONs should be uploaded to FHIR store in
	// batches using executeBundle in batch mode.
	batchUpload bool
	batchSize   int

	errorCounter *counter.Counter

	fhirJSONs  chan string
	maxWorkers int
	wg         *sync.WaitGroup

	errorFileOutputPath string

	errNDJSONFileMut sync.Mutex
	errorNDJSONFile  *os.File
}

// UploaderConfig is a config struct used when creating a NewUploader.
type UploaderConfig struct {
	FHIRStoreEndpoint string
	FHIRStoreID       string
	FHIRProjectID     string
	FHIRLocation      string
	FHIRDatasetID     string

	BatchUpload bool
	BatchSize   int

	MaxWorkers int

	ErrorCounter        *counter.Counter
	ErrorFileOutputPath string
}

// NewUploader initializes and returns an Uploader.
func NewUploader(config UploaderConfig) (*Uploader, error) {

	batchSize := defaultBatchSize
	if config.BatchSize != 0 {
		batchSize = config.BatchSize
	}

	u := &Uploader{
		fhirStoreEndpoint:   config.FHIRStoreEndpoint,
		fhirStoreProjectID:  config.FHIRProjectID,
		fhirStoreLocation:   config.FHIRLocation,
		fhirStoreDatasetID:  config.FHIRDatasetID,
		fhirStoreID:         config.FHIRStoreID,
		errorCounter:        config.ErrorCounter,
		maxWorkers:          config.MaxWorkers,
		errorFileOutputPath: config.ErrorFileOutputPath,
		batchUpload:         config.BatchUpload,
		batchSize:           batchSize,
	}

	if config.ErrorFileOutputPath != "" {
		f, err := os.OpenFile(path.Join(config.ErrorFileOutputPath, "resourcesWithErrors.ndjson"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, err
		}
		u.errorNDJSONFile = f
	}

	return u, nil
}

func (u *Uploader) init() {
	u.fhirJSONs = make(chan string, 100)
	u.wg = &sync.WaitGroup{}

	for i := 0; i < u.maxWorkers; i++ {
		if u.batchUpload {
			go u.uploadBatchWorker()
		} else {
			go u.uploadWorker()
		}
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

// Wait waits for all pending uploads to finish, and then returns. It may return
// an error if there is an issue closing any underlying files.
func (u *Uploader) Wait() error {
	u.wg.Wait()
	if u.errorNDJSONFile != nil {
		return u.errorNDJSONFile.Close()
	}
	return nil
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
			u.writeError(fhirJSON, err)
		}
		u.wg.Done()
	}
}

func (u *Uploader) uploadBatchWorker() {
	c, err := NewClient(context.Background(), u.fhirStoreEndpoint)
	if err != nil {
		log.Fatalf("error initializing FHIR store client: %v", err)
	}

	fhirBatchBuffer := make([][]byte, u.batchSize)
	lastChannelReadOK := true
	for lastChannelReadOK {
		var fhirJSON string
		numBufferItemsPopulated := 0
		// Attempt to populate the fhirBatchBuffer. Note that this could populate
		// from 0 up to u.batchSize elements before lastChannelReadOK is false.
		for i := 0; i < u.batchSize; i++ {
			fhirJSON, lastChannelReadOK = <-u.fhirJSONs
			if !lastChannelReadOK {
				break
			}
			fhirBatchBuffer[i] = []byte(fhirJSON)
			numBufferItemsPopulated++
		}

		if numBufferItemsPopulated == 0 {
			break
		}

		fhirBatch := fhirBatchBuffer[0:numBufferItemsPopulated]

		// Upload batch
		if err := c.UploadBatch(fhirBatch, u.fhirStoreProjectID, u.fhirStoreLocation, u.fhirStoreDatasetID, u.fhirStoreID); err != nil {
			log.Errorf("error uploading batch: %v", err)
			// TODO(b/225916126): in the future, try to unpack the error and only
			// write out the resources within the bundle that failed. For now, we
			// write out all resources in the bundle to be safe.
			for _, errResource := range fhirBatch {
				if u.errorCounter != nil {
					u.errorCounter.Increment()
				}
				u.writeError(string(errResource), err)
			}
		}

		for j := 0; j < numBufferItemsPopulated; j++ {
			u.wg.Done()
		}
	}
}

func (u *Uploader) writeError(fhirJSON string, err error) {
	if u.errorNDJSONFile != nil {
		data, jsonErr := json.Marshal(errorNDJSONLine{Err: err.Error(), FHIRResource: fhirJSON})
		if jsonErr != nil {
			log.Errorf("error marshaling data to write to error file: %v", jsonErr)
			return
		}
		u.errNDJSONFileMut.Lock()
		defer u.errNDJSONFileMut.Unlock()
		u.errorNDJSONFile.Write(data)
		u.errorNDJSONFile.Write([]byte("\n"))
	}
}

type errorNDJSONLine struct {
	Err          string `json:"err"`
	FHIRResource string `json:"fhir_resource"`
}
