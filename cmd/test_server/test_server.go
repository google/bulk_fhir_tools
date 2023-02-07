// Copyright 2023 Google LLC
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

// Binary test_server is a HTTP Server which serves (part of) the Bulk FHIR
// Export interface. This is geared towards reproducible integration testing
// with a sequence of expected requests returning fixed data, rather than
// supporting arbitrary requests to simulate the behaviour of a real server.
//
// The server runs based on NDJSON files under the directory given in the
// --data_dir flag, which should be organised as follows:
//
//	{--data_dir}/{export_group_id}/{transaction_time}/{resource_type}_{index}.ndjson
//
// For the provided export_group_id, the server will provide the dataset with
// the earliest transaction_time which is greater than the _since parameter (or
// the earliest transaction_time overall if the _since parameter is unset).

// If no dataset exists with a transaction_time greater than the _since
// parameter, this server will return a 404 error to the initial $export call -
// this is assumed to be an error in setting up the test. If you wish to test
// the case of there being no changes to the data, or no data at all, you should
// add a transaction_time folder which is empty.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"flag"
	"github.com/google/uuid"
	"github.com/julienschmidt/httprouter"
)

var (
	port         = flag.Int("port", 8000, "Port to listen on")
	dataDir      = flag.String("data_dir", "", "Directory to read data to be served from")
	jobDelay     = flag.Duration("job_delay", time.Minute, "How long jobs take to complete. Use time.ParseDuration syntax (e.g. 1h15m30s)")
	retryAfter   = flag.Int("retry_after", 10, "Value of the Retry-After header for incomplete jobs")
	clientID     = flag.String("client_id", "", "the value of client ID this server should accept.")
	clientSecret = flag.String("client_secret", "", "the value of client secret this server should accept.")
)

// token represents the only valid token this server recognizes in authenticated requests.
// TODO(b/266077987): add support for token TTLs, more advanced auth behavior.
const token = "thisisthetoken"

const authorizationHeader = "Authorization"

type fileKey struct {
	resourceType, index string
}

type outputItem struct {
	ResourceType string `json:"type"`
	URL          string `json:"url"`
}

type statusResponse struct {
	TransactionTime     string       `json:"transactionTime"`
	Request             string       `json:"request"`
	RequiresAccessToken bool         `json:"requiresAccessToken"`
	Output              []outputItem `json:"output"`
}

type exportJob struct {
	startTime, readyTime time.Time
	filepaths            map[fileKey]string
	response             statusResponse
}

type server struct {
	dataDir           string
	baseURL           string
	jobs              map[string]*exportJob
	validClientID     string
	validClientSecret string
}

const errorFormat = `{
 "resourceType": "OperationOutcome",
 "id": "1",
 "issue": [
  {
   "severity": "error",
   "code": "processing",
   "details": {
    "text": "%s"
   }
  }
 ]
}`

func writeError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/fhir+json")
	w.WriteHeader(code)
	fmt.Fprintf(w, errorFormat, message)
}

func (s *server) getToken(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	id, sec, ok := req.BasicAuth()
	if !ok {
		writeError(w, http.StatusUnauthorized, "Invalid basic auth configuration.")
		return
	}
	if id != s.validClientID || sec != s.validClientSecret {
		writeError(w, http.StatusUnauthorized, "Invalid credentials.")
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf(`{"access_token": "%s", "expires_in": 1200}`, token)))
}

func (s *server) startExport(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	job := &exportJob{
		startTime: time.Now(),
		readyTime: time.Now().Add(*jobDelay),
		filepaths: map[fileKey]string{},
		response: statusResponse{
			Request:             req.URL.String(),
			RequiresAccessToken: false,
		},
	}
	groupID := ps.ByName("groupID")
	requestID := uuid.New().String()

	dateEntries, err := os.ReadDir(filepath.Join(s.dataDir, groupID))
	if err != nil {
		log.Print(err)
		if os.IsNotExist(err) {
			writeError(w, http.StatusNotFound, fmt.Sprintf("Export group %s not found", groupID))
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if len(dateEntries) == 0 {
		writeError(w, http.StatusNotFound, fmt.Sprintf("No data found for export group %s", groupID))
		return
	}
	since := req.URL.Query().Get("_since")
	for _, de := range dateEntries {
		if de.Name() > since {
			job.response.TransactionTime = de.Name()
			break
		}
	}
	if job.response.TransactionTime == "" {
		writeError(w, http.StatusNotFound, fmt.Sprintf("No data found for export group %s _since %s", groupID, since))
		return
	}

	dataDir := filepath.Join(s.dataDir, groupID, job.response.TransactionTime)
	dataEntries, err := os.ReadDir(dataDir)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if len(dataEntries) == 0 {
		writeError(w, http.StatusNotFound, fmt.Sprintf("No data files found in %s", dataDir))
		return
	}
	for _, de := range dataEntries {
		resourceType, index, ok := strings.Cut(strings.Split(de.Name(), ".")[0], "_")
		if !ok {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("Data file %s in %s has wrong format; want {resource_type}_{index}.ndjson", de.Name(), dataDir))
			return
		}
		job.filepaths[fileKey{strings.ToLower(resourceType), index}] = filepath.Join(dataDir, de.Name())
		job.response.Output = append(job.response.Output, outputItem{
			ResourceType: resourceType,
			URL:          fmt.Sprintf("%s/requests/%s/%s/%s", s.baseURL, requestID, resourceType, index),
		})
	}
	s.jobs[requestID] = job

	w.Header().Set("Content-Location", fmt.Sprintf("%s/requests/%s", s.baseURL, requestID))
	w.WriteHeader(http.StatusAccepted)
}

func (s *server) exportStatus(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	requestID := ps.ByName("requestID")
	job, ok := s.jobs[requestID]
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("Request ID %s not found", requestID))
		return
	}

	if time.Now().Before(job.readyTime) {
		// Not ready yet
		progress := 100 * time.Now().Sub(job.startTime) / job.readyTime.Sub(job.startTime)
		w.Header().Set("X-Progress", fmt.Sprintf("%d%% complete", progress))
		w.Header().Set("Retry-After", fmt.Sprintf("%d", *retryAfter))
		w.WriteHeader(http.StatusAccepted)
		return
	}

	response, err := json.Marshal(job.response)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(response); err != nil {
		log.Print(err)
	}
}

func (s *server) serveResource(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	job, ok := s.jobs[ps.ByName("requestID")]
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("Request ID %s not found", ps.ByName("requestID")))
		return
	}

	key := fileKey{strings.ToLower(ps.ByName("resourceType")), ps.ByName("index")}
	filepath, ok := job.filepaths[key]
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("File %s/%s not found", ps.ByName("resourceType"), ps.ByName("index")))
		return
	}
	fh, err := os.Open(filepath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/fhir+ndjson")
	if _, err := io.Copy(w, fh); err != nil {
		log.Print(err)
	}
}

func requiresAuth(handle httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
		// Check token:
		if gotToken := req.Header.Get(authorizationHeader); gotToken != fmt.Sprintf("Bearer %s", token) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		// Token ok, call handler logic.
		handle(w, req, ps)
	}
}

func (s *server) registerHandlers() {
	r := httprouter.New()
	r.POST("/token", s.getToken)
	r.GET("/Group/:groupID/$export", requiresAuth(s.startExport))
	r.GET("/requests/:requestID", requiresAuth(s.exportStatus))
	r.GET("/requests/:requestID/:resourceType/:index", requiresAuth(s.serveResource))
	http.Handle("/", r)
}

func main() {
	flag.Parse()

	srv := &server{
		dataDir:           *dataDir,
		baseURL:           fmt.Sprintf("http://localhost:%d", *port),
		jobs:              map[string]*exportJob{},
		validClientID:     *clientID,
		validClientSecret: *clientSecret,
	}
	srv.registerHandlers()

	if err := http.ListenAndServe(fmt.Sprintf(":%d", *port), nil); err != nil {
		log.Fatal(err)
	}
}
