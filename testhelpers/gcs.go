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

package testhelpers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
)

// Note: this is tested in gcs/gcs_test.go

type gcsObjectKey struct {
	bucket, name string
}

// GCSObjectEntry holds the contents and content type of stored objects.
type GCSObjectEntry struct {
	Data        []byte
	ContentType string
}

// GCSServer provides a minimal implementation of the GCS API for use in tests.
type GCSServer struct {
	t          *testing.T
	objectsMut *sync.RWMutex
	objects    map[gcsObjectKey]GCSObjectEntry
	server     *httptest.Server
}

// NewGCSServer creates a new GCS Server for use in tests.
func NewGCSServer(t *testing.T) *GCSServer {
	gs := &GCSServer{
		t:          t,
		objectsMut: &sync.RWMutex{},
		objects:    map[gcsObjectKey]GCSObjectEntry{},
	}
	gs.server = httptest.NewServer(http.HandlerFunc(gs.handleHTTP))
	t.Cleanup(func() {
		gs.server.Close()
	})
	return gs
}

// AddObject adds an object to be served by the GCS server.
func (gs *GCSServer) AddObject(bucket, name string, obj GCSObjectEntry) {
	gs.objectsMut.Lock()
	defer gs.objectsMut.Unlock()
	gs.objects[gcsObjectKey{bucket, name}] = obj
}

// GetObject retrieves an object which has been uploaded to the server.
func (gs *GCSServer) GetObject(bucket, name string) (GCSObjectEntry, bool) {
	gs.objectsMut.RLock()
	defer gs.objectsMut.RUnlock()
	obj, ok := gs.objects[gcsObjectKey{bucket, name}]
	return obj, ok
}

// GetAllObjects returns all objects uploaded to this test server across all buckets. Use this
// only if needed for your test, otherwise prefer GetObject.
func (gs *GCSServer) GetAllObjects() []GCSObjectEntry {
	gs.objectsMut.RLock()
	defer gs.objectsMut.RUnlock()
	results := make([]GCSObjectEntry, 0, len(gs.objects))
	for _, obj := range gs.objects {
		results = append(results, obj)
	}
	return results
}

// GetAllPaths returns a slice representing the upload path of all items that have been uploaded to
// the test server in the form gs://bucket/path.
func (gs *GCSServer) GetAllPaths() []string {
	gs.objectsMut.RLock()
	defer gs.objectsMut.RUnlock()
	results := make([]string, 0, len(gs.objects))
	for path := range gs.objects {
		results = append(results, fmt.Sprintf("gs://%s/%s", path.bucket, path.name))
	}
	return results
}

// URL returns the URL of the GCS server to be passed to the client library.
func (gs *GCSServer) URL() string {
	return gs.server.URL
}

const uploadPathPrefix = "/upload/storage/v1/b/"

// this should match for paths like:
// /b - list buckets
// /b/bucketName/o - list objects
var listPathRegex = regexp.MustCompile(`^/b(?:/.*/o|)$`)

func (gs *GCSServer) handleHTTP(w http.ResponseWriter, req *http.Request) {
	if strings.HasPrefix(req.URL.Path, uploadPathPrefix) {
		gs.handleUpload(w, req)
	} else if listPathRegex.MatchString(req.URL.Path) {
		gs.handleList(w, req)
	} else {
		gs.handleDownload(w, req)
	}
}

// A simple struct to hold the json response for the list buckets and objects calls.
type objectIterResponse struct {
	Kind          string
	NextPageToken string
	Items         []objectAttrsResponse
}

// Holds the file or bucket attributes for the list buckets / objects calls.
type objectAttrsResponse struct {
	Kind   string
	ID     string
	Name   string
	Bucket string
	Prefix string
}

// handleList handles the list buckets and list objects calls.
// Does not support pagination (will only ever return a single item).
// TODO b/341405229 - add support for arbitrary buckets.
func (gs *GCSServer) handleList(w http.ResponseWriter, req *http.Request) {
	var r objectIterResponse
	if req.URL.Path == "/b" {
		// List all buckets.
		r = objectIterResponse{
			Kind:  "storage#buckets",
			Items: []objectAttrsResponse{},
		}
		for key := range gs.objects {
			currBucket := objectAttrsResponse{
				Kind: "storage#bucket",
				ID:   key.bucket,
				Name: key.bucket,
			}
			if slices.Contains(r.Items, currBucket) {
				continue
			}
			r.Items = append(r.Items, currBucket)
		}
	} else if strings.HasPrefix(req.URL.Path, "/b/") && strings.HasSuffix(req.URL.Path, "/o") {
		// List all objects in a bucket.
		bucketName := strings.Split(strings.TrimPrefix(req.URL.Path, "/b/"), "/")[0]
		// "/b/bucketName/o"
		r = objectIterResponse{
			Kind:  "storage#objects",
			Items: []objectAttrsResponse{},
		}
		queryPrefix := req.URL.Query().Get("prefix")
		// find all objects in the server that match the prefix.
		for key := range gs.objects {
			if strings.Contains(key.name, queryPrefix) && key.bucket == bucketName {
				r.Items = append(r.Items, objectAttrsResponse{
					Kind:   "storage#object",
					Name:   key.name,
					Bucket: bucketName,
					Prefix: queryPrefix,
				})
			}
		}
	} else {
		gs.t.Fatalf("unrecognised list endpoint %s", req.URL.Path)
	}

	j, err := json.Marshal(r)
	if err != nil {
		gs.t.Fatalf("failed to marshal json in GCS handleList: %v", err)
	}
	w.Write(j)
}

func (gs *GCSServer) handleUpload(w http.ResponseWriter, req *http.Request) {
	bucket := strings.Split(strings.TrimPrefix(req.URL.Path, uploadPathPrefix), "/")[0]
	name := req.URL.Query().Get("name")

	_, params, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
	if err != nil {
		gs.t.Fatalf("failed to parse media type header: %v", err)
	}
	mr := multipart.NewReader(req.Body, params["boundary"])

	// We ignore the first part, as we don't use the metadata.
	if _, err := mr.NextPart(); err != nil {
		gs.t.Fatalf("failed to get first part from GCS upload request: %v", err)
	}

	p, err := mr.NextPart()
	if err != nil {
		gs.t.Fatalf("failed to get second part from GCS upload request: %v", err)
	}
	data, err := io.ReadAll(p)
	if err != nil {
		gs.t.Fatalf("failed to read GCS upload request body: %v", err)
	}
	gs.AddObject(bucket, name, GCSObjectEntry{
		Data:        data,
		ContentType: p.Header.Get("Content-Type"),
	})

	if _, err := mr.NextPart(); err != io.EOF {
		gs.t.Error("expected exactly 2 parts in GCS upload request")
	}

	w.Write([]byte("{}"))
}

func (gs *GCSServer) handleDownload(w http.ResponseWriter, req *http.Request) {
	bucket, name, ok := strings.Cut(strings.TrimPrefix(req.URL.Path, "/"), "/")

	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "unrecognised endpoint %s", req.URL.Path)
		return
	}

	object, ok := gs.GetObject(bucket, name)

	if !ok {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "object %s not found", req.URL.Path)
		return
	}

	w.Header().Set("Content-Type", object.ContentType)

	if _, err := w.Write(object.Data); err != nil {
		gs.t.Log(err)
	}
}

// ReadAllGCSFHIRJSON reads ALL files in the gcsServer, attempets to extract the FHIR json for each
// resource, and adds it to the output [][]byte. If normalize=true, then NormalizeJSON is applied to
// the json bytes before being added to the output.
func ReadAllGCSFHIRJSON(t *testing.T, gcsServer *GCSServer, normalize bool) [][]byte {
	gcsObjects := gcsServer.GetAllObjects()

	gotData := make([][]byte, 0)
	for _, file := range gcsObjects {
		dataLines := bytes.Split(file.Data, []byte("\n"))
		for _, line := range dataLines {
			if len(line) == 0 {
				continue
			}
			line = bytes.TrimSuffix(line, []byte("\n"))
			if normalize {
				line = NormalizeJSON(t, line)
			}
			gotData = append(gotData, line)
		}
	}
	return gotData
}
