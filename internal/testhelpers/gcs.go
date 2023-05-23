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
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
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
	t       *testing.T
	objects map[gcsObjectKey]GCSObjectEntry
	server  *httptest.Server
}

// NewGCSServer creates a new GCS Server for use in tests.
func NewGCSServer(t *testing.T) *GCSServer {
	gs := &GCSServer{
		t:       t,
		objects: map[gcsObjectKey]GCSObjectEntry{},
	}
	gs.server = httptest.NewServer(http.HandlerFunc(gs.handleHTTP))
	t.Cleanup(func() {
		gs.server.Close()
	})
	return gs
}

// AddObject adds an object to be served by the GCS server.
func (gs *GCSServer) AddObject(bucket, name string, obj GCSObjectEntry) {
	gs.objects[gcsObjectKey{bucket, name}] = obj
}

// GetObject retrieves an object which has been uploaded to the server.
func (gs *GCSServer) GetObject(bucket, name string) (GCSObjectEntry, bool) {
	obj, ok := gs.objects[gcsObjectKey{bucket, name}]
	return obj, ok
}

// URL returns the URL of the GCS server to be passed to the client library.
func (gs *GCSServer) URL() string {
	return gs.server.URL
}

const uploadPathPrefix = "/upload/storage/v1/b/"
const listPathPrefix = "/b"

func (gs *GCSServer) handleHTTP(w http.ResponseWriter, req *http.Request) {
	if strings.HasPrefix(req.URL.Path, uploadPathPrefix) {
		gs.handleUpload(w, req)
	} else if strings.HasPrefix(req.URL.Path, listPathPrefix) {
		gs.handleList(w, req)
	} else {
		gs.handleDownload(w, req)
	}
}

func (gs *GCSServer) handleList(w http.ResponseWriter, req *http.Request) {
	r := struct {
		Kind          string
		NextPageToken string
		Items         []struct {
			Kind string
			ID   string
			Name string
		}
	}{
		Kind:          "storage#buckets",
		NextPageToken: "",
		Items: []struct {
			Kind string
			ID   string
			Name string
		}{{Kind: "storage#bucket", ID: "bucketName", Name: "bucketName"}},
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
