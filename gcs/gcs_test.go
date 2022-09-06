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

package gcs

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/medical_claims_tools/fhir"
)

func TestGCSClientWritesResourceToGCS(t *testing.T) {
	var timeExecuted = "2022-01-02"
	var bucketID = "TestBucket"
	var resourceName = "TestResource"
	var resourceData = "testtest 2"

	since, err := time.Parse("2006-01-02", timeExecuted)
	if err != nil {
		t.Fatal(err)
	}
	fhirSinceTime := fhir.ToFHIRInstant(since)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// Request to send the contents of the writer to GCS.
		requestPath := ("/upload/storage/v1/b/" +
			bucketID + "/o?alt=json&name=" +
			url.QueryEscape(fhirSinceTime+"/"+resourceName) +
			"&prettyPrint=false&projection=full&uploadType=multipart")

		if req.URL.String() == requestPath {

			data, err := io.ReadAll(req.Body)
			if err != nil {
				t.Fatal(err)
			}
			stringData := string(data)

			if !strings.Contains(stringData, resourceData) {
				t.Errorf("Expected body payload to contain %v", resourceData)
			}
		} else {
			t.Fatalf("Test server got unexpected url: %v", req.URL.String())
		}

		w.WriteHeader(200)
		w.Write([]byte("{}"))
	}))

	gcsClient, err := NewClient(bucketID, server.URL)
	if err != nil {
		t.Error("Unexpected error when getting NewClient: ", err)
	}

	writeCloser, err := gcsClient.GetFileWriter(resourceName, since)
	if err != nil {
		t.Error("Unexpected error when getting file writer: ", err)
	}

	// Write data piece by piece.
	_, err = writeCloser.Write([]byte(resourceData[0:5]))
	if err != nil {
		t.Error("Unexpected error when writing file: ", err)
	}

	_, err = writeCloser.Write([]byte(resourceData[5:len(resourceData)]))
	if err != nil {
		t.Error("Unexpected error when writing file: ", err)
	}

	err = writeCloser.Close()
	if err != nil {
		t.Error("Unexpected error when closing file and uploading data to GCS: ", err)
	}
}
