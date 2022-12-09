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
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestGCSClientWritesResourceToGCS(t *testing.T) {
	var bucketID = "TestBucket"
	var resourceName = "directory/TestResource"
	var resourceData = "testtest 2"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// Request to send the contents of the writer to GCS.
		requestPath := ("/upload/storage/v1/b/" +
			bucketID + "/o?alt=json&name=" +
			url.QueryEscape(resourceName) +
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

	ctx := context.Background()

	gcsClient, err := NewClient(ctx, bucketID, server.URL)
	if err != nil {
		t.Error("Unexpected error when getting NewClient: ", err)
	}

	writeCloser := gcsClient.GetFileWriter(ctx, resourceName)

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

func TestGCSClientReadsDataFromGCS(t *testing.T) {
	var bucketID = "TestBucket"
	var fileName = "TestFile"
	var fileData = "{ value : 'hello' }"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// Request to get a reader from GCS.
		requestPath := fmt.Sprintf("/%s/%s", bucketID, fileName)

		if req.URL.String() != requestPath {
			t.Errorf("Test server got unexpected url: %v", req.URL.String())
			return
		}

		w.WriteHeader(200)
		w.Write([]byte(fileData))
	}))

	ctx := context.Background()

	gcsClient, err := NewClient(ctx, bucketID, server.URL)
	if err != nil {
		t.Error("Unexpected error when creating NewClient: ", err)
	}

	reader, err := gcsClient.GetFileReader(ctx, fileName)
	if err != nil {
		t.Error("Unexpected error when getting file reader: ", err)
	}

	b, err := io.ReadAll(reader)
	if err != nil {
		t.Error("Unexpected error when reading file: ", err)
	}
	if string(b) != fileData {
		t.Error("Expected file data to contain: ", fileData)
	}

}

func TestJoinPath(t *testing.T) {
	for _, tc := range []struct {
		description string
		elems       []string
		want        string
	}{
		{
			description: "single element",
			elems:       []string{"foo"},
			want:        "foo",
		},
		{
			description: "simple multi element",
			elems:       []string{"foo", "bar", "baz"},
			want:        "foo/bar/baz",
		},
		{
			description: "multi element with path pieces",
			elems:       []string{"/foo/bar", "baz/", "/qux//"},
			want:        "foo/bar/baz/qux",
		},
		{
			description: "multi element with path pieces",
			elems:       []string{"/foo/bar", "baz/", "/qux/xyzzy.txt"},
			want:        "foo/bar/baz/qux/xyzzy.txt",
		},
		{
			description: "multi element with backslashes",
			elems:       []string{`foo\bar`, `baz\`, `\qux\xyzzy.txt`},
			want:        "foo/bar/baz/qux/xyzzy.txt",
		},
	} {
		t.Run(tc.description, func(t *testing.T) {
			if got := JoinPath(tc.elems...); got != tc.want {
				t.Errorf("JoinPaths(%#v) = %q; want %q", tc.elems, got, tc.want)
			}
		})
	}
}
