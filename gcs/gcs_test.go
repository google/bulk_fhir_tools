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
	"io"
	"testing"

	"github.com/google/medical_claims_tools/internal/testhelpers"
)

func TestGCSClientWritesResourceToGCS(t *testing.T) {
	var bucketID = "TestBucket"
	var resourceName = "directory/TestResource"
	var resourceData = "testtest 2"

	server := testhelpers.NewGCSServer(t)

	ctx := context.Background()

	gcsClient, err := NewClient(ctx, bucketID, server.URL())
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

	obj, ok := server.GetObject(bucketID, resourceName)
	if !ok {
		t.Fatalf("object %s/%s was not found", bucketID, resourceName)
	}

	if string(obj.Data) != resourceData {
		t.Errorf("expected file data to be %q; got %q", resourceData, string(obj.Data))
	}
}

func TestGCSClientReadsDataFromGCS(t *testing.T) {
	var bucketID = "TestBucket"
	var fileName = "TestFile"
	var fileData = "{ value : 'hello' }"

	server := testhelpers.NewGCSServer(t)
	server.AddObject(bucketID, fileName, testhelpers.GCSObjectEntry{
		Data: []byte(fileData),
	})

	ctx := context.Background()

	gcsClient, err := NewClient(ctx, bucketID, server.URL())
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

func TestGCSPathComponents(t *testing.T) {
	cases := []struct {
		name             string
		gcsPath          string
		wantBucket       string
		wantRelativePath string
		wantErr          error
	}{
		{
			name:             "ValidGCSPath",
			gcsPath:          "gs://testbucket/folder",
			wantBucket:       "testbucket",
			wantRelativePath: "folder",
			wantErr:          nil,
		},
		{
			name:             "ValidDeepGCSPath",
			gcsPath:          "gs://testbucket/folder1/folder2/item",
			wantBucket:       "testbucket",
			wantRelativePath: "folder1/folder2/item",
			wantErr:          nil,
		},
		{
			name:             "InvalidGCSPathWithoutPrefix",
			gcsPath:          "folder1/folder2/item",
			wantBucket:       "",
			wantRelativePath: "",
			wantErr:          ErrInvalidGCSPath,
		},
		{
			name:             "InvalidGCSPathWithoutFolder",
			gcsPath:          "gs://testbucket",
			wantBucket:       "",
			wantRelativePath: "",
			wantErr:          ErrInvalidGCSPath,
		},
		{
			name:             "InvalidGCSPathWithoutFolderTrailingSlash",
			gcsPath:          "gs://testbucket/",
			wantBucket:       "",
			wantRelativePath: "",
			wantErr:          ErrInvalidGCSPath,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bucket, relativePath, err := PathComponents(tc.gcsPath)
			if bucket != tc.wantBucket || relativePath != tc.wantRelativePath || err != tc.wantErr {
				t.Errorf("PathComponents(%q) = (%q, %q, %v); want (%q, %q, %v)", tc.gcsPath, bucket, relativePath, err, tc.wantBucket, tc.wantRelativePath, tc.wantErr)
			}
		})
	}
}
