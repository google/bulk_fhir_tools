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

// Package gcs contains helpers that facilitate data transfer of Resources into Google Cloud
// Storage.
package gcs

import (
	"context"
	"io"
	"net/http"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

// DefaultCloudStorageEndpoint represents the default cloud storage API endpoint.
// This should be passed to used unless in a test environment.
const DefaultCloudStorageEndpoint = "https://storage.googleapis.com/"

// Client represents a GCS API client belonging to some project.
type Client struct {
	*storage.Client
	endpointURL string
	bucketName  string
}

// NewClient creates and returns a new gcs client for use in writing resources to an existing GCS
// bucket. Note `bucketName` must belong to an existing bucket. See here for how to create a GCS
// bucket: https://cloud.google.com/storage/docs/creating-buckets.
// TODO(b/243677730): Add support for creating buckets.
func NewClient(ctx context.Context, bucketName, endpointURL string) (Client, error) {
	var storageClient *storage.Client
	var err error

	if endpointURL == DefaultCloudStorageEndpoint {
		storageClient, err = storage.NewClient(ctx, option.WithEndpoint(endpointURL))
	} else {
		// When not using the default Cloud Storage endpoint, we provide an empty
		// http.Client. This case is generally used in the test, so that the
		// storage.Client doesn't complain about not being able to find
		// credentials in the test environment.
		// TODO(b/211028663): we should try to find a better way to handle this
		// case, perhaps we can set fake default creds in the test setup.
		storageClient, err = storage.NewClient(ctx, option.WithHTTPClient(&http.Client{}), option.WithEndpoint(endpointURL))
	}
	gcsClient := Client{endpointURL: endpointURL, bucketName: bucketName, Client: storageClient}
	return gcsClient, err
}

// GetFileWriter returns a write closer that allows the user to write to a file named `fileName` in
// the pre defined GCS bucket.
// Closing the write closer will send the written data to GCS.
func (gcsClient Client) GetFileWriter(ctx context.Context, fileName string) io.WriteCloser {
	bkt := gcsClient.Bucket(gcsClient.bucketName)
	obj := bkt.Object(fileName)
	return obj.NewWriter(ctx)
}

// GetFileReader returns a reader for a file in GCS named `fileName`.
// ErrObjectNotExist will be returned if the object is not found.
//
// The caller must call Close on the returned Reader when done reading.
func (gcsClient Client) GetFileReader(ctx context.Context, fileName string) (io.ReadCloser, error) {
	bkt := gcsClient.Bucket(gcsClient.bucketName)
	return bkt.Object(fileName).NewReader(ctx)
}

// JoinPath is roughly equivalent to path/filepath.Join, except that it always
// uses forward slashes regardless of platform (because GCS does not recognize
// backslashes used by windows).
//
// Each path element backslashes converted to forward slashes, and has leading
// and trailing slashes removed. Elements are then joined with forward slashes.
//
// Warning: this may not be fully compatible with how directory paths are
// supposed to work, and should not be used except for writing to GCS. For
// writing files to a local filesystem, use path/filepath.Join.
func JoinPath(elems ...string) string {
	var cleaned []string
	for _, e := range elems {
		cleaned = append(cleaned, strings.Trim(strings.ReplaceAll(e, `\`, `/`), `/`))
	}
	return strings.Join(cleaned, `/`)
}
