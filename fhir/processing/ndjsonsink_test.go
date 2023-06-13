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

package processing_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/medical_claims_tools/fhir/processing"
	"github.com/google/medical_claims_tools/internal/testhelpers"

	cpb "github.com/google/fhir/go/proto/google/fhir/proto/r4/core/codes_go_proto"
	rpb "github.com/google/fhir/go/proto/google/fhir/proto/r4/core/resources/bundle_and_contained_resource_go_proto"
)

func TestNDJSONSink(t *testing.T) {
	ctx := context.Background()

	testdata := []testResourceWrapper{
		{resourceType: cpb.ResourceTypeCode_ACCOUNT, sourceURL: "url1", json: []byte("foo")},
		{resourceType: cpb.ResourceTypeCode_ACCOUNT, sourceURL: "url1", json: []byte("bar")},
		{resourceType: cpb.ResourceTypeCode_ACCOUNT, sourceURL: "url2", json: []byte("baz")},
		{resourceType: cpb.ResourceTypeCode_PATIENT, sourceURL: "url3", json: []byte("qux")},
	}

	tempdir := t.TempDir()
	sink, err := processing.NewNDJSONSink(ctx, tempdir)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for _, td := range testdata {
		wg.Add(1)
		td := td
		go func() {
			if err := sink.Write(ctx, &td); err != nil {
				t.Error(err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	if err := sink.Finalize(ctx); err != nil {
		t.Fatal(err)
	}

	wantDataLines := [][]byte{[]byte("foo"), []byte("bar"), []byte("baz"), []byte("qux")}

	gotData := testhelpers.ReadAllFHIRJSON(t, tempdir, false)
	if !cmp.Equal(gotData, wantDataLines, cmpopts.SortSlices(func(a, b []byte) bool { return string(a) < string(b) })) {
		t.Errorf("unexpected data in file shards. got: %v, want: %v", gotData, wantDataLines)
	}
}

// Note: the logic for the GCS variant is mostly the same as for the local file
// variant, so this test is kept much simpler.
func TestGCSNDJSONSink(t *testing.T) {
	ctx := context.Background()
	testdata := []testResourceWrapper{
		{resourceType: cpb.ResourceTypeCode_ACCOUNT, sourceURL: "url1", json: []byte("foo")},
		{resourceType: cpb.ResourceTypeCode_ACCOUNT, sourceURL: "url1", json: []byte("bar")},
		{resourceType: cpb.ResourceTypeCode_ACCOUNT, sourceURL: "url2", json: []byte("baz")},
		{resourceType: cpb.ResourceTypeCode_PATIENT, sourceURL: "url3", json: []byte("qux")},
	}

	bucketName := "bucket"
	directory := "directory"

	gcsServer := testhelpers.NewGCSServer(t)

	sink, err := processing.NewGCSNDJSONSink(ctx, gcsServer.URL(), bucketName, directory)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for _, td := range testdata {
		wg.Add(1)
		td := td
		go func() {
			if err := sink.Write(ctx, &td); err != nil {
				t.Error(err)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	if err := sink.Finalize(ctx); err != nil {
		t.Fatalf("error in Finalize: %v", err)
	}

	wantDataLines := [][]byte{[]byte("foo"), []byte("bar"), []byte("baz"), []byte("qux")}
	gotData := testhelpers.ReadAllGCSFHIRJSON(t, gcsServer, false)

	if !cmp.Equal(gotData, wantDataLines, cmpopts.SortSlices(func(a, b []byte) bool { return string(a) < string(b) })) {
		t.Errorf("unexpected data in file shards. got: %s, want: %s", gotData, wantDataLines)
	}

}

func TestNDJSONSink_WorkerError(t *testing.T) {
	// This test will pass a fake GCS server that always returns errors.
	ctx := context.Background()

	testdata := []testResourceWrapper{
		{resourceType: cpb.ResourceTypeCode_ACCOUNT, sourceURL: "url1", json: []byte("foo")},
		{resourceType: cpb.ResourceTypeCode_ACCOUNT, sourceURL: "url1", json: []byte("bar")},
		{resourceType: cpb.ResourceTypeCode_ACCOUNT, sourceURL: "url2", json: []byte("baz")},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	sink, err := processing.NewGCSNDJSONSink(ctx, server.URL, "bucket", "directory")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for _, td := range testdata {
		wg.Add(1)
		td := td
		go func() {
			if err := sink.Write(ctx, &td); err != nil {
				t.Error(err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	if err := sink.Finalize(ctx); !errors.Is(err, processing.ErrWorkerError) {
		t.Errorf("unexpected error in sink.Finalize. got: %v, want: %v", err, processing.ErrWorkerError)
	}

}

type testResourceWrapper struct {
	resourceType cpb.ResourceTypeCode_Value
	sourceURL    string
	proto        *rpb.ContainedResource
	json         []byte
}

func (trw *testResourceWrapper) Type() cpb.ResourceTypeCode_Value       { return trw.resourceType }
func (trw *testResourceWrapper) SourceURL() string                      { return trw.sourceURL }
func (trw *testResourceWrapper) Proto() (*rpb.ContainedResource, error) { return trw.proto, nil }
func (trw *testResourceWrapper) JSON() ([]byte, error)                  { return trw.json, nil }
