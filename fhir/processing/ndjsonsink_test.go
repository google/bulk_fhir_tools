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
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
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
	if entries, err := os.ReadDir(tempdir); err != nil {
		t.Fatal(err)
	} else if len(entries) != 3 {
		t.Errorf("directory %s contained %d entries; want 3", tempdir, len(entries))
	}

	wantFileNames := []string{
		"Account_0.ndjson",
		"Account_1.ndjson",
		"Patient_0.ndjson",
	}

	// wantData maps ResourceType to a slice of expected contents in file shards of that ResourceType.
	// For example:
	// "Patient": [][]byte{[]byte("one\n"), []byte("two\nthree\n")}
	// would indicate that we expect two file shards for the Patient ResourceType. One shard should
	// contain "one\n" and the other should contain "two\nthree\n". There is no constraint on which
	// shard should contain which contents (e.g. "Patient_0.ndjson" doesn't have to contain "one\n",
	// it could be the shard containing "two\nthree\n").
	wantData := map[string][][]byte{
		"Account": [][]byte{[]byte("foo\nbar\n"), []byte("baz\n")},
		"Patient": [][]byte{[]byte("qux\n")},
	}

	// matchedData indicates whether the contents in the wantData shard index were found in a file
	// shard for the ResourceType key.
	matchedData := map[string][]bool{
		"Account": make([]bool, len(wantData["Account"])),
		"Patient": make([]bool, len(wantData["Patient"])),
	}

	for _, fn := range wantFileNames {
		fullPath := filepath.Join(tempdir, fn)
		gotData, err := os.ReadFile(fullPath)
		if err != nil {
			t.Errorf("could not read %s: %v", fullPath, err)
		}
		dataLines := bytes.Split(gotData, []byte("\n"))
		resourceType := strings.Split(fn, "_")[0]
		for i, wantShardData := range wantData[resourceType] {
			wantDataLines := bytes.Split(wantShardData, []byte("\n"))

			if cmp.Equal(dataLines, wantDataLines, cmpopts.SortSlices(func(a, b []byte) bool { return string(a) < string(b) })) {
				matchedData[resourceType][i] = true
			}
		}
	}

	for resourceType, matchedSlice := range matchedData {
		for i, matched := range matchedSlice {
			if !matched {
				t.Errorf("file shard contents for %s ResourceType were not found in any shard. want contents: %s", resourceType, wantData[resourceType][i])
				t.Error(matchedData)
			}
		}
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

	wantFileNames := []string{
		"Account_0.ndjson",
		"Account_1.ndjson",
		"Patient_0.ndjson",
	}

	// wantData maps ResourceType to a slice of expected contents in file shards of that ResourceType.
	// For example:
	// "Patient": [][]byte{[]byte("one\n"), []byte("two\nthree\n")}
	// would indicate that we expect two file shards for the Patient ResourceType. One shard should
	// contain "one\n" and the other should contain "two\nthree\n". There is no constraint on which
	// shard should contain which contents (e.g. "Patient_0.ndjson" doesn't have to contain "one\n",
	// it could be the shard containing "two\nthree\n").
	wantData := map[string][][]byte{
		"Account": [][]byte{[]byte("foo\nbar\n"), []byte("baz\n")},
		"Patient": [][]byte{[]byte("qux\n")},
	}

	// matchedData indicates whether the contents in the wantData shard index were found in a file
	// shard for the ResourceType key.
	matchedData := map[string][]bool{
		"Account": make([]bool, len(wantData["Account"])),
		"Patient": make([]bool, len(wantData["Patient"])),
	}

	for _, fn := range wantFileNames {
		objName := directory + "/" + fn
		obj, ok := gcsServer.GetObject(bucketName, objName)
		if !ok {
			t.Fatalf("gs://%s/%s not found", bucketName, objName)
		}

		dataLines := bytes.Split(obj.Data, []byte("\n"))
		resourceType := strings.Split(fn, "_")[0]
		for i, wantShardData := range wantData[resourceType] {
			wantDataLines := bytes.Split(wantShardData, []byte("\n"))

			if cmp.Equal(dataLines, wantDataLines, cmpopts.SortSlices(func(a, b []byte) bool { return string(a) < string(b) })) {
				matchedData[resourceType][i] = true
			}
		}
	}

	for resourceType, matchedSlice := range matchedData {
		for i, matched := range matchedSlice {
			if !matched {
				t.Errorf("file shard contents for %s ResourceType were not found in any shard. want contents: %s", resourceType, wantData[resourceType][i])
				t.Error(matchedData)
			}
		}
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
