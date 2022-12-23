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
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/medical_claims_tools/fhir/processing"
	"github.com/google/medical_claims_tools/internal/testhelpers"

	cpb "github.com/google/fhir/go/proto/google/fhir/proto/r4/core/codes_go_proto"
)

type ndjsonTestdata struct {
	resourceType cpb.ResourceTypeCode_Value
	sourceURL    string
	data         string
}

func TestNDJSONSink(t *testing.T) {
	ctx := context.Background()
	testdata := []ndjsonTestdata{
		{cpb.ResourceTypeCode_ACCOUNT, "url1", "foo"},
		{cpb.ResourceTypeCode_ACCOUNT, "url1", "bar"},
		{cpb.ResourceTypeCode_ACCOUNT, "url2", "baz"},
		{cpb.ResourceTypeCode_PATIENT, "url3", "qux"},
	}
	wantData := [][]byte{
		[]byte("foo\nbar\n"),
		[]byte("baz\n"),
		[]byte("qux\n"),
	}
	for _, tc := range []struct {
		description   string
		filePrefix    string
		wantFileNames []string
	}{
		{
			description: "no prefix",
			wantFileNames: []string{
				"Account_0.ndjson",
				"Account_1.ndjson",
				"Patient_0.ndjson",
			},
		},
		{
			description: "with prefix",
			filePrefix:  "prefix",
			wantFileNames: []string{
				"prefix_Account_0.ndjson",
				"prefix_Account_1.ndjson",
				"prefix_Patient_0.ndjson",
			},
		},
	} {
		t.Run(tc.description, func(t *testing.T) {
			tempdir, err := os.MkdirTemp("", "")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(tempdir)
			sink, err := processing.NewNDJSONSink(ctx, tempdir, tc.filePrefix)
			if err != nil {
				t.Fatal(err)
			}
			p, err := processing.NewPipeline(nil, []processing.Sink{sink})
			if err != nil {
				t.Fatal(err)
			}
			for _, td := range testdata {
				if err := p.Process(ctx, td.resourceType, td.sourceURL, []byte(td.data)); err != nil {
					t.Fatal(err)
				}
			}
			if err := p.Finalize(ctx); err != nil {
				t.Fatal(err)
			}
			if entries, err := os.ReadDir(tempdir); err != nil {
				t.Fatal(err)
			} else if len(entries) != 3 {
				t.Errorf("directory %s contained %d entries; want 3", tempdir, len(entries))
			}
			for i, fn := range tc.wantFileNames {
				fullPath := filepath.Join(tempdir, fn)
				gotData, err := os.ReadFile(fullPath)
				if err != nil {
					t.Errorf("could not read %s: %v", fullPath, err)
				} else if !cmp.Equal(gotData, wantData[i]) {
					t.Errorf("wrong contents in %s; got %q; want %q", fullPath, gotData, wantData[i])
				}
			}
		})
	}
}

// Note: the logic for the GCS variant is mostly the same as for the local file
// variant, so this test is kept much simpler.
func TestGCSNDJSONSink(t *testing.T) {
	ctx := context.Background()

	patient1 := []byte(`{"resourceType":"Patient","id":"PatientID1"}`)
	bucketName := "bucket"
	directory := "directory"
	prefix := "prefix"

	gcsServer := testhelpers.NewGCSServer(t)

	sink, err := processing.NewGCSNDJSONSink(ctx, gcsServer.URL(), bucketName, directory, prefix)
	if err != nil {
		t.Fatal(err)
	}
	p, err := processing.NewPipeline(nil, []processing.Sink{sink})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Process(ctx, cpb.ResourceTypeCode_PATIENT, "url1", patient1); err != nil {
		t.Fatal(err)
	}
	if err := p.Finalize(ctx); err != nil {
		t.Fatal(err)
	}

	objName := directory + "/" + prefix + "_Patient_0.ndjson"
	obj, ok := gcsServer.GetObject(bucketName, objName)
	if !ok {
		t.Fatalf("gs://%s/%s not found", bucketName, objName)
	}
	if !bytes.Contains(obj.Data, patient1) {
		t.Errorf("gcs server unexpected data: got: %s, want: %s", obj.Data, patient1)
	}
}
