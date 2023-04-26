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

package main

import (
	"archive/zip"
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestLoadSynthea(t *testing.T) {
	server := newTestSyntheaServer(t)
	dataDir := t.TempDir()
	syntheaDir := filepath.Join(dataDir, syntheaGroupID, syntheaTimeStamp)

	if err := loadSynthea(server.URL, dataDir, 2); err != nil {
		t.Fatalf("Load returned an error: %v", err)
	}

	var want = []struct {
		name, ndjson string
	}{
		{"Patient_0.ndjson",
			`{"resourceType":"Patient","id":"1"}
{"resourceType":"Patient","id":"2"}`},
		{"Observation_0.ndjson",
			`{"resourceType":"Observation","id":"1"}`},
		{"Encounter_0.ndjson",
			`{"resourceType":"Encounter","id":"1"}
{"resourceType":"Encounter","id":"2"}`},
		{"Encounter_1.ndjson",
			`{"resourceType":"Encounter","id":"3"}`},
	}
	for _, w := range want {
		gotNDJSON, err := os.ReadFile(filepath.Join(syntheaDir, w.name))
		if err != nil {
			t.Errorf("unexpected error reading %s: %v", w.name, err)
		}
		if diff := cmp.Diff(w.ndjson, string(gotNDJSON)); diff != "" {
			t.Errorf("unexpected diff in %s ndjson (-want, +got):\n%s", w.name, diff)
		}
	}

	wantMatches := []string{"Patient_0.ndjson", "Observation_0.ndjson", "Encounter_0.ndjson", "Encounter_1.ndjson"}
	gotMatches, err := filepath.Glob(filepath.Join(syntheaDir, "*"))
	if err != nil {
		t.Errorf("Globbing returned an error: %v", err)
	}
	trans := cmp.Transformer("BaseFileNameAndSort", func(in []string) []string {
		out := make([]string, len(in))
		for i := range in {
			out[i] = filepath.Base(in[i])
		}
		sort.Strings(out)
		return out
	})
	if diff := cmp.Diff(wantMatches, gotMatches, trans); diff != "" {
		t.Errorf("Matched different files and/or directories (-want, +got): \n%s", diff)
	}
}

func newTestSyntheaServer(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var files = []struct {
			name, bundle string
		}{
			{"fhir/patient1.json",
				`{
				"resourceType": "Bundle",
				"type": "transaction",
				"entry": [
					{
						"fullUrl": "fullUrl",
						"resource": {
							"resourceType": "Patient",
							"id": "1"}
					},
					{
						"fullUrl": "fullUrl",
						"resource": {
							"resourceType": "Encounter",
							"id": "1"}
					},
					{
						"fullUrl": "fullUrl",
						"resource": {
							"resourceType": "Observation",
							"id": "1"}
					}
				 ]
			}`},
			{"fhir/patient2.json",
				`{
				"resourceType": "Bundle",
				"type": "transaction",
				"entry": [
					{
						"fullUrl": "fullUrl",
						"resource": {
							"resourceType": "Patient",
							"id": "2"}
					},
					{
						"fullUrl": "fullUrl",
						"resource": {
							"resourceType": "Encounter",
							"id": "2"}
					},
					{
						"fullUrl": "fullUrl",
						"resource": {
							"resourceType": "Encounter",
							"id": "3"}
					}
				 ]
			}`},
		}
		buf := new(bytes.Buffer)
		zw := zip.NewWriter(buf)

		for _, file := range files {
			fw, err := zw.Create(file.name)
			if err != nil {
				t.Fatalf("synthea test server unable to create zip file %s: %v", file.name, err)
			}
			_, err = fw.Write([]byte(file.bundle))
			if err != nil {
				t.Fatalf("synthea test server unable to write to zip file %s: %v", file.name, err)
			}
		}

		if err := zw.Close(); err != nil {
			t.Fatalf("synthea test server returned an error while closing zip writer: %v", err)
		}

		w.Header().Set("Content-Type", "application/zip")
		if _, err := w.Write(buf.Bytes()); err != nil {
			t.Fatalf("synthea test server returned an error while writing http response: %v", err)
		}
	}))

	t.Cleanup(func() { server.Close() })
	return server
}
