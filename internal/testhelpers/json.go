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
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// NormalizeJSON normalizes the input json bytes to look like how it would look
// as if marshaled from a json.Marshal. In particular, this may reorder some
// fields (e.g. json object keys are sorted alphabetically), but the json should
// be equivalent.
func NormalizeJSON(t *testing.T, jsonIn []byte) []byte {
	t.Helper()
	var tmp any
	err := json.Unmarshal(jsonIn, &tmp)
	if err != nil {
		t.Fatal(err)
	}
	output, err := json.Marshal(tmp)
	if err != nil {
		t.Fatal(err)
	}
	return output
}

// NormalizeJSONString normalizes the input json to look how it would look if
// marshaled from a json.Marshal.
func NormalizeJSONString(t *testing.T, jsonIn string) string {
	return string(NormalizeJSON(t, []byte(jsonIn)))
}

// ReadAllFHIRJSON reads all ndjsons in the output directory, extracts out the FHIR json for each
// resource, and adds it to the output [][]byte. If normalize=true, then NormalizeJSON is applied to
// the json bytes before being added to the output.
func ReadAllFHIRJSON(t *testing.T, outputDir string, normalize bool) [][]byte {
	t.Helper()
	files, err := os.ReadDir(outputDir)
	if err != nil {
		t.Fatalf("unable to read directory %s: %v", outputDir, err)
	}

	fullData := make([][]byte, 0)

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".ndjson") {
			continue
		}
		fullPath := filepath.Join(outputDir, file.Name())
		gotData, err := os.ReadFile(fullPath)
		if err != nil {
			t.Errorf("could not read %s: %v", fullPath, err)
		}
		dataLines := bytes.Split(gotData, []byte("\n"))

		for _, line := range dataLines {
			if len(line) == 0 {
				continue
			}
			line = bytes.TrimSuffix(line, []byte("\n"))
			if normalize {
				line = NormalizeJSON(t, line)
			}
			fullData = append(fullData, line)
		}
	}
	return fullData
}
