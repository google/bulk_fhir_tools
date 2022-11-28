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
	"testing"
)

// NormalizeJSON normalizes the input json bytes to look like how it would look
// as if marshaled from a json.Marshal. In particular, this may reorder some
// fields (e.g. json object keys are sorted alphabetically), but the json should
// be equivalent.
func NormalizeJSON(t *testing.T, jsonIn []byte) []byte {
	t.Helper()
	var tmp interface{}
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
