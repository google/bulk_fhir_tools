// Copyright 2021 Google LLC
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

package fhir_test

import (
	"bytes"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	"github.com/google/medical_claims_tools/fhir"

	dpb "github.com/google/fhir/go/proto/google/fhir/proto/stu3/datatypes_go_proto"
	rpb "github.com/google/fhir/go/proto/google/fhir/proto/stu3/resources_go_proto"
)

func TestUnmarshalR3(t *testing.T) {
	json := `{"resourceType":"Patient", "id": "exampleID1"}
	{"resourceType":"Patient", "id": "exampleID2"}
	{"resourceType":"Patient", "id": "exampleID3"}`

	expectedResults := []*rpb.ContainedResource{
		{
			OneofResource: &rpb.ContainedResource_Patient{
				Patient: &rpb.Patient{
					Id: &dpb.Id{Value: "exampleID1"},
				},
			},
		},
		{
			OneofResource: &rpb.ContainedResource_Patient{
				Patient: &rpb.Patient{
					Id: &dpb.Id{Value: "exampleID2"},
				},
			},
		},
		{
			OneofResource: &rpb.ContainedResource_Patient{
				Patient: &rpb.Patient{
					Id: &dpb.Id{Value: "exampleID3"},
				},
			},
		}}

	jsonReader := bytes.NewReader([]byte(json))
	resultChan, err := fhir.UnmarshalR3(jsonReader)
	if err != nil {
		t.Fatalf("UnmarshalR3(%s) unexpected error: %v", json, err)
	}

	results := make([]*rpb.ContainedResource, 0, len(expectedResults))
	for r := range resultChan {
		if r.Error != nil {
			t.Errorf("UnmarshalR3(%s) unexpected error when receiving from output channel: %v", json, err)
		} else {
			results = append(results, r.ContainedResource)
		}
	}
	if diff := cmp.Diff(expectedResults, results, protocmp.Transform()); diff != "" {
		t.Errorf("UnmarshalR3(%s) results returned unexpected diff (-want +got):\n%s", json, diff)
	}
}
