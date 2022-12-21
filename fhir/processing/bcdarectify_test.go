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
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/medical_claims_tools/fhir/processing"
	"github.com/google/medical_claims_tools/internal/testhelpers"

	cpb "github.com/google/fhir/go/proto/google/fhir/proto/r4/core/codes_go_proto"
)

func TestBcdaRectifyProcessor(t *testing.T) {
	cases := []struct {
		name         string
		resourceType cpb.ResourceTypeCode_Value
		jsonIn       []byte
		wantJSON     []byte
	}{
		{
			name:         "ExplanationOfBenefitWithoutProviderAndFocal",
			resourceType: cpb.ResourceTypeCode_EXPLANATION_OF_BENEFIT,
			jsonIn:       []byte(`{"resourceType": "ExplanationOfBenefit", "id": "123", "insurance": [{"coverage": {"reference": "coverage"}}]}`),
			wantJSON: []byte(`{` +
				`"id":"123",` +
				`"provider":{"extension":[{"url":"https://g.co/unmapped-by-bcda","valueString":"This is a required FHIR R4 Field, but not mapped by BCDA, which is why we expect it to be empty."}]},` +
				`"insurance": [{"_focal": {"extension":[{"url":"https://g.co/unmapped-by-bcda","valueString":"This is a required FHIR R4 Field, but not mapped by BCDA, which is why we expect it to be empty."}]},"coverage": {"reference": "coverage"},"focal": false}],` +
				`"resourceType":"ExplanationOfBenefit"}`),
		},
		{
			name:         "ExplanationOfBenefitWithoutProvider",
			resourceType: cpb.ResourceTypeCode_EXPLANATION_OF_BENEFIT,
			jsonIn:       []byte(`{"resourceType": "ExplanationOfBenefit", "id": "123", "insurance": [{"coverage": {"reference": "coverage"}, "focal": false}]}`),
			wantJSON: []byte(`{` +
				`"id":"123",` +
				`"provider":{"extension":[{"url":"https://g.co/unmapped-by-bcda","valueString":"This is a required FHIR R4 Field, but not mapped by BCDA, which is why we expect it to be empty."}]},` +
				`"insurance": [{"coverage": {"reference": "coverage"},"focal": false}],` +
				`"resourceType":"ExplanationOfBenefit"}`),
		},
		{
			name:         "ExplanationOfBenefitWithoutFocal",
			resourceType: cpb.ResourceTypeCode_EXPLANATION_OF_BENEFIT,
			jsonIn:       []byte(`{"resourceType": "ExplanationOfBenefit", "id": "123", "provider": {"reference": "provider"}, "insurance": [{"coverage": {"reference": "coverage"}}]}`),
			wantJSON: []byte(`{` +
				`"id":"123",` +
				`"provider": {"reference": "provider"},` +
				`"insurance": [{"_focal": {"extension":[{"url":"https://g.co/unmapped-by-bcda","valueString":"This is a required FHIR R4 Field, but not mapped by BCDA, which is why we expect it to be empty."}]},"coverage": {"reference": "coverage"},"focal": false}],` +
				`"resourceType":"ExplanationOfBenefit"}`),
		},
		{
			name:         "ExplanationOfBenefitFullyPopulated",
			resourceType: cpb.ResourceTypeCode_EXPLANATION_OF_BENEFIT,
			jsonIn:       []byte(`{"resourceType": "ExplanationOfBenefit", "id": "123", "provider":{"reference": "123"}, "insurance": [{"coverage": {"reference": "coverage"}, "focal": false}], "item": [{"sequence": 1, "productOrService": {"text": "test"}}]}`),
			wantJSON:     []byte(`{"resourceType": "ExplanationOfBenefit", "id": "123", "provider":{"reference": "123"}, "insurance": [{"coverage": {"reference": "coverage"}, "focal": false}], "item": [{"sequence": 1, "productOrService": {"text": "test"}}]}`),
		},
		{
			name:         "ExplanationOfBenefitWithItemsMissingProductOrService",
			resourceType: cpb.ResourceTypeCode_EXPLANATION_OF_BENEFIT,
			jsonIn:       []byte(`{"resourceType": "ExplanationOfBenefit", "id": "123", "provider":{"reference": "123"}, "insurance": [{"coverage": {"reference": "coverage"}, "focal": false}], "item": [{"sequence": 1}]}`),
			wantJSON:     []byte(`{"resourceType": "ExplanationOfBenefit", "id": "123", "provider":{"reference": "123"}, "insurance": [{"coverage": {"reference": "coverage"}, "focal": false}], "item": [{"sequence": 1, "productOrService": {"extension":[{"url":"https://g.co/unmapped-by-bcda","valueString":"This is a required FHIR R4 Field, but not mapped by BCDA, which is why we expect it to be empty."}]}}]}`),
		},
		{
			name:         "Patient",
			resourceType: cpb.ResourceTypeCode_PATIENT,
			jsonIn:       []byte(`{"resourceType": "Patient", "id": "123"}`),
			wantJSON:     []byte(`{"resourceType": "Patient", "id": "123"}`),
		},
		{
			name:         "SimpleCoverage",
			resourceType: cpb.ResourceTypeCode_COVERAGE,
			jsonIn:       []byte(`{"resourceType": "Coverage", "id": "123"}`),
			wantJSON:     []byte(`{"resourceType": "Coverage", "id": "123"}`),
		},
		{
			name:         "CoverageWithReplacableContract",
			resourceType: cpb.ResourceTypeCode_COVERAGE,
			jsonIn:       []byte(`{"resourceType": "Coverage", "id": "123", "contract": [{"reference": "Coverage/part-a-contract1"}]}`),
			wantJSON:     []byte(`{"resourceType": "Coverage", "id": "123", "contract": [{"reference": "Contract/part-a-contract1"}]}`),
		},
		{
			name:         "CoverageWithUnreplacableContract",
			resourceType: cpb.ResourceTypeCode_COVERAGE,
			jsonIn:       []byte(`{"resourceType": "Coverage", "id": "123", "contract": [{"reference": "Coverage/something-else"}]}`),
			wantJSON:     []byte(`{"resourceType": "Coverage", "id": "123", "contract": [{"reference": "Coverage/something-else"}]}`),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ts := &processing.TestSink{}
			p, err := processing.NewPipeline([]processing.Processor{processing.NewBCDARectifyProcessor()}, []processing.Sink{ts})
			if err != nil {
				t.Fatalf("NewPipeline() returned unexpected error: %v", err)
			}
			if err := p.Process(context.Background(), tc.resourceType, "", tc.jsonIn); err != nil {
				t.Fatalf("pipeline.Process(..., %s) returned unexpected error: %v", tc.jsonIn, err)
			}
			gotJSON, err := ts.WrittenResources[0].JSON()
			if err != nil {
				t.Fatalf("writtenResource.JSON() returned unexpected error: %v", err)
			}
			normalizedWantJSON := testhelpers.NormalizeJSON(t, tc.wantJSON)
			normalizedGotJSON := testhelpers.NormalizeJSON(t, gotJSON)
			if !cmp.Equal(normalizedGotJSON, normalizedWantJSON) {
				t.Errorf("pipeline.Process(..., %s) produced unexpected output. got: %s, want: %s", tc.jsonIn, normalizedGotJSON, normalizedWantJSON)
			}
		})
	}
}
