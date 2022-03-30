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

package fhir_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/medical_claims_tools/fhir"
	"github.com/google/medical_claims_tools/internal/testhelpers"
)

func TestRectifyBCDA(t *testing.T) {
	cases := []struct {
		name     string
		jsonIn   []byte
		wantJSON []byte
		wantErr  error
	}{
		{
			name:   "ExplanationOfBenefitWithoutProviderAndFocal",
			jsonIn: []byte(`{"resourceType": "ExplanationOfBenefit", "id": "123", "insurance": [{"coverage": {"reference": "coverage"}}]}`),
			wantJSON: []byte(`{` +
				`"id":"123",` +
				`"provider":{"extension":[{"url":"https://g.co/unmapped-by-bcda","valueString":"This is a required FHIR R4 Field, but not mapped by BCDA, which is why we expect it to be empty."}]},` +
				`"insurance": [{"_focal": {"extension":[{"url":"https://g.co/unmapped-by-bcda","valueString":"This is a required FHIR R4 Field, but not mapped by BCDA, which is why we expect it to be empty."}]},"coverage": {"reference": "coverage"},"focal": false}],` +
				`"resourceType":"ExplanationOfBenefit"}`),
			wantErr: nil,
		},
		{
			name:   "ExplanationOfBenefitWithoutProvider",
			jsonIn: []byte(`{"resourceType": "ExplanationOfBenefit", "id": "123", "insurance": [{"coverage": {"reference": "coverage"}, "focal": false}]}`),
			wantJSON: []byte(`{` +
				`"id":"123",` +
				`"provider":{"extension":[{"url":"https://g.co/unmapped-by-bcda","valueString":"This is a required FHIR R4 Field, but not mapped by BCDA, which is why we expect it to be empty."}]},` +
				`"insurance": [{"coverage": {"reference": "coverage"},"focal": false}],` +
				`"resourceType":"ExplanationOfBenefit"}`),
			wantErr: nil,
		},
		{
			name:   "ExplanationOfBenefitWithoutFocal",
			jsonIn: []byte(`{"resourceType": "ExplanationOfBenefit", "id": "123", "provider": {"reference": "provider"}, "insurance": [{"coverage": {"reference": "coverage"}}]}`),
			wantJSON: []byte(`{` +
				`"id":"123",` +
				`"provider": {"reference": "provider"},` +
				`"insurance": [{"_focal": {"extension":[{"url":"https://g.co/unmapped-by-bcda","valueString":"This is a required FHIR R4 Field, but not mapped by BCDA, which is why we expect it to be empty."}]},"coverage": {"reference": "coverage"},"focal": false}],` +
				`"resourceType":"ExplanationOfBenefit"}`),
			wantErr: nil,
		},
		{
			name:     "ExplanationOfBenefitFullyPopulated",
			jsonIn:   []byte(`{"resourceType": "ExplanationOfBenefit", "id": "123", "provider":{"reference": "123"}, "insurance": [{"coverage": {"reference": "coverage"}, "focal": false}], "item": [{"sequence": 1, "productOrService": {"text": "test"}}]}`),
			wantJSON: []byte(`{"resourceType": "ExplanationOfBenefit", "id": "123", "provider":{"reference": "123"}, "insurance": [{"coverage": {"reference": "coverage"}, "focal": false}], "item": [{"sequence": 1, "productOrService": {"text": "test"}}]}`),
			wantErr:  nil,
		},
		{
			name:     "ExplanationOfBenefitWithItemsMissingProductOrService",
			jsonIn:   []byte(`{"resourceType": "ExplanationOfBenefit", "id": "123", "provider":{"reference": "123"}, "insurance": [{"coverage": {"reference": "coverage"}, "focal": false}], "item": [{"sequence": 1}]}`),
			wantJSON: []byte(`{"resourceType": "ExplanationOfBenefit", "id": "123", "provider":{"reference": "123"}, "insurance": [{"coverage": {"reference": "coverage"}, "focal": false}], "item": [{"sequence": 1, "productOrService": {"extension":[{"url":"https://g.co/unmapped-by-bcda","valueString":"This is a required FHIR R4 Field, but not mapped by BCDA, which is why we expect it to be empty."}]}}]}`),
			wantErr:  nil,
		},
		{
			name:     "Patient",
			jsonIn:   []byte(`{"resourceType": "Patient", "id": "123"}`),
			wantJSON: []byte(`{"resourceType": "Patient", "id": "123"}`),
			wantErr:  nil,
		},
		{
			name:     "SimpleCoverage",
			jsonIn:   []byte(`{"resourceType": "Coverage", "id": "123"}`),
			wantJSON: []byte(`{"resourceType": "Coverage", "id": "123"}`),
			wantErr:  nil,
		},
		{
			name:     "CoverageWithReplacableContract",
			jsonIn:   []byte(`{"resourceType": "Coverage", "id": "123", "contract": [{"reference": "Coverage/part-a-contract1"}]}`),
			wantJSON: []byte(`{"resourceType": "Coverage", "id": "123", "contract": [{"reference": "Contract/part-a-contract1"}]}`),
			wantErr:  nil,
		},
		{
			name:     "CoverageWithUnreplacableContract",
			jsonIn:   []byte(`{"resourceType": "Coverage", "id": "123", "contract": [{"reference": "Coverage/something-else"}]}`),
			wantJSON: []byte(`{"resourceType": "Coverage", "id": "123", "contract": [{"reference": "Coverage/something-else"}]}`),
			wantErr:  nil,
		},
	}

	for _, tc := range cases {
		got, err := fhir.RectifyBCDA(tc.jsonIn)
		if err != tc.wantErr {
			t.Errorf("RectifyBCDA(%s) unexpected error. got: %v, want: %v", tc.jsonIn, err, tc.wantErr)
		}
		normalizedWantJSON := testhelpers.NormalizeJSON(t, tc.wantJSON)
		normalizedGotJSON := testhelpers.NormalizeJSON(t, got)
		if !cmp.Equal(normalizedGotJSON, normalizedWantJSON) {
			t.Errorf("RectifyBCDA(%s) unexpected output. got: %s, want: %s", tc.jsonIn, normalizedGotJSON, normalizedWantJSON)
		}
	}
}
