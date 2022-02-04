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
 
package fhir

import (
	"github.com/google/fhir/go/fhirversion"
	"github.com/google/fhir/go/jsonformat"
	dpb "github.com/google/fhir/go/proto/google/fhir/proto/r4/core/datatypes_go_proto"
	cpb "github.com/google/fhir/go/proto/google/fhir/proto/r4/core/resources/coverage_go_proto"
	eobpb "github.com/google/fhir/go/proto/google/fhir/proto/r4/core/resources/explanation_of_benefit_go_proto"
	"google.golang.org/protobuf/proto"
)

// RectifyBCDA takes an input BCDA derived FHIR JSON resource, and attempts to
// rectify it to fix known issues in source mapping (in the ways described
// below). This is a temporary, non-ideal, and minimalist approach, aimed to
// make the FHIR compatible with base R4 expectations so otherwise useful data
// can be easily uploaded to FHIR store with validation for other areas still
// intact.
func RectifyBCDA(jsonResource []byte) (jsonOut []byte, err error) {
	// TODO(b/202187231): consider checking the JSON to only unmarshal
	// ExplanationOfBenefit.
	un, err := jsonformat.NewUnmarshallerWithoutValidation("UTC", fhirversion.R4)
	if err != nil {
		return jsonResource, err
	}

	resource, err := un.UnmarshalR4(jsonResource)
	if err != nil {
		return jsonResource, err
	}

	if eob := resource.GetExplanationOfBenefit(); eob != nil {
		return rectifyExplanationOfBenefit(eob)
	} else if cov := resource.GetCoverage(); cov != nil {
		return rectifyCoverage(cov)
	}

	return jsonResource, nil
}

func rectifyExplanationOfBenefit(e *eobpb.ExplanationOfBenefit) (jsonOut []byte, err error) {
	// BCDA ExplanationOfBenefits don't have provider references mapped, which is
	// required by base FHIR R4. So, we put in an extension explaining that this
	// field is unmapped by the source data if we see the Provider isn't set at
	// all. If the message exists at all, we don't override it at this time.
	if e.Provider == nil {
		e.Provider = &dpb.Reference{
			Extension: []*dpb.Extension{getUnmappedExtension()},
		}
	}

	// Focal is also unset by BCDA, so we mark it with an extension too if not
	// present.
	for _, i := range e.GetInsurance() {
		if i.Focal == nil {
			i.Focal = &dpb.Boolean{Extension: []*dpb.Extension{getUnmappedExtension()}}
		}
	}

	// Sometimes productOrService is unset in ExplanationOfBenefit.item[x] (but is
	// required by FHIR) so we mark it with the extension if not present.
	for _, i := range e.GetItem() {
		if i.ProductOrService == nil {
			i.ProductOrService = &dpb.CodeableConcept{Extension: []*dpb.Extension{getUnmappedExtension()}}
		}
	}

	return marshalResource(e)
}

func rectifyCoverage(cov *cpb.Coverage) (jsonOut []byte, err error) {
	// BCDA Coverage resources have invalid Coverage.contract references that
	// appear to be placeholders (they reference other Coverages instead of other
	// Contracts, which leads to a validation failure). We look for a specific
	// reference that we expect to be a placeholder, and if it exists, replace it
	// with a similar placeholder (excpet that it's a Contract/ reference instead
	// of a Coverage/ reference). More details at b/175394994#comment24.
	for _, contract := range cov.GetContract() {
		if contract.GetCoverageId().GetValue() == "part-a-contract1" {
			// We only try to correct this placeholder reference for safety. We
			// replace the Coverage reference with a Contract reference with the same
			// value.
			contract.Reference = &dpb.Reference_ContractId{&dpb.ReferenceId{Value: "part-a-contract1"}}
		}
	}

	return marshalResource(cov)
}

func marshalResource(msg proto.Message) ([]byte, error) {
	m, err := jsonformat.NewMarshaller(false, "", "", fhirversion.R4)
	if err != nil {
		return nil, err
	}
	return m.MarshalResource(msg)
}

func getUnmappedExtension() *dpb.Extension {
	return &dpb.Extension{
		// TODO(b/175394994): figure out the right extension URI we want to use
		// before release, and if we want to create an extension profile for it.
		Url: &dpb.Uri{Value: "https://g.co/unmapped-by-bcda"},
		Value: &dpb.Extension_ValueX{
			Choice: &dpb.Extension_ValueX_StringValue{
				StringValue: &dpb.String{Value: "This is a required FHIR R4 Field, but not mapped by BCDA, which is why we expect it to be empty."},
			},
		},
	}
}
