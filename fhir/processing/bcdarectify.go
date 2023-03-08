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

package processing

import (
	"context"
	"errors"

	cpb "github.com/google/fhir/go/proto/google/fhir/proto/r4/core/codes_go_proto"
	dpb "github.com/google/fhir/go/proto/google/fhir/proto/r4/core/datatypes_go_proto"
	"github.com/google/medical_claims_tools/internal/metrics"
)

var fhirRectifyCounter *metrics.Counter = metrics.NewCounter("fhir-rectify-counter", "Count of FHIR Resources that do not meet the base R4 FHIR expectations and need to be rectified. The counter is tagged by the FHIR Resource type ex) OBSERVATION and type of rectification ex) MISSING_PROVIDER_REFERENCE.", "1", "FHIRResourceType", "RectificationType")

type bcdaRectifyProcessor struct {
	BaseProcessor
}

// Assert bcdaRectifyProcessor satisfies the Processor interface.
var _ Processor = &bcdaRectifyProcessor{}

// NewBCDARectifyProcessor creates a Processor which takes BCDA derived FHIR
// resources, and attempts to rectify them to fix known issues in source mapping
// (in the ways described below). This is a temporary, non-ideal, and minimalist
// approach, which aims to make the FHIR compatible with base R4 expectations so
// that otherwise useful data can be easily uploaded to FHIR store with
// validation for other areas still intact.
func NewBCDARectifyProcessor() Processor {
	return &bcdaRectifyProcessor{}
}

func (brp *bcdaRectifyProcessor) Process(ctx context.Context, resource ResourceWrapper) error {
	switch resource.Type() {
	case cpb.ResourceTypeCode_COVERAGE:
		return brp.rectifyCoverage(ctx, resource)
	case cpb.ResourceTypeCode_EXPLANATION_OF_BENEFIT:
		return brp.rectifyExplanationOfBenefit(ctx, resource)
	default:
		return brp.Output(ctx, resource)
	}
}

func (brp *bcdaRectifyProcessor) rectifyExplanationOfBenefit(ctx context.Context, resource ResourceWrapper) error {
	proto, err := resource.Proto()
	if err != nil {
		return err
	}
	e := proto.GetExplanationOfBenefit()
	if e == nil {
		return errors.New("resource was not ExplanationOfBenefit")
	}

	// BCDA ExplanationOfBenefits don't have provider references mapped, which is
	// required by base FHIR R4. So, we put in an extension explaining that this
	// field is unmapped by the source data if we see the Provider isn't set at
	// all. If the message exists at all, we don't override it at this time.
	if e.Provider == nil {
		e.Provider = &dpb.Reference{
			Extension: []*dpb.Extension{getUnmappedExtension()},
		}
		if err := fhirRectifyCounter.Record(ctx, 1, cpb.ResourceTypeCode_EXPLANATION_OF_BENEFIT.String(), "MISSING_PROVIDER_REFERENCE"); err != nil {
			return err
		}
	}

	// Focal is also unset by BCDA, so we mark it with an extension too if not
	// present.
	for _, i := range e.GetInsurance() {
		if i.Focal == nil {
			i.Focal = &dpb.Boolean{Extension: []*dpb.Extension{getUnmappedExtension()}}
			if err := fhirRectifyCounter.Record(ctx, 1, cpb.ResourceTypeCode_EXPLANATION_OF_BENEFIT.String(), "FOCAL_UNSET"); err != nil {
				return err
			}
		}
	}

	// Sometimes productOrService is unset in ExplanationOfBenefit.item[x] (but is
	// required by FHIR) so we mark it with the extension if not present.
	for _, i := range e.GetItem() {
		if i.ProductOrService == nil {
			i.ProductOrService = &dpb.CodeableConcept{Extension: []*dpb.Extension{getUnmappedExtension()}}
			if err := fhirRectifyCounter.Record(ctx, 1, cpb.ResourceTypeCode_EXPLANATION_OF_BENEFIT.String(), "PRODUCT_OR_SERVICE_UNSET"); err != nil {
				return err
			}
		}
	}

	return brp.Output(ctx, resource)
}

func (brp *bcdaRectifyProcessor) rectifyCoverage(ctx context.Context, resource ResourceWrapper) error {
	proto, err := resource.Proto()
	if err != nil {
		return err
	}
	cov := proto.GetCoverage()
	if cov == nil {
		return errors.New("resource was not ExplanationOfBenefit")
	}
	// BCDA Coverage resources have invalid Coverage.contract references that
	// appear to be placeholders (they reference other Coverages instead of other
	// Contracts, which leads to a validation failure). We look for a specific
	// reference that we expect to be a placeholder, and if it exists, replace it
	// with a similar placeholder (except that it's a Contract/ reference instead
	// of a Coverage/ reference). More details at b/175394994#comment24.
	for _, contract := range cov.GetContract() {
		if contract.GetCoverageId().GetValue() == "part-a-contract1" {
			// We only try to correct this placeholder reference for safety. We
			// replace the Coverage reference with a Contract reference with the same
			// value.
			contract.Reference = &dpb.Reference_ContractId{&dpb.ReferenceId{Value: "part-a-contract1"}}
			if err := fhirRectifyCounter.Record(ctx, 1, cpb.ResourceTypeCode_COVERAGE.String(), "PLACEHOLDER_COVERAGE_REFERENCE"); err != nil {
				return err
			}
		}
	}

	return brp.Output(ctx, resource)
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
