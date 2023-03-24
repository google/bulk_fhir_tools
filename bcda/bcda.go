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

// Package bcda contains helpers to create a bulkfhir.Client for interacting
// with Medicare's Beneficiary Claims Data API (BCDA).
package bcda

import (
	cpb "github.com/google/fhir/go/proto/google/fhir/proto/r4/core/codes_go_proto"
)

// ResourceTypes represents the set of resource types used by BCDA.
var ResourceTypes = []cpb.ResourceTypeCode_Value{
	cpb.ResourceTypeCode_PATIENT,
	cpb.ResourceTypeCode_COVERAGE,
	cpb.ResourceTypeCode_EXPLANATION_OF_BENEFIT,
}
