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

package metrics

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/bulk_fhir_tools/internal/metrics/aggregation"
)

func TestCounterLocal(t *testing.T) {
	ResetAll()

	cTags := NewCounter("CounterTags", "Counter Description", "1", aggregation.Count, "FHIRResource")
	cTags.Record(context.Background(), 1, "OBSERVATION")
	cTags.Record(context.Background(), 1, "OBSERVATION")
	cTags.Record(context.Background(), 3, "ENCOUNTER")

	cNoTags := NewCounter("CounterNoTags", "Counter Description", "1", aggregation.Count)
	cNoTags.Record(context.Background(), 1)
	cNoTags.Record(context.Background(), 3)

	cMaxValue := NewCounter("CounterMaxValue", "Counter Description", "1", aggregation.LastValueInGCPMaxValueInLocal)
	cMaxValue.Record(context.Background(), 8)
	cMaxValue.Record(context.Background(), 2)

	_ = NewCounter("CounterCloseWithNoRecord", "Counter Description", "1", aggregation.Count, "FHIRResource")

	wantCount := map[string]CounterResult{
		"CounterCloseWithNoRecord": {
			Count:       map[string]int64{},
			Name:        "CounterCloseWithNoRecord",
			Description: "Counter Description",
			Unit:        "1",
			Aggregation: aggregation.Count,
			TagKeys:     []string{"FHIRResource"},
		},
		"CounterMaxValue": {
			Count:       map[string]int64{"CounterMaxValue": 8},
			Name:        "CounterMaxValue",
			Description: "Counter Description",
			Unit:        "1",
			Aggregation: aggregation.LastValueInGCPMaxValueInLocal,
		},
		"CounterNoTags": {
			Count:       map[string]int64{"CounterNoTags": 4},
			Name:        "CounterNoTags",
			Description: "Counter Description",
			Unit:        "1",
			Aggregation: aggregation.Count,
		},
		"CounterTags": {
			Count:       map[string]int64{"ENCOUNTER": 3, "OBSERVATION": 2},
			Name:        "CounterTags",
			Description: "Counter Description",
			Unit:        "1",
			Aggregation: aggregation.Count,
			TagKeys:     []string{"FHIRResource"},
		},
	}

	gotCount, _, err := GetResults()
	if err != nil {
		t.Fatalf("GetResults failed; err = %s", err)
	}
	if diff := cmp.Diff(wantCount, gotCount); diff != "" {
		t.Errorf("getResults() return unexpected count (-want +got): \n%s", diff)
	}
}
