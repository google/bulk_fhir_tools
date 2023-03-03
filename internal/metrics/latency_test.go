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
)

func TestLatencyLocal(t *testing.T) {
	ResetAll()

	lTags := NewLatency("LatencyTags", "Latency Description", "ms", []float64{0, 3, 5}, "FHIRResource")
	lTags.Record(context.Background(), 1, "OBSERVATION")
	lTags.Record(context.Background(), 1, "OBSERVATION")
	lTags.Record(context.Background(), 3, "ENCOUNTER")

	lNoTags := NewLatency("LatencyNoTags", "Latency Description", "ms", []float64{0, 3, 5})
	lNoTags.Record(context.Background(), 1)
	lNoTags.Record(context.Background(), 3)

	_ = NewLatency("LatencyCloseWithNoRecord", "Latency Description", "ms", []float64{0, 3, 5}, "FHIRResource")

	wantLatency := map[string]LatencyResult{
		"LatencyTags": {
			Dist:        map[string][]int{"OBSERVATION": []int{0, 2, 0, 0}, "ENCOUNTER": []int{0, 0, 1, 0}},
			Name:        "LatencyTags",
			Description: "Latency Description",
			Unit:        "ms",
			Buckets:     []float64{0, 3, 5},
			TagKeys:     []string{"FHIRResource"},
		},
		"LatencyNoTags": {
			Dist:        map[string][]int{"LatencyNoTags": []int{0, 1, 1, 0}},
			Name:        "LatencyNoTags",
			Description: "Latency Description",
			Unit:        "ms",
			Buckets:     []float64{0, 3, 5},
		},
		"LatencyCloseWithNoRecord": {
			Dist:        map[string][]int{},
			Name:        "LatencyCloseWithNoRecord",
			Description: "Latency Description",
			Unit:        "ms",
			Buckets:     []float64{0, 3, 5},
			TagKeys:     []string{"FHIRResource"},
		},
	}

	_, gotLatency, err := GetResults()
	if err != nil {
		t.Fatalf("GetResults failed; err = %s", err)
	}
	if diff := cmp.Diff(wantLatency, gotLatency); diff != "" {
		t.Errorf("getResults() return unexpected latency (-want +got): \n%s", diff)
	}
}
