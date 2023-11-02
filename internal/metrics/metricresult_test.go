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
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/bulk_fhir_tools/internal/metrics/aggregation"
)

func TestCounterResultWithTag(t *testing.T) {
	c := CounterResult{Count: map[string]int64{"OBSERVATION": 4, "ENCOUNTER": 18}, Name: "CounterName", Description: "Descriptive Description", Unit: "1", Aggregation: aggregation.LastValueInGCPMaxValueInLocal, TagKeys: []string{"FHIRResource"}}
	want := "\nName: CounterName\nDescriptive Description\nUnits: 1\nAggregation Type: Max value recorded by the metric (for counters logged locally).\nENCOUNTER: 18\nOBSERVATION: 4\n"
	got := c.String()
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Counter.String() return unexpected diff (-want +got): \n%s", diff)
	}
}

func TestCounterResultWithoutTag(t *testing.T) {
	c := CounterResult{Count: map[string]int64{"CounterName": 4}, Name: "CounterName", Description: "Descriptive Description", Unit: "1", Aggregation: aggregation.Count, TagKeys: []string{}}
	want := "\nName: CounterName\nDescriptive Description\nUnits: 1\nAggregation Type: Total sum of values recorded by this metric (for counters logged locally).\nCount: 4\n"
	got := c.String()
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Counter.String() return unexpected diff (-want +got): \n%s", diff)
	}
}

func TestLatencyResultWithTag(t *testing.T) {
	l := LatencyResult{Dist: map[string][]int{"OBSERVATION": []int{1, 3, 0}, "ENCOUNTER": []int{4, 0, 0}}, Name: "LatencyName", Description: "Descriptive Description", Unit: "ms", Buckets: []float64{0, 3, 5}, TagKeys: []string{"FHIRResource"}}
	want := "\nName: LatencyName\nDescriptive Description\nUnits: ms\nBuckets: <0.00, <3.00, <5.00, >= all\nENCOUNTER: [4 0 0]\nOBSERVATION: [1 3 0]\n"
	got := l.String()
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Latency.String() return unexpected diff (-want +got): \n%s", diff)
	}
}

func TestLatencyResultWithoutTag(t *testing.T) {
	l := LatencyResult{Dist: map[string][]int{"LatencyName": []int{4, 0, 0}}, Name: "LatencyName", Description: "Descriptive Description", Unit: "ms", Buckets: []float64{0, 3, 5}, TagKeys: []string{}}
	want := "\nName: LatencyName\nDescriptive Description\nUnits: ms\nBuckets: <0.00, <3.00, <5.00, >= all\nDist: [4 0 0]\n"
	got := l.String()
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Latency.String() return unexpected diff (-want +got): \n%s", diff)
	}
}
