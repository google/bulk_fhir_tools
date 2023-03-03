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
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestMetrics(t *testing.T) {
	cTags := NewCounter("CounterTags", "Counter Description", "1", "FHIRResource")
	cTags.Record(context.Background(), 1, "OBSERVATION")
	cTags.Record(context.Background(), 1, "OBSERVATION")
	cTags.Record(context.Background(), 3, "ENCOUNTER")

	cNoTags := NewCounter("CounterNoTags", "Counter Description", "1")
	cNoTags.Record(context.Background(), 1)
	cNoTags.Record(context.Background(), 3)

	lTags := NewLatency("LatencyTags", "Latency Description", "ms", []float64{0, 3, 5}, "FHIRResource")
	lTags.Record(context.Background(), 1, "OBSERVATION")
	lTags.Record(context.Background(), 1, "OBSERVATION")
	lTags.Record(context.Background(), 3, "ENCOUNTER")

	lNoTags := NewLatency("LatencyNoTags", "Latency Description", "ms", []float64{0, 3, 5})
	lNoTags.Record(context.Background(), 1)
	lNoTags.Record(context.Background(), 3)

	_ = NewCounter("CounterCloseWithNoRecord", "Counter Description", "1", "FHIRResource")
	_ = NewLatency("LatencyCloseWithNoRecord", "Latency Description", "ms", []float64{0, 3, 5}, "FHIRResource")

	wantCount := map[string]CounterResult{
		"CounterCloseWithNoRecord": {
			Count:       map[string]int64{},
			Name:        "CounterCloseWithNoRecord",
			Description: "Counter Description",
			Unit:        "1",
			TagKeys:     []string{"FHIRResource"},
		},
		"CounterNoTags": {
			Count:       map[string]int64{"CounterNoTags": 4},
			Name:        "CounterNoTags",
			Description: "Counter Description",
			Unit:        "1",
		},
		"CounterTags": {
			Count:       map[string]int64{"ENCOUNTER": 3, "OBSERVATION": 2},
			Name:        "CounterTags",
			Description: "Counter Description",
			Unit:        "1",
			TagKeys:     []string{"FHIRResource"},
		},
	}
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

	if err := CloseAll(); err != nil {
		t.Fatalf("CloseAll failed; err = %s", err)
	}
	gotCount, gotLatency, err := GetResults()
	if err != nil {
		t.Fatalf("GetResults failed; err = %s", err)
	}

	sortOpt := cmpopts.SortSlices(func(a, b CounterResult) bool { return a.Name < b.Name })
	if diff := cmp.Diff(wantCount, gotCount, sortOpt); diff != "" {
		t.Errorf("getResults() return unexpected count (-want +got): \n%s", diff)
	}
	sortOpt = cmpopts.SortSlices(func(a, b LatencyResult) bool { return a.Name < b.Name })
	if diff := cmp.Diff(wantLatency, gotLatency, sortOpt); diff != "" {
		t.Errorf("getResults() return unexpected latency (-want +got): \n%s", diff)
	}
}

func TestInitAfterRecordError(t *testing.T) {
	c := NewCounter("TestInitAfterRecordError", "Counter Description", "ms")
	c.Record(context.Background(), 1)
	gotErr := InitAndExportGCP("")
	if !errors.Is(gotErr, errInitAfterRecord) {
		t.Errorf("InitAndExportGCP() wanted error: got %v want %v", gotErr, errInitAfterRecord)
	}
}
