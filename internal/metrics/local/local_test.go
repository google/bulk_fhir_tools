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

package local

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/medical_claims_tools/internal/metrics/aggregation"
)

func TestCounterWithTags(t *testing.T) {
	c := &Counter{}
	if err := c.Init("fhir-resource-counter", "A descriptive Description", "1", aggregation.Count, "FHIRResource", "FHIRVersion"); err != nil {
		t.Errorf("counter.Init() %v", err)
	}
	if err := c.Record(context.Background(), 1, "OBSERVATION", "R4"); err != nil {
		t.Errorf("counter.Record(%q, %q) %v", "OBSERVATION", "R4", err)
	}
	if err := c.Record(context.Background(), 1, "OBSERVATION", "R4"); err != nil {
		t.Errorf("counter.Record(%q, %q) %v", "OBSERVATION", "R4", err)
	}
	if err := c.Record(context.Background(), 1, "OBSERVATION", "STU3"); err != nil {
		t.Errorf("counter.Record(%q, %q) %v", "OBSERVATION", "STU3", err)
	}
	c.Close()
	got := c.MaybeGetResult()
	want := map[string]int64{"OBSERVATION-R4": 2, "OBSERVATION-STU3": 1}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("CloseAndGetResults() returned unexpected diff (-want +got):\n%s", diff)
	}
}

func TestCounterWithoutTags(t *testing.T) {
	cNoTags := &Counter{}
	if err := cNoTags.Init("fhir-resource-counter", "A descriptive Description", "1", aggregation.Count); err != nil {
		t.Errorf("counter.Init() %v", err)
	}
	if err := cNoTags.Record(context.Background(), 3); err != nil {
		t.Errorf("counter.Record() %v", err)
	}
	cNoTags.Close()
	got := cNoTags.MaybeGetResult()
	want := map[string]int64{"fhir-resource-counter": 3}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("CloseAndGetResults() returned unexpected diff (-want +got):\n%s", diff)
	}
}

func TestCounterMaxValueAggregation(t *testing.T) {
	c := &Counter{}
	if err := c.Init("fhir-resource-counter", "A descriptive Description", "1", aggregation.LastValueInGCPMaxValueInLocal, "FHIRResource", "FHIRVersion"); err != nil {
		t.Errorf("counter.Init() %v", err)
	}
	if err := c.Record(context.Background(), 1, "OBSERVATION", "R4"); err != nil {
		t.Errorf("counter.Record(%q, %q) %v", "OBSERVATION", "R4", err)
	}
	if err := c.Record(context.Background(), 18, "OBSERVATION", "R4"); err != nil {
		t.Errorf("counter.Record(%q, %q) %v", "OBSERVATION", "R4", err)
	}
	if err := c.Record(context.Background(), 3, "OBSERVATION", "R4"); err != nil {
		t.Errorf("counter.Record(%q, %q) %v", "OBSERVATION", "R4", err)
	}
	if err := c.Record(context.Background(), 1, "OBSERVATION", "STU3"); err != nil {
		t.Errorf("counter.Record(%q, %q) %v", "OBSERVATION", "STU3", err)
	}
	c.Close()
	got := c.MaybeGetResult()
	want := map[string]int64{"OBSERVATION-R4": 18, "OBSERVATION-STU3": 1}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("CloseAndGetResults() returned unexpected diff (-want +got):\n%s", diff)
	}
}

func TestCounterErrors(t *testing.T) {
	c := &Counter{}
	if got, want := c.Record(context.Background(), 1), errInit; got != want {
		t.Errorf("counter.Record() want error %v; got %v", want, got)
	}

	c.Init("fhir-resource-counter", "A descriptive Description", "1", aggregation.Count, "FhirResource")
	if got, want := c.Record(context.Background(), 1, "OBSERVATION", "ExtraTag"), errMatchingTags; got != want {
		t.Errorf("counter.Record() want error %v; got %v", want, got)
	}
}

func TestLatencyWithTags(t *testing.T) {
	l := &Latency{}
	if err := l.Init("fhir-resource-latency", "A descriptive Description", "ms", []float64{0, 3, 5}, "FHIRResource", "FHIRVersion"); err != nil {
		t.Errorf("latency.Init() %v", err)
	}
	if err := l.Record(context.Background(), float64(-3), "OBSERVATION", "R4"); err != nil {
		t.Errorf("latency.Record(%q, %q) %v", "OBSERVATION", "R4", err)
	}
	if err := l.Record(context.Background(), float64(3), "OBSERVATION", "R4"); err != nil {
		t.Errorf("latency.Record(%q, %q) %v", "OBSERVATION", "R4", err)
	}
	if err := l.Record(context.Background(), float64(12), "OBSERVATION", "STU3"); err != nil {
		t.Errorf("latency.Record(%q, %q) %v", "OBSERVATION", "STU3", err)
	}
	l.Close()
	got := l.MaybeGetResult()
	want := map[string][]int{"OBSERVATION-R4": []int{1, 0, 1, 0}, "OBSERVATION-STU3": []int{0, 0, 0, 1}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("CloseAndGetResults() returned unexpected diff (-want +got):\n%s", diff)
	}
}

func TestLatencyWithoutTags(t *testing.T) {
	lNoBuckets := &Latency{}
	if err := lNoBuckets.Init("fhir-resource-latency", "A descriptive Description", "s", []float64{0, 3, 5}); err != nil {
		t.Errorf("latency.Init() %v", err)
	}
	if err := lNoBuckets.Record(context.Background(), 2.99); err != nil {
		t.Errorf("latency.Record() %v", err)
	}
	lNoBuckets.Close()
	got := lNoBuckets.MaybeGetResult()
	want := map[string][]int{"fhir-resource-latency": []int{0, 1, 0, 0}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("CloseAndGetResults() returned unexpected diff (-want +got):\n%s", diff)
	}
}

func TestLatencyWithoutBuckets(t *testing.T) {
	lNoTags := &Latency{}
	if err := lNoTags.Init("fhir-resource-latency", "A descriptive Description", "s", []float64{}); err != nil {
		t.Errorf("latency.Init() %v", err)
	}
	if err := lNoTags.Record(context.Background(), 2.99); err != nil {
		t.Errorf("latency.Record() %v", err)
	}
	if err := lNoTags.Record(context.Background(), 44.4); err != nil {
		t.Errorf("latency.Record() %v", err)
	}
	lNoTags.Close()
	got := lNoTags.MaybeGetResult()
	want := map[string][]int{"fhir-resource-latency": []int{2}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("CloseAndGetResults() returned unexpected diff (-want +got):\n%s", diff)
	}
}

func TestLatencyErrors(t *testing.T) {
	l := &Latency{}
	if got, want := l.Record(context.Background(), 1), errInit; got != want {
		t.Errorf("latency.Record() want error %v; got %v", want, got)
	}

	l.Init("fhir-resource-latency", "A descriptive Description", "ms", []float64{0, 3, 5}, "FhirResource")
	if got, want := l.Record(context.Background(), 1, "OBSERVATION", "ExtraTag"), errMatchingTags; got != want {
		t.Errorf("latency.Record() want error %v; got %v", want, got)
	}
}
