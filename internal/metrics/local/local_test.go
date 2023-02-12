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
)

func TestCounterWithTags(t *testing.T) {
	c := &counter{}
	if err := c.Init("fhir-resource-counter", "A descriptive Description", "1", "FHIRResource", "FHIRVersion"); err != nil {
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
	got := c.CloseAndGetResults()
	want := CounterResults{
		Count:       map[string]int64{"OBSERVATION-R4": 2, "OBSERVATION-STU3": 1},
		Name:        "fhir-resource-counter",
		Description: "A descriptive Description",
		Unit:        "1",
		TagKeys:     []string{"FHIRResource", "FHIRVersion"},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("CloseAndGetResults() returned unexpected diff (-want +got):\n%s", diff)
	}
}

func TestCounterWithoutTags(t *testing.T) {
	cNoTags := &counter{}
	if err := cNoTags.Init("fhir-resource-counter", "A descriptive Description", "1"); err != nil {
		t.Errorf("counter.Init() %v", err)
	}
	if err := cNoTags.Record(context.Background(), 3); err != nil {
		t.Errorf("counter.Record() %v", err)
	}
	got := cNoTags.CloseAndGetResults()
	want := CounterResults{
		Count:       map[string]int64{"fhir-resource-counter": 3},
		Name:        "fhir-resource-counter",
		Description: "A descriptive Description",
		Unit:        "1",
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("CloseAndGetResults() returned unexpected diff (-want +got):\n%s", diff)
	}
}

func TestCounterErrors(t *testing.T) {
	c := &counter{}
	if got, want := c.Record(context.Background(), 1), errInit; got != want {
		t.Errorf("counter.Record() want error %v; got %v", want, got)
	}

	c.Init("fhir-resource-counter", "A descriptive Description", "1", "FhirResource")
	if got, want := c.Record(context.Background(), 1, "OBSERVATION", "ExtraTag"), errMatchingTags; got != want {
		t.Errorf("counter.Record() want error %v; got %v", want, got)
	}
}

func TestLatencyWithTags(t *testing.T) {
	l := &latency{}
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
	got := l.CloseAndGetResults()
	want := LatencyResults{
		Dist:        map[string][]int{"OBSERVATION-R4": []int{1, 0, 1, 0}, "OBSERVATION-STU3": []int{0, 0, 0, 1}},
		Name:        "fhir-resource-latency",
		Description: "A descriptive Description",
		Unit:        "ms",
		Buckets:     []float64{0, 3, 5},
		TagKeys:     []string{"FHIRResource", "FHIRVersion"},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("CloseAndGetResults() returned unexpected diff (-want +got):\n%s", diff)
	}
}

func TestLatencyWithoutTags(t *testing.T) {
	lNoBuckets := &latency{}
	if err := lNoBuckets.Init("fhir-resource-latency", "A descriptive Description", "s", []float64{0, 3, 5}); err != nil {
		t.Errorf("latency.Init() %v", err)
	}
	if err := lNoBuckets.Record(context.Background(), 2.99); err != nil {
		t.Errorf("latency.Record() %v", err)
	}
	got := lNoBuckets.CloseAndGetResults()
	want := LatencyResults{
		Dist:        map[string][]int{"fhir-resource-latency": []int{0, 1, 0, 0}},
		Name:        "fhir-resource-latency",
		Description: "A descriptive Description",
		Buckets:     []float64{0, 3, 5},
		Unit:        "s",
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("CloseAndGetResults() returned unexpected diff (-want +got):\n%s", diff)
	}
}

func TestLatencyWithoutBuckets(t *testing.T) {
	lNoTags := &latency{}
	if err := lNoTags.Init("fhir-resource-latency", "A descriptive Description", "s", []float64{}); err != nil {
		t.Errorf("latency.Init() %v", err)
	}
	if err := lNoTags.Record(context.Background(), 2.99); err != nil {
		t.Errorf("latency.Record() %v", err)
	}
	if err := lNoTags.Record(context.Background(), 44.4); err != nil {
		t.Errorf("latency.Record() %v", err)
	}
	got := lNoTags.CloseAndGetResults()
	want := LatencyResults{
		Dist:        map[string][]int{"fhir-resource-latency": []int{2}},
		Name:        "fhir-resource-latency",
		Description: "A descriptive Description",
		Buckets:     []float64{},
		Unit:        "s",
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("CloseAndGetResults() returned unexpected diff (-want +got):\n%s", diff)
	}
}

func TestLatencyErrors(t *testing.T) {
	l := &latency{}
	if got, want := l.Record(context.Background(), 1), errInit; got != want {
		t.Errorf("latency.Record() want error %v; got %v", want, got)
	}

	l.Init("fhir-resource-latency", "A descriptive Description", "ms", []float64{0, 3, 5}, "FhirResource")
	if got, want := l.Record(context.Background(), 1, "OBSERVATION", "ExtraTag"), errMatchingTags; got != want {
		t.Errorf("latency.Record() want error %v; got %v", want, got)
	}
}