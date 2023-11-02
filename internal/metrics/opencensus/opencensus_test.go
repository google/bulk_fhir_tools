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

package opencensus

import (
	"context"
	"testing"
	"time"

	"go.opencensus.io/metric/metricdata"
	"go.opencensus.io/metric/metricexport"
	metrictest "go.opencensus.io/metric/test"
	"github.com/google/bulk_fhir_tools/internal/metrics/aggregation"
)

type counter interface {
	Record(ctx context.Context, val int64, tagValues ...string) error
	Init(name, description, unit string, tagKeys ...string) error
}

type latency interface {
	Record(ctx context.Context, val float64, tagValues ...string) error
	Init(name, description, unit string, buckets []float64, tagKeys ...string) error
}

func TestCounter(t *testing.T) {
	c := &Counter{}
	if err := c.Init("fhir-resource-counter", "A descriptive Description", "1", aggregation.Count, "FhirResource"); err != nil {
		t.Errorf("counter.Init() %v", err)
	}

	exporter := metrictest.NewExporter(metricexport.NewReader())

	if err := c.Record(context.Background(), 1, "OBSERVATION"); err != nil {
		t.Errorf("counter.Record() %v", err)
	}

	exporter.ReadAndExport()

	tags := map[string]string{"FhirResource": "OBSERVATION"}
	pc, ok := exporter.GetPoint("fhir-resource-counter", tags)
	if !ok {
		t.Errorf("exporter.GetPoint(%v, %v): !ok\nDump=%v", "fhir-resource-counter", tags, exporter)
	}
	vv := &distributionSumValueVisitor{}
	pc.ReadValue(vv)
	if got, want := vv.Value, int64(1); got != want {
		t.Errorf("counter got=%v, want=%v", got, want)
	}
}

func TestCounterErrors(t *testing.T) {
	c := &Counter{}
	if got, want := c.Record(context.Background(), 1), errInit; got != want {
		t.Errorf("latency.Record() want error %v; got %v", want, got)
	}

	c.Init("fhir-resource-counter", "A descriptive Description", "1", aggregation.Count, "FhirResource")
	if got, want := c.Record(context.Background(), 1, "OBSERVATION", "ExtraTag"), errMatchingTags; got != want {
		t.Errorf("latency.Record() want error %v; got %v", want, got)
	}
}

func TestLatency(t *testing.T) {
	l := &Latency{}
	buckets := []float64{0, 5, 10, 15, 20}
	if err := l.Init("fhir-resource-latency", "A descriptive Description", "ms", buckets, "FhirResource"); err != nil {
		t.Errorf("latency.Init() %v", err)
	}
	exporter := metrictest.NewExporter(metricexport.NewReader())

	if err := l.Record(context.Background(), float64(5*time.Millisecond), "OBSERVATION"); err != nil {
		t.Errorf("latency.Record() %v", err)
	}

	exporter.ReadAndExport()

	tags := map[string]string{"FhirResource": "OBSERVATION"}
	pl, ok := exporter.GetPoint("fhir-resource-latency", tags)
	if !ok {
		t.Errorf("exporter.GetPoint(%v, %v): !ok\nDump=%v", "fhir-resource-latency", tags, exporter)
	}
	vv := &distributionSumValueVisitor{}
	pl.ReadValue(vv)
	if got, want := vv.Value, int64(5*time.Millisecond); got != want {
		t.Errorf("latency got=%v, want%v", got, want)
	}
}

func TestLatencyErrors(t *testing.T) {
	l := &Latency{}
	if got, want := l.Record(context.Background(), float64(5*time.Millisecond)), errInit; got != want {
		t.Errorf("latency.Record() want error %v; got %v", want, got)
	}

	l.Init("fhir-resource-latency", "A descriptive Description", "ms", []float64{0, 5, 10, 15, 20}, "FhirResource")
	if got, want := l.Record(context.Background(), float64(5*time.Millisecond), "OBSERVATION", "ExtraTag"), errMatchingTags; got != want {
		t.Errorf("latency.Record() want error %v; got %v", want, got)
	}
}

// distributionSumValueVisitor visits a metric data point, and if it's a
// distribution, gets the distribution's sum. Implements the ValueVisitor
// interface from the OpenCensus API.
type distributionSumValueVisitor struct {
	Value int64
}

func (vv *distributionSumValueVisitor) VisitInt64Value(v int64) {
	vv.Value = v
}
func (vv *distributionSumValueVisitor) VisitFloat64Value(v float64) {}
func (vv *distributionSumValueVisitor) VisitDistributionValue(v *metricdata.Distribution) {
	vv.Value = int64(v.Sum)
}
func (vv *distributionSumValueVisitor) VisitSummaryValue(v *metricdata.Summary) {}
