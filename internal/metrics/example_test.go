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

	"github.com/google/bulk_fhir_tools/internal/metrics/aggregation"
)

func ExampleNewCounter() {
	// Init functions should only be called once.
	// By default the metrics use a local implementation which logs the results of
	// the metrics upon call to CloseAll().
	// InitAndExportGCP() will write the metrics to GCP.

	// Counters with aggregation type Count keep a count for a particular set of tag values.
	c := NewCounter("ExampleCounterName", "Counter Description", "1", aggregation.Count, "FHIRResource", "FHIRVersion")

	// NewCounter and Init (ex InitAndExportGCP) can be called in any order.
	// Init must be called before the first call to Record unless the default local
	// implementation is being used.
	c.Record(context.Background(), 1, "OBSERVATION", "STU3")

	// The tagValues "OBSERVATION", "STU3" must be passed in the same order as the
	// tagKeys "FHIRResource", "FHIRVersion".
	c.Record(context.Background(), 1, "OBSERVATION", "STU4")
	c.Record(context.Background(), 3, "ENCOUNTER", "STU3")

	// CloseAll should be called once at the end of the program. For the local
	// implementation it will log all metrics. For GCP implementation it will
	// flush and close the exporter to GCP.
	CloseAll()
}

func ExampleNewLatency() {
	// Init functions should only be called once.
	// By default the metrics use a local implementation which logs the results of
	// the metrics upon call to CloseAll().
	// InitAndExportGCP() will write the metrics to GCP.

	// For Latency the distribution is defined by the Buckets. For example,
	// Buckets: [0, 3, 5] will create a distribution with 4 buckets where the last
	// bucket is anything > 5. Dist: <0, >=0 <3, >=3 <5, >=5. Recording a value of
	// 3.5 will increment the >=3 <5 bucket.
	l := NewLatency("ExampleLatencyName", "Latency Description", "ms", []float64{0, 3, 5}, "FHIRResource", "FHIRVersion")

	// NewLatency and Init (ex InitAndExportGCP) can be called in any order. Init
	// must be called before the first call to Record unless the default local
	// implementation is being used.
	l.Record(context.Background(), 1, "OBSERVATION", "STU3")

	// The tagValues "OBSERVATION", "STU3" must be passed in the same order as the
	// tagKeys "FHIRResource", "FHIRVersion".
	l.Record(context.Background(), 1, "OBSERVATION", "STU4")
	l.Record(context.Background(), 3, "ENCOUNTER", "STU3")

	// CloseAll should be called once at the end of the program. For the local
	// implementation it will log all metrics. For GCP implementation it will
	// flush and close the exporter to GCP.
	CloseAll()
}
