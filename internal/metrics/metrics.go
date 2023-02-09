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

// Package metrics defines a common metric interface that can be implemented by
// different metric clients.
package metrics

import (
	"context"
)

type counter interface {
	// Init should be called once before the Record method is called on this
	// counter. TagKeys are labels used for filtering the monitoring graphs.
	// Subsequent calls to Record() should provide the TagValues to the TagKeys in
	// the same order specified in Init. TagKeys should be a closed set of values,
	// for example FHIR Resource type. Please see the OpenCensus documentation for
	// details. Counters should not store any PHI.
	Init(name, description, unit string, tagKeys ...string) error

	// Record adds val to the counter. The tagValues must match the tagKeys provided
	// in the call to Init. Init must be called before the first call to Record.
	// Counters should not store any PHI.
	Record(ctx context.Context, val int64, tagValues ...string) error
}
type latency interface {
	// Init should be called once before the Record method is called on this
	// metric. TagKeys are labels used for filtering the monitoring graphs.
	// Subsequent calls to Record() should provide the TagValues to the TagKeys in
	// the same order specified in Init. TagKeys should be a closed set of values,
	// for example FHIR Resource type. Please see the OpenCensus documentation for
	// details. Metrics should not store any PHI.
	Init(name, description, unit string, buckets []float64, tagKeys ...string) error

	// Record adds val to the distribution. The tagValues must match the tagKeys provided
	// in the call to Init. Init must be called before the first call to Record.
	// Metrics should not store any PHI.
	Record(ctx context.Context, val float64, tagValues ...string) error
}
