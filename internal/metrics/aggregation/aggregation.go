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

// Package aggregation holds an enum of different aggregation types for Counters.
package aggregation

// Aggregation is an enum of the different aggregation types for counter metrics.
type Aggregation int

const (
	// Count keeps a count the number of times something occurs.
	Count Aggregation = iota
	// LastValueInGCPMaxValueInLocal when used by OpenCensus implementation uses the
	// view.LastValue aggregation, sending the last recorded value whenever the
	// metric is exported to GCP. Local implementation will return the max value
	// recorded by the metric.
	LastValueInGCPMaxValueInLocal Aggregation = iota
)
