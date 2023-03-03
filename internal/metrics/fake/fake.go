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

// Package fake implements metrics that can be used in tests that are run with
// t.Parallel(). The metric results cannot be asserted as they are a no-op.
package fake

import (
	"context"
)

// Counter is a no-op implementation of a counter for tests.
type Counter struct{}

// Init is a no-op implementation for tests.
func (c *Counter) Init(name, description, unit string, tagKeys ...string) error {
	return nil
}

// Record is a no-op implementation for tests.
func (c *Counter) Record(_ context.Context, val int64, tagValues ...string) error {
	return nil
}

// MaybeGetResult is a no-op implementation for tests.
func (c *Counter) MaybeGetResult() map[string]int64 {
	return nil
}

// Close is a no-op implementation for tests.
func (c *Counter) Close() {}

// Latency is a no-op implementation of a latency for tests.
type Latency struct{}

// Init is a no-op implementation for tests.
func (l *Latency) Init(name, description, unit string, buckets []float64, tagKeys ...string) error {
	return nil
}

// Record is a no-op implementation for tests.
func (l *Latency) Record(ctx context.Context, val float64, tagValues ...string) error {
	return nil
}

// MaybeGetResult is a no-op implementation for tests.
func (l *Latency) MaybeGetResult() map[string][]int { return nil }

// Close is a no-op implementation for tests.
func (l *Latency) Close() {}
