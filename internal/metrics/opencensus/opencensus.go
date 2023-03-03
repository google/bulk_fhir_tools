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

// Package opencensus wraps the opencensus client to implement the interface
// found in metrics.go.
package opencensus

import (
	"context"
	"errors"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

var (
	errMatchingTags = errors.New("there must be an equal number of tagKeys and tagValues")
	errInit         = errors.New("Init must be called before Record")
)

// Counter wraps a OpenCensus Int64 measure.
type Counter struct {
	tagKeys []tag.Key
	measure *stats.Int64Measure
}

// Init should be called once before the Record method is called on this
// counter. TagKeys are labels used for filtering the monitoring graphs.
// Subsequent calls to Record() should provide the TagValues to the TagKeys in
// the same order specified in Init. TagKeys should be a closed set of values,
// for example FHIR Resource type. Please see the OpenCensus documentation for
// details. Counters should not store any PHI.
func (c *Counter) Init(name, description, unit string, tagKeys ...string) error {
	c.measure = stats.Int64(name, description, unit)

	for _, k := range tagKeys {
		key, err := tag.NewKey(k)
		if err != nil {
			return err
		}
		c.tagKeys = append(c.tagKeys, key)
	}
	v := &view.View{
		Name:        name,
		Measure:     c.measure,
		Description: description,
		Aggregation: view.Count(),
		TagKeys:     c.tagKeys,
	}
	if err := view.Register(v); err != nil {
		return err
	}
	return nil
}

// Record adds val to the counter. The tagValues must match the tagKeys provided
// in the call to Init. Init must be called before the first call to Record.
// Counters should not store any PHI.
func (c *Counter) Record(ctx context.Context, val int64, tagValues ...string) error {
	if c.measure == nil {
		return errInit
	}
	if len(tagValues) != len(c.tagKeys) {
		return errMatchingTags
	}

	mts := make([]tag.Mutator, len(c.tagKeys))
	for i, t := range tagValues {
		mts[i] = tag.Upsert(c.tagKeys[i], t)
	}
	return stats.RecordWithTags(ctx, mts, c.measure.M(val))
}

// MaybeGetResult is not necessary or supported for opencensus Counters. This method is
// implemented to satisfy the interface in metrics.go.
func (c *Counter) MaybeGetResult() map[string]int64 { return nil }

// Close is not necessary or supported for opencensus Counters. This method is
// implemented to satisfy the interface in metrics.go.
func (c *Counter) Close() {
}

// Latency wraps an OpenCensus Float64 measure.
type Latency struct {
	tagKeys []tag.Key
	measure *stats.Float64Measure
}

// Init should be called once before the Record method is called on this
// metric. TagKeys are labels used for filtering the monitoring graphs.
// Subsequent calls to Record() should provide the TagValues to the TagKeys in
// the same order specified in Init. TagKeys should be a closed set of values,
// for example FHIR Resource type. Please see the OpenCensus documentation for
// details. Metrics should not store any PHI.
func (l *Latency) Init(name, description, unit string, buckets []float64, tagKeys ...string) error {
	l.measure = stats.Float64(name, description, unit)

	for _, k := range tagKeys {
		key, err := tag.NewKey(k)
		if err != nil {
			return err
		}
		l.tagKeys = append(l.tagKeys, key)
	}
	v := &view.View{
		Name:        name,
		Measure:     l.measure,
		Description: description,
		Aggregation: view.Distribution(buckets...),
		TagKeys:     l.tagKeys,
	}
	if err := view.Register(v); err != nil {
		return err
	}
	return nil
}

// Record adds val to the distribution. The tagValues must match the tagKeys provided
// in the call to Init. Init must be called before the first call to Record.
// Metrics should not store any PHI.
func (l *Latency) Record(ctx context.Context, val float64, tagValues ...string) error {
	if l.measure == nil {
		return errInit
	}
	if len(tagValues) != len(l.tagKeys) {
		return errMatchingTags
	}

	mts := make([]tag.Mutator, len(l.tagKeys))
	for i, t := range tagValues {
		mts[i] = tag.Upsert(l.tagKeys[i], t)
	}
	return stats.RecordWithTags(ctx, mts, l.measure.M(val))
}

// MaybeGetResult is not necessary or supported for opencensus Latency. This method is
// implemented to satisfy the interface in metrics.go.
func (l *Latency) MaybeGetResult() map[string][]int {
	return nil
}

// Close is not necessary or supported for opencensus Latency. This method is
// implemented to satisfy the interface in metrics.go.
func (l *Latency) Close() {
}
