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
// different metric clients. By default metrics use the local implementation,
// which log the results of the metrics when Closed. To use a different
// implementation call that specific init method ex InitAndExportGCP. Call Close after
// all counters have been recorded.
package metrics

import (
	"context"
	"errors"
	"sync"
	"time"

	"contrib.go.opencensus.io/exporter/stackdriver"
	log "github.com/google/medical_claims_tools/internal/logger"
)

// implementation should be set by Init and is used to decide which Close to
// call. For example InitAndExportGCP, sets implementation to gcpImp and as a result we
// call closeGCP in CloseAll.
var implementation = localImp

const (
	localImp = iota
	gcpImp   = iota
	fakeImp  = iota
)

// globalMu synchronizes the reading and writing to counterRegistry, latencyRegistry and globalRecordCalled globals.
var globalMu sync.Mutex

var counterRegistry map[string]*Counter = make(map[string]*Counter)
var latencyRegistry map[string]*Latency = make(map[string]*Latency)

// globalRecordCalled tracks whether we have called Record() on any created metric before calling Init.
var globalRecordCalled = false
var errInitAfterRecord = errors.New("initAndExportGCP was called after a metric called record")

var sd *stackdriver.Exporter

// InitAndExportGCP starts exporting metrics to GCP on a 60 second interval. Metrics can
// be created with NewCounter before calling InitAndExportGCP, but no callers should call
// Record() on any metric until InitAndExportGCP is called.
func InitAndExportGCP(projectID string) error {
	globalMu.Lock()
	defer globalMu.Unlock()
	if globalRecordCalled {
		return errInitAfterRecord
	}
	implementation = gcpImp

	var err error
	sd, err = stackdriver.NewExporter(stackdriver.Options{
		ProjectID:    projectID,
		MetricPrefix: "bulk-fhir-fetch",
		// According to the OpenCensus documentation 60 seconds is the minimum for GCP Monitoring.
		ReportingInterval: 60 * time.Second,
		OnError:           func(err error) { log.Infof("GCP exporter OnError: %+v", err) },
	})
	if err != nil {
		return err
	}
	return sd.StartMetricsExporter()
}

// InitNoOp initializes all metrics to have no-op behavior on all calls. Since
// many metrics may be globals or package-specific globals, this makes it easier
// to run t.Parallel tests where metric results are not checked or
// inconsequential.
func InitNoOp() {
	globalMu.Lock()
	defer globalMu.Unlock()
	implementation = fakeImp
}

// ResetAll resets the count/dist of all created metrics. It should only
// be called in tests. Metric results cannot be asserted in tests run in
// t.Parallel(). If using t.Parallel() consider InitNoOp instead.
func ResetAll() {
	globalMu.Lock()
	defer globalMu.Unlock()
	globalRecordCalled = false

	// Go through the registry and return all metrics to the state they were in
	// after the call to NewCounter/NewLatency.
	for _, c := range counterRegistry {
		// Reset the once.
		c.once = &sync.Once{}
		c.counterImp = nil
	}
	for _, l := range latencyRegistry {
		l.once = &sync.Once{}
		l.latencyImp = nil
	}
}

// CloseAll should be called only after all metrics have been recorded. It will
// call the correct close method on all created metrics based on which
// implementation was used. If the local implementation was used, the metric
// results will be logged.
func CloseAll() error {
	for _, c := range counterRegistry {
		if err := c.initialize(); err != nil {
			return err
		}
		c.counterImp.Close()
	}

	for _, l := range latencyRegistry {
		if err := l.initialize(); err != nil {
			return err
		}
		l.latencyImp.Close()
	}

	if implementation == localImp {
		// No close needed.
	} else if implementation == gcpImp {
		closeGCP()
	} else {
		return errors.New("in metrics.Close, implementation is set to an unknown value, this should never happen")
	}

	counterRes, latencyRes, err := GetResults()
	if err != nil {
		return err
	}

	for _, res := range counterRes {
		log.Info(res.String())
	}
	for _, res := range latencyRes {
		log.Info(res.String())
	}

	return nil
}

// GetResults returns the results from all metrics. This should only be called in tests.
func GetResults() (map[string]CounterResult, map[string]LatencyResult, error) {
	for _, c := range counterRegistry {
		if err := c.initialize(); err != nil {
			return nil, nil, err
		}
	}

	for _, l := range latencyRegistry {
		if err := l.initialize(); err != nil {
			return nil, nil, err
		}
	}

	counterRes := make(map[string]CounterResult)
	for _, c := range counterRegistry {
		res := CounterResult{Count: c.counterImp.MaybeGetResult(), Name: c.name, Description: c.description, Unit: c.unit, TagKeys: c.tagKeys}
		counterRes[c.name] = res
	}

	latencyRes := make(map[string]LatencyResult)
	for _, l := range latencyRegistry {
		res := LatencyResult{Dist: l.latencyImp.MaybeGetResult(), Name: l.name, Description: l.description, Unit: l.unit, Buckets: l.buckets, TagKeys: l.tagKeys}
		latencyRes[l.name] = res
	}

	return counterRes, latencyRes, nil
}

// closeGCP flushes the remaining metrics and stops exporting.
func closeGCP() {
	sd.Flush()
	sd.StopMetricsExporter()
}

type counterInterface interface {
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

	// MaybeGetResult returns the result of the counter. The results map a concatenation
	// of the tagValues to count for those tagValues. If no tags are used then the
	// result will map the name to the count. In some implementations calling
	// MaybeGetResult is not supported, in which case the map is nil.
	MaybeGetResult() map[string]int64

	// Close the counter.
	Close()
}
type latencyInterface interface {
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

	// MaybeGetResult returns the results of the latency. The results map a concatenation
	// of the tagValues to distribution for those tagValues. If no tags are used
	// then the results will map the name to the distribution. The distribution is
	// defined by the Buckets. For example,
	// Buckets: [0, 3, 5] will create a distribution with 4 buckets where the last
	// bucket is anything > 5. Dist: <0, >=0 <3, >=3 <5, >=5. In some
	// implementations calling MayGetResult is not supported, in which case the map
	// is nil.
	MaybeGetResult() map[string][]int

	// Close the latency.
	Close()
}
