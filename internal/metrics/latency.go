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
	"sync"

	log "github.com/google/medical_claims_tools/internal/logger"
	"github.com/google/medical_claims_tools/internal/metrics/local"
	"github.com/google/medical_claims_tools/internal/metrics/opencensus"
)

// Latency holds an implementation of the latencyInterface. Call NewLatency() to get a Latency.
type Latency struct {
	latencyImp latencyInterface

	once sync.Once

	name        string
	description string
	unit        string
	buckets     []float64
	tagKeys     []string
}

// NewLatency creates a Latency. Subsequent calls to Record() should provide the
// TagValues to the TagKeys in the same order specified in NewLatency. TagKeys should
// be a closed set of values, for example FHIR Resource type. The distribution
// is defined by the Buckets. For example, Buckets: [0, 3, 5] will create a
// distribution with 4 buckets where the last bucket is anything > 5. Dist: <0,
// >=0 <3, >=3 <5, >=5. InitAndExportGCP and NewLatency can be called in any order.
// Subsequent calls to NewLatency with the same name will be ignored and log a
// warning. Latency should not store any PHI.
func NewLatency(name, description, unit string, buckets []float64, tagKeys ...string) *Latency {
	globalMu.Lock()
	defer globalMu.Unlock()
	if l, ok := latencyRegistry[name]; ok {
		log.Warning("NewLatency is being called multiple times with the same name. Name should be unique between latencies.")
		return l
	}
	latency := Latency{name: name, description: description, unit: unit, buckets: buckets, tagKeys: tagKeys}
	latencyRegistry[name] = &latency
	return &latency
}

// Record adds 1 to the correct bucket in the distribution. The tagValues must
// match the tagKeys provided in the call to NewLatency. InitAndExportGCP MUST
// be called before the first call to Record unless the default local counter
// implementation is used. Latency should not store any PHI.
func (l *Latency) Record(ctx context.Context, val float64, tagValues ...string) error {
	if err := l.initialize(); err != nil {
		return err
	}

	return l.latencyImp.Record(ctx, val, tagValues...)
}

func (l *Latency) initialize() error {
	var err error
	l.once.Do(func() {
		globalMu.Lock()
		globalRecordCalled = true
		globalMu.Unlock()

		if implementation == localImp {
			l.latencyImp = &local.Latency{}
		} else if implementation == gcpImp {
			l.latencyImp = &opencensus.Latency{}
		} else {
			err = errors.New("in metrics.NewCounter, implementation is set to an unknown value, this should never happen")
		}
		l.latencyImp.Init(l.name, l.description, l.unit, l.buckets, l.tagKeys...)
	})
	return err
}
