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

// Counter holds an implementation of the counterInterface. Call NewCounter() to get a Counter.
type Counter struct {
	counterImp counterInterface

	once sync.Once

	name        string
	description string
	unit        string
	tagKeys     []string
}

// NewCounter creates a counter. Name should be unique between all metrics.
// Subsequent calls to Record() should provide the TagValues to the TagKeys in
// the same order specified in NewCounter. TagKeys should be a closed set of
// values, for example FHIR Resource type. InitAndExportGCP and NewCounter can
// be called in any order. Subsequent calls to NewCounter with the same name
// will be ignored and log a warning. Counters should not store any PHI.
func NewCounter(name, description, unit string, tagKeys ...string) *Counter {
	globalMu.Lock()
	defer globalMu.Unlock()
	if c, ok := counterRegistry[name]; ok {
		log.Warning("NewCounter is being called multiple times with the same name. Name should be unique between counters.")
		return c
	}
	counter := Counter{name: name, description: description, unit: unit, tagKeys: tagKeys}
	counterRegistry[name] = &counter
	return &counter
}

// Record adds val to the counter. The tagValues must match the tagKeys provided
// in the call to NewCounter. InitAndExportGCP MUST be called before the first
// call to Record unless the default local counter implementation is used.
// Counters should not store any PHI.
func (c *Counter) Record(ctx context.Context, val int64, tagValues ...string) error {
	if err := c.initialize(); err != nil {
		return err
	}

	return c.counterImp.Record(ctx, val, tagValues...)
}

func (c *Counter) initialize() error {
	var err error
	c.once.Do(func() {
		globalMu.Lock()
		globalRecordCalled = true
		globalMu.Unlock()

		if implementation == localImp {
			c.counterImp = &local.Counter{}
		} else if implementation == gcpImp {
			c.counterImp = &opencensus.Counter{}
		} else {
			err = errors.New("in metrics.NewCounter, implementation is set to an unknown value, this should never happen")
		}
		c.counterImp.Init(c.name, c.description, c.unit, c.tagKeys...)
	})
	return err
}
