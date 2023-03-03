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

// Package local contains a simple often non-blocking (on Increment) thread-safe
// counters that can be used across multiple goroutines, with results collected
// at the end.
package local

import (
	"context"
	"errors"
	"strings"
	"sync"
)

var (
	errMatchingTags = errors.New("there must be an equal number of tagKeys and tagValues")
	errInit         = errors.New("Init must be called before Record")
)

// Counter is a custom implementation of a counter.
type Counter struct {
	count         map[string]int64
	name          string
	tagKeys       []string
	initialized   bool
	countMu       sync.Mutex
	incrementChan chan counterData
	wg            *sync.WaitGroup
	once          sync.Once
}

type counterData struct {
	val       int64
	tagValues string
}

// Init should be called once before the Record method is called on this
// counter. Subsequent calls to Record() should provide the TagValues to the
// TagKeys in the same order specified in Init. TagKeys should be a closed set
// of values, for example FHIR Resource type. Counters should not store any PHI.
func (c *Counter) Init(name, description, unit string, tagKeys ...string) error {
	c.once.Do(func() {
		c.incrementChan = make(chan counterData, 50)
		c.wg = &sync.WaitGroup{}
		c.name = name
		c.tagKeys = tagKeys
		c.count = make(map[string]int64)
		c.initialized = true
		go c.countWorker()
	})
	return nil
}

// Record adds val to the counter. The tagValues must match the tagKeys provided
// in the call to Init. Init must be called before the first call to Record.
// Counters should not store any PHI.
func (c *Counter) Record(_ context.Context, val int64, tagValues ...string) error {
	if !c.initialized {
		return errInit
	}
	if len(tagValues) != len(c.tagKeys) {
		return errMatchingTags
	}

	delta := counterData{val: val}
	if len(tagValues) == 0 {
		delta.tagValues = c.name
	} else {
		delta.tagValues = strings.Join(tagValues, "-")
	}

	c.wg.Add(1)
	c.incrementChan <- delta
	return nil
}

// MaybeGetResult returns a copy of the results for this counter.
func (c *Counter) MaybeGetResult() map[string]int64 {
	c.wg.Wait()
	c.countMu.Lock()
	copy := make(map[string]int64)
	for k, v := range c.count {
		copy[k] = v
	}
	c.countMu.Unlock()
	return copy
}

// Close processes any remaining queued increments. This should be called only
// after all Increments to this counter have been called or sent already.
func (c *Counter) Close() {
	close(c.incrementChan)
	c.wg.Wait()
}

func (c *Counter) countWorker() {
	for delta := range c.incrementChan {
		c.countMu.Lock()
		c.count[delta.tagValues] += delta.val
		c.countMu.Unlock()
		c.wg.Done()
	}
}

// Latency is a custom implementation of distribution/histogram metric.
type Latency struct {
	dist    map[string][]int
	name    string
	buckets []float64
	tagKeys []string

	initialized   bool
	countMu       sync.Mutex
	incrementChan chan latencyData
	wg            *sync.WaitGroup
	once          sync.Once
}

type latencyData struct {
	bucketIndex int
	tagValues   string
}

// Init should be called once before the Record method is called on this
// counter. Subsequent calls to Record() should provide the TagValues to the
// TagKeys in the same order specified in Init. TagKeys should be a closed set
// of values, for example FHIR Resource type. Counters should not store any PHI.
// The distribution is defined by the Buckets. For example,
// Buckets: [0, 3, 5] will create a distribution with 4 buckets where the last
// bucket is anything > 5. Dist: <0, >=0 <3, >=3 <5, >=5.
func (l *Latency) Init(name, description, unit string, buckets []float64, tagKeys ...string) error {
	l.once.Do(func() {
		l.incrementChan = make(chan latencyData, 50)
		l.wg = &sync.WaitGroup{}
		l.name = name
		l.tagKeys = tagKeys
		l.buckets = buckets
		l.dist = make(map[string][]int)
		l.initialized = true
		go l.latencyWorker()
	})
	return nil
}

// Record adds 1 to the correct bucket in the distribution. The tagValues must
// match the tagKeys provided in the call to Init. Init must be called before
// the first call to Record. Counters should not store any PHI.
func (l *Latency) Record(ctx context.Context, val float64, tagValues ...string) error {
	if !l.initialized {
		return errInit
	}
	if len(tagValues) != len(l.tagKeys) {
		return errMatchingTags
	}

	delta := latencyData{}
	if len(tagValues) == 0 {
		delta.tagValues = l.name
	} else {
		delta.tagValues = strings.Join(tagValues, "-")
	}

	delta.bucketIndex = len(l.buckets) // Default to the last, catch all bucket
	for i, bucket := range l.buckets {
		if val < bucket {
			delta.bucketIndex = i
			break
		}
	}

	l.wg.Add(1)
	l.incrementChan <- delta
	return nil
}

// MaybeGetResult returns a copy of the results of the latency.
func (l *Latency) MaybeGetResult() map[string][]int {
	l.wg.Wait()
	l.countMu.Lock()
	cop := make(map[string][]int)
	for k, v := range l.dist {
		cop[k] = make([]int, len(v))
		copy(cop[k], v)
	}
	l.countMu.Unlock()
	return cop
}

// Close processes any remaining queued increments. This should be called only
// after all Increments to this latency have been called or sent already.
func (l *Latency) Close() {
	close(l.incrementChan)
	l.wg.Wait()
}

func (l *Latency) latencyWorker() {
	for delta := range l.incrementChan {
		l.countMu.Lock()
		if _, ok := l.dist[delta.tagValues]; !ok {
			l.dist[delta.tagValues] = make([]int, len(l.buckets)+1)
		}
		l.dist[delta.tagValues][delta.bucketIndex]++
		l.countMu.Unlock()
		l.wg.Done()
	}
}
