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
	"fmt"
	"strings"
	"sync"
)

var (
	errMatchingTags = errors.New("there must be an equal number of tagKeys and tagValues")
	errInit         = errors.New("Init must be called before Record")
)

type counter struct {
	results       CounterResults
	initialized   bool
	countMu       sync.Mutex
	incrementChan chan counterData
	wg            *sync.WaitGroup
	once          sync.Once
}

// CounterResults holds the results for the counter. It can be retrieved by
// calling CloseAndGetResults.
type CounterResults struct {
	// Count maps a concatenation of the tagValues to count for those tagValues.
	// If no tags are used then Count will map the name to the count.
	Count       map[string]int64
	Name        string
	Description string
	Unit        string
	TagKeys     []string
}

func (c *CounterResults) String() string {
	header := fmt.Sprintf("\nName: %s\n%s\nUnits: %s\n", c.Name, c.Description, c.Unit)
	var body string
	if len(c.TagKeys) == 0 {
		body = fmt.Sprintf("Count: %d\n", c.Count[c.Name])
	} else {
		for k, v := range c.Count {
			body = body + fmt.Sprintf("%s: %d\n", k, v)
		}
	}
	return header + body
}

type counterData struct {
	val       int64
	tagValues string
}

// Init should be called once before the Record method is called on this
// counter. Subsequent calls to Record() should provide the TagValues to the
// TagKeys in the same order specified in Init. TagKeys should be a closed set
// of values, for example FHIR Resource type. Counters should not store any PHI.
func (c *counter) Init(name, description, unit string, tagKeys ...string) error {
	c.once.Do(func() {
		c.incrementChan = make(chan counterData, 50)
		c.wg = &sync.WaitGroup{}
		c.results.Name = name
		c.results.Description = description
		c.results.Unit = unit
		c.results.TagKeys = tagKeys
		c.results.Count = make(map[string]int64)
		c.initialized = true
		go c.countWorker()
	})
	return nil
}

// Record adds val to the counter. The tagValues must match the tagKeys provided
// in the call to Init. Init must be called before the first call to Record.
// Counters should not store any PHI.
func (c *counter) Record(_ context.Context, val int64, tagValues ...string) error {
	if !c.initialized {
		return errInit
	}
	if len(tagValues) != len(c.results.TagKeys) {
		return errMatchingTags
	}

	delta := counterData{val: val}
	if len(tagValues) == 0 {
		delta.tagValues = c.results.Name
	} else {
		delta.tagValues = strings.Join(tagValues, "-")
	}

	c.wg.Add(1)
	c.incrementChan <- delta
	return nil
}

// CloseAndGetResults processes any remaining queued increments and returns the
// final count for this counter. This should be called only after all Increments
// to this counter have been called or sent already.
func (c *counter) CloseAndGetResults() CounterResults {
	close(c.incrementChan)
	c.wg.Wait()
	return c.results
}

func (c *counter) countWorker() {
	for delta := range c.incrementChan {
		c.countMu.Lock()
		c.results.Count[delta.tagValues] += delta.val
		c.countMu.Unlock()
		c.wg.Done()
	}
}

type latency struct {
	results       LatencyResults
	initialized   bool
	countMu       sync.Mutex
	incrementChan chan latencyData
	wg            *sync.WaitGroup
	once          sync.Once
}

// LatencyResults holds the results of the latency. It can be retrieved by
// calling CloseAndGetResults.
type LatencyResults struct {
	// Dist maps a concatenation of the tagValues to distribution for those
	// tagValues. If no tags are used then Dist will map the name to the
	// distribution. The distribution is defined by the Buckets. For example,
	// Buckets: [0, 3, 5] will create a distribution with 4 buckets where the last
	// bucket is anything > 5. Dist: <0, >=0 <3, >=3 <5, >=5.
	Dist        map[string][]int
	Name        string
	Description string
	Unit        string
	Buckets     []float64
	TagKeys     []string
}

func (l *LatencyResults) String() string {
	header := fmt.Sprintf("\nName: %s\n%s\nUnits: %s\nBuckets: ", l.Name, l.Description, l.Unit)
	for _, bucket := range l.Buckets {
		header += fmt.Sprintf("<%.2f, ", bucket)
	}
	header += ">= all\n"
	var body string
	if len(l.TagKeys) == 0 {
		body = fmt.Sprintf("Count: %d\n", l.Dist[l.Name])
	} else {
		for k, v := range l.Dist {
			body = body + fmt.Sprintf("%s: %v\n", k, v)
		}
	}
	return header + body
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
func (l *latency) Init(name, description, unit string, buckets []float64, tagKeys ...string) error {
	l.once.Do(func() {
		l.incrementChan = make(chan latencyData, 50)
		l.wg = &sync.WaitGroup{}
		l.results.Name = name
		l.results.Description = description
		l.results.Unit = unit
		l.results.TagKeys = tagKeys
		l.results.Buckets = buckets
		l.results.Dist = make(map[string][]int)
		l.initialized = true
		go l.latencyWorker()
	})
	return nil
}

// Record adds 1 to the correct bucket in the distribution. The tagValues must
// match the tagKeys provided in the call to Init. Init must be called before
// the first call to Record. Counters should not store any PHI.
func (l *latency) Record(ctx context.Context, val float64, tagValues ...string) error {
	if !l.initialized {
		return errInit
	}
	if len(tagValues) != len(l.results.TagKeys) {
		return errMatchingTags
	}

	delta := latencyData{}
	if len(tagValues) == 0 {
		delta.tagValues = l.results.Name
	} else {
		delta.tagValues = strings.Join(tagValues, "-")
	}

	delta.bucketIndex = len(l.results.Buckets) // Default to the last, catch all bucket
	for i, bucket := range l.results.Buckets {
		if val < bucket {
			delta.bucketIndex = i
			break
		}
	}

	l.wg.Add(1)
	l.incrementChan <- delta
	return nil
}

// CloseAndGetResults processes any remaining queued increments and returns the
// final results for this latency. This should be called only after all Increments
// to this latency have been called or sent already.
func (l *latency) CloseAndGetResults() LatencyResults {
	close(l.incrementChan)
	l.wg.Wait()
	return l.results
}

func (l *latency) latencyWorker() {
	for delta := range l.incrementChan {
		l.countMu.Lock()
		if _, ok := l.results.Dist[delta.tagValues]; !ok {
			l.results.Dist[delta.tagValues] = make([]int, len(l.results.Buckets)+1)
		}
		l.results.Dist[delta.tagValues][delta.bucketIndex]++
		l.countMu.Unlock()
		l.wg.Done()
	}
}
