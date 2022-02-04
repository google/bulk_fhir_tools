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

// Package counter holds a set of utilities for thread-safe counters.
package counter

import "sync"

// New initializes and returns a new Counter.
func New() *Counter {
	cnt := &Counter{}
	cnt.init()
	return cnt
}

// Counter is a simple often non-blocking (on Increment) thread-safe counter
// that can be used across multiple goroutines, with results collected at the
// end.
type Counter struct {
	count   int
	countMu sync.Mutex

	incrementChan chan int
	wg            *sync.WaitGroup
}

func (c *Counter) init() {
	c.incrementChan = make(chan int, 50)
	c.wg = &sync.WaitGroup{}
	go c.countWorker()
}

// Increment adds one to the counter.
func (c *Counter) Increment() {
	c.incrementChan <- 1
	c.wg.Add(1)
}

// CloseAndGetCount processes any remaining queued increments and returns the
// final count for this counter. This should be called only after all Increments
// to this counter have been called or sent already.
func (c *Counter) CloseAndGetCount() int {
	close(c.incrementChan)
	c.wg.Wait()
	return c.count
}

func (c *Counter) countWorker() {
	for delta := range c.incrementChan {
		c.countMu.Lock()
		c.count = c.count + delta
		c.countMu.Unlock()
		c.wg.Done()
	}
}
