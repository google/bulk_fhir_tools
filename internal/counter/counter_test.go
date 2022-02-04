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

package counter_test

import (
	"sync"
	"testing"

	"github.com/google/medical_claims_tools/internal/counter"
)

func TestCounter(t *testing.T) {
	cnt := counter.New()

	// Create 5 go routines that add 1 to cnt
	wg := &sync.WaitGroup{}
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			cnt.Increment()
			wg.Done()
		}()
	}
	wg.Wait() // Wait for all increments to be sent

	result := cnt.CloseAndGetCount()
	if result != 5 {
		t.Errorf("unexpected count. got: %v, want: %v", result, 5)
	}
}
