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

package processing

import (
	"context"
)

// TestSink can be used for testing processors by capturing processed resources.
type TestSink struct {
	WrittenResources []ResourceWrapper
	FinalizeCalled   bool
}

// Write is Sink.Write
func (ts *TestSink) Write(ctx context.Context, resource ResourceWrapper) error {
	ts.WrittenResources = append(ts.WrittenResources, resource)
	return nil
}

// Finalize is Sink.Finalize
func (ts *TestSink) Finalize(ctx context.Context) error {
	ts.FinalizeCalled = true
	return nil
}

// Assert that TestSink satisfies the Sink interface.
var _ Sink = &TestSink{}
