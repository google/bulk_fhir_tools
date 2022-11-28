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

package processing_test

import (
	"context"

	"github.com/google/medical_claims_tools/fhir/processing"
)

// testSink can be used for testing processors by capturing processed resources.
type testSink struct {
	writtenResources []processing.ResourceWrapper
	finalizeCalled   bool
}

func (ts *testSink) Write(ctx context.Context, resource processing.ResourceWrapper) error {
	ts.writtenResources = append(ts.writtenResources, resource)
	return nil
}

func (ts *testSink) Finalize(ctx context.Context) error {
	ts.finalizeCalled = true
	return nil
}

// Assert that testSink satisfies the Sink interface.
var _ processing.Sink = &testSink{}
