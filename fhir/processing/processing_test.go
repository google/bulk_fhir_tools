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
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/medical_claims_tools/fhir/processing"

	cpb "github.com/google/fhir/go/proto/google/fhir/proto/r4/core/codes_go_proto"
)

// testProcessor is a no-op processor for testing.
type testProcessor struct {
	processing.BaseProcessor
}

func (tp *testProcessor) Process(ctx context.Context, resource processing.ResourceWrapper) error {
	return tp.Output(ctx, resource)
}

func TestTeeFHIRSink(t *testing.T) {
	ts1 := &testSink{}
	ts2 := &testSink{}
	ctx := context.Background()
	p, err := processing.NewPipeline([]processing.Processor{&testProcessor{}}, []processing.Sink{ts1, ts2})
	if err != nil {
		t.Fatal(err)
	}
	resourceType := cpb.ResourceTypeCode_ACCOUNT
	sourceURL := "http://source"
	data := []byte("data")
	if err := p.Process(ctx, cpb.ResourceTypeCode_ACCOUNT, "http://source", data); err != nil {
		t.Fatalf("p.Process() returned unexpected error: %v", err)
	}
	if err := p.Finalize(ctx); err != nil {
		t.Fatalf("p.Finalize() returned unexpected error: %v", err)
	}
	for i, ts := range []*testSink{ts1, ts2} {
		if len(ts.writtenResources) != 1 {
			t.Fatalf("testSink %d captured %d resources, want 1", i, len(ts.writtenResources))
		}
		if ts.writtenResources[0].Type() != resourceType {
			t.Errorf("testSink %d captured unexpected resource type: got %s, want %s", i, ts.writtenResources[0].Type(), resourceType)
		}
		if ts.writtenResources[0].SourceURL() != sourceURL {
			t.Errorf("testSink %d captured unexpected resource type: got %q, want %q", i, ts.writtenResources[0].SourceURL(), sourceURL)
		}
		json, err := ts.writtenResources[0].JSON()
		if err != nil {
			t.Errorf("testSink %d JSON() returned unexpected error: %v", i, err)
		} else if !cmp.Equal(json, data) {
			t.Errorf("testSink %d captured unexpected data: got %s, want %s", i, json, data)
		}
		if !ts.finalizeCalled {
			t.Errorf("Finalize not called on testSink %d", i)
		}
	}
}
