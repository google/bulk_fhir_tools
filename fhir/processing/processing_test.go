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
	"github.com/google/medical_claims_tools/internal/metrics"

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
	metrics.ResetAll()
	ts1 := &processing.TestSink{}
	ts2 := &processing.TestSink{}
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
	for i, ts := range []*processing.TestSink{ts1, ts2} {
		if len(ts.WrittenResources) != 1 {
			t.Fatalf("TestSink %d captured %d resources, want 1", i, len(ts.WrittenResources))
		}
		if ts.WrittenResources[0].Type() != resourceType {
			t.Errorf("TestSink %d captured unexpected resource type: got %s, want %s", i, ts.WrittenResources[0].Type(), resourceType)
		}
		if ts.WrittenResources[0].SourceURL() != sourceURL {
			t.Errorf("TestSink %d captured unexpected resource type: got %q, want %q", i, ts.WrittenResources[0].SourceURL(), sourceURL)
		}
		json, err := ts.WrittenResources[0].JSON()
		if err != nil {
			t.Errorf("TestSink %d JSON() returned unexpected error: %v", i, err)
		} else if !cmp.Equal(json, data) {
			t.Errorf("TestSink %d captured unexpected data: got %s, want %s", i, json, data)
		}
		if !ts.FinalizeCalled {
			t.Errorf("Finalize not called on TestSink %d", i)
		}
	}
	gotCount, _, err := metrics.GetResults()
	wantCount := map[string]int64{"ACCOUNT": 1}
	if err != nil {
		t.Errorf("GetResults failed; err = %s", err)
	}
	if diff := cmp.Diff(wantCount, gotCount["fhir-resource-counter"].Count); diff != "" {
		t.Errorf("GetResults() return unexpected count (-want +got): \n%s", diff)
	}
}

func TestOperationOutcomeCounter(t *testing.T) {
	metrics.ResetAll()
	ctx := context.Background()
	p, err := processing.NewPipeline([]processing.Processor{&testProcessor{}}, []processing.Sink{})
	if err != nil {
		t.Fatal(err)
	}

	data := []byte(`{"resourceType": "OperationOutcome", "id": "123", "issue":[{"severity": "warning", "code": "forbidden"}, {"severity": "error", "code": "not-found"}, {"severity": "error", "code": "not-found"}]}`)
	if err := p.Process(ctx, cpb.ResourceTypeCode_OPERATION_OUTCOME, "http://source", data); err != nil {
		t.Fatalf("p.Process() returned unexpected error: %v", err)
	}
	if err := p.Finalize(ctx); err != nil {
		t.Fatalf("p.Finalize() returned unexpected error: %v", err)
	}

	gotCount, _, err := metrics.GetResults()
	wantCount := map[string]int64{"WARNING-FORBIDDEN": 1, "ERROR-NOT_FOUND": 2}
	if err != nil {
		t.Errorf("GetResults failed; err = %s", err)
	}
	if diff := cmp.Diff(wantCount, gotCount["operation-outcome-counter"].Count); diff != "" {
		t.Errorf("GetResults() return unexpected count (-want +got): \n%s", diff)
	}
}

func TestMalformedOperationOutcomeCounter(t *testing.T) {
	metrics.ResetAll()
	ctx := context.Background()
	p, err := processing.NewPipeline([]processing.Processor{&testProcessor{}}, []processing.Sink{})
	if err != nil {
		t.Fatal(err)
	}

	data := []byte(`{"resourceType": "OperationOutcome", "id": "123", "issue":[{"severity": "warning"}, {"diagnostics": "diagnostic string"}]}`)
	if err := p.Process(ctx, cpb.ResourceTypeCode_OPERATION_OUTCOME, "http://source", data); err != nil {
		t.Fatalf("p.Process() returned unexpected error: %v", err)
	}
	if err := p.Finalize(ctx); err != nil {
		t.Fatalf("p.Finalize() returned unexpected error: %v", err)
	}

	gotCount, _, err := metrics.GetResults()
	wantCount := map[string]int64{"WARNING-INVALID_UNINITIALIZED": 1, "INVALID_UNINITIALIZED-INVALID_UNINITIALIZED": 1}
	if err != nil {
		t.Errorf("GetResults failed; err = %s", err)
	}
	if diff := cmp.Diff(wantCount, gotCount["operation-outcome-counter"].Count); diff != "" {
		t.Errorf("GetResults() returned unexpected count (-want +got): \n%s", diff)
	}
}
