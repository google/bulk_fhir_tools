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
	"testing"

	"github.com/google/bulk_fhir_tools/internal/metrics/aggregation"
)

func TestInitAfterRecordError(t *testing.T) {
	c := NewCounter("TestInitAfterRecordError", "Counter Description", "ms", aggregation.Count)
	c.Record(context.Background(), 1)
	gotErr := InitAndExportGCP("")
	if !errors.Is(gotErr, errInitAfterRecord) {
		t.Errorf("InitAndExportGCP() wanted error: got %v want %v", gotErr, errInitAfterRecord)
	}
}
