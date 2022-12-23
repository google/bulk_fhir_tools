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

package bulkfhir

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/medical_claims_tools/internal/testhelpers"
)

func TestInMemoryTransactionTimeStore(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		description          string
		timestampStr         string
		wantErr              bool
		wantInitialTimestamp time.Time
	}{
		{
			description:          "empty",
			timestampStr:         "",
			wantInitialTimestamp: time.Time{},
		},
		{
			description:  "invalid",
			timestampStr: "invalid",
			wantErr:      true,
		},
		{
			description:          "UTC",
			timestampStr:         "2022-11-25T14:54:33Z",
			wantInitialTimestamp: time.Date(2022, 11, 25, 14, 54, 33, 0, time.UTC),
		},
		{
			description:          "UTC fractional seconds",
			timestampStr:         "2022-11-25T14:54:33.123Z",
			wantInitialTimestamp: time.Date(2022, 11, 25, 14, 54, 33, 123000000, time.UTC),
		},
		{
			description:          "with timezone",
			timestampStr:         "2022-11-25T14:54:33-05:30",
			wantInitialTimestamp: time.Date(2022, 11, 25, 20, 24, 33, 0, time.UTC),
		},
		{
			description:          "with timezone and fractional seconds",
			timestampStr:         "2022-11-25T14:54:33.123-05:30",
			wantInitialTimestamp: time.Date(2022, 11, 25, 20, 24, 33, 123000000, time.UTC),
		},
	} {
		t.Run(tc.description, func(t *testing.T) {
			s, err := NewInMemoryTransactionTimeStore(tc.timestampStr)
			if err != nil {
				if !tc.wantErr {
					t.Fatalf("got unexpected error from NewInMemoryTransactionTimeStore(%q): %v", tc.timestampStr, err)
				}
				return
			} else if tc.wantErr {
				t.Fatalf("expected error from NewInMemoryTransactionTimeStore(%q); got nil", tc.timestampStr)
			}
			got, err := s.Load(ctx)
			if err != nil {
				t.Fatalf("got unexpected error from inMemoryTransactionTimeStore.Load(): %v", err)
			}
			if !got.Equal(tc.wantInitialTimestamp) {
				t.Errorf("unexpected timestamp from inMemoryTransactionTimeStore.Load(): want %s; got %s", tc.wantInitialTimestamp, got.In(time.UTC))
			}
			if err := s.Store(ctx, time.Now()); err != nil {
				t.Fatalf("got unexpected error from inMemoryTransactionTimeStore.Store(): %v", err)
			}
		})
	}
}

func TestLocalFileTransactionTimeStore(t *testing.T) {
	ctx := context.Background()

	filename := filepath.Join(t.TempDir(), "since.txt")

	s := NewLocalFileTransactionTimeStore(filename)

	got, err := s.Load(ctx)
	if err != nil {
		t.Fatalf("unexpected error from Load(): %v", err)
	}
	if !got.IsZero() {
		t.Errorf("expected initial timestamp to be zero; got %s", got)
	}

	time1 := time.Date(2022, 11, 25, 14, 54, 33, 0, time.UTC)
	testStoreAndRetrieve(ctx, t, s, time1)

	time2 := time.Date(2022, 11, 26, 14, 51, 22, 0, time.UTC)
	testStoreAndRetrieve(ctx, t, s, time2)

	// Note: we check the contents of the file solely to assert the behaviour
	// that timestamps are appended to the file, rather than replacing its
	// contents

	gotContentBytes, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("unexpected error reading %s: %v", filename, err)
	}
	gotContents := string(gotContentBytes)

	wantContents := "2022-11-25T14:54:33.000+00:00\n2022-11-26T14:51:22.000+00:00\n"

	if diff := cmp.Diff(wantContents, gotContents); diff != "" {
		t.Errorf("unexpected diff in since file (-want, +got):\n%s", diff)
	}
}

func TestGCSTransactionTimeStore(t *testing.T) {
	ctx := context.Background()

	gcsServer := testhelpers.NewGCSServer(t)

	sinceFile := "gs://sinceBucket/sinceFile"

	s, err := NewGCSTransactionTimeStore(ctx, gcsServer.URL(), sinceFile)
	if err != nil {
		t.Fatalf("unexpected error from NewGCSTransactionTimeStore(%q, %q)", gcsServer.URL(), sinceFile)
	}

	got, err := s.Load(ctx)
	if err != nil {
		t.Fatalf("unexpected error from gcsTransactionTimeStore.Load(): %v", err)
	}
	if !got.IsZero() {
		t.Errorf("expected initial timestamp to be zero; got %s", got)
	}

	time1 := time.Date(2022, 11, 25, 14, 54, 33, 0, time.UTC)
	testStoreAndRetrieve(ctx, t, s, time1)

	time2 := time.Date(2022, 11, 26, 14, 51, 22, 0, time.UTC)
	testStoreAndRetrieve(ctx, t, s, time2)

	// Note: we check the contents of the file solely to assert the behaviour
	// that timestamps are appended to the file, rather than replacing its
	// contents

	obj, ok := gcsServer.GetObject("sinceBucket", "sinceFile")
	if !ok {
		t.Fatalf("%s not found", sinceFile)
	}

	gotContents := string(obj.Data)

	wantContents := "2022-11-25T14:54:33.000+00:00\n2022-11-26T14:51:22.000+00:00\n"

	if diff := cmp.Diff(wantContents, gotContents); diff != "" {
		t.Errorf("unexpected diff in since file (-want, +got):\n%s", diff)
	}
}

func testStoreAndRetrieve(ctx context.Context, t *testing.T, s TransactionTimeStore, ts time.Time) {
	t.Helper()
	if err := s.Store(ctx, ts); err != nil {
		t.Fatalf("got unexpected error from Store(): %v", err)
	}
	got, err := s.Load(ctx)
	if err != nil {
		t.Fatalf("got unexpected error from Load(): %v", err)
	}
	if !got.Equal(ts) {
		t.Errorf("unexpected timestamp from Load(): want %s; got %s", ts, got)
	}
}
