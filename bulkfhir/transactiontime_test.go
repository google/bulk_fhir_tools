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
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
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

	var gcsFileContents []byte
	gcsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/sinceBucket/sinceFile":
			if gcsFileContents == nil {
				w.WriteHeader(http.StatusNotFound)
			} else {
				w.Write(gcsFileContents)
			}
		case "/upload/storage/v1/b/sinceBucket/o":
			// We expect that the GCS upload request uses the multipart format, and
			// look for a text/plain part.
			//
			// TODO(b/260404461): replace this with storage-testbench or a common GCS
			//                    server implementation
			_, params, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
			if err != nil {
				t.Fatalf("failed to parse media type header: %v", err)
			}
			mr := multipart.NewReader(req.Body, params["boundary"])
			gotContents := false
			for {
				p, err := mr.NextPart()
				if err == io.EOF {
					return
				}
				if err != nil {
					t.Fatalf("failed to get next part: %v", err)
				}
				if strings.Contains(p.Header.Get("Content-Type"), "text/plain") {
					data, err := io.ReadAll(p)
					if err != nil {
						t.Fatalf("failed to read request body: %v", err)
					}
					gcsFileContents = data
					gotContents = true
					break
				}
			}
			if !gotContents {
				t.Fatal("failed to extract file contents from GCS request")
			}
			w.Write([]byte("{}"))
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))

	sinceFile := "gs://sinceBucket/sinceFile"

	s, err := NewGCSTransactionTimeStore(gcsServer.URL, sinceFile)
	if err != nil {
		t.Fatalf("unexpected error from NewGCSTransactionTimeStore(%q, %q)", gcsServer.URL, sinceFile)
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

	gotContents := string(gcsFileContents)

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
