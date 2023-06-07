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
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/medical_claims_tools/bulkfhir"
	"github.com/google/medical_claims_tools/fhir/processing"
	"github.com/google/medical_claims_tools/internal/metrics"
	"github.com/google/medical_claims_tools/internal/testhelpers"

	cpb "github.com/google/fhir/go/proto/google/fhir/proto/r4/core/codes_go_proto"
)

func TestDocumentsProcessor(t *testing.T) {
	ctx := context.Background()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/auth":
			w.Write([]byte(`{"access_token": "123", "expires_in": 1200}`))
		case "/document.pdf":
			// We don't check file contents, so this doesn't need to be real PDF data.
			w.Write([]byte(`pdf data`))
		case "/document.xml":
			// We don't check file contents, so this doesn't need to be real XML data.
			w.Write([]byte(`xml data`))
		case "/nonexistent.pdf":
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(fmt.Sprintf("Unrecognised path %s", req.URL.Path)))
		}
	}))
	defer server.Close()

	for _, tc := range []struct {
		description  string
		resourceType cpb.ResourceTypeCode_Value
		input        string
		wantError    bool
		wantJSON     string
		wantCount    map[string]int64
	}{
		{
			description:  "patient resource ignored",
			resourceType: cpb.ResourceTypeCode_PATIENT,
			input:        `{"resourceType": "Patient", "id": "123"}`,
			wantJSON:     `{"resourceType": "Patient", "id": "123"}`,
			wantCount:    map[string]int64{},
		},
		{
			description:  "With documents",
			resourceType: cpb.ResourceTypeCode_DOCUMENT_REFERENCE,
			input:        `{"resourceType": "DocumentReference", "id": "documentid", "content": [{"attachment": {"contentType": "application/pdf", "url": "` + server.URL + `/document.pdf"}}, {"attachment": {"contentType": "text/xml", "url": "` + server.URL + `/document.xml"}}]}`,
			wantJSON:     `{"resourceType": "DocumentReference", "id": "documentid", "content": [{"attachment": {"contentType": "application/pdf", "url": "FILEPATH0"}}, {"attachment": {"contentType": "text/xml", "url": "FILEPATH1"}}]}`,
			wantCount:    map[string]int64{"OK": 2},
		},
		{
			description:  "Non-existent document",
			resourceType: cpb.ResourceTypeCode_DOCUMENT_REFERENCE,
			input:        `{"resourceType": "DocumentReference", "id": "documentid", "content": [{"attachment": {"contentType": "application/pdf", "url": "` + server.URL + `/nonexistent.pdf"}}]}`,
			wantJSON:     `{"resourceType": "DocumentReference", "id": "documentid", "content": [{"attachment": {"contentType": "application/pdf", "url": "` + server.URL + `/nonexistent.pdf"}}]}`,
			wantCount:    map[string]int64{"Not Found": 1},
		},
		{
			description:  "Inline data unchanged",
			resourceType: cpb.ResourceTypeCode_DOCUMENT_REFERENCE,
			input:        `{"resourceType": "DocumentReference", "id": "documentid", "content": [{"attachment": {"contentType": "text/plain", "data": "SGVsbG8gV29ybGQh"}}]}`,
			wantJSON:     `{"resourceType": "DocumentReference", "id": "documentid", "content": [{"attachment": {"contentType": "text/plain", "data": "SGVsbG8gV29ybGQh"}}]}`,
			wantCount:    map[string]int64{},
		},
	} {
		t.Run(tc.description, func(t *testing.T) {
			metrics.ResetAll()
			tempdir, err := os.MkdirTemp("", "")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(tempdir)
			ts := &processing.TestSink{}
			authenticator, err := bulkfhir.NewHTTPBasicOAuthAuthenticator("username", "password", server.URL+"/auth", nil)
			if err != nil {
				t.Fatal(err)
			}
			proc, err := processing.NewDocumentsProcessor(ctx, &processing.DocumentsProcessorConfig{
				Authenticator:  authenticator,
				HTTPClient:     http.DefaultClient,
				LocalDirectory: tempdir,
			})
			if err != nil {
				t.Fatalf("NewDocumentProcessor() returned unexpected error: %v", err)
			}
			p, err := processing.NewPipeline([]processing.Processor{proc}, []processing.Sink{ts})
			if err != nil {
				t.Fatalf("NewPipeline() returned unexpected error: %v", err)
			}
			err = p.Process(context.Background(), tc.resourceType, "", []byte(tc.input))
			if tc.wantError && err == nil {
				t.Fatalf("pipeline.Process(..., %s) did not return expected error", tc.input)
			} else if !tc.wantError && err != nil {
				t.Fatalf("pipeline.Process(..., %s) returned unexpected error: %v", tc.input, err)
			}

			// We can't guarantee the filename of the output files, because they
			// depend on the hosts MIME type configuration (we have seen cases where
			// different host produce unusual filename extensions. Instead, we verify
			// that the filename in the output resource exists, and then populate the
			// wanted resource with the filename that was written.

			gotResource, err := ts.WrittenResources[0].Proto()
			// Note we are expecting the processing.ErrorDoNotModifyProto error, because we are calling
			// Proto() in a sink.
			if err != nil && err != processing.ErrorDoNotModifyProto {
				t.Fatalf("writtenResource.Proto() returned unexpected error: %v", err)
			}
			for i, c := range gotResource.GetDocumentReference().GetContent() {
				url := c.GetAttachment().GetUrl().GetValue()
				if strings.HasPrefix(url, "file://") {
					_, err := os.Stat(strings.TrimPrefix(url, "file://"))
					if err != nil {
						t.Errorf("Failed to stat expected file %s: %v", url, err)
					}
				}
				// We're inserting a file path potentially containing backslashes (on
				// Windows) into an encoded JSON string, so we need to escape any
				// backslashes.
				tc.wantJSON = strings.Replace(tc.wantJSON, fmt.Sprintf("FILEPATH%d", i), strings.ReplaceAll(url, `\`, `\\`), 1)
			}

			gotJSON, err := ts.WrittenResources[0].JSON()
			if err != nil {
				t.Fatalf("writtenResource.JSON() returned unexpected error: %v", err)
			}
			normalizedWantJSON := testhelpers.NormalizeJSON(t, []byte(tc.wantJSON))
			normalizedGotJSON := testhelpers.NormalizeJSON(t, gotJSON)
			if !cmp.Equal(normalizedGotJSON, normalizedWantJSON) {
				t.Errorf("pipeline.Process(..., %s) produced unexpected output. got: %s, want: %s", tc.input, normalizedGotJSON, normalizedWantJSON)
			}

			gotCount, _, err := metrics.GetResults()
			if err != nil {
				t.Errorf("GetResults failed; err = %s", err)
			}
			if diff := cmp.Diff(tc.wantCount, gotCount["document-retrieval-counter"].Count); diff != "" {
				t.Errorf("GetResults() return unexpected count (-want +got): \n%s", diff)
			}
		})
	}
}
