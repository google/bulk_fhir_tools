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

package bcda_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/medical_claims_tools/bcda"
	"github.com/google/medical_claims_tools/bulkfhir"
)

func TestNewClient(t *testing.T) {
	// This tests that the bcda package creates a bulkfhir.Client that hits the
	// proper bcda style endpoint URLs only. Tests checking that bulkfhir.Client
	// behaves correctly in general found in bulkfhir/client_test.go.
	wantAuthEndpoint := "/auth/token"
	wantBulkDataExportEndpoint := "/api/v2/Group/all/$export"
	wantJobStatusEndpointSuffix := "/api/v2/jobs/1"

	var correctAuthCalled boolMutex
	var correctBulkDataExportCalled boolMutex
	var correctJobStatusCalled boolMutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.String() {
		case wantAuthEndpoint:
			correctAuthCalled.Lock()
			correctAuthCalled.called = true
			correctAuthCalled.Unlock()
			w.Write([]byte(`{"access_token": "token", "expires_in": 1200}`))
		case wantBulkDataExportEndpoint:
			correctBulkDataExportCalled.Lock()
			correctBulkDataExportCalled.called = true
			correctBulkDataExportCalled.Unlock()
			w.Header()["Content-Location"] = []string{"some/info/"}
			w.WriteHeader(http.StatusAccepted)
		case wantJobStatusEndpointSuffix:
			correctJobStatusCalled.Lock()
			correctJobStatusCalled.called = true
			correctJobStatusCalled.Unlock()
			w.Header()["X-Progress"] = []string{fmt.Sprintf("(%d%%)", 20)}
			w.WriteHeader(http.StatusAccepted)
		default:
			t.Errorf("unexpected call received by bcda server at: %v", req.URL.String())
		}
	}))
	defer server.Close()

	baseURL := server.URL
	cl, err := bcda.NewClient(baseURL, "clientID", "clientSecret")
	if err != nil {
		t.Errorf("unexpected error from NewClient %v", err)
	}

	_, err = cl.StartBulkDataExport(nil, time.Time{}, bulkfhir.ExportGroupAll)
	if err != nil {
		t.Errorf("got unexpected error from StartBulkDataExport: %v", err)
	}

	jobStatusURL := server.URL + wantJobStatusEndpointSuffix
	_, err = cl.JobStatus(jobStatusURL)
	if err != nil {
		t.Errorf("got unexpected error from JobStatus: %v", err)
	}

	if !correctAuthCalled.called {
		t.Errorf("correct Authenticate endpoint was not called. expected call at: %v", wantAuthEndpoint)
	}
	if !correctBulkDataExportCalled.called {
		t.Errorf("correct StartBulkDataExport endpoint not called. expected call at %v", wantBulkDataExportEndpoint)
	}
	if !correctJobStatusCalled.called {
		t.Errorf("correct JobStatus endpoint not called. expected call at suffix %v", wantJobStatusEndpointSuffix)
	}
}

type boolMutex struct {
	called bool
	sync.Mutex
}
