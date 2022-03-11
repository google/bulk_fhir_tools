// Copyright 2021 Google LLC
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

package bcda

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestClient_Authenticate(t *testing.T) {
	versionCases := []struct {
		name    string
		version Version
	}{
		{
			"V1",
			V1,
		},
		{
			"V2",
			V2,
		},
	}
	for _, versionCase := range versionCases {
		t.Run(versionCase.name, func(t *testing.T) {
			testAuthenticate(t, versionCase.version)
		})
	}
}

func testAuthenticate(t *testing.T, version Version) {
	clientID := "clientID"
	clientSecret := "clientSecret"

	expectedPath := "/auth/token"
	expectedToken := "123"
	expectedAcceptValue := "application/json"
	// Start an httptest server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.String() != expectedPath {
			t.Errorf("Authenticate(%s, %s) made request with unexpected path. got: %v, want: %v", clientID, clientSecret, req.URL.String(), expectedPath)
		}

		id, sec, ok := req.BasicAuth()
		if !ok {
			t.Errorf("Authenticate(%s, %s) basic auth not OK.", clientID, clientSecret)
		}
		if id != clientID {
			t.Errorf("Authenticate(%s, %s) sent unexpected clientID, got %s, want: %s", clientID, clientSecret, id, clientID)
		}
		if sec != clientSecret {
			t.Errorf("Authenticate(%s, %s) sent unexpected clientSecret, got %s, want: %s", clientID, clientSecret, sec, clientSecret)
		}

		accValues, ok := req.Header["Accept"]
		if !ok {
			t.Errorf("Authenticate(%s, %s) did not sent Accept header", clientID, clientSecret)
		}

		found := false
		for _, val := range accValues {
			if val == expectedAcceptValue {
				found = true
			}
		}
		if !found {
			t.Errorf("Authenticate(%s, %s) did not sent expected Accept header value of %v", clientID, clientSecret, expectedAcceptValue)
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(fmt.Sprintf("{\"access_token\": \"%s\"}", expectedToken)))
	}))
	defer server.Close()

	cl, err := NewClient(server.URL, version)
	if err != nil {
		t.Fatalf("NewClient(%v, %v) error: %v", server.URL, version, err)
	}

	token, err := cl.Authenticate(clientID, clientSecret)
	if err != nil {
		t.Errorf("Authenticate(%s, %s) returned unexpected error: %v", clientID, clientSecret, err)
	}
	if token != expectedToken {
		t.Errorf("Authenticate(%s, %s) returned unexpected token. got %v, want: %v", clientID, clientSecret, token, expectedToken)
	}
}

func TestClient_StartBulkDataExport(t *testing.T) {
	versionCases := []struct {
		name    string
		version Version
	}{
		{
			"V1",
			V1,
		},
		{
			"V2",
			V2,
		},
	}

	for _, versionCase := range versionCases {
		t.Run(versionCase.name, func(t *testing.T) {
			testStartBulkDataExport(t, versionCase.version)
		})
	}
}

func testStartBulkDataExport(t *testing.T, version Version) {
	t.Run("without token", func(t *testing.T) {
		c := Client{httpClient: &http.Client{}, version: version}
		_, err := c.StartBulkDataExport([]ResourceType{}, time.Time{})
		if err != ErrorUnauthorized {
			t.Errorf("StartBulkDataExport returned incorrect error. got: %v, want: %v", err, ErrorUnauthorized)
		}
	})

	t.Run("unauthorized", func(t *testing.T) {
		server := newUnauthorizedServer(t)
		cl := Client{token: "123", baseURL: server.URL, httpClient: &http.Client{}, version: version}
		_, err := cl.StartBulkDataExport(AllResourceTypes, time.Time{})
		if err != ErrorUnauthorized {
			t.Errorf("StartBulkDataExport unexpected error returned: got: %v, want: %v", err, ErrorUnauthorized)
		}
	})

	t.Run("full test with since and resource types", func(t *testing.T) {
		token := "123"
		resourceTypes := []ResourceType{Patient, ExplanationOfBenefit, Coverage}
		since := time.Date(2013, 12, 9, 11, 0, 0, 123000000, time.UTC)

		expectedPath := "/api/v1/Group/all/$export"
		if version == V2 {
			expectedPath = "/api/v2/Group/all/$export"
		}
		expectedAcceptValue := "application/fhir+json"
		expectedSince := "2013-12-09T11:00:00.123+00:00"
		expectedTypes := "Patient,ExplanationOfBenefit,Coverage"
		expectedJobID := "jobid"

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if req.URL.Path != expectedPath {
				t.Errorf("StartBulkDataExport(%v, %v) made request with unexpected path. got: %v, want: %v", resourceTypes, since, req.URL.String(), expectedPath)
			}

			if got := len(req.URL.Query()["_since"]); got != 1 {
				t.Errorf("StartBulkDataExport(%v, %v): unexpected number of _since params. got: %d, want: %d", resourceTypes, since, got, 1)
			}
			if got := req.URL.Query()["_since"][0]; got != expectedSince {
				t.Errorf("StartBulkDataExport(%v, %v): sent unexpected _since value, got %v, want: %v", resourceTypes, since, got, expectedSince)
			}

			if got := len(req.URL.Query()["_type"]); got != 1 {
				t.Errorf("StartBulkDataExport(%v, %v): unexpected number of _type params. got: %d, want: %d", resourceTypes, since, got, 1)
			}
			if got := req.URL.Query()["_type"][0]; got != expectedTypes {
				t.Errorf("StartBulkDataExport(%v, %v): sent unexpected _type value, got %v, want: %v", resourceTypes, since, got, expectedTypes)
			}

			accValues, ok := req.Header["Accept"]
			if !ok {
				t.Errorf("StartBulkDataExport(%v, %v) did not sent Accept header", resourceTypes, since)
			}

			found := false
			for _, val := range accValues {
				if val == expectedAcceptValue {
					found = true
				}
			}
			if !found {
				t.Errorf("StartBulkDataExport(%v, %v) did not sent expected Accept header value of %v", resourceTypes, since, expectedAcceptValue)
			}

			w.Header()["Content-Location"] = []string{"some/info/" + expectedJobID}
			w.WriteHeader(http.StatusAccepted)
		}))
		defer server.Close()

		cl := Client{token: token, baseURL: server.URL, httpClient: &http.Client{}, version: version}
		id, err := cl.StartBulkDataExport(resourceTypes, since)
		if err != nil {
			t.Errorf("StartBulkDataExport(%v, %v) returned unexpected error: %v", resourceTypes, since, err)
		}
		if id != expectedJobID {
			t.Errorf("StartBulkDataExport(%v, %v) returned unexpected jobID got: %v, want: %v", resourceTypes, since, id, expectedJobID)
		}
	})

	t.Run("without combinations of since and resourceTypes", func(t *testing.T) {
		tests := []struct {
			name          string
			resourceTypes []ResourceType
			since         time.Time
		}{
			{
				name:          "with empty since",
				resourceTypes: AllResourceTypes,
				since:         time.Time{},
			},
			{
				name:          "with empty resource types",
				resourceTypes: []ResourceType{},
				since:         time.Unix(0, 1233810057012345600),
			},
			{
				name:          "with both empty resource types and since",
				resourceTypes: []ResourceType{},
				since:         time.Time{},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				token := "123"
				expectedJobID := "jobid"
				// Expected number of header values for _since and _type
				expectedNumSince := 0
				expectedNumResourceType := 0
				if !tc.since.IsZero() {
					expectedNumSince = 1
				}
				if len(tc.resourceTypes) > 0 {
					expectedNumResourceType = 1
				}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					if got := len(req.URL.Query()["_since"]); got != expectedNumSince {
						t.Errorf("StartBulkDataExport(%v, %v): unexpected number of _since params. got: %d, want: %d", tc.resourceTypes, tc.since, got, expectedNumSince)
					}
					if got := len(req.URL.Query()["_type"]); got != expectedNumResourceType {
						t.Errorf("StartBulkDataExport(%v, %v): unexpected number of _type params. got: %d, want: %d", tc.resourceTypes, tc.since, got, expectedNumResourceType)
					}

					w.Header()["Content-Location"] = []string{"some/info/" + expectedJobID}
				}))
				defer server.Close()

				cl := Client{token: token, baseURL: server.URL, httpClient: &http.Client{}, version: version}
				id, err := cl.StartBulkDataExport(tc.resourceTypes, tc.since)
				if err != nil {
					t.Errorf("StartBulkDataExport(%v, %v) returned unexpected error: %v", tc.resourceTypes, tc.since, err)
				}
				if id != expectedJobID {
					t.Errorf("StartBulkDataExport(%v, %v) returned unexpected jobID got: %v, want: %v", tc.resourceTypes, tc.since, id, expectedJobID)
				}
			})
		}
	})

	t.Run("server returns unexpected Content-Location", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.Header()["Content-Location"] = []string{"some/info/jobid", "extra content location"}
		}))
		defer server.Close()

		token := "123"
		cl := Client{token: token, baseURL: server.URL, httpClient: &http.Client{}, version: version}
		_, err := cl.StartBulkDataExport(AllResourceTypes, time.Time{})
		if !errors.Is(err, ErrorGreaterThanOneContentLocation) {
			t.Errorf("StartBulkDataExport(%v, %v) unexpected underlying error got: %v want: %v", AllResourceTypes, time.Time{}, err, ErrorGreaterThanOneContentLocation)
		}
	})
}

func TestClient_GetJobStatus(t *testing.T) {
	versionCases := []struct {
		name    string
		version Version
	}{
		{
			"V1",
			V1,
		},
		{
			"V2",
			V2,
		},
	}
	for _, versionCase := range versionCases {
		t.Run(versionCase.name, func(t *testing.T) {
			testGetJobStatus(t, versionCase.version)
		})
	}
}

func testGetJobStatus(t *testing.T, version Version) {
	t.Run("without token", func(t *testing.T) {
		c := Client{httpClient: &http.Client{}, version: version}
		_, err := c.JobStatus("id")
		if err != ErrorUnauthorized {
			t.Errorf("GetJobStatus returned incorrect error. got: %v, want: %v", err, ErrorUnauthorized)
		}
	})

	t.Run("unauthorized", func(t *testing.T) {
		server := newUnauthorizedServer(t)
		cl := Client{token: "123", baseURL: server.URL, httpClient: &http.Client{}, version: version}
		_, err := cl.JobStatus("id")
		if err != ErrorUnauthorized {
			t.Errorf("GetJobStatus returned unexpected error returned: got: %v, want: %v", err, ErrorUnauthorized)
		}
	})

	t.Run("unexpected number of X-Progress", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.Header()["X-Progress"] = []string{fmt.Sprintf("(%d%%)", 60), fmt.Sprintf("(%d%%)", 160)}
			w.WriteHeader(http.StatusAccepted)
		}))
		cl := Client{token: "123", baseURL: server.URL, httpClient: &http.Client{}, version: version}
		_, err := cl.JobStatus("id")
		if !errors.Is(err, ErrorUnexpectedNumberOfXProgress) {
			t.Errorf("JobStatus returned unexpected underlying error. got: %v, want: %v", err, ErrorUnexpectedNumberOfXProgress)
		}
	})

	t.Run("valid request", func(t *testing.T) {
		jobID := "id"
		token := "123"

		expectedPath := fmt.Sprintf("/api/v1/jobs/%s", jobID)
		if version == V2 {
			expectedPath = fmt.Sprintf("/api/v2/jobs/%s", jobID)
		}
		expectedProgress := 60
		expectedAuth := fmt.Sprintf("Bearer %s", token)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if req.URL.Path != expectedPath {
				t.Errorf("GetJobStatus(%v) made request with unexpected path. got: %v, want: %v", jobID, req.URL.String(), expectedPath)
			}

			authValues, ok := req.Header["Authorization"]
			if !ok {
				t.Errorf("GetJobStatus(%v) did not send Authorization header", jobID)
			}
			if got, want := len(authValues), 1; got != want {
				t.Errorf("GetJobStatus(%v) sent incorrect number of Authorization header values, got: %d, want: %d", jobID, got, want)
			}
			if got, want := authValues[0], expectedAuth; got != want {
				t.Errorf("GetJobStatus(%v) sent wrong Authorization header value, got: %s, want: %s", jobID, got, want)
			}

			w.Header()["X-Progress"] = []string{fmt.Sprintf("(%d%%)", expectedProgress)}
			w.WriteHeader(http.StatusAccepted)
		}))
		cl := Client{token: token, baseURL: server.URL, httpClient: &http.Client{}, version: version}
		_, err := cl.JobStatus(jobID)
		if err != nil {
			t.Errorf("GetJobStatus(%v) returned unexpected error: %v", jobID, err)
		}
	})

	t.Run("job in progress", func(t *testing.T) {
		jobID := "id"
		token := "123"

		expectedProgress := 60
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.Header()["X-Progress"] = []string{fmt.Sprintf("(%d%%)", expectedProgress)}
			w.WriteHeader(http.StatusAccepted)
		}))

		cl := Client{token: token, baseURL: server.URL, httpClient: &http.Client{}, version: version}
		jobStatus, err := cl.JobStatus(jobID)
		if err != nil {
			t.Errorf("GetJobStatus(%v) returned unexpected error: %v", jobID, err)
		}
		if jobStatus.IsComplete {
			t.Errorf("GetJobStatus(%v) got complete JobStatus, expected incomplete", jobID)
		}
		if got, want := jobStatus.PercentComplete, expectedProgress; got != want {
			t.Errorf("GetJobStatus(%v) returned incorrect percent complete: got: %d, want: %d", jobID, got, want)
		}
	})

	t.Run("job completed", func(t *testing.T) {
		jobID := "id"
		token := "123"
		transactionTime := "2020-09-17T17:53:11.476Z"
		expectedTransactionTime := time.Date(2020, 9, 17, 17, 53, 11, 476000000, time.UTC)
		expectedResourceType := "Patient"
		expectedURL := "url"
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.Write([]byte(fmt.Sprintf("{\"output\": [{\"type\": \"%s\", \"url\": \"%s\"}], \"transactionTime\": \"%s\"}", expectedResourceType, expectedURL, transactionTime)))
			w.WriteHeader(http.StatusOK)
		}))

		cl := Client{token: token, baseURL: server.URL, httpClient: &http.Client{}, version: version}
		jobStatus, err := cl.JobStatus(jobID)
		if err != nil {
			t.Errorf("GetJobStatus(%v) returned unexpected error: %v", jobID, err)
		}
		if !jobStatus.IsComplete {
			t.Errorf("GetJobStatus(%v) got incomplete JobStatus, expected complete", jobID)
		}
		if got, want := len(jobStatus.ResultURLs), 1; got != want {
			t.Errorf("GetJobStatus(%v) unexpected number of ResultURLs: got: %d, want: %d", jobID, got, want)
		}
		if _, ok := jobStatus.ResultURLs[Patient]; !ok {
			t.Errorf("GetJobStatus(%v) ResultURLs no value for key Patient", jobID)
		}
		if got, want := jobStatus.ResultURLs[Patient][0], expectedURL; got != want {
			t.Errorf("GetJobStatus(%v) ResultURLs returned unexpected value for key Patient: got %s, want %s", jobID, got, want)
		}
		if got, want := jobStatus.TransactionTime, expectedTransactionTime; !got.Equal(want) {
			t.Errorf("GetJobStatus(%v) returned incorrect transaction time: got %s, want %s", jobID, got, want)
		}
	})

	t.Run("job completed multiple url", func(t *testing.T) {
		jobID := "id"
		jsonResponse := `{"transactionTime": "2020-09-15T17:53:11.476Z",
												"output":[
												{"type": "Patient","url": "url_1"},
												{"type": "Patient","url": "url_2"},
												{"type": "Patient","url": "url_3"},
												{"type": "Coverage","url": "url_4"},
												{"type": "ExplanationOfBenefit","url": "url_5"},
												{"type": "ExplanationOfBenefit","url": "url_6"},
												{"type": "OperationOutcome","url": "url_7"},
												{"type": "OperationOutcome","url": "url_8"}]}`
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.Write([]byte(jsonResponse))
			w.WriteHeader(http.StatusOK)
		}))

		cl := Client{token: "123", baseURL: server.URL, httpClient: &http.Client{}, version: version}
		jobStatus, err := cl.JobStatus(jobID)
		if err != nil {
			t.Errorf("GetJobStatus(%v) returned unexpected error: %v", jobID, err)
		}
		if !jobStatus.IsComplete {
			t.Errorf("GetJobStatus(%v) got incomplete JobStatus, expected complete", jobID)
		}
		expectedMap := map[ResourceType][]string{
			Patient:              {"url_1", "url_2", "url_3"},
			Coverage:             {"url_4"},
			ExplanationOfBenefit: {"url_5", "url_6"},
			OperationOutcome:     {"url_7", "url_8"},
		}
		if diff := cmp.Diff(expectedMap, jobStatus.ResultURLs, cmpopts.SortMaps(func(k1, k2 string) bool { return k1 < k2 })); diff != "" {
			t.Errorf("GetJobStatus(%v) returned unexpected diff (-want +got):\n%s", jobID, diff)
		}
	})

	t.Run("invalid progress", func(t *testing.T) {
		jobID := "id"
		token := "123"

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.Header()["X-Progress"] = []string{"invalid"}
			w.WriteHeader(http.StatusAccepted)
		}))

		cl := Client{token: token, baseURL: server.URL, httpClient: &http.Client{}, version: version}
		_, err := cl.JobStatus(jobID)
		if err != ErrorUnableToParseProgress {
			t.Errorf("GetJobStatus(%v) returned unexpected error: got: %v, want: %v", err, jobID, ErrorUnableToParseProgress)
		}
	})

	t.Run("invalid transaction time", func(t *testing.T) {
		jobID := "id"
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.Write([]byte(`{"output": [{"type": "Patient", "url": "url"}], "transactionTime" : "2013-12-09T11:00Z"}`))
			w.WriteHeader(http.StatusOK)
		}))

		cl := Client{token: "123", baseURL: server.URL, httpClient: &http.Client{}, version: version}
		_, err := cl.JobStatus(jobID)
		if err == nil {
			t.Errorf("GetJobStatus(%v) succeeded, want error", jobID)
		}
	})
}

func TestClient_GetData(t *testing.T) {
	versionCases := []struct {
		name    string
		version Version
	}{
		{
			"V1",
			V1,
		},
		{
			"V2",
			V2,
		},
	}
	for _, versionCase := range versionCases {
		t.Run(versionCase.name, func(t *testing.T) {
			testGetData(t, versionCase.version)
		})
	}
}

func testGetData(t *testing.T, version Version) {
	t.Run("without token", func(t *testing.T) {
		c := Client{httpClient: &http.Client{}, version: version}
		_, err := c.GetData("url")
		if err != ErrorUnauthorized {
			t.Errorf("GetData returned incorrect error. got: %v, want: %v", err, ErrorUnauthorized)
		}
	})

	t.Run("unauthorized", func(t *testing.T) {
		server := newUnauthorizedServer(t)
		cl := Client{token: "123", baseURL: server.URL, httpClient: &http.Client{}, version: version}
		_, err := cl.GetData(server.URL + "/id")
		if err != ErrorUnauthorized {
			t.Errorf("GetData returned unexpected error returned: got: %v, want: %v", err, ErrorUnauthorized)
		}
	})

	t.Run("not-OK http response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		c := Client{token: "123", httpClient: &http.Client{}, version: version}
		_, err := c.GetData(server.URL)
		if !errors.Is(err, ErrorUnexpectedStatusCode) {
			t.Errorf("GetData(%v) returned incorrect underlying error. got: %v, want: %v", server.URL, err, ErrorUnexpectedStatusCode)
		}
	})

	t.Run("valid GetData", func(t *testing.T) {
		token := "123"
		expectedAuth := fmt.Sprintf("Bearer %s", token)
		expectedResponse := []byte("the response")
		expectedPath := "/data"
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if req.URL.Path != expectedPath {
				t.Errorf("GetData(%v) made request with unexpected path. got: %v, want: %v", req.URL.String(), req.URL.Path, expectedPath)
			}
			authValues, ok := req.Header["Authorization"]
			if !ok {
				t.Errorf("GetData(%v) did not send Authorization header", req.URL.String())
			}
			if got, want := len(authValues), 1; got != want {
				t.Errorf("GetData(%v) sent incorrect number of Authorization header values, got: %d, want: %d", req.URL.String(), got, want)
			}
			if got, want := authValues[0], expectedAuth; got != want {
				t.Errorf("GetData(%v) sent wrong Authorization header value, got: %s, want: %s", req.URL.String(), got, want)
			}

			w.WriteHeader(http.StatusOK)
			w.Write(expectedResponse)
		}))

		cl := Client{baseURL: server.URL, token: token, httpClient: &http.Client{}, version: version}
		path := server.URL + expectedPath
		r, err := cl.GetData(path)
		if err != nil {
			t.Errorf("GetData(%v) returned unexpected error: %v", path, err)
		}
		t.Cleanup(func() {
			if err := r.Close(); err != nil {
				t.Errorf("Unexpected error closing returned ReadCloser: %v", err)
			}
		})
		data, err := ioutil.ReadAll(r)
		if err != nil {
			t.Errorf("Unexpected error reading returned ReadCloser: %v", err)
		}

		if diff := cmp.Diff(data, expectedResponse); diff != "" {
			t.Errorf("GetData(%v) returned unexpected response diff. (-want +got):\n%s", path, diff)
		}
	})
}

func TestClient_MonitorJobStatus(t *testing.T) {
	versionCases := []struct {
		name    string
		version Version
	}{
		{
			"V1",
			V1,
		},
		{
			"V2",
			V2,
		},
	}
	for _, versionCase := range versionCases {
		t.Run(versionCase.name, func(t *testing.T) {
			testMonitorJobStatus(t, versionCase.version)
		})
	}
}

func testMonitorJobStatus(t *testing.T, version Version) {
	t.Run("timeout", func(t *testing.T) {
		period := 2 * time.Millisecond
		timeout := 20 * time.Millisecond
		jobID := "job"

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			// Indicates job in progress
			w.Header()["X-Progress"] = []string{fmt.Sprintf("(%d%%)", 60)}
			w.WriteHeader(http.StatusAccepted)
		}))
		cl := Client{token: "123", baseURL: server.URL, httpClient: &http.Client{}, version: version}
		results := make([]*MonitorResult, 0, 1)
		for st := range cl.MonitorJobStatus(jobID, period, timeout) {
			results = append(results, st)
		}
		if got, want := results[len(results)-1].Error, ErrorTimeout; got != want {
			t.Errorf("MonitorJobStatus(%v,%v,%v) did not return correct error. got: %v, want: %v", jobID, period, timeout, got, want)
		}
	})

	t.Run("valid cases", func(t *testing.T) {
		jobID := "456"
		wantPath := fmt.Sprintf("/api/v1/jobs/%s", jobID)
		if version == V2 {
			wantPath = fmt.Sprintf("/api/v2/jobs/%s", jobID)
		}
		wantResource := Patient
		wantURL := "url"
		wantProgress := 60
		inProgressJobStatus := JobStatus{IsComplete: false, PercentComplete: wantProgress}
		completeJobStatus := JobStatus{
			IsComplete:      true,
			ResultURLs:      map[ResourceType][]string{wantResource: []string{wantURL}},
			TransactionTime: time.Date(2020, 9, 15, 17, 53, 11, 476000000, time.UTC)}

		cases := []struct {
			name            string
			period          time.Duration
			timeout         time.Duration
			wantJobStatuses []JobStatus
			// completeAfterNChecks tells the test server to report a completed status
			// on the Nth GetJobStatus.
			completeAfterNChecks int
		}{
			{
				name:                 "already completed job",
				period:               20 * time.Millisecond,
				timeout:              100 * time.Millisecond,
				wantJobStatuses:      []JobStatus{completeJobStatus},
				completeAfterNChecks: 1, // already completed
			},
			{
				name:                 "soon to be completed job",
				period:               20 * time.Millisecond,
				timeout:              300 * time.Millisecond,
				wantJobStatuses:      []JobStatus{inProgressJobStatus, completeJobStatus},
				completeAfterNChecks: 2,
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				var counter struct {
					sync.Mutex
					count int
				}

				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					if req.URL.Path != wantPath {
						t.Errorf("MonitorJobStatus(%v) made request with unexpected path. got: %v, want: %v", jobID, req.URL.String(), wantPath)
						return
					}
					counter.Lock()
					defer counter.Unlock()
					counter.count++
					if counter.count >= tc.completeAfterNChecks {
						// Write out the completed result
						w.WriteHeader(http.StatusOK)
						w.Write([]byte(fmt.Sprintf("{\"output\": [{\"type\": \"%s\", \"url\": \"%s\"}], \"transactionTime\": \"2020-09-15T17:53:11.476Z\"}", wantResource, wantURL)))
					} else {
						// Write out in progress result
						w.Header()["X-Progress"] = []string{fmt.Sprintf("(%d%%)", wantProgress)}
						w.WriteHeader(http.StatusAccepted)
					}
				}))

				cl := Client{token: "123", baseURL: server.URL, httpClient: &http.Client{}, version: version}
				results := make([]JobStatus, 0, 1)

				for st := range cl.MonitorJobStatus(jobID, tc.period, tc.timeout) {
					if st.Error != nil {
						t.Errorf("MonitorJobStatus(%v,%v,%v) returned unexpected error: %v", jobID, tc.period, tc.timeout, st.Error)
					}
					results = append(results, st.Status)
				}

				if diff := cmp.Diff(tc.wantJobStatuses, results); diff != "" {
					t.Errorf("MonitorJobStatus(%v,%v,%v) unexpected diff in result (-want +got):\n%s", jobID, tc.period, tc.timeout, diff)
				}
			})
		}
	})
}

// newUnauthorizedServer returns an httptest.Server that will always return
// with a HTTP 401 unauthorized status code. It uses t.Cleanup to close the
// server when the test is complete.
func newUnauthorizedServer(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	t.Cleanup(func() { server.Close() })
	return server
}
