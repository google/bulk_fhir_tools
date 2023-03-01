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

package bulkfhir

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

	cpb "github.com/google/fhir/go/proto/google/fhir/proto/r4/core/codes_go_proto"
)

type testAuthenticator struct{}

func (testAuthenticator) Authenticate(hc *http.Client) error            { return nil }
func (testAuthenticator) AuthenticateIfNecessary(hc *http.Client) error { return nil }
func (testAuthenticator) AddAuthenticationToRequest(hc *http.Client, req *http.Request) error {
	return nil
}

func TestClient_StartBulkDataExport(t *testing.T) {
	t.Run("unauthorized", func(t *testing.T) {
		server := newUnauthorizedServer(t)
		cl := Client{authenticator: testAuthenticator{}, baseURL: server.URL, httpClient: &http.Client{}}
		_, err := cl.StartBulkDataExport(nil, time.Time{}, ExportGroupAll)
		if err != ErrorUnauthorized {
			t.Errorf("StartBulkDataExport unexpected error returned: got: %v, want: %v", err, ErrorUnauthorized)
		}
	})

	t.Run("full test with since and resource types", func(t *testing.T) {
		resourceTypes := []cpb.ResourceTypeCode_Value{
			cpb.ResourceTypeCode_PATIENT,
			cpb.ResourceTypeCode_EXPLANATION_OF_BENEFIT,
			cpb.ResourceTypeCode_COVERAGE,
		}
		since := time.Date(2013, 12, 9, 11, 0, 0, 123000000, time.UTC)
		group := "mygroup"

		expectedPath := "/Group/mygroup/$export"
		expectedAcceptValue := "application/fhir+json"
		expectedSince := "2013-12-09T11:00:00.123+00:00"
		expectedTypes := "Patient,ExplanationOfBenefit,Coverage"
		expectedJobStatusURL := "/some/url/job/1"

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

			w.Header()["Content-Location"] = []string{expectedJobStatusURL}
			w.WriteHeader(http.StatusAccepted)
		}))
		defer server.Close()

		cl := Client{authenticator: testAuthenticator{}, baseURL: server.URL, httpClient: &http.Client{}}
		jobURL, err := cl.StartBulkDataExport(resourceTypes, since, group)
		if err != nil {
			t.Errorf("StartBulkDataExport(%v, %v) returned unexpected error: %v", resourceTypes, since, err)
		}
		if jobURL != expectedJobStatusURL {
			t.Errorf("StartBulkDataExport(%v, %v) returned unexpected job status URL got: %v, want: %v", resourceTypes, since, jobURL, expectedJobStatusURL)
		}
	})

	t.Run("without combinations of since and resourceTypes", func(t *testing.T) {
		tests := []struct {
			name          string
			resourceTypes []cpb.ResourceTypeCode_Value
			since         time.Time
		}{
			{
				name: "with empty since",
				resourceTypes: []cpb.ResourceTypeCode_Value{
					cpb.ResourceTypeCode_PATIENT,
					cpb.ResourceTypeCode_COVERAGE,
					cpb.ResourceTypeCode_EXPLANATION_OF_BENEFIT,
				},
				since: time.Time{},
			},
			{
				name:          "with empty resource types",
				resourceTypes: nil,
				since:         time.Unix(0, 1233810057012345600),
			},
			{
				name:          "with both empty resource types and since",
				resourceTypes: nil,
				since:         time.Time{},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				expectedJobStatusURL := "/some/url/job/1"
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

					w.Header()["Content-Location"] = []string{expectedJobStatusURL}
				}))
				defer server.Close()

				cl := Client{authenticator: testAuthenticator{}, baseURL: server.URL, httpClient: &http.Client{}}
				jobURL, err := cl.StartBulkDataExport(tc.resourceTypes, tc.since, ExportGroupAll)
				if err != nil {
					t.Errorf("StartBulkDataExport(%v, %v) returned unexpected error: %v", tc.resourceTypes, tc.since, err)
				}
				if jobURL != expectedJobStatusURL {
					t.Errorf("StartBulkDataExport(%v, %v) returned unexpected jobID got: %v, want: %v", tc.resourceTypes, tc.since, jobURL, expectedJobStatusURL)
				}
			})
		}
	})

	t.Run("server returns unexpected Content-Location", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.Header()["Content-Location"] = []string{"some/info/jobid", "extra content location"}
		}))
		defer server.Close()

		cl := Client{authenticator: testAuthenticator{}, baseURL: server.URL, httpClient: &http.Client{}}
		_, err := cl.StartBulkDataExport(nil, time.Time{}, ExportGroupAll)
		if !errors.Is(err, ErrorGreaterThanOneContentLocation) {
			t.Errorf("StartBulkDataExport(nil, %v) unexpected underlying error got: %v want: %v", time.Time{}, err, ErrorGreaterThanOneContentLocation)
		}
	})
}

func TestClient_GetJobStatus(t *testing.T) {
	t.Run("unauthorized", func(t *testing.T) {
		server := newUnauthorizedServer(t)
		cl := Client{authenticator: testAuthenticator{}, baseURL: server.URL, httpClient: &http.Client{}}
		_, err := cl.JobStatus(server.URL + "/some/url")
		if err != ErrorUnauthorized {
			t.Errorf("GetJobStatus returned unexpected error returned: got: %v, want: %v", err, ErrorUnauthorized)
		}
	})

	t.Run("valid request", func(t *testing.T) {
		jobID := "id"

		expectedURLSuffix := "/jobs/20"
		expectedProgress := 60
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if req.URL.Path != expectedURLSuffix {
				t.Errorf("GetJobStatus made request with unexpected path. got: %v, want: %v", req.URL.String(), expectedURLSuffix)
			}

			w.Header()["X-Progress"] = []string{fmt.Sprintf("(%d%%)", expectedProgress)}
			w.WriteHeader(http.StatusAccepted)
		}))
		jobStatusURL := server.URL + expectedURLSuffix
		cl := Client{authenticator: testAuthenticator{}, baseURL: server.URL, httpClient: &http.Client{}}
		_, err := cl.JobStatus(jobStatusURL)
		if err != nil {
			t.Errorf("GetJobStatus(%v) returned unexpected error: %v", jobID, err)
		}
	})

	t.Run("job in progress", func(t *testing.T) {
		cases := []struct {
			name             string
			progressString   string
			expectedProgress int
		}{
			{
				name:             "ProgressParentheses",
				progressString:   "(60%)",
				expectedProgress: 60,
			},
			{
				name:             "ProgressWithOtherText",
				progressString:   "61% Progress",
				expectedProgress: 61,
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					w.Header()["X-Progress"] = []string{tc.progressString}
					w.WriteHeader(http.StatusAccepted)
				}))
				jobStatusURL := server.URL
				cl := Client{authenticator: testAuthenticator{}, baseURL: server.URL, httpClient: &http.Client{}}
				jobStatus, err := cl.JobStatus(jobStatusURL)
				if err != nil {
					t.Errorf("GetJobStatus(%v) returned unexpected error: %v", jobStatusURL, err)
				}
				if jobStatus.IsComplete {
					t.Errorf("GetJobStatus(%v) got complete JobStatus, expected incomplete", jobStatusURL)
				}
				if got, want := jobStatus.PercentComplete, tc.expectedProgress; got != want {
					t.Errorf("GetJobStatus(%v) returned incorrect percent complete: got: %d, want: %d", jobStatusURL, got, want)
				}
			})
		}

	})

	t.Run("job completed", func(t *testing.T) {
		transactionTime := "2020-09-17T17:53:11.476Z"
		expectedTransactionTime := time.Date(2020, 9, 17, 17, 53, 11, 476000000, time.UTC)
		expectedResourceType := "Patient"
		expectedURL := "url"
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.Write([]byte(fmt.Sprintf("{\"output\": [{\"type\": \"%s\", \"url\": \"%s\"}], \"transactionTime\": \"%s\"}", expectedResourceType, expectedURL, transactionTime)))
		}))
		jobStatusURL := server.URL

		cl := Client{authenticator: testAuthenticator{}, baseURL: server.URL, httpClient: &http.Client{}}
		jobStatus, err := cl.JobStatus(jobStatusURL)
		if err != nil {
			t.Errorf("GetJobStatus(%v) returned unexpected error: %v", jobStatusURL, err)
		}
		if !jobStatus.IsComplete {
			t.Errorf("GetJobStatus(%v) got incomplete JobStatus, expected complete", jobStatusURL)
		}
		if got, want := len(jobStatus.ResultURLs), 1; got != want {
			t.Errorf("GetJobStatus(%v) unexpected number of ResultURLs: got: %d, want: %d", jobStatusURL, got, want)
		}
		if _, ok := jobStatus.ResultURLs[cpb.ResourceTypeCode_PATIENT]; !ok {
			t.Errorf("GetJobStatus(%v) ResultURLs no value for key Patient", jobStatusURL)
		}
		if got, want := jobStatus.ResultURLs[cpb.ResourceTypeCode_PATIENT][0], expectedURL; got != want {
			t.Errorf("GetJobStatus(%v) ResultURLs returned unexpected value for key Patient: got %s, want %s", jobStatusURL, got, want)
		}
		if got, want := jobStatus.TransactionTime, expectedTransactionTime; !got.Equal(want) {
			t.Errorf("GetJobStatus(%v) returned incorrect transaction time: got %s, want %s", jobStatusURL, got, want)
		}
	})

	t.Run("job completed multiple url", func(t *testing.T) {
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
		}))
		jobStatusURL := server.URL

		cl := Client{authenticator: testAuthenticator{}, baseURL: server.URL, httpClient: &http.Client{}}
		jobStatus, err := cl.JobStatus(jobStatusURL)
		if err != nil {
			t.Errorf("GetJobStatus(%v) returned unexpected error: %v", jobStatusURL, err)
		}
		if !jobStatus.IsComplete {
			t.Errorf("GetJobStatus(%v) got incomplete JobStatus, expected complete", jobStatusURL)
		}
		expectedMap := map[cpb.ResourceTypeCode_Value][]string{
			cpb.ResourceTypeCode_PATIENT:                {"url_1", "url_2", "url_3"},
			cpb.ResourceTypeCode_COVERAGE:               {"url_4"},
			cpb.ResourceTypeCode_EXPLANATION_OF_BENEFIT: {"url_5", "url_6"},
			cpb.ResourceTypeCode_OPERATION_OUTCOME:      {"url_7", "url_8"},
		}
		if diff := cmp.Diff(expectedMap, jobStatus.ResultURLs, cmpopts.SortMaps(func(k1, k2 string) bool { return k1 < k2 })); diff != "" {
			t.Errorf("GetJobStatus(%v) returned unexpected diff (-want +got):\n%s", jobStatusURL, diff)
		}
	})

	t.Run("unexpected number of X-Progress", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.Header()["X-Progress"] = []string{fmt.Sprintf("(%d%%)", 60), fmt.Sprintf("(%d%%)", 160)}
			w.WriteHeader(http.StatusAccepted)
		}))
		jobStatusURL := server.URL

		cl := Client{authenticator: testAuthenticator{}, baseURL: server.URL, httpClient: &http.Client{}}
		jobStatus, err := cl.JobStatus(jobStatusURL)
		if err != nil {
			t.Errorf("GetJobStatus(%v) returned unexpected error: %v", jobStatusURL, err)
		}
		if jobStatus.PercentComplete != -1 {
			t.Errorf("GetJobStatus(%v) returned unexpected progress; got %d, want -1", jobStatusURL, jobStatus.PercentComplete)
		}
	})

	t.Run("invalid progress", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.Header()["X-Progress"] = []string{"invalid"}
			w.WriteHeader(http.StatusAccepted)
		}))
		jobStatusURL := server.URL

		cl := Client{authenticator: testAuthenticator{}, baseURL: server.URL, httpClient: &http.Client{}}
		jobStatus, err := cl.JobStatus(jobStatusURL)
		if err != nil {
			t.Errorf("GetJobStatus(%v) returned unexpected error: %v", jobStatusURL, err)
		}
		if jobStatus.PercentComplete != -1 {
			t.Errorf("GetJobStatus(%v) returned unexpected progress; got %d, want -1", jobStatusURL, jobStatus.PercentComplete)
		}
	})

	t.Run("with numeric Retry-After", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.Header()["Retry-After"] = []string{"120"}
			w.WriteHeader(http.StatusAccepted)
		}))
		jobStatusURL := server.URL

		cl := Client{authenticator: testAuthenticator{}, baseURL: server.URL, httpClient: &http.Client{}}
		jobStatus, err := cl.JobStatus(jobStatusURL)
		if err != nil {
			t.Errorf("GetJobStatus(%v) returned unexpected error: %v", jobStatusURL, err)
		}
		if jobStatus.RetryAfter != 120*time.Second {
			t.Errorf("GetJobStatus(%v) returned unexpected Retry-After; got %s, want %s", jobStatusURL, jobStatus.RetryAfter, 120*time.Second)
		}
	})

	t.Run("with date Retry-After", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.Header()["Retry-After"] = []string{time.Now().Add(120 * time.Second).Format(time.RFC1123)}
			w.WriteHeader(http.StatusAccepted)
		}))
		jobStatusURL := server.URL

		cl := Client{authenticator: testAuthenticator{}, baseURL: server.URL, httpClient: &http.Client{}}
		jobStatus, err := cl.JobStatus(jobStatusURL)
		if err != nil {
			t.Errorf("GetJobStatus(%v) returned unexpected error: %v", jobStatusURL, err)
		}
		delta := jobStatus.RetryAfter - 120*time.Second
		if delta < 0 {
			delta = -delta
		}
		if delta > time.Second {
			t.Errorf("GetJobStatus(%v) returned unexpected Retry-After; got %s, want approx %s", jobStatusURL, jobStatus.RetryAfter, 120*time.Second)
		}
	})

	t.Run("invalid transaction time", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.Write([]byte(`{"output": [{"type": "Patient", "url": "url"}], "transactionTime" : "2013-12-09T11:00Z"}`))
		}))
		jobStatusURL := server.URL

		cl := Client{authenticator: testAuthenticator{}, baseURL: server.URL, httpClient: &http.Client{}}
		_, err := cl.JobStatus(jobStatusURL)
		if err == nil {
			t.Errorf("GetJobStatus(%v) succeeded, want error", jobStatusURL)
		}
	})
}

func TestClient_GetData(t *testing.T) {
	t.Run("unauthorized", func(t *testing.T) {
		server := newUnauthorizedServer(t)
		cl := Client{authenticator: testAuthenticator{}, baseURL: server.URL, httpClient: &http.Client{}}
		_, err := cl.GetData(server.URL + "/id")
		if err != ErrorUnauthorized {
			t.Errorf("GetData returned unexpected error returned: got: %v, want: %v", err, ErrorUnauthorized)
		}
	})

	t.Run("not-OK http response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		c := Client{authenticator: testAuthenticator{}, httpClient: &http.Client{}}
		_, err := c.GetData(server.URL)
		if !errors.Is(err, ErrorUnexpectedStatusCode) {
			t.Errorf("GetData(%v) returned incorrect underlying error. got: %v, want: %v", server.URL, err, ErrorUnexpectedStatusCode)
		}
	})

	t.Run("retryable not-OK http response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		c := Client{authenticator: testAuthenticator{}, httpClient: &http.Client{}}
		_, err := c.GetData(server.URL)
		if !errors.Is(err, ErrorRetryableHTTPStatus) {
			t.Errorf("GetData(%v) returned incorrect underlying error. got: %v, want: %v", server.URL, err, ErrorRetryableHTTPStatus)
		}
	})

	t.Run("valid GetData", func(t *testing.T) {
		expectedResponse := []byte("the response")
		expectedPath := "/data"
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if req.URL.Path != expectedPath {
				t.Errorf("GetData(%v) made request with unexpected path. got: %v, want: %v", req.URL.String(), req.URL.Path, expectedPath)
			}

			w.Write(expectedResponse)
		}))

		cl := Client{baseURL: server.URL, authenticator: testAuthenticator{}, httpClient: &http.Client{}}
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
	t.Run("timeout", func(t *testing.T) {
		period := 2 * time.Millisecond
		timeout := 20 * time.Millisecond

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			// Indicates job in progress
			w.Header()["X-Progress"] = []string{fmt.Sprintf("(%d%%)", 60)}
			w.WriteHeader(http.StatusAccepted)
		}))
		jobStatusURL := server.URL
		cl := Client{authenticator: testAuthenticator{}, baseURL: server.URL, httpClient: &http.Client{}}
		results := make([]*MonitorResult, 0, 1)
		for st := range cl.MonitorJobStatus(jobStatusURL, period, timeout) {
			results = append(results, st)
		}
		if got, want := results[len(results)-1].Error, ErrorTimeout; got != want {
			t.Errorf("MonitorJobStatus(%v,%v,%v) did not return correct error. got: %v, want: %v", jobStatusURL, period, timeout, got, want)
		}
	})

	t.Run("not found", func(t *testing.T) {
		period := time.Second
		timeout := time.Minute

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		jobStatusURL := server.URL
		cl := Client{authenticator: testAuthenticator{}, baseURL: server.URL, httpClient: &http.Client{}}
		results := []*MonitorResult{}
		for st := range cl.MonitorJobStatus(jobStatusURL, period, timeout) {
			results = append(results, st)
		}
		if len(results) != 1 {
			t.Fatalf("MonitorJobStatus(%v,%v,%v) output %d results; want 1", jobStatusURL, period, timeout, len(results))
		}
		if got, want := results[len(results)-1].Error, ErrorExportJobNotFound; !errors.Is(got, want) {
			t.Errorf("MonitorJobStatus(%v,%v,%v) did not output expected error. got: %v, want: %v", jobStatusURL, period, timeout, got, want)
		}
	})

	t.Run("valid cases", func(t *testing.T) {
		jobStatusURLSuffix := "/jobs/20"
		wantResource := cpb.ResourceTypeCode_PATIENT
		resourceName := "Patient"
		wantResultURL := "url"
		wantProgress := 60
		inProgressJobStatus := JobStatus{IsComplete: false, PercentComplete: wantProgress}
		completeJobStatus := JobStatus{
			IsComplete:      true,
			ResultURLs:      map[cpb.ResourceTypeCode_Value][]string{wantResource: []string{wantResultURL}},
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
					if req.URL.Path != jobStatusURLSuffix {
						t.Errorf("MonitorJobStatus made request with unexpected path. got: %v, want: %v", req.URL.String(), jobStatusURLSuffix)
						return
					}
					counter.Lock()
					defer counter.Unlock()
					counter.count++
					if counter.count >= tc.completeAfterNChecks {
						// Write out the completed result
						w.Write([]byte(fmt.Sprintf("{\"output\": [{\"type\": \"%s\", \"url\": \"%s\"}], \"transactionTime\": \"2020-09-15T17:53:11.476Z\"}", resourceName, wantResultURL)))
					} else {
						// Write out in progress result
						w.Header()["X-Progress"] = []string{fmt.Sprintf("(%d%%)", wantProgress)}
						w.WriteHeader(http.StatusAccepted)
					}
				}))
				jobStatusURL := server.URL + jobStatusURLSuffix

				cl := Client{authenticator: testAuthenticator{}, baseURL: server.URL, httpClient: &http.Client{}}
				results := make([]JobStatus, 0, 1)

				for st := range cl.MonitorJobStatus(jobStatusURL, tc.period, tc.timeout) {
					if st.Error != nil {
						t.Errorf("MonitorJobStatus(%v,%v,%v) returned unexpected error: %v", jobStatusURL, tc.period, tc.timeout, st.Error)
					}
					results = append(results, st.Status)
				}

				if diff := cmp.Diff(tc.wantJobStatuses, results); diff != "" {
					t.Errorf("MonitorJobStatus(%v,%v,%v) unexpected diff in result (-want +got):\n%s", jobStatusURL, tc.period, tc.timeout, diff)
				}
			})
		}
	})

	t.Run("withAuthRetry", func(t *testing.T) {
		var counter struct {
			sync.Mutex
			count int
		}
		var authCalled struct {
			sync.Mutex
			called bool
		}
		jobStatusURLSuffix := "/jobs/20"
		wantResource := cpb.ResourceTypeCode_PATIENT
		resourceName := "Patient"
		wantURL := "url"
		completeJobStatus := JobStatus{
			IsComplete:      true,
			ResultURLs:      map[cpb.ResourceTypeCode_Value][]string{wantResource: []string{wantURL}},
			TransactionTime: time.Date(2020, 9, 15, 17, 53, 11, 476000000, time.UTC)}

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if req.URL.Path == "/auth/token" {
				authCalled.Lock()
				authCalled.called = true
				authCalled.Unlock()
				w.WriteHeader(200)
				w.Write([]byte(`{"access_token": "token", "expires_in": 1200}`))
				return
			}

			if req.URL.Path != jobStatusURLSuffix {
				t.Errorf("MonitorJobStatus made request with unexpected path. got: %v, want: %v", req.URL.String(), jobStatusURLSuffix)
				return
			}

			counter.Lock()
			defer counter.Unlock()
			counter.count++
			if counter.count == 1 {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			// Write out the completed result
			w.Write([]byte(fmt.Sprintf("{\"output\": [{\"type\": \"%s\", \"url\": \"%s\"}], \"transactionTime\": \"2020-09-15T17:53:11.476Z\"}", resourceName, wantURL)))
		}))
		jobStatusURL := server.URL + jobStatusURLSuffix

		wantJobStatuses := []JobStatus{completeJobStatus}

		auth, err := NewHTTPBasicOAuthAuthenticator("username", "password", server.URL+"/auth/token", nil)
		if err != nil {
			t.Fatal(err)
		}
		auth.(*BearerTokenAuthenticator).token = &BearerToken{
			Token:  "123",
			Expiry: time.Now().Add(5 * time.Minute),
		}
		cl := Client{authenticator: auth, baseURL: server.URL, httpClient: &http.Client{}}
		results := make([]JobStatus, 0, 1)

		monitorPeriod := time.Millisecond
		monitorTimeout := 2 * time.Second

		for st := range cl.MonitorJobStatus(jobStatusURL, monitorPeriod, monitorTimeout) {
			if st.Error != nil {
				t.Errorf("MonitorJobStatus(%v,%v,%v) returned unexpected error: %v", jobStatusURL, monitorPeriod, monitorTimeout, st.Error)
			}
			results = append(results, st.Status)
		}

		authCalled.Lock()
		defer authCalled.Unlock()
		if !authCalled.called {
			t.Errorf("expected authentication to be retried")
		}

		if diff := cmp.Diff(wantJobStatuses, results); diff != "" {
			t.Errorf("MonitorJobStatus(%v,%v,%v) unexpected diff in result (-want +got):\n%s", jobStatusURL, monitorPeriod, monitorTimeout, diff)
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
