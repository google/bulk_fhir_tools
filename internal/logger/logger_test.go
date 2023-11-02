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

package logger_test

import (
	"context"
	"testing"

	gcpLog "cloud.google.com/go/logging"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"github.com/google/bulk_fhir_tools/internal/logger"
	"github.com/google/bulk_fhir_tools/internal/testhelpers"
)

func TestGCPLogger(t *testing.T) {
	testProjectID := "PROJECT_ID"
	ctx := context.Background()
	svr, lhandler, err := testhelpers.GCPLogServer()
	if err != nil {
		t.Fatalf("creating fake GCP Log Server: %v", err)
	}
	defer svr.Close()

	conn, err := grpc.Dial(svr.Addr(), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		t.Fatalf("dialing %q: %v", svr.Addr(), err)
	}
	c, err := gcpLog.NewClient(ctx, testProjectID, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("creating client for fake at %q: %v", svr.Addr(), err)
	}

	logger.InitGCPWithClient(context.Background(), c)
	logger.Info("No worries.")
	logger.Warning("Just a warning.")
	logger.Error("Yikes an error!")
	logger.Infof("Jobs completed: %d", 3)
	logger.Warningf("Number of times warned: %d", 2)
	logger.Errorf("Failed counters: %d", 4)

	if err := logger.Close(); err != nil {
		t.Errorf("Error closing the global logger client: %v", err)
	}
	logs := lhandler.GetLogsAfterClose()

	type lg struct {
		Text string
		Sev  string
	}

	got := make([]lg, len(logs["projects/PROJECT_ID/logs/bulk-fhir-fetch"]))
	for i, l := range logs["projects/PROJECT_ID/logs/bulk-fhir-fetch"] {
		got[i].Text = l.GetTextPayload()
		got[i].Sev = l.GetSeverity().String()
	}
	want := []lg{
		{Text: "No worries.\n", Sev: "INFO"},
		{Text: "Just a warning.\n", Sev: "WARNING"},
		{Text: "Jobs completed: 3\n", Sev: "INFO"},
		{Text: "Number of times warned: 2\n", Sev: "WARNING"},
		{Text: "Yikes an error!\n", Sev: "ERROR"},
		{Text: "Failed counters: 4\n", Sev: "ERROR"},
		{Text: "GCP Logging client had 0 errors\n", Sev: "INFO"},
		{
			Text: "Logging library Close() called, GCP logging is terminating. Any logs made after this Close call will go to the task STDOUT/STDERR.\n",
			Sev:  "INFO",
		},
	}

	sortLogs := cmpopts.SortSlices(func(a, b lg) bool { return a.Text < b.Text })
	if diff := cmp.Diff(want, got, sortLogs); diff != "" {
		t.Errorf("GetLogsAfterClose() returned unexpected difference in log payloads and severity (-want +got):\n%s", diff)
	}

	if got, want := logger.GetNErrs(), 0; got != want {
		t.Errorf("getNErrs() = %v, want %v", got, want)
	}
}
