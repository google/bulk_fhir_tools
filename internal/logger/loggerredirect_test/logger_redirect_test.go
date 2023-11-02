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

// loggerredirect_test is a separate package to test logging behavior
// where a redirect of os.Stdout is needed.
package loggerredirect_test

import (
	"context"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	gcpLog "cloud.google.com/go/logging"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"github.com/google/bulk_fhir_tools/internal/logger"
	"github.com/google/bulk_fhir_tools/internal/testhelpers"
)

// TestFallbackGCPLoggerAfterClose tests a fallback behavior if the user mistakenly makes logging
// calls after logging.Close is called. The fallback behavior is to continue logging to STDOUT and
// STDERR.
func TestFallbackGCPLoggerAfterClose(t *testing.T) {
	testProjectID := "PROJECT_ID"
	ctx := context.Background()
	svr, _, err := testhelpers.GCPLogServer()
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

	// Temporarily redirect Stdout, Stderr
	origSTDOUT := os.Stdout
	origSTDERR := os.Stderr
	defer func() {
		os.Stdout = origSTDOUT
		os.Stderr = origSTDERR
	}()

	stdOutReader, stdOutWriter, err := os.Pipe()
	if err != nil {
		t.Errorf("Error setting up os.Pipe: %v", err)
		return
	}

	stdErrReader, stdErrWriter, err := os.Pipe()
	if err != nil {
		t.Errorf("Error setting up os.Pipe: %v", err)
		return
	}

	os.Stdout = stdOutWriter
	os.Stderr = stdErrWriter

	logger.InitGCPWithClient(context.Background(), c)

	if err := logger.Close(); err != nil {
		t.Errorf("Error closing the global logger client: %v", err)
	}

	// Try logging after close, see if it goes to STDOUT, STDERR
	infoStr := "testing"
	logger.Info(infoStr)
	warnStr := "I'm a warning"
	logger.Warning(warnStr)
	errStr := "I'm an error"
	logger.Error(errStr)

	stdOutWriter.Close()
	stdErrWriter.Close()

	stdOutData, err := ioutil.ReadAll(stdOutReader)
	if err != nil {
		t.Errorf("Unexpected error reading from redirected STDOUT: %v", err)
	}
	stdErrData, err := ioutil.ReadAll(stdErrReader)
	if err != nil {
		t.Errorf("Unexpected error reading from redirected STDERR: %v", err)
	}

	if !strings.Contains(string(stdOutData), infoStr) {
		t.Errorf("Expected info log string to be contained in STDOUT. got:%v, want (to be contained):%v", string(stdOutData), infoStr)
	}

	if !strings.Contains(string(stdOutData), warnStr) {
		t.Errorf("Expected warning log string to be contained in STDOUT. got:%v, want (to be contained):%v", string(stdOutData), warnStr)
	}

	if !strings.Contains(string(stdErrData), errStr) {
		t.Errorf("Expected error log string to be contained in STDERR. got:%v, want (to be contained):%v", string(stdErrData), errStr)
	}
}
