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

// Package logger is a shim over different implementations of the Go Standard
// Logger. By default this package logs to stdout and stderr. InitGCP can
// be called to log to GCP Logging. Close should be called at the end of the
// program.
package logger

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	gcpLog "cloud.google.com/go/logging"
)

var globalLogger *logger
var once sync.Once

// Multiple calls to the OnError function never happen concurrently, so there is
// no need for locking nErrs, provided you don't read it until after the logging
// client is closed.
var nErrs int

const logID = "bulk-fhir-fetch"

type logger struct {
	infoLogger    *log.Logger
	warningLogger *log.Logger
	errorLogger   *log.Logger

	client *gcpLog.Client
}

func init() {
	initDefaultLoggers()
}

func initDefaultLoggers() {
	globalLogger = &logger{
		infoLogger:    log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime),
		warningLogger: log.New(os.Stdout, "WARNING: ", log.Ldate|log.Ltime),
		errorLogger:   log.New(os.Stderr, "ERROR: ", log.Ldate|log.Ltime),
		client:        nil,
	}
}

// InitGCP initializes the logger to write to GCP Logging. InitGCP should be
// called once before any logs are written. All logs are written with the logID
// "bulk-fhir-fetch".
func InitGCP(ctx context.Context, projectID string) error {
	c, err := gcpLog.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("Error creating gcp logging client: %v", err)
	}
	InitGCPWithClient(ctx, c)
	log.Print("Logs for Bulk FHIR Fetch are being sent to GCP.")
	log.Printf("View here: https://console.cloud.google.com/logs/query;query=logName%%3D%%22projects%%2F%s%%2Flogs%%2F%s%%22?project=%s", projectID, logID, projectID)
	log.Printf("Note: you may need to adjust the time window in the logs viewer as needed.")
	return nil
}

// InitGCPWithClient initializes the logger to write to GCP Logging.
// InitGCPWithClient should be called once before any logs are written. All logs
// are written with the logID "bulk-fhir-fetch".
func InitGCPWithClient(ctx context.Context, c *gcpLog.Client) {
	once.Do(func() {
		globalLogger.client = c

		// Print all errors to stdout, and count them. Multiple calls to the OnError
		// function never happen concurrently.
		globalLogger.client.OnError = func(e error) {
			log.Printf("GCP Logging Client Error: %v", e)
			nErrs++
		}

		// "bulk-fhir-fetch" is the logID, useful when searching or filtering logs in the GCP console.
		logger := globalLogger.client.Logger(logID)
		globalLogger.infoLogger = logger.StandardLogger(gcpLog.Info)
		globalLogger.warningLogger = logger.StandardLogger(gcpLog.Warning)
		globalLogger.errorLogger = logger.StandardLogger(gcpLog.Error)
	})
}

// Info logs with severity Info.
func Info(v ...any) {
	globalLogger.infoLogger.Print(v...)
}

// Infof formats the string and logs with severity Info.
func Infof(format string, v ...any) {
	globalLogger.infoLogger.Printf(format, v...)
}

// Warning logs with severity Warning.
func Warning(v ...any) {
	globalLogger.warningLogger.Print(v...)
}

// Warningf formats the string and logs with severity Warning.
func Warningf(format string, v ...any) {
	globalLogger.warningLogger.Printf(format, v...)
}

// Error logs with severity Error.
func Error(v ...any) {
	globalLogger.errorLogger.Print(v...)
}

// Errorf formats the string and logs with severity Error.
func Errorf(format string, v ...any) {
	globalLogger.errorLogger.Printf(format, v...)
}

// Fatal is equivalent to logging to Error() followed by a call to os.Exit(1).
func Fatal(v ...any) {
	globalLogger.errorLogger.Fatal(v...)
}

// Fatalf is equivalent to logging to Errorf() followed by a call to os.Exit(1).
func Fatalf(format string, v ...any) {
	globalLogger.errorLogger.Fatalf(format, v...)
}

// Close should be called before the program exits to flush any buffered log
// entries to the GCP Logging service.
func Close() error {
	if globalLogger.client != nil {
		libraryAndSystemInfoLog("Logging library Close() called, GCP logging is terminating. Any logs made after this Close call will go to the task STDOUT/STDERR.")
		libraryAndSystemInfoLog(fmt.Sprintf("GCP Logging client had %d errors\n", nErrs))
		err := globalLogger.client.Close()
		// Reset default STDOUT/STDERR loggers in case any logs called after Close().
		initDefaultLoggers()
		return err
	}
	return nil
}

// GetNErrs is for testing purposes. It should not be called until after the logger is closed.
func GetNErrs() int { return nErrs }

// libraryAndSystemInfoLog sends the input string to both the system log (log.Println) and this
// logging library's Info log.
func libraryAndSystemInfoLog(s string) {
	log.Println(s)
	Info(s)
}
