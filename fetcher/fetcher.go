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

// Package fetcher provides a utilities that combines together various other
// libraries to run a fetch end-to-end.
package fetcher

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/google/medical_claims_tools/bulkfhir"
	"github.com/google/medical_claims_tools/fhir"
	"github.com/google/medical_claims_tools/fhir/processing"
	log "github.com/google/medical_claims_tools/internal/logger"
	"github.com/google/medical_claims_tools/internal/metrics"

	cpb "github.com/google/fhir/go/proto/google/fhir/proto/r4/core/codes_go_proto"
)

// ErrInvalidTransactionTime is returned (wrapped) when a TransactionTimeStore
// fails to produce a valid timestamp. This is primarily used for testing.
var ErrInvalidTransactionTime = errors.New("failed to get transaction timestamp")

const (
	defaultJobStatusPeriod  = 5 * time.Second
	defaultJobStatusTimeout = 6 * time.Hour
	defaultDataRetryCount   = 5
)

const (
	// maxTokenSize represents the maximum newline delimited token size in bytes
	// expected when parsing FHIR NDJSON.
	maxTokenSize = 500 * 1024
	// initialBufferSize indicates the initial buffer size in bytes to use when
	// parsing a FHIR NDJSON token.
	initialBufferSize = 5 * 1024
)

var processURLTime *metrics.Latency = metrics.NewLatency("process-url-time", "Bulk FHIR Server's provide a list of URLs to download FHIR ndjson from. ProcessURLTime records the time to download and process data from a particular Job URL.", "min", []float64{0, 1, 3, 7, 15, 30, 45, 60, 75, 90, 120, 150, 180, 210, 240, 270, 300, 330, 360, 390, 420, 450, 480})

// Fetcher is a utility for running a bulk FHIR fetch end-to-end.
type Fetcher struct {
	Client               *bulkfhir.Client
	Pipeline             *processing.Pipeline
	TransactionTimeStore bulkfhir.TransactionTimeStore
	TransactionTime      *bulkfhir.TransactionTime

	// If specified, no new job is started, and the Fetcher waits for this job to
	// complete before processing data from it.
	JobURL string

	// Resource types to request if no JobURL is specified. May be empty.
	ResourceTypes []cpb.ResourceTypeCode_Value

	// Group to export if no JobURL is specified. If empty, defaults to exporting
	// data for all patients.
	ExportGroup string

	// The following parameters may all be omitted, and sane defaults will be used.

	// How frequently to poll for job status if the server does not return a
	// Retry-After header
	JobStatusPeriod time.Duration

	// How long to poll for job status for before giving up.
	JobStatusTimeout time.Duration

	// How many times to retry fetching each data URL.
	DataRetryCount int
}

// Run the bulk FHIR fetch end-to-end. Note that while this does finalize the
// configured processing pipeline, it does not close the bulk FHIR client.
func (f *Fetcher) Run(ctx context.Context) error {
	f.setDefaultParameters()

	if err := f.maybeStartJob(ctx); err != nil {
		return err
	}

	jobStatus, err := f.waitForJob()
	if err != nil {
		return err
	}

	f.TransactionTime.Set(jobStatus.TransactionTime)

	if err := f.processData(ctx, jobStatus); err != nil {
		return err
	}

	if err := f.TransactionTimeStore.Store(ctx, jobStatus.TransactionTime); err != nil {
		return fmt.Errorf("failed to store transaction timestamp: %v", err)
	}

	log.Info("Bulk FHIR fetch job and processing complete.")
	return nil
}

func (f *Fetcher) setDefaultParameters() {
	if f.JobStatusPeriod == 0 {
		f.JobStatusPeriod = defaultJobStatusPeriod
	}
	if f.JobStatusTimeout == 0 {
		f.JobStatusTimeout = defaultJobStatusTimeout
	}
	if f.DataRetryCount == 0 {
		f.DataRetryCount = defaultDataRetryCount
	}
}

func (f *Fetcher) maybeStartJob(ctx context.Context) error {
	if f.JobURL != "" {
		return nil
	}

	since, err := f.TransactionTimeStore.Load(ctx)
	if err != nil {
		// We match the text of ErrInvalidTransactionTime in tests; fmt.Errorf does
		// not allow using multiple %w verbs.
		return fmt.Errorf("%v: %w", ErrInvalidTransactionTime, err)
	}
	if f.ExportGroup != "" {
		f.JobURL, err = f.Client.StartBulkDataExport(f.ResourceTypes, since, f.ExportGroup)
	} else {
		log.Warning("No export Group ID set, so defaulting to the Patient endpoint to export all resources.")
		f.JobURL, err = f.Client.StartBulkDataExportAll(f.ResourceTypes, since)
	}
	if err != nil {
		return fmt.Errorf("unable to start Bulk FHIR export job: %w", err)
	}
	log.Infof("Started Bulk FHIR export job: %s\n", f.JobURL)
	return nil
}

func (f *Fetcher) waitForJob() (bulkfhir.JobStatus, error) {
	var monitorResult *bulkfhir.MonitorResult
	for monitorResult = range f.Client.MonitorJobStatus(f.JobURL, f.JobStatusPeriod, f.JobStatusTimeout) {
		if monitorResult.Error != nil {
			log.Errorf("error while checking job status: %v", monitorResult.Error)
		}
		if !monitorResult.Status.IsComplete {
			if monitorResult.Status.PercentComplete >= 0 {
				log.Infof("Bulk FHIR export job pending, progress: %d", monitorResult.Status.PercentComplete)
			} else {
				log.Info("Bulk FHIR export job pending, progress unknown")
			}
		}
	}

	jobStatus := monitorResult.Status
	if !jobStatus.IsComplete {
		return jobStatus, fmt.Errorf("Bulk FHIR export job did not finish before the timeout of %s: %w", f.JobStatusTimeout, monitorResult.Error)
	}

	log.Infof("Bulk FHIR export job finished. Transaction Time: %s", fhir.ToFHIRInstant(jobStatus.TransactionTime))
	return jobStatus, nil
}

func (f *Fetcher) processData(ctx context.Context, jobStatus bulkfhir.JobStatus) error {
	log.Infof("Starting data download and processing.")

	for resourceType, urls := range jobStatus.ResultURLs {
		for _, url := range urls {
			start := time.Now()
			if err := f.processURL(ctx, resourceType, url); err != nil {
				return err
			}
			if err := processURLTime.Record(ctx, float64(time.Since(start)/time.Minute)); err != nil {
				return err
			}
		}
	}

	if err := f.Pipeline.Finalize(ctx); err != nil {
		return fmt.Errorf("failed to finalize output pipeline: %w", err)
	}
	return nil
}

func (f *Fetcher) processURL(ctx context.Context, resourceType cpb.ResourceTypeCode_Value, url string) error {
	r, err := f.getDataWithRetries(url)
	if err != nil {
		return err
	}
	defer r.Close()
	s := bufio.NewScanner(r)
	// The default bufio.MaxScanTokenSize of 64kB is too small for some resources.
	s.Buffer(make([]byte, initialBufferSize), maxTokenSize)
	for s.Scan() {
		if err := f.Pipeline.Process(ctx, resourceType, url, s.Bytes()); err != nil {
			return err
		}
	}
	return s.Err()
}

func (f *Fetcher) getDataWithRetries(url string) (io.ReadCloser, error) {
	r, err := f.Client.GetData(url)
	numRetries := 0
	// Retry both unauthorized and other retryable errors by re-authenticating,
	// as sometimes they appear to be related.
	for (errors.Is(err, bulkfhir.ErrorUnauthorized) || errors.Is(err, bulkfhir.ErrorRetryableHTTPStatus)) && numRetries < 5 {
		time.Sleep(2 * time.Second)
		log.Infof("Got retryable error from Bulk FHIR server. Re-authenticating and trying again.")
		if err := f.Client.Authenticate(); err != nil {
			return nil, fmt.Errorf("failed to authenticate: %w", err)
		}
		r, err = f.Client.GetData(url)
		numRetries++
	}
	if err != nil {
		return nil, fmt.Errorf("failed to fetch data from %s: %w", url, err)
	}
	return r, nil
}
