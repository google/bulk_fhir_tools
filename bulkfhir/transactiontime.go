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
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	log "github.com/google/bulk_fhir_tools/internal/logger"
	"cloud.google.com/go/storage"
	"github.com/google/bulk_fhir_tools/fhir"
	"github.com/google/bulk_fhir_tools/gcs"
)

// ErrUnsetTransactionTime is returned from TransactionTime.Get if it is
// called before TransactionTime.Set is called.
var ErrUnsetTransactionTime = errors.New("TransactionTime.Set has not been called")

// A TransactionTime holds the transaction time for a bulk FHIR export. It
// is used to allow constructing processing pipelines before the export
// operation is started; pipeline steps may hold a pointer to the
// TransactionTime, and call Get once they receive a resource to process or
// store (by which time the cache should have been populated).
type TransactionTime struct {
	timestamp time.Time
}

// Set the timestamp in the cache.
func (tt *TransactionTime) Set(timestamp time.Time) {
	tt.timestamp = timestamp
}

// Get the timestamp from the cache. Returns an error if Set() has not yet been
// called.
func (tt *TransactionTime) Get() (time.Time, error) {
	if tt.timestamp.IsZero() {
		return time.Time{}, ErrUnsetTransactionTime
	}
	return tt.timestamp, nil
}

// NewTransactionTime returns a new TransactionTime.
func NewTransactionTime() *TransactionTime {
	return &TransactionTime{}
}

// TransactionTimeStore manages the transaction time of Bulk FHIR fetches. The
// transaction timestamp of a successful export is saved so that it can be used
// as the _since parameter for the subsequent export.
type TransactionTimeStore interface {
	// Load a previously stored transaction time. If no transaction time has
	// previously been stored (i.e. if the program has never been successfully run
	// with the current configuration), this should return a zero time with no
	// error.
	Load(ctx context.Context) (time.Time, error)
	// Store() saves the given timestamp to persistent storage so that it can be
	// retrieved by Load() the next time the program is run.
	Store(ctx context.Context, ts time.Time) error
}

type inMemoryTransactionTimeStore struct {
	since time.Time
}

func (imtts *inMemoryTransactionTimeStore) Load(ctx context.Context) (time.Time, error) {
	return imtts.since, nil
}

func (imtts *inMemoryTransactionTimeStore) Store(ctx context.Context, ts time.Time) error {
	return nil
}

// NewInMemoryTransactionTimeStore returns an implementation of
// TransactionTimeStore which does not persist the since timestamp anywhere. It
// is initialised with a string timestamp, which may be blank.
func NewInMemoryTransactionTimeStore(timestamp string) (TransactionTimeStore, error) {
	if timestamp == "" {
		return &inMemoryTransactionTimeStore{}, nil
	}

	parsed, err := fhir.ParseFHIRInstant(timestamp)
	if err != nil {
		return nil, fmt.Errorf("invalid since timestamp %q, should be in form YYYY-MM-DDThh:mm:ss.imss+zz:zz: %w", timestamp, err)
	}
	return &inMemoryTransactionTimeStore{since: parsed}, nil
}

type gcsTransactionTimeStore struct {
	client                gcs.Client
	relativePath, fullURI string
}

func (gtts *gcsTransactionTimeStore) Load(ctx context.Context) (time.Time, error) {
	reader, err := gtts.client.GetFileReader(ctx, gtts.relativePath)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			// If that GCS file has not been created, assume that this is the first
			// time the file has been used and return an empty time to fetch all data.
			return time.Time{}, nil
		}
		return time.Time{}, fmt.Errorf("failed to get GCS reader for %s: %w", gtts.fullURI, err)
	}
	ts, err := readTimestampFromFile(reader)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get since timestamp from %s: %w", gtts.fullURI, err)
	}
	return ts, nil
}

func (gtts *gcsTransactionTimeStore) Store(ctx context.Context, ts time.Time) error {
	writer := gtts.client.GetFileWriter(ctx, gtts.relativePath)
	if err := gtts.copyPreviousContent(ctx, writer); err != nil {
		return err
	}
	if err := writeTimestampToFile(ts, writer); err != nil {
		return fmt.Errorf("failed to write since timestamp to %s: %w", gtts.fullURI, err)
	}
	return nil
}

func (gtts *gcsTransactionTimeStore) copyPreviousContent(ctx context.Context, writer io.Writer) error {
	reader, err := gtts.client.GetFileReader(ctx, gtts.relativePath)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil
		}
		return fmt.Errorf("failed to get GCS reader for %s to copy existing content: %w", gtts.fullURI, err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			log.Errorf("failed to close GCS reader for %s after copying: %v", gtts.fullURI, err)
		}
	}()
	if _, err := io.Copy(writer, reader); err != nil {
		return fmt.Errorf("failed to copy existing content in %s: %w", gtts.fullURI, err)
	}
	return nil
}

// NewGCSTransactionTimeStore returns an implementation of TransactionTimeStore
// which persists the since timestamp to a file in GCS at the given URI. A new
// line is appended to the file on each run, so that the entire history of
// transaction times may be seen.
func NewGCSTransactionTimeStore(ctx context.Context, gcsEndpoint, uri string) (TransactionTimeStore, error) {
	bucket, relativePath, err := gcs.PathComponents(uri)
	if err != nil {
		return nil, err
	}
	client, err := gcs.NewClient(ctx, bucket, gcsEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to get GCS client: %w", err)
	}
	return &gcsTransactionTimeStore{
		client:       client,
		relativePath: relativePath,
		fullURI:      uri,
	}, nil
}

type localFileTransactionTimeStore struct {
	path string
}

func (lftts *localFileTransactionTimeStore) Load(ctx context.Context) (time.Time, error) {
	reader, err := os.Open(lftts.path)
	if err != nil {
		if os.IsNotExist(err) {
			// If that file has not been created, assume that this is the first time
			// the file has been used and return an empty time to fetch all data.
			return time.Time{}, nil
		}
		return time.Time{}, fmt.Errorf("failed to open %s: %w", lftts.path, err)
	}
	ts, err := readTimestampFromFile(reader)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get since timestamp from %s: %w", lftts.path, err)
	}
	return ts, nil
}

func (lftts *localFileTransactionTimeStore) Store(ctx context.Context, ts time.Time) error {
	writer, err := os.OpenFile(lftts.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open %s: %w", lftts.path, err)
	}
	if err := writeTimestampToFile(ts, writer); err != nil {
		return fmt.Errorf("failed to write since timestamp to %s: %w", lftts.path, err)
	}
	return nil
}

// NewLocalFileTransactionTimeStore returns an implementation of
// TransactionTimeStore which persists the since timestamp to a local file at
// the given path. A new line is appended to the file on each run, so that the
// entire history of transaction times may be seen.
func NewLocalFileTransactionTimeStore(path string) TransactionTimeStore {
	return &localFileTransactionTimeStore{path: path}
}

func readTimestampFromFile(reader io.ReadCloser) (time.Time, error) {
	// Since files may get arbitrarily large. If this becomes a problem, we should
	// change this code to read only the end of the file (at the expense of more
	// complex code).
	s := bufio.NewScanner(reader)
	lastLine := ""
	for s.Scan() {
		lastLine = s.Text()
	}
	if err := s.Err(); err != nil {
		return time.Time{}, err
	}
	if err := reader.Close(); err != nil {
		log.Errorf("failed to close since file: %v", err)
	}
	parsedSince, err := fhir.ParseFHIRInstant(lastLine)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse timestamp: %w", err)
	}
	return parsedSince, nil
}

func writeTimestampToFile(ts time.Time, writer io.WriteCloser) error {
	if _, err := writer.Write(append([]byte(fhir.ToFHIRInstant(ts)), '\n')); err != nil {
		return err
	}
	return writer.Close()
}
