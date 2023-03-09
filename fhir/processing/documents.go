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

package processing

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"mime"
	"net/http"
	"os"
	"path/filepath"

	"cloud.google.com/go/storage"
	"github.com/google/medical_claims_tools/bulkfhir"
	"github.com/google/medical_claims_tools/gcs"
	log "github.com/google/medical_claims_tools/internal/logger"
	"github.com/google/medical_claims_tools/internal/metrics"

	cpb "github.com/google/fhir/go/proto/google/fhir/proto/r4/core/codes_go_proto"
	dpb "github.com/google/fhir/go/proto/google/fhir/proto/r4/core/datatypes_go_proto"
	drpb "github.com/google/fhir/go/proto/google/fhir/proto/r4/core/resources/document_reference_go_proto"
)

var documentRetrievalCounter *metrics.Counter = metrics.NewCounter("document-retrieval-counter", "Count by HTTP Status when retrieving DocumentReference resources.", "1", "HTTPStatus")

type fileWriter interface {
	writeFile(ctx context.Context, filename string, data []byte) (string, error)
}

type gcsFileWriter struct {
	client            gcs.Client
	bucket, directory string
}

func (gfw *gcsFileWriter) writeFile(ctx context.Context, filename string, data []byte) (string, error) {
	gcsPath := filepath.Join(gfw.directory, filename)
	gcsURI := fmt.Sprintf("gs://%s/%s", gfw.bucket, gcsPath)
	gcsObj := gfw.client.Bucket(gfw.bucket).Object(gcsPath)
	attrs, err := gcsObj.Attrs(ctx)
	needsUpload := true
	if err != nil {
		if !errors.Is(err, storage.ErrObjectNotExist) {
			return "", fmt.Errorf("error getting document attributes from %s: %w", gcsURI, err)
		}
	} else {
		// The file exists, so check if the CRC32C checksum of the file matches. If
		// so, we treat the file as unchanged and don't upload it again.
		crc32c := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
		if attrs.CRC32C == crc32c {
			needsUpload = false
		}
	}
	if needsUpload {
		w := gcsObj.NewWriter(ctx)
		if _, err := w.Write(data); err != nil {
			return "", fmt.Errorf("error writing document to %s: %w", gcsURI, err)
		}
		if err := w.Close(); err != nil {
			return "", fmt.Errorf("error closing document %s: %w", gcsURI, err)
		}
	}
	return gcsURI, nil
}

type localFileWriter struct {
	directory string
}

func (lfw *localFileWriter) writeFile(ctx context.Context, filename string, data []byte) (string, error) {
	fullPath := filepath.Join(lfw.directory, filename)
	return fmt.Sprintf("file://%s", fullPath), os.WriteFile(fullPath, data, 0666)
}

type documentsProcessor struct {
	BaseProcessor
	authenticator bulkfhir.Authenticator
	httpClient    *http.Client
	fileWriter    fileWriter
}

var _ Processor = &documentsProcessor{}

// DocumentsProcessorConfig contains the configuration needed for creating a
// Documents Processor.
type DocumentsProcessorConfig struct {
	// Some APIs may require different authentication to be used for retrieving
	// documents vs the authentication used for the Bulk FHIR export process.
	Authenticator                        bulkfhir.Authenticator
	HTTPClient                           *http.Client
	LocalDirectory                       string
	GCSEndpoint, GCSBucket, GCSDirectory string
}

// NewDocumentsProcessor creates a Processor which downloads documents from
// the URLs found in DocumentReference resources, and replaces those URLs with
// URIs for the downloaded files.
func NewDocumentsProcessor(ctx context.Context, cfg *DocumentsProcessorConfig) (Processor, error) {
	var fw fileWriter
	if cfg.LocalDirectory != "" {
		fw = &localFileWriter{cfg.LocalDirectory}
	} else {
		gcsClient, err := gcs.NewClient(ctx, cfg.GCSBucket, cfg.GCSEndpoint)
		if err != nil {
			return nil, err
		}
		fw = &gcsFileWriter{
			client:    gcsClient,
			bucket:    cfg.GCSBucket,
			directory: cfg.GCSDirectory,
		}
	}
	return &documentsProcessor{
		authenticator: cfg.Authenticator,
		httpClient:    cfg.HTTPClient,
		fileWriter:    fw,
	}, nil
}

func (dp *documentsProcessor) Process(ctx context.Context, resource ResourceWrapper) error {
	switch resource.Type() {
	case cpb.ResourceTypeCode_DOCUMENT_REFERENCE:
		return dp.processDocument(ctx, resource)
	default:
		return dp.Output(ctx, resource)
	}
}

func (dp *documentsProcessor) processDocument(ctx context.Context, resource ResourceWrapper) error {
	proto, err := resource.Proto()
	if err != nil {
		return err
	}
	dr := proto.GetDocumentReference()
	if dr == nil {
		return fmt.Errorf("resource %s was not DocumentReference", dr.GetId().GetValue())
	}

	for i, c := range dr.GetContent() {
		if err := dp.downloadFileAndUpdateResource(ctx, dr.GetId().GetValue(), i, c); err != nil {
			return err
		}
	}

	return dp.Output(ctx, resource)
}

func (dp *documentsProcessor) downloadFileAndUpdateResource(ctx context.Context, documentID string, index int, content *drpb.DocumentReference_Content) error {
	url := content.GetAttachment().GetUrl().GetValue()
	if url == "" {
		return nil
	}

	req, err := http.NewRequest(http.MethodGet, url, http.NoBody)
	if err != nil {
		return err
	}
	if err := dp.authenticator.AddAuthenticationToRequest(dp.httpClient, req); err != nil {
		return err
	}
	resp, err := dp.httpClient.Do(req)
	if err != nil {
		return err
	}

	documentRetrievalCounter.Record(ctx, 1, http.StatusText(resp.StatusCode))
	if resp.StatusCode != http.StatusOK {
		log.Errorf("request for %s returned unexpected status %s", url, resp.Status)
		return nil
	}
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		return err
	}
	if err := resp.Body.Close(); err != nil {
		return err
	}

	// Best effort attempt to determine an appropriate file extension; if not
	// available we just save the file without an extension.
	var ext string
	exts, err := mime.ExtensionsByType(content.GetAttachment().GetContentType().GetValue())
	if err == nil && len(exts) > 0 {
		ext = exts[0]
	}

	fileURL, err := dp.fileWriter.writeFile(ctx, fmt.Sprintf("%s_%d%s", documentID, index, ext), buf.Bytes())
	if err != nil {
		return err
	}
	content.GetAttachment().Url = &dpb.Url{Value: fileURL}
	return nil
}

func (dp *documentsProcessor) Finalize(ctx context.Context) error {
	return nil
}
