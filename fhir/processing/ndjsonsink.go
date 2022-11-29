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
	"context"
	"fmt"
	"io"

	"os"
	"path/filepath"

	"github.com/google/medical_claims_tools/bulkfhir"

	cpb "github.com/google/fhir/go/proto/google/fhir/proto/r4/core/codes_go_proto"
)

type fileKey struct {
	resourceType cpb.ResourceTypeCode_Value
	sourceURL    string
}

type ndjsonSink struct {
	files                 map[fileKey]io.WriteCloser
	fileIndex             map[cpb.ResourceTypeCode_Value]int
	directory, filePrefix string
}

// NewNDJSONSink creates a new Sink which writes resources to NDJSON files in
// the given directory. Resources are grouped by the URL they were retrieved
// from, with their file name containing the resource type and an incremented
// index to distinguish them. If filePrefix is non-empty, it is prepended to the
// generated filename separated with an underscore. Thus, example filenames
// would be Patient_1.ndjson with no filePrefix, or myPrefix_Patient_1.ndjson
// with filePrefix set.
func NewNDJSONSink(ctx context.Context, directory, filePrefix string) (Sink, error) {
	if stat, err := os.Stat(directory); err != nil {
		return nil, fmt.Errorf("could not stat directory %q - %w", directory, err)
	} else if !stat.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", directory)
	}
	return &ndjsonSink{
		files:      map[fileKey]io.WriteCloser{},
		fileIndex:  map[cpb.ResourceTypeCode_Value]int{},
		directory:  directory,
		filePrefix: filePrefix,
	}, nil
}

func (ns *ndjsonSink) getWriter(ctx context.Context, resource ResourceWrapper) (io.Writer, error) {
	key := fileKey{resource.Type(), resource.SourceURL()}
	w := ns.files[key]
	if w != nil {
		return w, nil
	}
	typeName, err := bulkfhir.ResourceTypeCodeToName(key.resourceType)
	if err != nil {
		return nil, err
	}
	idx := ns.fileIndex[key.resourceType]
	ns.fileIndex[resource.Type()] = idx + 1
	filename := fmt.Sprintf("%s_%d.ndjson", typeName, idx)
	if ns.filePrefix != "" {
		filename = fmt.Sprintf("%s_%s", ns.filePrefix, filename)
	}
	filename = filepath.Join(ns.directory, filename)
	w, err = os.Create(filename)
	if err != nil {
		return nil, err
	}
	ns.files[key] = w
	return w, nil
}

func (ns *ndjsonSink) Write(ctx context.Context, resource ResourceWrapper) error {
	w, err := ns.getWriter(ctx, resource)
	if err != nil {
		return err
	}
	json, err := resource.JSON()
	if err != nil {
		return err
	}
	_, err = w.Write(append(json, byte('\n')))
	return err
}

func (ns *ndjsonSink) Finalize(ctx context.Context) error {
	for _, wc := range ns.files {
		if err := wc.Close(); err != nil {
			return err
		}
	}
	return nil
}
