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

// Package fhir holds utilities for working with claims-related fast healthcare
// interoperability resources (FHIR).
package fhir

import (
	"bufio"
	"io"

	"github.com/google/fhir/go/fhirversion"
	"github.com/google/fhir/go/jsonformat"
	rpb "github.com/google/fhir/go/proto/google/fhir/proto/stu3/resources_go_proto"
)

const (
	// maxTokenSize represents the maximum newline delimited token size in bytes
	// expected when parsing FHIR NDJSON.
	maxTokenSize = 500 * 1024
	// initialBufferSize indicates the initial buffer size in bytes to use when
	// parsing a FHIR NDJSON token.
	initialBufferSize = 5 * 1024
)

// Result represents the result of attempting to unmarshal one entry from FHIR
// NDJSON into FHIR proto
type Result struct {
	ContainedResource *rpb.ContainedResource
	Error             error
}

// UnmarshalR3 takes an input Reader to a R3 FHIR NDJSON stream and attempts to
// unmarshal each newline delimited entry into a FHIR protocol buffer which is
// sent on the returned Result channel. If an error occurred while unmarshaling
// a particular entry, the Error field in the Result sent on the channel will be
// set.
func UnmarshalR3(in io.Reader) (<-chan *Result, error) {
	// This is not clear in the jsonformat docs, but the provided "UTC" timezone is
	// only used when the source data does not include a timezone or the precision
	// is >= DAY, when timezone does not matter.
	un, err := jsonformat.NewUnmarshaller("UTC", fhirversion.STU3)
	if err != nil {
		return nil, err
	}

	out := make(chan *Result)
	s := bufio.NewScanner(in)
	s.Buffer(make([]byte, initialBufferSize), maxTokenSize)
	go func() {
		for s.Scan() {
			resource, err := un.UnmarshalR3(s.Bytes())
			out <- &Result{resource, err}
		}
		if s.Err() != nil {
			out <- &Result{nil, err}
		}
		close(out)
	}()

	return out, nil
}
