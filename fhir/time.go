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

package fhir

import "time"

const (
	layoutSecondsUTC        = "2006-01-02T15:04:05Z"
	layoutSeconds           = "2006-01-02T15:04:05-07:00"
	fhirTimeOutputFormatStr = "2006-01-02T15:04:05.000-07:00"
)

// ParseFHIRInstant parses a FHIR instant string into a time.Time.
func ParseFHIRInstant(instant string) (time.Time, error) {
	t, err := time.Parse(layoutSecondsUTC, instant)
	if err != nil {
		return time.Parse(layoutSeconds, instant)
	}
	return t, nil
}

// ToFHIRInstant takes a time.Time and returns the string FHIR Instant
// representation of it.
func ToFHIRInstant(t time.Time) string {
	return t.Format(fhirTimeOutputFormatStr)
}
