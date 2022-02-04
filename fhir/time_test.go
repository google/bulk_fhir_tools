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

package fhir_test

import (
	"testing"
	"time"

	"github.com/google/medical_claims_tools/fhir"
)

func TestParseFHIRInstant(t *testing.T) {
	tests := []struct {
		name     string
		instant  string
		wantTime time.Time
	}{
		{
			"Start of unix epoch time with second precision",
			"1970-01-01T00:00:00+00:00",
			time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			"Datetime at a random second in UTC timezone with second precision",
			"2012-06-03T23:45:32Z",
			time.Date(2012, 6, 3, 23, 45, 32, 0, time.UTC),
		},
		{
			"Datetime on the hour in UTC timezone with second precision",
			"2013-12-09T11:00:00Z",
			time.Date(2013, 12, 9, 11, 0, 0, 0, time.UTC),
		},
		{
			"Datetime with nanosecond in UTC timezone with ms precision",
			"2013-12-09T11:00:00.123Z",
			time.Date(2013, 12, 9, 11, 0, 0, 123000000, time.UTC),
		},
		{
			"Datetime in EST timezone with second precision",
			"2009-09-28T02:37:43-05:00",
			time.Date(2009, 9, 28, 2, 37, 43, 0, time.FixedZone("EST", -5*60*60)),
		},
		{
			"Datetime with nanosecond in EST timezone with ms precision",
			"2009-09-28T02:37:43.123-05:00",
			time.Date(2009, 9, 28, 2, 37, 43, 123000000, time.FixedZone("EST", -5*60*60)),
		},
		{
			"Datetime with nanosecond in EST timezone with microsecond precision",
			"2009-09-28T02:37:43.123456-05:00",
			time.Date(2009, 9, 28, 2, 37, 43, 123456000, time.FixedZone("EST", -5*60*60)),
		},
		{
			"Datetime with nanosecond with trailing zeros in EST timezone with microsecond precision",
			"2009-09-28T02:37:43.123000-05:00",
			time.Date(2009, 9, 28, 2, 37, 43, 123000000, time.FixedZone("EST", -5*60*60)),
		},
		{
			"Datetime with nanosecond in UTC timezone with microsecond precision",
			"2013-12-09T11:00:00.123456Z",
			time.Date(2013, 12, 9, 11, 0, 0, 123456000, time.UTC),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotTime, err := fhir.ParseFHIRInstant(tc.instant)
			if err != nil {
				t.Errorf("ParseFHIRInstant(%q) returned unexpected error: %v", tc.instant, err)
			}
			if !tc.wantTime.Equal(gotTime) {
				t.Errorf("ParseFHIRInstant(%q) returned incorrect time, got: %v want: %v", tc.instant, gotTime, tc.wantTime)
			}
		})
	}
}

func TestParseFHIRInstant_Invalid(t *testing.T) {
	tests := []string{
		"2013-12-09T11:00:00",
		"2013-12-09T11:00Z",
	}
	for _, tc := range tests {
		if _, err := fhir.ParseFHIRInstant(tc); err == nil {
			t.Errorf("ParseFHIRInstant(%q) succeeded, want error", tc)
		}
	}
}

func TestToFHIRInstant(t *testing.T) {
	tests := []struct {
		name  string
		input time.Time
		want  string
	}{
		{
			name:  "UTCWithMSPrecision",
			input: time.Date(2013, 12, 9, 11, 0, 0, 123000000, time.UTC),
			want:  "2013-12-09T11:00:00.123+00:00",
		},
		{
			name:  "ESTWithMSPrecision",
			input: time.Date(2009, 9, 28, 2, 37, 43, 123000000, time.FixedZone("EST", -5*60*60)),
			want:  "2009-09-28T02:37:43.123-05:00",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := fhir.ToFHIRInstant(tc.input); got != tc.want {
				t.Errorf("ToFHIRInstant(%v) unexpected output. got: %v, want: %v", tc.input, got, tc.want)
			}
		})
	}
}
