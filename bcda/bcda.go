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

// Package bcda contains helpers to create a bulkfhir.Client for interacting
// with Medicare's Beneficiary Claims Data API (BCDA).
package bcda

import (
	"errors"

	"github.com/google/medical_claims_tools/bulkfhir"
)

// ErrorInvalidVersion indicates the API version provided is invalid.
var ErrorInvalidVersion = errors.New("invalid API version provided")

// Version represents a BCDA API version.
type Version int

const (
	// V1 represents the V1 BCDA API version.
	V1 Version = iota
	// V2 represents the V2 BCDA API version.
	V2
)

// TODO(b/239856442): rename exported methods to include bulk fhir. For example
// NewBulkFHIRClient.

// NewClient creates and returns a new BCDA API Client for the input baseURL,
// targeted at the provided API version.
func NewClient(baseURL string, v Version, clientID, clientSecret string) (*bulkfhir.Client, error) {
	if err := validateVersion(v); err != nil {
		return nil, err
	}

	return bulkfhir.NewClient(getVersionedBaseURL(baseURL, v), getDefaultAuthURL(baseURL), clientID, clientSecret)
}

func getVersionedBaseURL(baseURL string, v Version) string {
	versionedBaseURL := baseURL + "/api/v1"
	if v == V2 {
		versionedBaseURL = baseURL + "/api/v2"
	}
	return versionedBaseURL
}

func getDefaultAuthURL(baseURL string) string {
	return baseURL + "/auth/token"
}

func validateVersion(v Version) error {
	if v != V1 && v != V2 {
		return ErrorInvalidVersion
	}
	return nil
}
