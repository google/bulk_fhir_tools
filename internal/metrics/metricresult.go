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

package metrics

import (
	"fmt"
	"sort"
	"strings"

	"github.com/google/bulk_fhir_tools/internal/metrics/aggregation"
)

// CounterResult holds the results for the counter. It can be printed using String().
type CounterResult struct {
	// Count maps a concatenation of the tagValues to count for those tagValues.
	// If no tags are used then Count will map the name to the count.
	Count       map[string]int64
	Name        string
	Description string
	Unit        string
	Aggregation aggregation.Aggregation
	TagKeys     []string
}

// String returns a printable result. The result string is stable. There is no
// randomness in the ordering the results are printed.
func (c *CounterResult) String() string {
	aggrDescription := ""
	if c.Aggregation == aggregation.LastValueInGCPMaxValueInLocal {
		aggrDescription = "Max value recorded by the metric (for counters logged locally)."
	} else {
		aggrDescription = "Total sum of values recorded by this metric (for counters logged locally)."
	}

	header := fmt.Sprintf("\nName: %s\n%s\nUnits: %s\nAggregation Type: %s\n", c.Name, c.Description, c.Unit, aggrDescription)
	body := make([]string, 0)

	if len(c.Count) == 0 {
		body = append(body, "Record was never called on this metric.")
	} else if len(c.TagKeys) == 0 {
		body = append(body, fmt.Sprintf("Count: %d\n", c.Count[c.Name]))
	} else {
		for k, v := range c.Count {
			body = append(body, fmt.Sprintf("%s: %d\n", k, v))
		}
		sort.Strings(body)
	}
	return header + strings.Join(body, "")
}

// LatencyResult holds the results of the latency. It can be printed using String().
type LatencyResult struct {
	// Dist maps a concatenation of the tagValues to distribution for those
	// tagValues. If no tags are used then Dist will map the name to the
	// distribution. The distribution is defined by the Buckets. For example,
	// Buckets: [0, 3, 5] will create a distribution with 4 buckets where the last
	// bucket is anything > 5. Dist: <0, >=0 <3, >=3 <5, >=5.
	Dist        map[string][]int
	Name        string
	Description string
	Unit        string
	Buckets     []float64
	TagKeys     []string
}

// String returns a printable result. The result string is stable. There is no
// randomness in the ordering the results are printed.
func (l *LatencyResult) String() string {
	header := fmt.Sprintf("\nName: %s\n%s\nUnits: %s\nBuckets: ", l.Name, l.Description, l.Unit)
	for _, bucket := range l.Buckets {
		header += fmt.Sprintf("<%.2f, ", bucket)
	}
	header += ">= all\n"
	body := make([]string, 0)
	if len(l.Dist) == 0 {
		body = append(body, "Record was never called on this metric.")
	} else if len(l.TagKeys) == 0 {
		body = append(body, fmt.Sprintf("Dist: %d\n", l.Dist[l.Name]))
	} else {
		for k, v := range l.Dist {
			body = append(body, fmt.Sprintf("%s: %v\n", k, v))

		}
		sort.Strings(body)
	}
	return header + strings.Join(body, "")
}
