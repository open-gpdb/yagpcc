// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright
// ownership. The ASF licenses this file to You under the Apache
// License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the
// License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package master

import (
	"bytes"
	"encoding/json"
	"fmt"
	"unicode/utf8"

	"github.com/open-gpdb/yagpcc/internal/metrics"
)

// FileRecordLimit includes the trailing newline. Keep in sync with the
// Unified Agent file_input max_bytes_in_line used for the file archive.
const FileRecordLimit = 1 << 20
const truncationMarker = "...[truncated]"

type archiveText struct {
	parent   map[string]interface{}
	key      string
	original string
}

// limitArchiveJSON only changes the serialized file copy. The same source
// objects are concurrently consumed by other archive writers.
func limitArchiveJSON(data []byte, limit int) ([]byte, bool, error) {
	if len(data)+1 <= limit {
		return data, false, nil
	}
	var record map[string]interface{}
	dec := json.NewDecoder(bytes.NewReader(data))
	// IDs and integer metrics must not pass through float64 (loss above 2^53).
	dec.UseNumber()
	if err := dec.Decode(&record); err != nil {
		return nil, false, err
	}
	var fields []archiveText
	groups := []struct {
		parent string
		keys   []string
	}{
		{"GpStatInfo", []string{"Query"}},
		{"RunningQueryInfo", []string{"QueryText", "PlanText"}},
		{"queryInfo", []string{"queryText", "planText", "templateQueryText", "templatePlanText"}},
	}
	longest := 0
	for _, group := range groups {
		parent, ok := record[group.parent].(map[string]interface{})
		if !ok {
			continue
		}
		for _, key := range group.keys {
			value, ok := parent[key].(string)
			if !ok || value == "" {
				continue
			}
			fields = append(fields, archiveText{parent, key, value})
			if len(value) > longest {
				longest = len(value)
			}
		}
	}
	// A shared prefix budget preserves short fields while retaining prefixes
	// of all large query/plan fields. Search against actual encoded JSON size.
	encode := func(budget int) ([]byte, error) {
		for _, field := range fields {
			value := field.original
			if len(value) > budget {
				end := budget
				for end > 0 && !utf8.RuneStart(value[end]) {
					end--
				}
				value = value[:end] + truncationMarker
			}
			field.parent[field.key] = value
		}
		return json.Marshal(record)
	}
	best, err := encode(0)
	if err != nil {
		return nil, false, err
	}
	if len(best)+1 > limit {
		return nil, false, fmt.Errorf("archive record exceeds %d bytes even after removing query/plan text", limit)
	}
	// Shorter fields can lose the marker at their full length, so the search
	// guarantees a fitting result, not the maximum possible retained prefix.
	low, high := 1, longest-1
	for low <= high {
		mid := low + (high-low)/2
		candidate, err := encode(mid)
		if err != nil {
			return nil, false, err
		}
		if len(candidate)+1 <= limit {
			best = candidate
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return best, true, nil
}

func (fw *FileWriters) limitRecord(data []byte, stream string) ([]byte, error) {
	result, truncated, err := limitArchiveJSON(data, FileRecordLimit)
	outcome := "truncated"
	if err != nil {
		outcome = "dropped"
	}
	if err != nil || truncated {
		if m := metrics.YagpccMetrics; m != nil && m.FileOversizedRecords != nil {
			m.FileOversizedRecords.WithLabelValues(stream, outcome).Inc()
		}
		if err != nil {
			fw.logger.Warnf("dropping oversized %s archive record: bytes=%d: %v", stream, len(data), err)
		}
	}
	return result, err
}
