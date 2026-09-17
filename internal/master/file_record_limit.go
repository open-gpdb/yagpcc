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

const truncationMarker = "...[truncated]"

type truncatableTextField struct {
	object       map[string]interface{}
	fieldName    string
	originalText string
}

func (fw *FileWriters) limitRecord(data []byte, stream string) ([]byte, error) {
	result, truncated, err := limitArchiveJSON(data, fw.fileRecordLimit)
	outcome := archiveRecordOutcome(truncated, err)

	if outcome == "dropped" {
		fw.logger.Warnf("dropping oversized %s archive record: bytes=%d: %v", stream, len(data), err)
	}

	if m := metrics.YagpccMetrics; m != nil && m.FileArchiveRecords != nil {
		m.FileArchiveRecords.WithLabelValues(stream, outcome).Inc()
	}
	return result, err
}

// limitArchiveJSON only changes the serialized file copy. The same source
// objects are concurrently consumed by other archive writers.
func limitArchiveJSON(data []byte, limit int64) ([]byte, bool, error) {
	if int64(len(data))+1 <= limit {
		return data, false, nil
	}

	var record map[string]interface{}
	dec := json.NewDecoder(bytes.NewReader(data))
	// IDs and integer metrics must not pass through float64 (loss above 2^53).
	dec.UseNumber()
	if err := dec.Decode(&record); err != nil {
		return nil, false, err
	}

	fields := collectTruncatableTextFields(record)
	result, err := fitArchiveRecord(record, fields, limit)
	if err != nil {
		return nil, false, err
	}
	return result, true, nil
}

func collectTruncatableTextFields(record map[string]interface{}) []truncatableTextField {
	groups := []struct {
		objectName string
		fieldNames []string
	}{
		{"GpStatInfo", []string{"Query"}},
		{"RunningQueryInfo", []string{"QueryText", "PlanText"}},
		{"queryInfo", []string{"queryText", "planText", "templateQueryText", "templatePlanText"}},
	}

	var fields []truncatableTextField
	for _, group := range groups {
		object, ok := record[group.objectName].(map[string]interface{})
		if !ok {
			continue
		}
		for _, name := range group.fieldNames {
			text, ok := object[name].(string)
			if !ok || text == "" {
				continue
			}
			fields = append(fields, truncatableTextField{
				object: object, fieldName: name, originalText: text,
			})
		}
	}
	return fields
}

func fitArchiveRecord(record map[string]interface{}, fields []truncatableTextField, limit int64) ([]byte, error) {
	budget := 0
	for _, field := range fields {
		budget = max(budget, len(field.originalText))
	}

	for {
		result, err := marshalArchiveWithTextBudget(record, fields, budget)
		if err != nil {
			return nil, err
		}
		excess := int64(len(result)) + 1 - limit
		if excess <= 0 {
			return result, nil
		}
		if budget == 0 {
			return nil, fmt.Errorf("archive record exceeds %d bytes even after removing query/plan text", limit)
		}

		affected := 0
		for _, field := range fields {
			if len(field.originalText) >= budget {
				affected++
			}
		}
		reduction := 1 + (excess-1)/int64(affected)
		// excess counts JSON bytes, but budget counts source text bytes. Escaping
		// can inflate excess, so subtracting it directly may discard all text.
		// Halving is a conservative step: re-marshal before cutting further.
		// Allow a one-byte step so a budget of 1 can still reach zero.
		reduction = min(reduction, int64(max(1, budget/2)))
		budget -= int(reduction)
	}
}

func marshalArchiveWithTextBudget(record map[string]interface{}, fields []truncatableTextField, budget int) ([]byte, error) {
	for _, field := range fields {
		field.object[field.fieldName] = truncateArchiveText(field.originalText, budget)
	}
	return json.Marshal(record)
}

// budget counts original text bytes, excluding the marker and JSON escaping.
func truncateArchiveText(text string, budget int) string {
	if len(text) <= budget {
		return text
	}
	end := budget
	for end > 0 && !utf8.RuneStart(text[end]) {
		end--
	}
	return text[:end] + truncationMarker
}

func archiveRecordOutcome(truncated bool, err error) string {
	if err != nil {
		return "dropped"
	}
	if truncated {
		return "truncated"
	}
	return "unchanged"
}
