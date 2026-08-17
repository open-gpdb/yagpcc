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

package clickhouse

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func newTestMetrics(t *testing.T) (*Metrics, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	return NewMetrics(reg), reg
}

func TestNewMetricsRegistersAllCollectors(t *testing.T) {
	m, reg := newTestMetrics(t)

	// Vector metrics only surface a family once a label tuple is observed.
	m.Inserts.WithLabelValues("sessions_part", InsertStatusSuccess).Inc()
	m.BatchDuration.WithLabelValues("sessions_part").Observe(0)
	m.DroppedRows.WithLabelValues("sessions_part", DropReasonMappingError).Inc()

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	got := map[string]bool{}
	for _, f := range families {
		got[f.GetName()] = true
	}
	want := []string{
		"yagpcc_ch_inserts_total",
		"yagpcc_ch_batch_duration_seconds",
		"yagpcc_ch_dropped_rows_total",
	}
	for _, name := range want {
		if !got[name] {
			t.Errorf("metric %q not registered; got %v", name, got)
		}
	}
}

func TestNewMetricsRejectsDuplicateRegistration(t *testing.T) {
	reg := prometheus.NewRegistry()
	_ = NewMetrics(reg)

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on duplicate NewMetrics, got none")
		}
	}()
	_ = NewMetrics(reg)
}

func TestObserveInsert(t *testing.T) {
	m, reg := newTestMetrics(t)

	m.ObserveInsert("statements_part", 150*time.Millisecond)
	m.ObserveInsert("statements_part", 50*time.Millisecond)
	m.ObserveInsertError("statements_part", 10*time.Millisecond)

	if got := testutil.ToFloat64(m.Inserts.WithLabelValues("statements_part", InsertStatusSuccess)); got != 2 {
		t.Errorf("inserts success: got %v want 2", got)
	}
	if got := testutil.ToFloat64(m.Inserts.WithLabelValues("statements_part", InsertStatusError)); got != 1 {
		t.Errorf("inserts error: got %v want 1", got)
	}

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	var count uint64
	for _, f := range families {
		if f.GetName() != "yagpcc_ch_batch_duration_seconds" {
			continue
		}
		for _, mtr := range f.GetMetric() {
			count += mtr.GetHistogram().GetSampleCount()
		}
	}
	if count != 3 {
		t.Errorf("batch_duration observations: got %d want 3", count)
	}
}

func TestIncDropped(t *testing.T) {
	m, _ := newTestMetrics(t)

	m.IncDropped("segments_part", DropReasonMappingError)
	m.IncDropped("segments_part", DropReasonInsertError)
	m.IncDropped("segments_part", DropReasonInsertError)

	if got := testutil.ToFloat64(m.DroppedRows.WithLabelValues("segments_part", DropReasonMappingError)); got != 1 {
		t.Errorf("dropped mapping_error: got %v want 1", got)
	}
	if got := testutil.ToFloat64(m.DroppedRows.WithLabelValues("segments_part", DropReasonInsertError)); got != 2 {
		t.Errorf("dropped insert_error: got %v want 2", got)
	}

	// Cross-table independence: another table must stay at zero.
	if got := testutil.ToFloat64(m.DroppedRows.WithLabelValues("sessions_part", DropReasonMappingError)); got != 0 {
		t.Errorf("dropped leaked across tables: got %v want 0", got)
	}
}
