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
	dto "github.com/prometheus/client_model/go"
)

// newTestMetrics returns a Metrics wired to a fresh registry so each test is
// isolated from prometheus.DefaultRegisterer (which would panic on
// re-registration across runs in the same package).
func newTestMetrics(t *testing.T) (*Metrics, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	return NewMetrics(reg), reg
}

func TestNewMetricsRegistersAllCollectors(t *testing.T) {
	m, reg := newTestMetrics(t)

	// Touch every vec-based metric so Gather emits a family for it.
	// Gauges show up unconditionally; CounterVec/GaugeVec/HistogramVec only
	// surface a series once a label tuple is observed.
	m.Inserts.WithLabelValues(TableQueryEvents, InsertStatusSuccess).Inc()
	m.BufferSize.WithLabelValues(TableQueryEvents).Set(0)
	m.BatchDuration.WithLabelValues(TableQueryEvents).Observe(0)
	m.DroppedRows.WithLabelValues(TableQueryEvents, "buffer_full").Inc()

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
		"yagpcc_ch_buffer_size",
		"yagpcc_ch_batch_duration_seconds",
		"yagpcc_ch_dropped_rows_total",
		"yagpcc_ch_schema_mismatch",
		"yagpcc_ch_unreachable",
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

func TestQueryEventHooksUpdateMetrics(t *testing.T) {
	m, reg := newTestMetrics(t)
	h := m.QueryEventHooks()

	h.OnInsertSuccess(7)
	h.OnInsertSuccess(3)
	h.OnInsertError()
	h.OnBatchDuration(150 * time.Millisecond)
	h.OnBufferSize(42)
	h.OnDrop(dropReasonBufferFull)
	h.OnDrop(dropReasonFilter)
	h.OnDrop(dropReasonMappingError)
	h.OnDrop(dropReasonInsertError)

	if got := testutil.ToFloat64(m.Inserts.WithLabelValues(TableQueryEvents, InsertStatusSuccess)); got != 2 {
		t.Errorf("inserts success: got %v want 2", got)
	}
	if got := testutil.ToFloat64(m.Inserts.WithLabelValues(TableQueryEvents, InsertStatusError)); got != 1 {
		t.Errorf("inserts error: got %v want 1", got)
	}
	if got := testutil.ToFloat64(m.BufferSize.WithLabelValues(TableQueryEvents)); got != 42 {
		t.Errorf("buffer_size: got %v want 42", got)
	}
	for _, reason := range []string{"buffer_full", "filter", "mapping_error", "insert_error"} {
		if got := testutil.ToFloat64(m.DroppedRows.WithLabelValues(TableQueryEvents, reason)); got != 1 {
			t.Errorf("dropped_rows[%s]: got %v want 1", reason, got)
		}
	}

	// Histogram is harder to read by exact bucket — verify the sample lands
	// for the query_events table with the expected non-zero sum.
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	var histSum float64
	var histCount uint64
	for _, f := range families {
		if f.GetName() != "yagpcc_ch_batch_duration_seconds" {
			continue
		}
		for _, mtr := range f.GetMetric() {
			if !hasLabel(mtr.GetLabel(), "table", TableQueryEvents) {
				continue
			}
			histSum = mtr.GetHistogram().GetSampleSum()
			histCount = mtr.GetHistogram().GetSampleCount()
		}
	}
	if histCount != 1 {
		t.Errorf("batch_duration sample count: got %d want 1", histCount)
	}
	if histSum < 0.149 || histSum > 0.151 {
		t.Errorf("batch_duration sample sum: got %v want ~0.15", histSum)
	}
}

func TestAggregatedHooksUpdateMetrics(t *testing.T) {
	m, _ := newTestMetrics(t)
	h := m.AggregatedHooks()

	h.OnInsertSuccess(5)
	h.OnInsertError()
	h.OnDrop(dropReasonMappingError)
	h.OnBatchDuration(20 * time.Millisecond)

	if got := testutil.ToFloat64(m.Inserts.WithLabelValues(TableAggregatedMetrics, InsertStatusSuccess)); got != 1 {
		t.Errorf("inserts success: got %v want 1", got)
	}
	if got := testutil.ToFloat64(m.Inserts.WithLabelValues(TableAggregatedMetrics, InsertStatusError)); got != 1 {
		t.Errorf("inserts error: got %v want 1", got)
	}
	if got := testutil.ToFloat64(m.DroppedRows.WithLabelValues(TableAggregatedMetrics, "mapping_error")); got != 1 {
		t.Errorf("dropped_rows mapping_error: got %v want 1", got)
	}
}

func TestSessionSnapshotHooksUpdateMetrics(t *testing.T) {
	m, _ := newTestMetrics(t)
	h := m.SessionSnapshotHooks()

	h.OnInsertSuccess(11)
	h.OnInsertError()
	h.OnDrop(dropReasonInsertError)
	h.OnBatchDuration(5 * time.Millisecond)

	if got := testutil.ToFloat64(m.Inserts.WithLabelValues(TableSessionSnapshots, InsertStatusSuccess)); got != 1 {
		t.Errorf("inserts success: got %v want 1", got)
	}
	if got := testutil.ToFloat64(m.Inserts.WithLabelValues(TableSessionSnapshots, InsertStatusError)); got != 1 {
		t.Errorf("inserts error: got %v want 1", got)
	}
	if got := testutil.ToFloat64(m.DroppedRows.WithLabelValues(TableSessionSnapshots, "insert_error")); got != 1 {
		t.Errorf("dropped_rows insert_error: got %v want 1", got)
	}
}

func TestSetSchemaMismatchAndUnreachable(t *testing.T) {
	m, _ := newTestMetrics(t)

	m.SetSchemaMismatch(true)
	if got := testutil.ToFloat64(m.SchemaMismatch); got != 1 {
		t.Errorf("schema_mismatch after true: got %v want 1", got)
	}
	m.SetSchemaMismatch(false)
	if got := testutil.ToFloat64(m.SchemaMismatch); got != 0 {
		t.Errorf("schema_mismatch after false: got %v want 0", got)
	}

	m.SetUnreachable(true)
	if got := testutil.ToFloat64(m.Unreachable); got != 1 {
		t.Errorf("unreachable after true: got %v want 1", got)
	}
	m.SetUnreachable(false)
	if got := testutil.ToFloat64(m.Unreachable); got != 0 {
		t.Errorf("unreachable after false: got %v want 0", got)
	}
}

func TestHooksUseCorrectTableLabels(t *testing.T) {
	m, _ := newTestMetrics(t)

	m.QueryEventHooks().OnInsertSuccess(1)
	m.AggregatedHooks().OnInsertSuccess(1)
	m.SessionSnapshotHooks().OnInsertSuccess(1)

	for _, table := range []string{TableQueryEvents, TableAggregatedMetrics, TableSessionSnapshots} {
		if got := testutil.ToFloat64(m.Inserts.WithLabelValues(table, InsertStatusSuccess)); got != 1 {
			t.Errorf("inserts[%s,success]: got %v want 1", table, got)
		}
	}

	// Cross-table independence: bumping query_events must not move
	// aggregated_metrics.
	m.QueryEventHooks().OnInsertError()
	if got := testutil.ToFloat64(m.Inserts.WithLabelValues(TableAggregatedMetrics, InsertStatusError)); got != 0 {
		t.Errorf("aggregated error counter leaked from query_events: got %v want 0", got)
	}
}

// hasLabel reports whether the supplied label set contains name=value.
func hasLabel(labels []*dto.LabelPair, name, value string) bool {
	for _, l := range labels {
		if l.GetName() == name && l.GetValue() == value {
			return true
		}
	}
	return false
}
