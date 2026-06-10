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

// File: metrics.go declares the Prometheus collectors that expose ClickHouse
// sink health (yagpcc_ch_inserts_total, yagpcc_ch_buffer_size,
// yagpcc_ch_batch_duration_seconds, yagpcc_ch_dropped_rows_total,
// yagpcc_ch_schema_mismatch, yagpcc_ch_unreachable) and the hook adapters that
// wire each writer's callback fields straight into them.
package clickhouse

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Insert status label values for yagpcc_ch_inserts_total.
const (
	InsertStatusSuccess = "success"
	InsertStatusError   = "error"
)

// Metrics owns the Prometheus collectors for the ClickHouse sink. Build it
// once at orchestrator construction and pass the matching hook adapter to each
// writer's Hooks field. Concurrent updates are safe — the underlying
// CounterVec/GaugeVec/HistogramVec are thread-safe.
type Metrics struct {
	Inserts        *prometheus.CounterVec
	BufferSize     *prometheus.GaugeVec
	BatchDuration  *prometheus.HistogramVec
	DroppedRows    *prometheus.CounterVec
	SchemaMismatch prometheus.Gauge
	Unreachable    prometheus.Gauge
}

// batchDurationBuckets covers the range called for in the plan: from 10ms
// (a tiny batch landing on a warm connection) up to 30s (slow CH or large
// batch with a network hiccup). Twelve buckets is enough resolution to spot a
// p99 regression without bloating the cardinality.
var batchDurationBuckets = []float64{
	0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30,
}

// NewMetrics constructs Metrics and registers every collector on reg. Pass
// prometheus.DefaultRegisterer in production; tests should use a fresh
// prometheus.NewRegistry() so a previous test does not poison registration
// (MustRegister panics on duplicates).
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Inserts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "yagpcc_ch_inserts_total",
			Help: "ClickHouse insert batches grouped by destination table and outcome.",
		}, []string{"table", "status"}),
		BufferSize: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "yagpcc_ch_buffer_size",
			Help: "Current row count buffered per ClickHouse destination table.",
		}, []string{"table"}),
		BatchDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "yagpcc_ch_batch_duration_seconds",
			Help:    "Wall-clock duration of one ClickHouse INSERT batch (prepare+append+send).",
			Buckets: batchDurationBuckets,
		}, []string{"table"}),
		DroppedRows: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "yagpcc_ch_dropped_rows_total",
			Help: "Rows the ClickHouse sink dropped (buffer overflow, filter, mapping error, insert error).",
		}, []string{"table", "reason"}),
		SchemaMismatch: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "yagpcc_ch_schema_mismatch",
			Help: "1 when the embedded schema version disagrees with the deployed yagpcc database, 0 otherwise.",
		}),
		Unreachable: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "yagpcc_ch_unreachable",
			Help: "1 when the sink last failed to reach ClickHouse, 0 otherwise. Suitable for alerting.",
		}),
	}
	reg.MustRegister(
		m.Inserts,
		m.BufferSize,
		m.BatchDuration,
		m.DroppedRows,
		m.SchemaMismatch,
		m.Unreachable,
	)
	return m
}

// hooksFor builds a generic set of update closures bound to one table label.
// Used by the typed *Hooks adapters below so each writer family stays
// independent of the metrics package while the wiring stays in one place.
type tableHooks struct {
	drop      func(reason string)
	inserted  func(rows int)
	insertErr func()
	batchDur  func(d time.Duration)
	bufSize   func(n int)
}

func (m *Metrics) tableHooks(table string) tableHooks {
	return tableHooks{
		drop: func(reason string) {
			m.DroppedRows.WithLabelValues(table, reason).Inc()
		},
		inserted: func(rows int) {
			m.Inserts.WithLabelValues(table, InsertStatusSuccess).Inc()
			_ = rows // row count is not in inserts_total — batch count is the natural unit per scrape
		},
		insertErr: func() {
			m.Inserts.WithLabelValues(table, InsertStatusError).Inc()
		},
		batchDur: func(d time.Duration) {
			m.BatchDuration.WithLabelValues(table).Observe(d.Seconds())
		},
		bufSize: func(n int) {
			m.BufferSize.WithLabelValues(table).Set(float64(n))
		},
	}
}

// QueryEventHooks returns Hooks bound to the query_events table. Pass it
// directly to QueryEventWriterConfig.Hooks.
func (m *Metrics) QueryEventHooks() QueryEventWriterHooks {
	h := m.tableHooks(TableQueryEvents)
	return QueryEventWriterHooks{
		OnDrop:          func(r dropReason) { h.drop(string(r)) },
		OnInsertSuccess: h.inserted,
		OnInsertError:   h.insertErr,
		OnBatchDuration: h.batchDur,
		OnBufferSize:    h.bufSize,
	}
}

// AggregatedHooks returns Hooks bound to the aggregated_metrics table.
func (m *Metrics) AggregatedHooks() AggregatedWriterHooks {
	h := m.tableHooks(TableAggregatedMetrics)
	return AggregatedWriterHooks{
		OnDrop:          func(r dropReason) { h.drop(string(r)) },
		OnInsertSuccess: h.inserted,
		OnInsertError:   h.insertErr,
		OnBatchDuration: h.batchDur,
	}
}

// SessionSnapshotHooks returns Hooks bound to the session_snapshots table.
func (m *Metrics) SessionSnapshotHooks() SessionSnapshotWriterHooks {
	h := m.tableHooks(TableSessionSnapshots)
	return SessionSnapshotWriterHooks{
		OnDrop:          func(r dropReason) { h.drop(string(r)) },
		OnInsertSuccess: h.inserted,
		OnInsertError:   h.insertErr,
		OnBatchDuration: h.batchDur,
	}
}

// SetSchemaMismatch flips the schema_mismatch gauge. The orchestrator (Task
// 13) sets it to 1 when verify_only sees a divergent _yagpcc_meta version and
// resets it to 0 once auto-migration succeeds.
func (m *Metrics) SetSchemaMismatch(mismatch bool) {
	if mismatch {
		m.SchemaMismatch.Set(1)
	} else {
		m.SchemaMismatch.Set(0)
	}
}

// SetUnreachable flips the unreachable gauge. The orchestrator updates it
// from the periodic ping / flush outcomes so an external alert can fire on a
// sustained 1.
func (m *Metrics) SetUnreachable(unreachable bool) {
	if unreachable {
		m.Unreachable.Set(1)
	} else {
		m.Unreachable.Set(0)
	}
}
