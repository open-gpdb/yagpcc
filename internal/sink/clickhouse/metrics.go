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
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Insert status label values for yagpcc_ch_inserts_total.
const (
	InsertStatusSuccess = "success"
	InsertStatusError   = "error"
)

// Drop reason label values for yagpcc_ch_dropped_rows_total. Queue overflow is
// accounted for by the batch pipeline; the ClickHouse writer only drops rows it
// cannot map or that a failed INSERT discards.
const (
	DropReasonMappingError = "mapping_error"
	DropReasonInsertError  = "insert_error"
)

// batchDurationBuckets span a tiny warm-connection batch (10ms) up to a slow CH
// or network hiccup (30s) — enough resolution to spot a p99 regression without
// bloating cardinality.
var batchDurationBuckets = []float64{
	0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30,
}

// Metrics owns the Prometheus collectors for the ClickHouse writer. Build it
// once and share it across the per-table writers; the underlying vectors are
// safe for concurrent use.
type Metrics struct {
	Inserts       *prometheus.CounterVec
	BatchDuration *prometheus.HistogramVec
	DroppedRows   *prometheus.CounterVec
}

// NewMetrics constructs Metrics and registers every collector on reg. Pass
// prometheus.DefaultRegisterer in production; tests should use a fresh
// prometheus.NewRegistry so a previous run does not poison registration
// (MustRegister panics on duplicates).
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Inserts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "yagpcc_ch_inserts_total",
			Help: "ClickHouse insert batches grouped by destination table and outcome.",
		}, []string{"table", "status"}),
		BatchDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "yagpcc_ch_batch_duration_seconds",
			Help:    "Wall-clock duration of one ClickHouse INSERT batch (prepare+append+send).",
			Buckets: batchDurationBuckets,
		}, []string{"table"}),
		DroppedRows: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "yagpcc_ch_dropped_rows_total",
			Help: "Rows the ClickHouse writer dropped, by table and reason (mapping_error/insert_error).",
		}, []string{"table", "reason"}),
	}
	reg.MustRegister(m.Inserts, m.BatchDuration, m.DroppedRows)
	return m
}

// ObserveInsert records a successful batch INSERT into table and its duration.
func (m *Metrics) ObserveInsert(table string, d time.Duration) {
	m.Inserts.WithLabelValues(table, InsertStatusSuccess).Inc()
	m.BatchDuration.WithLabelValues(table).Observe(d.Seconds())
}

// ObserveInsertError records a failed batch INSERT into table and its duration.
func (m *Metrics) ObserveInsertError(table string, d time.Duration) {
	m.Inserts.WithLabelValues(table, InsertStatusError).Inc()
	m.BatchDuration.WithLabelValues(table).Observe(d.Seconds())
}

// IncDropped increments the dropped-rows counter for table with reason.
func (m *Metrics) IncDropped(table, reason string) {
	m.DroppedRows.WithLabelValues(table, reason).Inc()
}
