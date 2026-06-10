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

// File: tables.go owns the per-table writers (QueryEventWriter,
// AggregatedWriter, SessionSnapshotWriter): batching, INSERT preparation and
// per-table failure accounting.
package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
)

// queryEventsInsertSQL is the INSERT statement passed to PrepareBatch. It
// names only the table — the driver discovers column order from the server's
// reply and uses the order returned by toQueryEventRow.
const queryEventsInsertSQL = "INSERT INTO yagpcc.query_events"

// aggregatedMetricsInsertSQL is the INSERT statement for the aggregated_metrics
// table. Same convention as queryEventsInsertSQL: column order is implied by
// toAggregatedRow.
const aggregatedMetricsInsertSQL = "INSERT INTO yagpcc.aggregated_metrics"

// sessionSnapshotsInsertSQL is the INSERT statement for the session_snapshots
// table. Column order is implied by toSessionRow.
const sessionSnapshotsInsertSQL = "INSERT INTO yagpcc.session_snapshots"

// TableQueryEvents is the metric/log label used for the query_events table.
const TableQueryEvents = "query_events"

// TableAggregatedMetrics is the metric/log label used for the
// aggregated_metrics table.
const TableAggregatedMetrics = "aggregated_metrics"

// TableSessionSnapshots is the metric/log label used for the session_snapshots
// table.
const TableSessionSnapshots = "session_snapshots"

// dropReason is the value used for the `reason` label of the
// dropped_rows_total counter. The four values match the plan's metrics
// section so Task 12 can wire them straight through.
type dropReason string

const (
	dropReasonBufferFull   dropReason = "buffer_full"
	dropReasonFilter       dropReason = "filter"
	dropReasonMappingError dropReason = "mapping_error"
	dropReasonInsertError  dropReason = "insert_error"
)

// batchPreparer is the subset of driver.Conn the QueryEventWriter needs.
// Tests use an in-memory fake; production code passes the real driver.Conn.
type batchPreparer interface {
	PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error)
}

// QueryEventWriterHooks lets the orchestrator (Task 13) plug Prometheus
// counters / histograms / gauges into the writer without coupling this file
// to the metrics package. A nil hook is treated as a no-op so unit tests can
// leave them unset.
type QueryEventWriterHooks struct {
	OnDrop          func(reason dropReason)
	OnInsertSuccess func(rows int)
	OnInsertError   func()
	OnBatchDuration func(d time.Duration)
	OnBufferSize    func(size int)
}

func (h QueryEventWriterHooks) drop(r dropReason) {
	if h.OnDrop != nil {
		h.OnDrop(r)
	}
}

func (h QueryEventWriterHooks) inserted(rows int) {
	if h.OnInsertSuccess != nil {
		h.OnInsertSuccess(rows)
	}
}

func (h QueryEventWriterHooks) insertErr() {
	if h.OnInsertError != nil {
		h.OnInsertError()
	}
}

func (h QueryEventWriterHooks) batchDur(d time.Duration) {
	if h.OnBatchDuration != nil {
		h.OnBatchDuration(d)
	}
}

func (h QueryEventWriterHooks) bufSize(n int) {
	if h.OnBufferSize != nil {
		h.OnBufferSize(n)
	}
}

// QueryEventWriterConfig groups the construction-time parameters of
// QueryEventWriter. Defaults applied by NewQueryEventWriter:
//   - BatchSize: 10000 when <= 0
//   - BufferCapacity: 100000 when <= 0
//   - OverflowMode: OverflowDropOldest
//   - SchemaVersion: ExpectedSchemaVersion when 0
type QueryEventWriterConfig struct {
	Conn           batchPreparer
	BatchSize      int
	BufferCapacity int
	OverflowMode   OverflowMode
	MinDuration    time.Duration
	SchemaVersion  uint32
	YagpccVersion  string
	Hooks          QueryEventWriterHooks
}

// QueryEventWriter buffers query_events rows and flushes them in batches to
// ClickHouse. Concurrent Write calls are serialised through the underlying
// Buffer; Flush is intended to be called from a single goroutine (the
// periodic flusher in writer.go).
type QueryEventWriter struct {
	conn          batchPreparer
	buffer        *Buffer
	batchSize     int
	minDuration   time.Duration
	schemaVersion uint32
	yagpccVersion string
	hooks         QueryEventWriterHooks
}

// NewQueryEventWriter constructs a QueryEventWriter. Returns an error when
// Conn is nil, MinDuration is negative, or the configured buffer capacity is
// invalid.
func NewQueryEventWriter(cfg QueryEventWriterConfig) (*QueryEventWriter, error) {
	if cfg.Conn == nil {
		return nil, errors.New("clickhouse: query_events writer needs a Conn")
	}
	if cfg.MinDuration < 0 {
		return nil, fmt.Errorf("clickhouse: MinDuration must be >= 0, got %s", cfg.MinDuration)
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 10000
	}
	if cfg.BufferCapacity <= 0 {
		cfg.BufferCapacity = 100000
	}
	if cfg.SchemaVersion == 0 {
		cfg.SchemaVersion = ExpectedSchemaVersion
	}
	w := &QueryEventWriter{
		conn:          cfg.Conn,
		batchSize:     cfg.BatchSize,
		minDuration:   cfg.MinDuration,
		schemaVersion: cfg.SchemaVersion,
		yagpccVersion: cfg.YagpccVersion,
		hooks:         cfg.Hooks,
	}
	hooks := cfg.Hooks
	onOverflow := func() { hooks.drop(dropReasonBufferFull) }
	buf, err := NewBuffer(cfg.BufferCapacity, cfg.OverflowMode, onOverflow)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: query_events buffer: %w", err)
	}
	w.buffer = buf
	return w, nil
}

// Write enqueues a query_events row built from qT. The row is dropped (with
// the matching counter incremented) when:
//   - qT is unusable (nil / no QueryStat); reason = mapping_error;
//   - the status is END/DONE/ERROR/CANCELED and the recorded duration is
//     shorter than MinDuration; reason = filter.
//
// Other statuses (SUBMIT, START, CANCELLING) bypass the duration filter so
// the event-log keeps a timeline of the query regardless of how short it ends
// up being.
func (w *QueryEventWriter) Write(qT *pbm.TotalQueryData) {
	if qT == nil || qT.QueryStat == nil {
		w.hooks.drop(dropReasonMappingError)
		return
	}
	if w.shouldFilter(qT) {
		w.hooks.drop(dropReasonFilter)
		return
	}
	row := toQueryEventRow(qT, w.schemaVersion, w.yagpccVersion)
	if row == nil {
		w.hooks.drop(dropReasonMappingError)
		return
	}
	w.buffer.Append(row)
	w.hooks.bufSize(w.buffer.Len())
}

// shouldFilter implements the min_duration_ms filter. Only terminal statuses
// have a meaningful end-time: shorter-than-threshold runs are dropped so the
// event log doesn't overflow with trivial queries. Non-terminal events
// (SUBMIT/START/CANCELLING) always pass.
func (w *QueryEventWriter) shouldFilter(qT *pbm.TotalQueryData) bool {
	if w.minDuration <= 0 {
		return false
	}
	switch qT.QueryStat.QueryStatus {
	case pbc.QueryStatus_QUERY_STATUS_END,
		pbc.QueryStatus_QUERY_STATUS_DONE,
		pbc.QueryStatus_QUERY_STATUS_QUERY_DONE,
		pbc.QueryStatus_QUERY_STATUS_ERROR,
		pbc.QueryStatus_QUERY_STATUS_CANCELED:
	default:
		return false
	}
	d := queryEventDuration(qT)
	return d > 0 && d < w.minDuration
}

// Flush drains up to BatchSize rows from the buffer and INSERTs them. Returns
// the number of rows successfully sent and any error from the driver.
//
// On error the rows that were already drained are NOT re-queued: at-most-once
// semantics (matches the plan's drop_oldest contract). The insert_error
// counter is incremented and the error is wrapped with the failing stage so
// callers can log without losing context.
func (w *QueryEventWriter) Flush(ctx context.Context) (int, error) {
	rows := w.buffer.Drain(w.batchSize)
	w.hooks.bufSize(w.buffer.Len())
	if len(rows) == 0 {
		return 0, nil
	}
	started := time.Now()
	defer func() { w.hooks.batchDur(time.Since(started)) }()

	batch, err := w.conn.PrepareBatch(ctx, queryEventsInsertSQL)
	if err != nil {
		w.hooks.insertErr()
		return 0, fmt.Errorf("prepare query_events batch: %w", err)
	}
	for _, raw := range rows {
		row, ok := raw.([]any)
		if !ok {
			w.hooks.drop(dropReasonMappingError)
			continue
		}
		if err := batch.Append(row...); err != nil {
			_ = batch.Abort()
			w.hooks.insertErr()
			return 0, fmt.Errorf("append query_events row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		w.hooks.insertErr()
		return 0, fmt.Errorf("send query_events batch: %w", err)
	}
	w.hooks.inserted(len(rows))
	return len(rows), nil
}

// BufferLen exposes the current size for tests and metrics. Cheap (mutex
// acquire) — fine for a periodic gauge update.
func (w *QueryEventWriter) BufferLen() int {
	return w.buffer.Len()
}

// CloseBuffer marks the underlying buffer as closed so any goroutine blocked
// on a full OverflowBlock buffer wakes up and unwinds. Subsequent Write calls
// drop the row. Existing queued rows can still be drained via Flush.
func (w *QueryEventWriter) CloseBuffer() {
	w.buffer.Close()
}

// AggregatedWriterHooks mirrors QueryEventWriterHooks for the aggregated
// metrics writer. The set of events is a subset: aggregated rows arrive in
// pre-formed batches from AggregatedStorage, so there is no buffer-level
// overflow or min_duration filter — drops happen only when a bucket fails to
// map or the INSERT itself errors.
type AggregatedWriterHooks struct {
	OnDrop          func(reason dropReason)
	OnInsertSuccess func(rows int)
	OnInsertError   func()
	OnBatchDuration func(d time.Duration)
}

func (h AggregatedWriterHooks) drop(r dropReason) {
	if h.OnDrop != nil {
		h.OnDrop(r)
	}
}

func (h AggregatedWriterHooks) inserted(rows int) {
	if h.OnInsertSuccess != nil {
		h.OnInsertSuccess(rows)
	}
}

func (h AggregatedWriterHooks) insertErr() {
	if h.OnInsertError != nil {
		h.OnInsertError()
	}
}

func (h AggregatedWriterHooks) batchDur(d time.Duration) {
	if h.OnBatchDuration != nil {
		h.OnBatchDuration(d)
	}
}

// AggregatedWriterConfig holds construction-time parameters for
// AggregatedWriter. Conn is required; Hooks may be left zero.
type AggregatedWriterConfig struct {
	Conn  batchPreparer
	Hooks AggregatedWriterHooks
}

// AggregatedWriter inserts pre-aggregated bucket rows into
// yagpcc.aggregated_metrics. SummingMergeTree on the server side merges
// duplicates with the same ORDER BY key, so re-inserting the same bucket on
// retry is harmless from a totals standpoint — but at-most-once still applies:
// a failed batch is NOT re-queued by the writer.
type AggregatedWriter struct {
	conn  batchPreparer
	hooks AggregatedWriterHooks
}

// NewAggregatedWriter constructs an AggregatedWriter. Returns an error when
// Conn is nil — that's the only mandatory dependency.
func NewAggregatedWriter(cfg AggregatedWriterConfig) (*AggregatedWriter, error) {
	if cfg.Conn == nil {
		return nil, errors.New("clickhouse: aggregated_metrics writer needs a Conn")
	}
	return &AggregatedWriter{
		conn:  cfg.Conn,
		hooks: cfg.Hooks,
	}, nil
}

// FlushBuckets INSERTs the supplied buckets in a single batch and returns the
// number of rows successfully sent.
//
// Behaviour:
//   - empty input → no PrepareBatch call, returns (0, nil);
//   - nil entries are skipped with a mapping_error drop hook;
//   - PrepareBatch / Append / Send errors are wrapped with the failing stage,
//     the insert_error hook is fired and the batch is aborted on Append error;
//   - on success the insert_success hook reports the row count.
//
// At-most-once: the slice is consumed regardless of outcome — callers that
// want retry semantics must keep their own copy.
func (w *AggregatedWriter) FlushBuckets(ctx context.Context, buckets []AggregatedBucket) (int, error) {
	if len(buckets) == 0 {
		return 0, nil
	}
	started := time.Now()
	defer func() { w.hooks.batchDur(time.Since(started)) }()

	batch, err := w.conn.PrepareBatch(ctx, aggregatedMetricsInsertSQL)
	if err != nil {
		w.hooks.insertErr()
		return 0, fmt.Errorf("prepare aggregated_metrics batch: %w", err)
	}
	appended := 0
	for i := range buckets {
		row := toAggregatedRow(&buckets[i])
		if row == nil {
			w.hooks.drop(dropReasonMappingError)
			continue
		}
		if err := batch.Append(row...); err != nil {
			_ = batch.Abort()
			w.hooks.insertErr()
			return 0, fmt.Errorf("append aggregated_metrics row: %w", err)
		}
		appended++
	}
	if appended == 0 {
		// Nothing to send; PrepareBatch already opened a request, so abort to
		// release server-side resources rather than sending an empty batch.
		_ = batch.Abort()
		return 0, nil
	}
	if err := batch.Send(); err != nil {
		w.hooks.insertErr()
		return 0, fmt.Errorf("send aggregated_metrics batch: %w", err)
	}
	w.hooks.inserted(appended)
	return appended, nil
}

// SessionSnapshotWriterHooks lets the orchestrator (Task 13) wire Prometheus
// counters into the session-snapshots writer. Mirrors AggregatedWriterHooks:
// snapshots are pushed in pre-formed batches by the scheduler, so there is no
// buffer-level overflow counter — drops happen only on mapping or insert
// errors.
type SessionSnapshotWriterHooks struct {
	OnDrop          func(reason dropReason)
	OnInsertSuccess func(rows int)
	OnInsertError   func()
	OnBatchDuration func(d time.Duration)
}

func (h SessionSnapshotWriterHooks) drop(r dropReason) {
	if h.OnDrop != nil {
		h.OnDrop(r)
	}
}

func (h SessionSnapshotWriterHooks) inserted(rows int) {
	if h.OnInsertSuccess != nil {
		h.OnInsertSuccess(rows)
	}
}

func (h SessionSnapshotWriterHooks) insertErr() {
	if h.OnInsertError != nil {
		h.OnInsertError()
	}
}

func (h SessionSnapshotWriterHooks) batchDur(d time.Duration) {
	if h.OnBatchDuration != nil {
		h.OnBatchDuration(d)
	}
}

// SessionsProvider is the source of the per-tick session snapshot. The
// orchestrator (Task 13) wraps SessionsStorage so that this package never
// imports internal/gp directly. Returning an empty slice is benign: the
// writer skips the INSERT entirely. Provider is invoked from the scheduler
// goroutine and must respect ctx cancellation.
type SessionsProvider func(ctx context.Context) []Session

// SessionSnapshotWriterConfig groups the construction-time parameters of
// SessionSnapshotWriter. Conn and Provider are mandatory; Interval must be
// strictly positive (the orchestrator sources it from
// cfg.SessionSnapshotIntervalSec).
type SessionSnapshotWriterConfig struct {
	Conn     batchPreparer
	Interval time.Duration
	Provider SessionsProvider
	Hooks    SessionSnapshotWriterHooks
}

// SessionSnapshotWriter periodically asks Provider for the active session
// list and INSERTs one row per session into yagpcc.session_snapshots.
//
// The writer is intentionally stateless beyond construction: each tick
// produces one independent batch (no buffering). The orchestrator decides
// whether to construct the writer at all (e.g., skips it when
// cfg.Sinks.SessionSnapshots == false), so there is no Enabled flag here.
type SessionSnapshotWriter struct {
	conn     batchPreparer
	interval time.Duration
	provider SessionsProvider
	hooks    SessionSnapshotWriterHooks
}

// NewSessionSnapshotWriter constructs a SessionSnapshotWriter. Returns an
// error when Conn is nil, Provider is nil, or Interval is non-positive.
func NewSessionSnapshotWriter(cfg SessionSnapshotWriterConfig) (*SessionSnapshotWriter, error) {
	if cfg.Conn == nil {
		return nil, errors.New("clickhouse: session_snapshots writer needs a Conn")
	}
	if cfg.Provider == nil {
		return nil, errors.New("clickhouse: session_snapshots writer needs a Provider")
	}
	if cfg.Interval <= 0 {
		return nil, fmt.Errorf("clickhouse: session_snapshots interval must be > 0, got %s", cfg.Interval)
	}
	return &SessionSnapshotWriter{
		conn:     cfg.Conn,
		interval: cfg.Interval,
		provider: cfg.Provider,
		hooks:    cfg.Hooks,
	}, nil
}

// FlushSnapshot inserts a single batch of session rows. Empty input is a
// no-op (no PrepareBatch call). Behaviour matches AggregatedWriter.FlushBuckets:
// nil entries are skipped with a mapping_error drop hook; PrepareBatch /
// Append / Send errors are wrapped with the failing stage and the
// insert_error hook is fired. At-most-once: the slice is consumed regardless
// of outcome.
func (w *SessionSnapshotWriter) FlushSnapshot(ctx context.Context, sessions []Session) (int, error) {
	if len(sessions) == 0 {
		return 0, nil
	}
	started := time.Now()
	defer func() { w.hooks.batchDur(time.Since(started)) }()

	batch, err := w.conn.PrepareBatch(ctx, sessionSnapshotsInsertSQL)
	if err != nil {
		w.hooks.insertErr()
		return 0, fmt.Errorf("prepare session_snapshots batch: %w", err)
	}
	appended := 0
	for i := range sessions {
		row := toSessionRow(&sessions[i])
		if row == nil {
			w.hooks.drop(dropReasonMappingError)
			continue
		}
		if err := batch.Append(row...); err != nil {
			_ = batch.Abort()
			w.hooks.insertErr()
			return 0, fmt.Errorf("append session_snapshots row: %w", err)
		}
		appended++
	}
	if appended == 0 {
		_ = batch.Abort()
		return 0, nil
	}
	if err := batch.Send(); err != nil {
		w.hooks.insertErr()
		return 0, fmt.Errorf("send session_snapshots batch: %w", err)
	}
	w.hooks.inserted(appended)
	return appended, nil
}

// RunOnce performs a single snapshot+flush cycle: it asks Provider for the
// current sessions and forwards them to FlushSnapshot. Errors come back via
// the hooks (insert_error counter) so the scheduler loop never aborts on
// transient CH failures.
func (w *SessionSnapshotWriter) RunOnce(ctx context.Context) (int, error) {
	sessions := w.provider(ctx)
	return w.FlushSnapshot(ctx, sessions)
}

// Run drives the snapshot scheduler until ctx is cancelled. Each tick
// invokes RunOnce; per-tick errors are absorbed by the hook (so a temporary
// CH outage does not stop the loop). Returns ctx.Err() on cancellation.
func (w *SessionSnapshotWriter) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			_, _ = w.RunOnce(ctx)
		}
	}
}
