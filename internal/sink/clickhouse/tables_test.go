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
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"google.golang.org/protobuf/types/known/timestamppb"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
)

// fakeBatch records Append calls and lets each test fail Append/Send to
// exercise error paths in QueryEventWriter.Flush.
type fakeBatch struct {
	mu        sync.Mutex
	rows      [][]any
	appendErr error
	sendErr   error
	sent      bool
	aborted   bool
}

func (f *fakeBatch) Abort() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.aborted = true
	return nil
}

func (f *fakeBatch) Append(v ...any) error {
	if f.appendErr != nil {
		return f.appendErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	row := make([]any, len(v))
	copy(row, v)
	f.rows = append(f.rows, row)
	return nil
}

func (f *fakeBatch) AppendStruct(any) error        { return errors.New("not implemented") }
func (f *fakeBatch) Column(int) driver.BatchColumn { return nil }
func (f *fakeBatch) Flush() error                  { return nil }
func (f *fakeBatch) IsSent() bool                  { return f.sent }
func (f *fakeBatch) Rows() int                     { return len(f.rows) }
func (f *fakeBatch) Columns() []column.Interface   { return nil }
func (f *fakeBatch) Close() error                  { return nil }

func (f *fakeBatch) Send() error {
	if f.sendErr != nil {
		return f.sendErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = true
	return nil
}

// fakeBatchConn is a minimal batchPreparer for QueryEventWriter tests. Each
// PrepareBatch call returns the next batch in the configured queue and
// records the SQL it was called with.
type fakeBatchConn struct {
	mu         sync.Mutex
	batches    []*fakeBatch
	prepareErr error
	calls      int
	lastQuery  string
}

func (c *fakeBatchConn) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	c.lastQuery = query
	if c.prepareErr != nil {
		return nil, c.prepareErr
	}
	if len(c.batches) == 0 {
		// Default: hand out a fresh empty batch so simple tests don't have to
		// pre-populate the queue.
		return &fakeBatch{}, nil
	}
	b := c.batches[0]
	c.batches = c.batches[1:]
	return b, nil
}

func makeQueryEvent(status pbc.QueryStatus, durationMs int64) *pbm.TotalQueryData {
	start := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Duration(durationMs) * time.Millisecond)
	return &pbm.TotalQueryData{
		QueryStat: &pbm.QueryStat{
			QueryStatus: status,
			QueryKey:    &pbc.QueryKey{Tmid: 1, Ssid: 2, Ccnt: 3},
			QueryInfo: &pbc.QueryInfo{
				QueryId:    1,
				PlanId:     1,
				UserName:   "u",
				SubmitTime: timestamppb.New(start.Add(-time.Second)),
			},
			StartTime:   timestamppb.New(start),
			EndTime:     timestamppb.New(end),
			CollectTime: timestamppb.New(end),
		},
	}
}

type recordedHooks struct {
	drops          map[dropReason]int
	insertSuccess  int
	insertedRows   int
	insertErrors   int
	batchDurations int
	bufSizeMu      sync.Mutex
	lastBufSize    int
}

func newRecordedHooks() *recordedHooks {
	return &recordedHooks{drops: make(map[dropReason]int)}
}

func (r *recordedHooks) hooks() QueryEventWriterHooks {
	return QueryEventWriterHooks{
		OnDrop:          func(reason dropReason) { r.drops[reason]++ },
		OnInsertSuccess: func(n int) { r.insertSuccess++; r.insertedRows += n },
		OnInsertError:   func() { r.insertErrors++ },
		OnBatchDuration: func(time.Duration) { r.batchDurations++ },
		OnBufferSize: func(n int) {
			r.bufSizeMu.Lock()
			defer r.bufSizeMu.Unlock()
			r.lastBufSize = n
		},
	}
}

func TestNewQueryEventWriter_Validation(t *testing.T) {
	if _, err := NewQueryEventWriter(QueryEventWriterConfig{}); err == nil {
		t.Error("expected error for nil Conn")
	}
	if _, err := NewQueryEventWriter(QueryEventWriterConfig{
		Conn:        &fakeBatchConn{},
		MinDuration: -1 * time.Second,
	}); err == nil {
		t.Error("expected error for negative MinDuration")
	}
	w, err := NewQueryEventWriter(QueryEventWriterConfig{Conn: &fakeBatchConn{}})
	if err != nil {
		t.Fatalf("default config: unexpected error %v", err)
	}
	if w.batchSize != 10000 {
		t.Errorf("default BatchSize = %d, want 10000", w.batchSize)
	}
	if w.buffer.Cap() != 100000 {
		t.Errorf("default buffer Cap = %d, want 100000", w.buffer.Cap())
	}
	if w.schemaVersion != ExpectedSchemaVersion {
		t.Errorf("default schema version = %d, want %d", w.schemaVersion, ExpectedSchemaVersion)
	}
}

func TestQueryEventWriter_Write_NilDropped(t *testing.T) {
	rh := newRecordedHooks()
	w, _ := NewQueryEventWriter(QueryEventWriterConfig{
		Conn:  &fakeBatchConn{},
		Hooks: rh.hooks(),
	})
	w.Write(nil)
	w.Write(&pbm.TotalQueryData{})
	if rh.drops[dropReasonMappingError] != 2 {
		t.Errorf("mapping_error drops = %d, want 2", rh.drops[dropReasonMappingError])
	}
	if w.BufferLen() != 0 {
		t.Errorf("buffer should be empty after invalid input, got %d", w.BufferLen())
	}
}

func TestQueryEventWriter_Write_FilterShortQueries(t *testing.T) {
	rh := newRecordedHooks()
	w, _ := NewQueryEventWriter(QueryEventWriterConfig{
		Conn:        &fakeBatchConn{},
		MinDuration: 100 * time.Millisecond,
		Hooks:       rh.hooks(),
	})
	// Below threshold + END status → dropped via filter.
	w.Write(makeQueryEvent(pbc.QueryStatus_QUERY_STATUS_END, 50))
	if rh.drops[dropReasonFilter] != 1 {
		t.Errorf("filter drops = %d, want 1", rh.drops[dropReasonFilter])
	}
	if w.BufferLen() != 0 {
		t.Errorf("buffer = %d after filter drop, want 0", w.BufferLen())
	}
	// At threshold (>= min) → kept.
	w.Write(makeQueryEvent(pbc.QueryStatus_QUERY_STATUS_END, 100))
	if w.BufferLen() != 1 {
		t.Errorf("buffer = %d, want 1 (>= threshold should pass)", w.BufferLen())
	}
}

func TestQueryEventWriter_Write_NonTerminalBypassesFilter(t *testing.T) {
	rh := newRecordedHooks()
	w, _ := NewQueryEventWriter(QueryEventWriterConfig{
		Conn:        &fakeBatchConn{},
		MinDuration: 1 * time.Hour,
		Hooks:       rh.hooks(),
	})
	w.Write(makeQueryEvent(pbc.QueryStatus_QUERY_STATUS_SUBMIT, 1))
	w.Write(makeQueryEvent(pbc.QueryStatus_QUERY_STATUS_START, 1))
	w.Write(makeQueryEvent(pbc.QueryStatus_QUERY_STATUS_CANCELLING, 1))
	if w.BufferLen() != 3 {
		t.Errorf("non-terminal events should bypass filter, buf=%d want 3", w.BufferLen())
	}
	if rh.drops[dropReasonFilter] != 0 {
		t.Errorf("filter drops = %d, want 0", rh.drops[dropReasonFilter])
	}
}

func TestQueryEventWriter_Flush_EmptyNoOp(t *testing.T) {
	conn := &fakeBatchConn{}
	w, _ := NewQueryEventWriter(QueryEventWriterConfig{Conn: conn})
	n, err := w.Flush(context.Background())
	if err != nil {
		t.Fatalf("Flush empty: %v", err)
	}
	if n != 0 {
		t.Errorf("Flush(empty) = %d, want 0", n)
	}
	if conn.calls != 0 {
		t.Errorf("PrepareBatch called %d times on empty buffer", conn.calls)
	}
}

func TestQueryEventWriter_Flush_SuccessSendsBatch(t *testing.T) {
	rh := newRecordedHooks()
	batch := &fakeBatch{}
	conn := &fakeBatchConn{batches: []*fakeBatch{batch}}
	w, _ := NewQueryEventWriter(QueryEventWriterConfig{
		Conn:          conn,
		BatchSize:     10,
		Hooks:         rh.hooks(),
		YagpccVersion: "v1",
	})
	w.Write(makeQueryEvent(pbc.QueryStatus_QUERY_STATUS_END, 1000))
	w.Write(makeQueryEvent(pbc.QueryStatus_QUERY_STATUS_END, 2000))

	n, err := w.Flush(context.Background())
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if n != 2 {
		t.Errorf("Flush returned %d, want 2", n)
	}
	if conn.lastQuery != queryEventsInsertSQL {
		t.Errorf("PrepareBatch query = %q, want %q", conn.lastQuery, queryEventsInsertSQL)
	}
	if !batch.sent {
		t.Errorf("batch.Send was not called")
	}
	if len(batch.rows) != 2 {
		t.Fatalf("batch.rows = %d, want 2", len(batch.rows))
	}
	// Sanity-check column count of the first appended row matches the layout.
	if len(batch.rows[0]) != 36 {
		t.Errorf("appended row length = %d, want 36", len(batch.rows[0]))
	}
	if rh.insertSuccess != 1 || rh.insertedRows != 2 {
		t.Errorf("insert hook: success=%d rows=%d, want 1/2", rh.insertSuccess, rh.insertedRows)
	}
	if rh.batchDurations != 1 {
		t.Errorf("batch duration hook called %d times, want 1", rh.batchDurations)
	}
	if w.BufferLen() != 0 {
		t.Errorf("buffer remained at %d, want 0", w.BufferLen())
	}
}

func TestQueryEventWriter_Flush_BatchSizeRespected(t *testing.T) {
	conn := &fakeBatchConn{batches: []*fakeBatch{{}, {}}}
	w, _ := NewQueryEventWriter(QueryEventWriterConfig{
		Conn:      conn,
		BatchSize: 2,
	})
	for i := 0; i < 3; i++ {
		w.Write(makeQueryEvent(pbc.QueryStatus_QUERY_STATUS_END, 500))
	}
	n, err := w.Flush(context.Background())
	if err != nil || n != 2 {
		t.Fatalf("first Flush: n=%d err=%v", n, err)
	}
	if w.BufferLen() != 1 {
		t.Errorf("after first flush buffer = %d, want 1", w.BufferLen())
	}
	n, err = w.Flush(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("second Flush: n=%d err=%v", n, err)
	}
}

func TestQueryEventWriter_Flush_PrepareError(t *testing.T) {
	rh := newRecordedHooks()
	conn := &fakeBatchConn{prepareErr: errors.New("boom")}
	w, _ := NewQueryEventWriter(QueryEventWriterConfig{Conn: conn, Hooks: rh.hooks()})
	w.Write(makeQueryEvent(pbc.QueryStatus_QUERY_STATUS_END, 1000))
	n, err := w.Flush(context.Background())
	if err == nil {
		t.Fatal("expected error from PrepareBatch")
	}
	if n != 0 {
		t.Errorf("Flush returned %d on error, want 0", n)
	}
	if rh.insertErrors != 1 {
		t.Errorf("insert_error hook called %d times, want 1", rh.insertErrors)
	}
}

func TestQueryEventWriter_Flush_AppendError(t *testing.T) {
	rh := newRecordedHooks()
	batch := &fakeBatch{appendErr: errors.New("col mismatch")}
	conn := &fakeBatchConn{batches: []*fakeBatch{batch}}
	w, _ := NewQueryEventWriter(QueryEventWriterConfig{Conn: conn, Hooks: rh.hooks()})
	w.Write(makeQueryEvent(pbc.QueryStatus_QUERY_STATUS_END, 1000))
	_, err := w.Flush(context.Background())
	if err == nil {
		t.Fatal("expected error from Append")
	}
	if !batch.aborted {
		t.Error("batch.Abort was not called on append error")
	}
	if rh.insertErrors != 1 {
		t.Errorf("insert_error hook called %d times, want 1", rh.insertErrors)
	}
}

func TestQueryEventWriter_Flush_SendError(t *testing.T) {
	rh := newRecordedHooks()
	batch := &fakeBatch{sendErr: errors.New("server gone")}
	conn := &fakeBatchConn{batches: []*fakeBatch{batch}}
	w, _ := NewQueryEventWriter(QueryEventWriterConfig{Conn: conn, Hooks: rh.hooks()})
	w.Write(makeQueryEvent(pbc.QueryStatus_QUERY_STATUS_END, 1000))
	if _, err := w.Flush(context.Background()); err == nil {
		t.Fatal("expected error from Send")
	}
	if rh.insertErrors != 1 {
		t.Errorf("insert_error hook called %d times, want 1", rh.insertErrors)
	}
}

type recordedAggHooks struct {
	drops          map[dropReason]int
	insertSuccess  int
	insertedRows   int
	insertErrors   int
	batchDurations int
}

func newRecordedAggHooks() *recordedAggHooks {
	return &recordedAggHooks{drops: make(map[dropReason]int)}
}

func (r *recordedAggHooks) hooks() AggregatedWriterHooks {
	return AggregatedWriterHooks{
		OnDrop:          func(reason dropReason) { r.drops[reason]++ },
		OnInsertSuccess: func(n int) { r.insertSuccess++; r.insertedRows += n },
		OnInsertError:   func() { r.insertErrors++ },
		OnBatchDuration: func(time.Duration) { r.batchDurations++ },
	}
}

func makeBucket(qid uint64, executions uint64) AggregatedBucket {
	return AggregatedBucket{
		BucketTime:      time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC),
		QueryID:         qid,
		PlanID:          1,
		User:            "u",
		Database:        "db",
		ResourceGroup:   "rsg",
		Executions:      executions,
		TotalCPUSec:     1.0,
		TotalRunningSec: 2.0,
		TotalRSSBytes:   1024,
		TotalIOBytes:    2048,
		TotalNTuples:    100,
		AvgDurationMs:   42.0,
		MaxDurationMs:   100,
	}
}

func TestNewAggregatedWriter_NilConn(t *testing.T) {
	if _, err := NewAggregatedWriter(AggregatedWriterConfig{}); err == nil {
		t.Error("expected error for nil Conn")
	}
}

func TestAggregatedWriter_FlushBuckets_Empty(t *testing.T) {
	conn := &fakeBatchConn{}
	w, err := NewAggregatedWriter(AggregatedWriterConfig{Conn: conn})
	if err != nil {
		t.Fatalf("NewAggregatedWriter: %v", err)
	}
	n, err := w.FlushBuckets(context.Background(), nil)
	if err != nil || n != 0 {
		t.Fatalf("Flush(nil) = %d, %v; want 0, nil", n, err)
	}
	n, err = w.FlushBuckets(context.Background(), []AggregatedBucket{})
	if err != nil || n != 0 {
		t.Fatalf("Flush(empty) = %d, %v; want 0, nil", n, err)
	}
	if conn.calls != 0 {
		t.Errorf("PrepareBatch called %d times for empty input", conn.calls)
	}
}

func TestAggregatedWriter_FlushBuckets_Success(t *testing.T) {
	rh := newRecordedAggHooks()
	batch := &fakeBatch{}
	conn := &fakeBatchConn{batches: []*fakeBatch{batch}}
	w, _ := NewAggregatedWriter(AggregatedWriterConfig{Conn: conn, Hooks: rh.hooks()})
	buckets := []AggregatedBucket{
		makeBucket(1, 5),
		makeBucket(2, 7),
	}
	n, err := w.FlushBuckets(context.Background(), buckets)
	if err != nil {
		t.Fatalf("FlushBuckets: %v", err)
	}
	if n != 2 {
		t.Errorf("FlushBuckets = %d, want 2", n)
	}
	if conn.lastQuery != aggregatedMetricsInsertSQL {
		t.Errorf("PrepareBatch query = %q, want %q", conn.lastQuery, aggregatedMetricsInsertSQL)
	}
	if !batch.sent {
		t.Error("batch.Send was not called")
	}
	if len(batch.rows) != 2 {
		t.Fatalf("batch.rows = %d, want 2", len(batch.rows))
	}
	if len(batch.rows[0]) != 14 {
		t.Errorf("appended row length = %d, want 14", len(batch.rows[0]))
	}
	if rh.insertSuccess != 1 || rh.insertedRows != 2 {
		t.Errorf("insert hook: success=%d rows=%d, want 1/2", rh.insertSuccess, rh.insertedRows)
	}
	if rh.batchDurations != 1 {
		t.Errorf("batch duration hook called %d times, want 1", rh.batchDurations)
	}
}

func TestAggregatedWriter_FlushBuckets_PrepareError(t *testing.T) {
	rh := newRecordedAggHooks()
	conn := &fakeBatchConn{prepareErr: errors.New("boom")}
	w, _ := NewAggregatedWriter(AggregatedWriterConfig{Conn: conn, Hooks: rh.hooks()})
	n, err := w.FlushBuckets(context.Background(), []AggregatedBucket{makeBucket(1, 1)})
	if err == nil {
		t.Fatal("expected error from PrepareBatch")
	}
	if n != 0 {
		t.Errorf("FlushBuckets = %d on error, want 0", n)
	}
	if rh.insertErrors != 1 {
		t.Errorf("insert_error hook = %d, want 1", rh.insertErrors)
	}
}

func TestAggregatedWriter_FlushBuckets_AppendError(t *testing.T) {
	rh := newRecordedAggHooks()
	batch := &fakeBatch{appendErr: errors.New("col mismatch")}
	conn := &fakeBatchConn{batches: []*fakeBatch{batch}}
	w, _ := NewAggregatedWriter(AggregatedWriterConfig{Conn: conn, Hooks: rh.hooks()})
	_, err := w.FlushBuckets(context.Background(), []AggregatedBucket{makeBucket(1, 1)})
	if err == nil {
		t.Fatal("expected error from Append")
	}
	if !batch.aborted {
		t.Error("batch.Abort was not called on append error")
	}
	if rh.insertErrors != 1 {
		t.Errorf("insert_error hook = %d, want 1", rh.insertErrors)
	}
}

func TestAggregatedWriter_FlushBuckets_SendError(t *testing.T) {
	rh := newRecordedAggHooks()
	batch := &fakeBatch{sendErr: errors.New("server gone")}
	conn := &fakeBatchConn{batches: []*fakeBatch{batch}}
	w, _ := NewAggregatedWriter(AggregatedWriterConfig{Conn: conn, Hooks: rh.hooks()})
	if _, err := w.FlushBuckets(context.Background(), []AggregatedBucket{makeBucket(1, 1)}); err == nil {
		t.Fatal("expected error from Send")
	}
	if rh.insertErrors != 1 {
		t.Errorf("insert_error hook = %d, want 1", rh.insertErrors)
	}
}

func TestQueryEventWriter_BufferOverflowHook(t *testing.T) {
	rh := newRecordedHooks()
	w, err := NewQueryEventWriter(QueryEventWriterConfig{
		Conn:           &fakeBatchConn{},
		BufferCapacity: 2,
		OverflowMode:   OverflowDropOldest,
		Hooks:          rh.hooks(),
	})
	if err != nil {
		t.Fatalf("NewQueryEventWriter: %v", err)
	}
	for i := 0; i < 5; i++ {
		w.Write(makeQueryEvent(pbc.QueryStatus_QUERY_STATUS_START, 0))
	}
	if rh.drops[dropReasonBufferFull] != 3 {
		t.Errorf("buffer_full drops = %d, want 3", rh.drops[dropReasonBufferFull])
	}
}

type recordedSessHooks struct {
	mu             sync.Mutex
	drops          map[dropReason]int
	insertSuccess  int
	insertedRows   int
	insertErrors   int
	batchDurations int
}

func newRecordedSessHooks() *recordedSessHooks {
	return &recordedSessHooks{drops: make(map[dropReason]int)}
}

func (r *recordedSessHooks) hooks() SessionSnapshotWriterHooks {
	return SessionSnapshotWriterHooks{
		OnDrop: func(reason dropReason) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.drops[reason]++
		},
		OnInsertSuccess: func(n int) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.insertSuccess++
			r.insertedRows += n
		},
		OnInsertError: func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.insertErrors++
		},
		OnBatchDuration: func(time.Duration) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.batchDurations++
		},
	}
}

func (r *recordedSessHooks) snapshot() (success, rows, errs, durs int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.insertSuccess, r.insertedRows, r.insertErrors, r.batchDurations
}

func makeSession(id int32) Session {
	now := time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)
	backend := now.Add(-time.Hour)
	queryStart := now.Add(-time.Minute)
	return Session{
		SnapshotTime: now,
		SessionID:    id,
		PID:          int32(10000 + id),
		User:         "alice",
		Database:     "warehouse",
		Application:  "psql",
		ClientAddr:   "10.0.0.1",
		BackendStart: backend,
		QueryStart:   &queryStart,
		State:        "active",
		Waiting:      false,
		Query:        "SELECT 1",
	}
}

func TestNewSessionSnapshotWriter_Validation(t *testing.T) {
	provider := func(context.Context) []Session { return nil }
	if _, err := NewSessionSnapshotWriter(SessionSnapshotWriterConfig{
		Interval: time.Second,
		Provider: provider,
	}); err == nil {
		t.Error("expected error for nil Conn")
	}
	if _, err := NewSessionSnapshotWriter(SessionSnapshotWriterConfig{
		Conn:     &fakeBatchConn{},
		Interval: time.Second,
	}); err == nil {
		t.Error("expected error for nil Provider")
	}
	if _, err := NewSessionSnapshotWriter(SessionSnapshotWriterConfig{
		Conn:     &fakeBatchConn{},
		Provider: provider,
	}); err == nil {
		t.Error("expected error for zero Interval")
	}
	if _, err := NewSessionSnapshotWriter(SessionSnapshotWriterConfig{
		Conn:     &fakeBatchConn{},
		Provider: provider,
		Interval: -1 * time.Second,
	}); err == nil {
		t.Error("expected error for negative Interval")
	}
	w, err := NewSessionSnapshotWriter(SessionSnapshotWriterConfig{
		Conn:     &fakeBatchConn{},
		Provider: provider,
		Interval: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("valid config: unexpected error %v", err)
	}
	if w.interval != 5*time.Second {
		t.Errorf("interval = %s, want 5s", w.interval)
	}
}

func TestSessionSnapshotWriter_FlushSnapshot_Empty(t *testing.T) {
	conn := &fakeBatchConn{}
	w, _ := NewSessionSnapshotWriter(SessionSnapshotWriterConfig{
		Conn:     conn,
		Provider: func(context.Context) []Session { return nil },
		Interval: time.Second,
	})
	n, err := w.FlushSnapshot(context.Background(), nil)
	if err != nil || n != 0 {
		t.Fatalf("FlushSnapshot(nil) = %d, %v", n, err)
	}
	n, err = w.FlushSnapshot(context.Background(), []Session{})
	if err != nil || n != 0 {
		t.Fatalf("FlushSnapshot(empty) = %d, %v", n, err)
	}
	if conn.calls != 0 {
		t.Errorf("PrepareBatch called %d times for empty input", conn.calls)
	}
}

func TestSessionSnapshotWriter_FlushSnapshot_Success(t *testing.T) {
	rh := newRecordedSessHooks()
	batch := &fakeBatch{}
	conn := &fakeBatchConn{batches: []*fakeBatch{batch}}
	w, _ := NewSessionSnapshotWriter(SessionSnapshotWriterConfig{
		Conn:     conn,
		Provider: func(context.Context) []Session { return nil },
		Interval: time.Second,
		Hooks:    rh.hooks(),
	})
	sessions := []Session{makeSession(1), makeSession(2), makeSession(3)}
	n, err := w.FlushSnapshot(context.Background(), sessions)
	if err != nil {
		t.Fatalf("FlushSnapshot: %v", err)
	}
	if n != 3 {
		t.Errorf("FlushSnapshot = %d, want 3", n)
	}
	if conn.lastQuery != sessionSnapshotsInsertSQL {
		t.Errorf("PrepareBatch query = %q, want %q", conn.lastQuery, sessionSnapshotsInsertSQL)
	}
	if !batch.sent {
		t.Error("batch.Send was not called")
	}
	if len(batch.rows) != 3 {
		t.Fatalf("batch.rows = %d, want 3", len(batch.rows))
	}
	if len(batch.rows[0]) != 14 {
		t.Errorf("appended row length = %d, want 14", len(batch.rows[0]))
	}
	success, rows, errs, durs := rh.snapshot()
	if success != 1 || rows != 3 {
		t.Errorf("insert hook: success=%d rows=%d, want 1/3", success, rows)
	}
	if errs != 0 {
		t.Errorf("insert errors = %d, want 0", errs)
	}
	if durs != 1 {
		t.Errorf("batch durations = %d, want 1", durs)
	}
}

func TestSessionSnapshotWriter_FlushSnapshot_PrepareError(t *testing.T) {
	rh := newRecordedSessHooks()
	conn := &fakeBatchConn{prepareErr: errors.New("boom")}
	w, _ := NewSessionSnapshotWriter(SessionSnapshotWriterConfig{
		Conn:     conn,
		Provider: func(context.Context) []Session { return nil },
		Interval: time.Second,
		Hooks:    rh.hooks(),
	})
	n, err := w.FlushSnapshot(context.Background(), []Session{makeSession(1)})
	if err == nil {
		t.Fatal("expected error from PrepareBatch")
	}
	if n != 0 {
		t.Errorf("FlushSnapshot = %d on error, want 0", n)
	}
	_, _, errs, _ := rh.snapshot()
	if errs != 1 {
		t.Errorf("insert_error hook = %d, want 1", errs)
	}
}

func TestSessionSnapshotWriter_FlushSnapshot_AppendError(t *testing.T) {
	rh := newRecordedSessHooks()
	batch := &fakeBatch{appendErr: errors.New("col mismatch")}
	conn := &fakeBatchConn{batches: []*fakeBatch{batch}}
	w, _ := NewSessionSnapshotWriter(SessionSnapshotWriterConfig{
		Conn:     conn,
		Provider: func(context.Context) []Session { return nil },
		Interval: time.Second,
		Hooks:    rh.hooks(),
	})
	_, err := w.FlushSnapshot(context.Background(), []Session{makeSession(1)})
	if err == nil {
		t.Fatal("expected error from Append")
	}
	if !batch.aborted {
		t.Error("batch.Abort was not called on append error")
	}
	_, _, errs, _ := rh.snapshot()
	if errs != 1 {
		t.Errorf("insert_error hook = %d, want 1", errs)
	}
}

func TestSessionSnapshotWriter_FlushSnapshot_SendError(t *testing.T) {
	rh := newRecordedSessHooks()
	batch := &fakeBatch{sendErr: errors.New("server gone")}
	conn := &fakeBatchConn{batches: []*fakeBatch{batch}}
	w, _ := NewSessionSnapshotWriter(SessionSnapshotWriterConfig{
		Conn:     conn,
		Provider: func(context.Context) []Session { return nil },
		Interval: time.Second,
		Hooks:    rh.hooks(),
	})
	if _, err := w.FlushSnapshot(context.Background(), []Session{makeSession(1)}); err == nil {
		t.Fatal("expected error from Send")
	}
	_, _, errs, _ := rh.snapshot()
	if errs != 1 {
		t.Errorf("insert_error hook = %d, want 1", errs)
	}
}

func TestSessionSnapshotWriter_RunOnce_DrivesProvider(t *testing.T) {
	rh := newRecordedSessHooks()
	batch := &fakeBatch{}
	conn := &fakeBatchConn{batches: []*fakeBatch{batch}}
	var providerCalls int
	provider := func(context.Context) []Session {
		providerCalls++
		return []Session{makeSession(1), makeSession(2)}
	}
	w, _ := NewSessionSnapshotWriter(SessionSnapshotWriterConfig{
		Conn:     conn,
		Provider: provider,
		Interval: time.Second,
		Hooks:    rh.hooks(),
	})
	n, err := w.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if n != 2 {
		t.Errorf("RunOnce = %d, want 2", n)
	}
	if providerCalls != 1 {
		t.Errorf("provider called %d times, want 1", providerCalls)
	}
	if len(batch.rows) != 2 {
		t.Errorf("batch rows = %d, want 2", len(batch.rows))
	}
}

func TestSessionSnapshotWriter_RunOnce_EmptyProvider(t *testing.T) {
	conn := &fakeBatchConn{}
	w, _ := NewSessionSnapshotWriter(SessionSnapshotWriterConfig{
		Conn:     conn,
		Provider: func(context.Context) []Session { return nil },
		Interval: time.Second,
	})
	n, err := w.RunOnce(context.Background())
	if err != nil || n != 0 {
		t.Fatalf("RunOnce(empty) = %d, %v", n, err)
	}
	if conn.calls != 0 {
		t.Errorf("PrepareBatch called %d times for empty snapshot", conn.calls)
	}
}

func TestSessionSnapshotWriter_Run_TickerDrivesFlushes(t *testing.T) {
	rh := newRecordedSessHooks()
	// Pre-populate batches so the ticker can fire several times before the
	// fake conn's auto-batch fallback kicks in.
	conn := &fakeBatchConn{batches: []*fakeBatch{{}, {}, {}, {}, {}, {}, {}, {}}}
	provider := func(context.Context) []Session {
		return []Session{makeSession(1)}
	}
	w, _ := NewSessionSnapshotWriter(SessionSnapshotWriterConfig{
		Conn:     conn,
		Provider: provider,
		Interval: 5 * time.Millisecond,
		Hooks:    rh.hooks(),
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// Wait until at least two ticks have happened, then cancel.
	deadline := time.After(2 * time.Second)
	for {
		success, _, _, _ := rh.snapshot()
		if success >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for scheduled flushes")
		default:
			time.Sleep(2 * time.Millisecond)
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not exit after cancel")
	}
	success, rows, errs, _ := rh.snapshot()
	if success < 2 {
		t.Errorf("insert success ticks = %d, want >= 2", success)
	}
	if rows < success {
		t.Errorf("insert rows = %d, want >= %d", rows, success)
	}
	if errs != 0 {
		t.Errorf("insert errors during scheduled run = %d, want 0", errs)
	}
}

func TestSessionSnapshotWriter_Run_AbsorbsTickError(t *testing.T) {
	rh := newRecordedSessHooks()
	// PrepareBatch fails on every call → each tick should record an error
	// but the loop should keep running until cancel.
	conn := &fakeBatchConn{prepareErr: errors.New("ch unreachable")}
	provider := func(context.Context) []Session {
		return []Session{makeSession(1)}
	}
	w, _ := NewSessionSnapshotWriter(SessionSnapshotWriterConfig{
		Conn:     conn,
		Provider: provider,
		Interval: 5 * time.Millisecond,
		Hooks:    rh.hooks(),
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	deadline := time.After(2 * time.Second)
	for {
		_, _, errs, _ := rh.snapshot()
		if errs >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for tick errors")
		default:
			time.Sleep(2 * time.Millisecond)
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not exit after cancel")
	}
	success, _, errs, _ := rh.snapshot()
	if success != 0 {
		t.Errorf("insert success during failing run = %d, want 0", success)
	}
	if errs < 2 {
		t.Errorf("insert errors = %d, want >= 2", errs)
	}
}

func TestSessionSnapshotWriter_Run_CancelBeforeFirstTick(t *testing.T) {
	conn := &fakeBatchConn{}
	w, _ := NewSessionSnapshotWriter(SessionSnapshotWriterConfig{
		Conn:     conn,
		Provider: func(context.Context) []Session { return []Session{makeSession(1)} },
		Interval: time.Hour,
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := w.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("Run returned %v, want context.Canceled", err)
	}
	if conn.calls != 0 {
		t.Errorf("PrepareBatch called %d times before any tick", conn.calls)
	}
}
