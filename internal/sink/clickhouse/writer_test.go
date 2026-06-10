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
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/config"
)

// fakeOrchestratorConn implements orchestratorConn for writer_test.go. It
// reuses the migration_runner_test.go fake's response queue style: tests pre-
// program a list of selectResponses and any number of fakeBatch results from
// PrepareBatch; Exec calls are recorded so assertions can match on rendered
// DDL fragments.
type fakeOrchestratorConn struct {
	mu sync.Mutex

	pingErr  error
	closeErr error

	execLog   []execCall
	execErrAt map[int]error

	selectResponses []selectResponse
	selectIdx       int

	prepareErr error
	batches    []*fakeBatch

	pingCount  int32
	closeCount int32
}

func (f *fakeOrchestratorConn) Ping(_ context.Context) error {
	atomic.AddInt32(&f.pingCount, 1)
	return f.pingErr
}

func (f *fakeOrchestratorConn) Close() error {
	atomic.AddInt32(&f.closeCount, 1)
	return f.closeErr
}

func (f *fakeOrchestratorConn) Exec(_ context.Context, query string, args ...any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	idx := len(f.execLog)
	f.execLog = append(f.execLog, execCall{query: query, args: append([]any(nil), args...)})
	if err, ok := f.execErrAt[idx]; ok {
		return err
	}
	return nil
}

func (f *fakeOrchestratorConn) Select(_ context.Context, dest any, _ string, _ ...any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.selectIdx >= len(f.selectResponses) {
		return errors.New("fakeOrchestratorConn: no select response queued")
	}
	resp := f.selectResponses[f.selectIdx]
	f.selectIdx++
	if resp.err != nil {
		return resp.err
	}
	dv := reflect.ValueOf(dest)
	if dv.Kind() != reflect.Ptr || dv.Elem().Kind() != reflect.Slice {
		return errors.New("fakeOrchestratorConn: dest must be pointer to slice")
	}
	slice := dv.Elem()
	elemType := slice.Type().Elem()
	out := reflect.MakeSlice(slice.Type(), 0, len(resp.rows))
	for _, r := range resp.rows {
		rv := reflect.ValueOf(r)
		if !rv.Type().ConvertibleTo(elemType) {
			return errors.New("fakeOrchestratorConn: row type does not match dest slice element")
		}
		out = reflect.Append(out, rv.Convert(elemType))
	}
	slice.Set(out)
	return nil
}

func (f *fakeOrchestratorConn) PrepareBatch(_ context.Context, _ string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.prepareErr != nil {
		return nil, f.prepareErr
	}
	if len(f.batches) == 0 {
		return &fakeBatch{}, nil
	}
	b := f.batches[0]
	f.batches = f.batches[1:]
	return b, nil
}

// baseEnabledConfig builds a fully populated ClickhouseConfig with reasonable
// defaults so individual tests only override the fields they care about.
func baseEnabledConfig() *config.ClickhouseConfig {
	c := config.DefaultClickhouseConfig()
	c.Enabled = true
	c.Addrs = []string{"127.0.0.1:9000"}
	c.Password = "x"
	c.SchemaManagement = config.SchemaManagementAuto
	// Short flush interval keeps timing-sensitive tests fast.
	c.FlushInterval = 5 * time.Millisecond
	c.SessionSnapshotIntervalSec = 1
	c.MinDurationMs = 0
	return &c
}

// stubConn returns a fakeOrchestratorConn pre-programmed for the auto path
// against a fresh database (no _yagpcc_meta yet).
func stubConn() *fakeOrchestratorConn {
	return &fakeOrchestratorConn{
		selectResponses: []selectResponse{metaExistsResp(false)},
	}
}

func factoryReturning(c *fakeOrchestratorConn) connFactory {
	return func(_ context.Context, _ *config.ClickhouseConfig) (orchestratorConn, error) {
		return c, nil
	}
}

func TestNew_Disabled_NoConnect(t *testing.T) {
	cfg := config.DefaultClickhouseConfig()
	cfg.Enabled = false

	called := false
	factory := func(context.Context, *config.ClickhouseConfig) (orchestratorConn, error) {
		called = true
		return stubConn(), nil
	}
	w, err := newWithFactory(context.Background(), &cfg, factory, Deps{}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("newWithFactory: %v", err)
	}
	if called {
		t.Error("factory must not be called when Enabled=false")
	}
	if w.IsEnabled() {
		t.Error("IsEnabled must be false when cfg.Enabled=false")
	}
	// Submit / FlushAggregates must be no-ops.
	w.Submit(nil)
	if n, err := w.FlushAggregates(context.Background(), []AggregatedBucket{{}}); err != nil || n != 0 {
		t.Errorf("FlushAggregates on disabled writer = (%d,%v), want (0,nil)", n, err)
	}
}

func TestNew_NilCfg(t *testing.T) {
	_, err := newWithFactory(context.Background(), nil, factoryReturning(stubConn()), Deps{}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected error for nil cfg")
	}
}

func TestNew_AutoSchema_BuildsWritersAndPings(t *testing.T) {
	cfg := baseEnabledConfig()
	conn := stubConn()
	reg := prometheus.NewRegistry()

	w, err := newWithFactory(context.Background(), cfg, factoryReturning(conn), Deps{
		Registerer:    reg,
		Sessions:      func(context.Context) []Session { return nil },
		YagpccVersion: "1.0.0",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("newWithFactory: %v", err)
	}
	if !w.IsEnabled() {
		t.Fatal("expected enabled writer")
	}
	if atomic.LoadInt32(&conn.pingCount) != 1 {
		t.Errorf("expected exactly one Ping, got %d", conn.pingCount)
	}
	if w.events == nil || w.agg == nil || w.sess == nil {
		t.Errorf("expected all three writers; got events=%v agg=%v sess=%v",
			w.events != nil, w.agg != nil, w.sess != nil)
	}
	// Auto path must apply migrations: at least one CREATE TABLE for query_events
	// must show up in the exec log.
	if findExec(conn.execLog, "yagpcc.query_events") < 0 {
		t.Error("auto schema management did not run migrations")
	}
	if got := testutil.ToFloat64(w.metrics.SchemaMismatch); got != 0 {
		t.Errorf("schema_mismatch gauge = %v, want 0", got)
	}
	_ = w.Close()
}

func TestNew_AutoSchema_NoSessionsProvider(t *testing.T) {
	cfg := baseEnabledConfig()
	conn := stubConn()

	w, err := newWithFactory(context.Background(), cfg, factoryReturning(conn), Deps{}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("newWithFactory: %v", err)
	}
	if w.sess != nil {
		t.Error("session writer must not be built when Sessions provider is nil")
	}
	if w.events == nil || w.agg == nil {
		t.Error("events and aggregated writers must still be built")
	}
}

func TestNew_AutoSchema_SinksDisabled(t *testing.T) {
	cfg := baseEnabledConfig()
	cfg.Sinks.QueryEvents = false
	cfg.Sinks.AggregatedMetrics = false
	cfg.Sinks.SessionSnapshots = false
	conn := stubConn()

	w, err := newWithFactory(context.Background(), cfg, factoryReturning(conn), Deps{}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("newWithFactory: %v", err)
	}
	// Schema management ran (auto), so connection is alive, but no writers
	// were built — IsEnabled must report false because there is no work to do.
	if w.IsEnabled() {
		t.Error("IsEnabled must be false when every sink is disabled")
	}
}

func TestNew_PingFailure_ReturnsErrorAndClosesConn(t *testing.T) {
	cfg := baseEnabledConfig()
	conn := stubConn()
	conn.pingErr = errors.New("dial refused")

	reg := prometheus.NewRegistry()
	_, err := newWithFactory(context.Background(), cfg, factoryReturning(conn), Deps{Registerer: reg}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected error from ping failure")
	}
	if atomic.LoadInt32(&conn.closeCount) != 1 {
		t.Errorf("expected Close after ping failure, got count=%d", conn.closeCount)
	}
}

func TestNew_FactoryError(t *testing.T) {
	cfg := baseEnabledConfig()
	wantErr := errors.New("dial: nope")
	factory := func(context.Context, *config.ClickhouseConfig) (orchestratorConn, error) {
		return nil, wantErr
	}
	_, err := newWithFactory(context.Background(), cfg, factory, Deps{}, &bytes.Buffer{})
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped %v, got %v", wantErr, err)
	}
}

func TestNew_VerifyOnly_Match(t *testing.T) {
	cfg := baseEnabledConfig()
	cfg.SchemaManagement = config.SchemaManagementVerifyOnly
	conn := &fakeOrchestratorConn{
		selectResponses: []selectResponse{
			metaExistsResp(true),
			maxVersionResp(int32(ExpectedSchemaVersion)),
		},
	}
	reg := prometheus.NewRegistry()
	w, err := newWithFactory(context.Background(), cfg, factoryReturning(conn), Deps{Registerer: reg}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("newWithFactory: %v", err)
	}
	if !w.IsEnabled() {
		t.Fatal("expected enabled writer when verify_only matches")
	}
	if got := testutil.ToFloat64(w.metrics.SchemaMismatch); got != 0 {
		t.Errorf("schema_mismatch gauge = %v, want 0", got)
	}
}

func TestNew_VerifyOnly_Mismatch_FailsSoft(t *testing.T) {
	cfg := baseEnabledConfig()
	cfg.SchemaManagement = config.SchemaManagementVerifyOnly
	// Report a current version that's lower than the binary expects → upgrade
	// required → soft-fail.
	conn := &fakeOrchestratorConn{
		selectResponses: []selectResponse{
			metaExistsResp(true),
			maxVersionResp(int32(ExpectedSchemaVersion - 1)),
		},
	}
	reg := prometheus.NewRegistry()
	w, err := newWithFactory(context.Background(), cfg, factoryReturning(conn), Deps{Registerer: reg}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("verify_only mismatch must not return an error, got: %v", err)
	}
	if w.IsEnabled() {
		t.Error("IsEnabled must be false after verify_only mismatch")
	}
	if got := testutil.ToFloat64(w.metrics.SchemaMismatch); got != 1 {
		t.Errorf("schema_mismatch gauge = %v, want 1", got)
	}
	// Connection must have been released on the soft-fail path.
	if atomic.LoadInt32(&conn.closeCount) != 1 {
		t.Errorf("expected Close on soft-fail path, got count=%d", conn.closeCount)
	}
	// Submit / FlushAggregates must remain no-ops.
	w.Submit(nil)
	if n, _ := w.FlushAggregates(context.Background(), []AggregatedBucket{{}}); n != 0 {
		t.Errorf("FlushAggregates on disabled writer = %d, want 0", n)
	}
}

func TestNew_DumpOnly_PrintsSchemaAndDisables(t *testing.T) {
	cfg := baseEnabledConfig()
	cfg.SchemaManagement = config.SchemaManagementDumpOnly
	stdout := &bytes.Buffer{}
	called := false
	factory := func(context.Context, *config.ClickhouseConfig) (orchestratorConn, error) {
		called = true
		return stubConn(), nil
	}

	w, err := newWithFactory(context.Background(), cfg, factory, Deps{}, stdout)
	if err != nil {
		t.Fatalf("dump_only: %v", err)
	}
	if called {
		t.Error("dump_only must not open a ClickHouse connection")
	}
	if w.IsEnabled() {
		t.Error("dump_only must leave the writer disabled")
	}
	if !strings.Contains(stdout.String(), "yagpcc.query_events") {
		t.Errorf("dump output missing query_events DDL; got %q", stdout.String())
	}
}

func TestStopped_ClosedAfterRunReturns(t *testing.T) {
	cfg := config.DefaultClickhouseConfig()
	cfg.Enabled = false
	w, err := newWithFactory(context.Background(), &cfg, factoryReturning(stubConn()), Deps{}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("newWithFactory: %v", err)
	}
	// Before Run, Stopped() must not be closed.
	select {
	case <-w.Stopped():
		t.Fatal("Stopped channel must not be closed before Run")
	default:
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already-cancelled: Run returns immediately for disabled writer.
	if err := w.Run(ctx); err == nil {
		t.Fatal("Run must propagate ctx error")
	}
	select {
	case <-w.Stopped():
	case <-time.After(time.Second):
		t.Fatal("Stopped channel was not closed after Run returned")
	}
}

func TestStopped_NilReceiver(t *testing.T) {
	var w *ClickhouseWriter
	select {
	case <-w.Stopped():
	default:
		t.Fatal("nil-receiver Stopped channel must already be closed")
	}
}

func TestNew_UnknownSchemaManagement(t *testing.T) {
	cfg := baseEnabledConfig()
	cfg.SchemaManagement = "bogus"
	conn := stubConn()

	_, err := newWithFactory(context.Background(), cfg, factoryReturning(conn), Deps{}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected error for unknown schema_management")
	}
	if !strings.Contains(err.Error(), "bogus") {
		t.Errorf("error %q should mention the unknown mode", err)
	}
	if atomic.LoadInt32(&conn.closeCount) != 1 {
		t.Errorf("connection must be closed on hard schema-management failure, got %d", conn.closeCount)
	}
}

func TestSubmit_DropsBelowMinDuration(t *testing.T) {
	cfg := baseEnabledConfig()
	cfg.MinDurationMs = 100
	conn := stubConn()

	w, err := newWithFactory(context.Background(), cfg, factoryReturning(conn), Deps{}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("newWithFactory: %v", err)
	}
	// Short DONE event must be filtered out by Write.
	short := makeQueryEvent(pbc.QueryStatus_QUERY_STATUS_DONE, 10)
	w.Submit(short)
	if w.events.BufferLen() != 0 {
		t.Errorf("short event should have been filtered, buffer len = %d", w.events.BufferLen())
	}
	long := makeQueryEvent(pbc.QueryStatus_QUERY_STATUS_DONE, 500)
	w.Submit(long)
	if w.events.BufferLen() != 1 {
		t.Errorf("long event should be buffered, len = %d", w.events.BufferLen())
	}
}

func TestFlushAggregates_RoutesToAggregatedWriter(t *testing.T) {
	cfg := baseEnabledConfig()
	cfg.Sinks.QueryEvents = false
	cfg.Sinks.SessionSnapshots = false
	conn := stubConn()
	conn.batches = []*fakeBatch{{}}

	reg := prometheus.NewRegistry()
	w, err := newWithFactory(context.Background(), cfg, factoryReturning(conn), Deps{Registerer: reg}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("newWithFactory: %v", err)
	}

	buckets := []AggregatedBucket{
		{BucketTime: time.Unix(0, 0).UTC(), QueryID: 1, PlanID: 2, User: "u", Database: "d", ResourceGroup: "rg"},
	}
	n, err := w.FlushAggregates(context.Background(), buckets)
	if err != nil {
		t.Fatalf("FlushAggregates: %v", err)
	}
	if n != 1 {
		t.Errorf("rows sent = %d, want 1", n)
	}
}

func TestFlushAggregates_ErrorSetsUnreachable(t *testing.T) {
	cfg := baseEnabledConfig()
	cfg.Sinks.QueryEvents = false
	cfg.Sinks.SessionSnapshots = false
	conn := stubConn()
	conn.prepareErr = errors.New("ch down")

	reg := prometheus.NewRegistry()
	w, err := newWithFactory(context.Background(), cfg, factoryReturning(conn), Deps{Registerer: reg}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("newWithFactory: %v", err)
	}

	buckets := []AggregatedBucket{{BucketTime: time.Unix(0, 0)}}
	_, err = w.FlushAggregates(context.Background(), buckets)
	if err == nil {
		t.Fatal("expected error from FlushAggregates when prepare fails")
	}
	if got := testutil.ToFloat64(w.metrics.Unreachable); got != 1 {
		t.Errorf("unreachable gauge = %v, want 1 after flush failure", got)
	}
}

func TestFlushAggregates_EmptyAndDisabled(t *testing.T) {
	// No-op writer (disabled).
	disabledCfg := config.DefaultClickhouseConfig()
	wDisabled, err := newWithFactory(context.Background(), &disabledCfg, factoryReturning(stubConn()), Deps{}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("disabled newWithFactory: %v", err)
	}
	if n, err := wDisabled.FlushAggregates(context.Background(), nil); err != nil || n != 0 {
		t.Errorf("disabled flush = (%d,%v), want (0,nil)", n, err)
	}

	// Enabled writer with empty buckets.
	cfg := baseEnabledConfig()
	conn := stubConn()
	w, err := newWithFactory(context.Background(), cfg, factoryReturning(conn), Deps{}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("enabled newWithFactory: %v", err)
	}
	if n, err := w.FlushAggregates(context.Background(), nil); err != nil || n != 0 {
		t.Errorf("empty flush = (%d,%v), want (0,nil)", n, err)
	}
}

func TestRun_DisabledReturnsOnContextCancel(t *testing.T) {
	cfg := config.DefaultClickhouseConfig()
	w, err := newWithFactory(context.Background(), &cfg, factoryReturning(stubConn()), Deps{}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("disabled newWithFactory: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	cancel()
	select {
	case got := <-done:
		if !errors.Is(got, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestRun_FlushesEventsOnTickerAndOnShutdown(t *testing.T) {
	cfg := baseEnabledConfig()
	cfg.Sinks.AggregatedMetrics = false
	cfg.Sinks.SessionSnapshots = false
	cfg.FlushInterval = 5 * time.Millisecond

	conn := stubConn()
	// Pre-populate enough fake batches to absorb both the periodic ticks and
	// the final shutdown flush.
	for i := 0; i < 16; i++ {
		conn.batches = append(conn.batches, &fakeBatch{})
	}

	w, err := newWithFactory(context.Background(), cfg, factoryReturning(conn), Deps{}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("newWithFactory: %v", err)
	}

	// Buffer one row before Run starts so the first tick has something to flush.
	w.Submit(makeQueryEvent(pbc.QueryStatus_QUERY_STATUS_DONE, 500))
	if w.events.BufferLen() != 1 {
		t.Fatalf("expected 1 buffered event, got %d", w.events.BufferLen())
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// Wait until the flusher drains the row.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if w.events.BufferLen() == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("periodic flusher did not drain the buffered event")
		}
		time.Sleep(2 * time.Millisecond)
	}

	// Buffer another row, then cancel to exercise the final-flush code path.
	w.Submit(makeQueryEvent(pbc.QueryStatus_QUERY_STATUS_DONE, 600))
	cancel()
	select {
	case got := <-done:
		if !errors.Is(got, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if w.events.BufferLen() != 0 {
		t.Errorf("final flush left %d buffered events", w.events.BufferLen())
	}
}

// TestRun_FinalFlushDrainsMultipleBatches verifies the shutdown drain loop
// keeps calling Flush until the buffer is empty, even when more than
// BatchSize rows accumulated during a CH outage. The original implementation
// called Flush once and silently dropped the tail.
func TestRun_FinalFlushDrainsMultipleBatches(t *testing.T) {
	cfg := baseEnabledConfig()
	cfg.Sinks.AggregatedMetrics = false
	cfg.Sinks.SessionSnapshots = false
	// Disable the periodic flusher so all rows are still queued at shutdown.
	cfg.FlushInterval = 0
	cfg.BatchSize = 4
	cfg.BufferMaxRows = 32

	conn := stubConn()
	for i := 0; i < 16; i++ {
		conn.batches = append(conn.batches, &fakeBatch{})
	}

	w, err := newWithFactory(context.Background(), cfg, factoryReturning(conn), Deps{}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("newWithFactory: %v", err)
	}

	// Queue 3*BatchSize rows so a single Flush can't drain them all.
	const queued = 12
	for i := 0; i < queued; i++ {
		w.Submit(makeQueryEvent(pbc.QueryStatus_QUERY_STATUS_DONE, 500))
	}
	if w.events.BufferLen() != queued {
		t.Fatalf("expected %d buffered events, got %d", queued, w.events.BufferLen())
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if remaining := w.events.BufferLen(); remaining != 0 {
		t.Errorf("final flush left %d buffered events; expected 0", remaining)
	}
}

func TestRun_FlushErrorMarksUnreachable(t *testing.T) {
	cfg := baseEnabledConfig()
	cfg.Sinks.AggregatedMetrics = false
	cfg.Sinks.SessionSnapshots = false
	cfg.FlushInterval = 5 * time.Millisecond

	conn := stubConn()
	// Make every PrepareBatch fail so the flusher always errors.
	conn.prepareErr = errors.New("ch down")

	reg := prometheus.NewRegistry()
	w, err := newWithFactory(context.Background(), cfg, factoryReturning(conn), Deps{Registerer: reg}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("newWithFactory: %v", err)
	}
	w.Submit(makeQueryEvent(pbc.QueryStatus_QUERY_STATUS_DONE, 500))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	// Wait for the flusher to attempt at least one tick.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if testutil.ToFloat64(w.metrics.Unreachable) == 1 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Errorf("unreachable gauge never went to 1; got %v", testutil.ToFloat64(w.metrics.Unreachable))
}

func TestRun_DrivesSessionsScheduler(t *testing.T) {
	cfg := baseEnabledConfig()
	cfg.Sinks.QueryEvents = false
	cfg.Sinks.AggregatedMetrics = false
	cfg.SessionSnapshotIntervalSec = 1 // ticker uses time.Duration; we'll observe through Provider calls

	conn := stubConn()
	for i := 0; i < 8; i++ {
		conn.batches = append(conn.batches, &fakeBatch{})
	}
	var calls int32
	provider := func(context.Context) []Session {
		atomic.AddInt32(&calls, 1)
		return nil // empty → no INSERT, but Provider was called → counts as a tick
	}
	w, err := newWithFactory(context.Background(), cfg, factoryReturning(conn), Deps{Sessions: provider}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("newWithFactory: %v", err)
	}
	if w.sess == nil {
		t.Fatal("session writer should be built when Sessions provider is non-nil")
	}

	// Use a much shorter interval through reflection-free path: replace the
	// session writer with one that ticks every 5ms so the test stays fast.
	short, err := NewSessionSnapshotWriter(SessionSnapshotWriterConfig{
		Conn:     conn,
		Interval: 5 * time.Millisecond,
		Provider: provider,
	})
	if err != nil {
		t.Fatalf("NewSessionSnapshotWriter: %v", err)
	}
	w.sess = short

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// Wait for the provider to be called at least twice (i.e. multiple ticks).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&calls) >= 2 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if atomic.LoadInt32(&calls) < 2 {
		t.Errorf("provider called %d times, want >= 2", calls)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit after cancel")
	}
}

func TestClose_IdempotentAndSafeOnNil(t *testing.T) {
	var nilW *ClickhouseWriter
	if err := nilW.Close(); err != nil {
		t.Errorf("Close on nil writer = %v, want nil", err)
	}
	cfg := baseEnabledConfig()
	conn := stubConn()
	w, err := newWithFactory(context.Background(), cfg, factoryReturning(conn), Deps{}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("newWithFactory: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	if got := atomic.LoadInt32(&conn.closeCount); got != 1 {
		t.Errorf("Close was called %d times on the underlying conn, want exactly 1", got)
	}
}

func TestSubmit_NilOrDisabledIsSafe(t *testing.T) {
	var nilW *ClickhouseWriter
	nilW.Submit(nil)

	disabledCfg := config.DefaultClickhouseConfig()
	w, err := newWithFactory(context.Background(), &disabledCfg, factoryReturning(stubConn()), Deps{}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("disabled newWithFactory: %v", err)
	}
	w.Submit(nil) // must not panic
}

func TestIsEnabled_NilReceiverReturnsFalse(t *testing.T) {
	var nilW *ClickhouseWriter
	if nilW.IsEnabled() {
		t.Error("IsEnabled() on nil writer must be false")
	}
}
