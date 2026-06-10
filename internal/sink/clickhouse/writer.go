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

// File: writer.go contains the top-level ClickhouseWriter orchestrator that
// owns the connection, per-table writers and lifecycle (Run, Submit,
// FlushAggregates, graceful shutdown).
package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	"github.com/open-gpdb/yagpcc/internal/config"
)

// shutdownFlushTimeout caps the final flush triggered by Run shutdown so a
// stuck CH cannot hold yagpcc-master from exiting indefinitely.
const shutdownFlushTimeout = 30 * time.Second

// orchestratorConn is the slice of driver.Conn the orchestrator uses:
// migrations (Exec/Select), batched INSERTs (PrepareBatch), health
// (Ping) and shutdown (Close). The real *clickhouse.Conn satisfies this
// interface; tests inject a fake to avoid spinning up a real CH server.
type orchestratorConn interface {
	MigrationConn
	batchPreparer
	Ping(ctx context.Context) error
	Close() error
}

// connFactory opens a ClickHouse connection from cfg. Production code wires
// NewClient; tests substitute a fake.
type connFactory func(ctx context.Context, cfg *config.ClickhouseConfig) (orchestratorConn, error)

// defaultConnFactory adapts the production NewClient (which returns
// driver.Conn) to the orchestratorConn interface used internally.
func defaultConnFactory(ctx context.Context, cfg *config.ClickhouseConfig) (orchestratorConn, error) {
	conn, err := NewClient(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// Deps groups the dependencies the orchestrator cannot synthesize on its own.
// All fields are optional; sensible no-op defaults apply (zap.NewNop logger,
// no metrics, no session_snapshots sink).
type Deps struct {
	Logger        *zap.Logger
	Registerer    prometheus.Registerer
	Sessions      SessionsProvider
	YagpccVersion string
}

// ClickhouseWriter is the orchestrator: it owns the conn, per-table writers
// and lifecycle. Construct via New, drive via Run, feed via Submit /
// FlushAggregates. Safe for concurrent calls to Submit / FlushAggregates;
// Run is intended to be called once from a single goroutine.
type ClickhouseWriter struct {
	cfg           *config.ClickhouseConfig
	conn          orchestratorConn
	metrics       *Metrics
	events        *QueryEventWriter
	agg           *AggregatedWriter
	sess          *SessionSnapshotWriter
	logger        *zap.Logger
	stdout        io.Writer
	yagpccVersion string
	stopOnce      sync.Once
	stopped       chan struct{}
}

// New constructs a ClickhouseWriter from cfg + deps.
//
// When cfg.Enabled is false the function returns a no-op writer immediately
// (no connection is opened). When enabled, New:
//  1. opens a connection via NewClient and pings it;
//  2. dispatches on cfg.SchemaManagement:
//     - "auto"        → ApplyMigrations;
//     - "verify_only" → VerifySchema; on mismatch logs, raises the
//     schema_mismatch metric and returns a writer with all sinks disabled
//     (sink OFF, master keeps running);
//     - "dump_only"   → renders the cumulative DDL to stdout and returns a
//     no-op writer; the CLI in Task 15 turns this into an explicit exit.
//  3. constructs the per-table writers requested via cfg.Sinks and the
//     supplied SessionsProvider.
//
// New is also the place where Metrics is created and registered against
// deps.Registerer (when non-nil); pass prometheus.DefaultRegisterer in
// production. Tests typically pass a fresh prometheus.NewRegistry().
func New(ctx context.Context, cfg *config.ClickhouseConfig, deps Deps) (*ClickhouseWriter, error) {
	return newWithFactory(ctx, cfg, defaultConnFactory, deps, os.Stdout)
}

// newWithFactory is the internal constructor reachable from tests. It mirrors
// New but lets the caller plug a fake connection factory and capture the
// dump_only output. Production code calls New, which fills in NewClient and
// os.Stdout.
func newWithFactory(
	ctx context.Context,
	cfg *config.ClickhouseConfig,
	factory connFactory,
	deps Deps,
	stdout io.Writer,
) (*ClickhouseWriter, error) {
	if cfg == nil {
		return nil, errors.New("clickhouse: cfg is nil")
	}
	logger := deps.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	if stdout == nil {
		stdout = os.Stdout
	}

	w := &ClickhouseWriter{
		cfg:           cfg,
		logger:        logger,
		stdout:        stdout,
		yagpccVersion: deps.YagpccVersion,
		stopped:       make(chan struct{}),
	}

	if !cfg.Enabled {
		logger.Info("clickhouse sink disabled")
		return w, nil
	}

	// dump_only is a side-effect-only mode: render the cumulative DDL to
	// stdout and return a no-op writer. Skip opening a connection because
	// the operator has not yet provisioned ClickHouse — that is the whole
	// reason they are inspecting the DDL.
	if cfg.SchemaManagement == config.SchemaManagementDumpOnly {
		dump, err := DumpSchema(DumpOptions{RetentionDays: cfg.RetentionDays})
		if err != nil {
			return nil, fmt.Errorf("clickhouse: dump_only: %w", err)
		}
		if _, err := io.WriteString(w.stdout, dump); err != nil {
			return nil, fmt.Errorf("clickhouse: dump_only: write stdout: %w", err)
		}
		return w, nil
	}

	if deps.Registerer != nil {
		w.metrics = NewMetrics(deps.Registerer)
	}

	conn, err := factory(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: open connection: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		w.setUnreachable(true)
		return nil, fmt.Errorf("clickhouse: ping: %w", err)
	}
	w.setUnreachable(false)
	w.conn = conn

	if err := w.runSchemaManagement(ctx); err != nil {
		// Schema-management failures fall into two camps:
		//   - auto/dump_only: hard failure, propagate to caller (yagpcc fails to
		//     start so operators notice).
		//   - verify_only: we already converted the mismatch into a soft state
		//     inside runSchemaManagement (logs + schema_mismatch metric=1 +
		//     leaves w.conn=nil so writers are not constructed). A non-nil err
		//     here means the verify itself crashed (network, query syntax) —
		//     also a hard failure.
		_ = conn.Close()
		w.conn = nil
		return nil, fmt.Errorf("clickhouse: schema management: %w", err)
	}
	if w.conn == nil {
		// runSchemaManagement chose to disable the sink (verify_only mismatch
		// or dump_only). Return a no-op writer; the connection has already
		// been closed by runSchemaManagement.
		return w, nil
	}

	if err := w.buildWriters(deps); err != nil {
		_ = w.conn.Close()
		w.conn = nil
		return nil, fmt.Errorf("clickhouse: build writers: %w", err)
	}
	logger.Info("clickhouse sink started",
		zap.Strings("addrs", cfg.Addrs),
		zap.String("database", cfg.Database),
		zap.String("schema_management", cfg.SchemaManagement),
		zap.Bool("query_events", w.events != nil),
		zap.Bool("aggregated_metrics", w.agg != nil),
		zap.Bool("session_snapshots", w.sess != nil),
	)
	return w, nil
}

// runSchemaManagement dispatches on cfg.SchemaManagement. It mutates w.conn:
// for verify_only mismatches it closes the connection and sets w.conn = nil
// so the caller treats the writer as disabled. dump_only is handled earlier
// in newWithFactory before the connection is opened.
func (w *ClickhouseWriter) runSchemaManagement(ctx context.Context) error {
	switch w.cfg.SchemaManagement {
	case config.SchemaManagementAuto, "":
		yagpccVersion := w.yagpccVersion
		if yagpccVersion == "" {
			yagpccVersion = "unknown"
		}
		opts := MigrateOptions{
			RetentionDays: w.cfg.RetentionDays,
			YagpccVersion: yagpccVersion,
		}
		if err := ApplyMigrations(ctx, w.conn, opts); err != nil {
			return err
		}
		w.setSchemaMismatch(false)
		return nil

	case config.SchemaManagementVerifyOnly:
		if err := VerifySchema(ctx, w.conn); err != nil {
			if errors.Is(err, ErrSchemaUpgradeRequired) || errors.Is(err, ErrSchemaDowngradeRequired) {
				w.logger.Error("clickhouse schema mismatch — sink disabled, master continues",
					zap.Error(err),
				)
				w.setSchemaMismatch(true)
				_ = w.conn.Close()
				w.conn = nil
				return nil
			}
			return err
		}
		w.setSchemaMismatch(false)
		return nil

	default:
		return fmt.Errorf("unknown schema_management %q", w.cfg.SchemaManagement)
	}
}

// buildWriters constructs the per-table writers selected by cfg.Sinks. It is
// only called when w.conn is non-nil (i.e. the schema is in sync and the
// sink is meant to actually write).
func (w *ClickhouseWriter) buildWriters(deps Deps) error {
	if w.cfg.Sinks.QueryEvents {
		var hooks QueryEventWriterHooks
		if w.metrics != nil {
			hooks = w.metrics.QueryEventHooks()
		}
		overflow, err := ParseOverflowMode(w.cfg.OnBufferOverflow)
		if err != nil {
			return fmt.Errorf("query_events: %w", err)
		}
		ev, err := NewQueryEventWriter(QueryEventWriterConfig{
			Conn:           w.conn,
			BatchSize:      w.cfg.BatchSize,
			BufferCapacity: w.cfg.BufferMaxRows,
			OverflowMode:   overflow,
			MinDuration:    time.Duration(w.cfg.MinDurationMs) * time.Millisecond,
			SchemaVersion:  ExpectedSchemaVersion,
			YagpccVersion:  deps.YagpccVersion,
			Hooks:          hooks,
		})
		if err != nil {
			return fmt.Errorf("query_events: %w", err)
		}
		w.events = ev
	}

	if w.cfg.Sinks.AggregatedMetrics {
		var hooks AggregatedWriterHooks
		if w.metrics != nil {
			hooks = w.metrics.AggregatedHooks()
		}
		ag, err := NewAggregatedWriter(AggregatedWriterConfig{
			Conn:  w.conn,
			Hooks: hooks,
		})
		if err != nil {
			return fmt.Errorf("aggregated_metrics: %w", err)
		}
		w.agg = ag
	}

	if w.cfg.Sinks.SessionSnapshots && deps.Sessions != nil {
		var hooks SessionSnapshotWriterHooks
		if w.metrics != nil {
			hooks = w.metrics.SessionSnapshotHooks()
		}
		ss, err := NewSessionSnapshotWriter(SessionSnapshotWriterConfig{
			Conn:     w.conn,
			Interval: time.Duration(w.cfg.SessionSnapshotIntervalSec) * time.Second,
			Provider: deps.Sessions,
			Hooks:    hooks,
		})
		if err != nil {
			return fmt.Errorf("session_snapshots: %w", err)
		}
		w.sess = ss
	}
	return nil
}

// Run drives the periodic-flush and session-snapshot goroutines until ctx is
// cancelled. Returns ctx.Err() once shutdown is complete.
//
// On shutdown a single final flush of the query_events buffer is attempted
// using a background context bounded by shutdownFlushTimeout, so a partly
// buffered query_events batch is not lost when yagpcc-master receives a
// SIGINT during normal operation. The aggregated and session writers are
// stateless beyond construction, so they need no shutdown work.
//
// Run is a no-op (returns ctx.Err() after Wait) when the writer is disabled
// (cfg.Enabled=false, schema mismatch in verify_only, or dump_only).
func (w *ClickhouseWriter) Run(ctx context.Context) error {
	if !w.IsEnabled() {
		<-ctx.Done()
		w.markStopped()
		return ctx.Err()
	}

	var wg sync.WaitGroup
	if w.events != nil && w.cfg.FlushInterval > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.runEventsFlusher(ctx)
		}()
	}
	if w.sess != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = w.sess.Run(ctx)
		}()
	}

	<-ctx.Done()
	// Close the events buffer before waiting on the flusher so any goroutine
	// blocked on a full OverflowBlock buffer wakes up and unwinds.
	if w.events != nil {
		w.events.CloseBuffer()
	}
	wg.Wait()
	w.finalFlush()
	w.markStopped()
	return ctx.Err()
}

// runEventsFlusher drives QueryEventWriter.Flush on a fixed cadence. Errors
// are logged at warn level and the unreachable gauge is set to mirror the
// outcome of the last attempt; the loop never exits on error so a transient
// CH outage does not cripple the sink.
func (w *ClickhouseWriter) runEventsFlusher(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := w.events.Flush(ctx)
			if err != nil {
				w.logger.Warn("clickhouse query_events flush failed",
					zap.Int("rows_drained", n),
					zap.Error(err),
				)
				w.setUnreachable(true)
				continue
			}
			w.setUnreachable(false)
		}
	}
}

// finalFlush attempts a best-effort drain of the query_events buffer with a
// bounded background context. Called from Run during graceful shutdown.
// Loops Flush until the buffer is empty, the timeout elapses, or a batch
// fails — a single Flush call only drains BatchSize rows, so a buffer that
// accumulated >BatchSize entries during a CH outage would otherwise lose
// the tail when Close runs and the connection drops. Errors are logged but
// otherwise ignored — by this point ctx is already cancelled, so the caller
// has nothing to do with them.
func (w *ClickhouseWriter) finalFlush() {
	if w.events == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), shutdownFlushTimeout)
	defer cancel()
	total := 0
	for ctx.Err() == nil {
		n, err := w.events.Flush(ctx)
		if err != nil {
			w.logger.Warn("clickhouse final flush failed",
				zap.Int("rows_drained", n),
				zap.Int("rows_total", total),
				zap.Int("rows_remaining", w.events.BufferLen()),
				zap.Error(err),
			)
			return
		}
		if n == 0 {
			break
		}
		total += n
	}
	if total > 0 {
		w.logger.Info("clickhouse final flush ok",
			zap.Int("rows", total),
			zap.Int("rows_remaining", w.events.BufferLen()),
		)
	}
}

// Submit is the entry point from master/background.go. It forwards qT into
// the query_events writer when it is configured. Cheap and non-blocking
// (Write returns after the buffer Append, which only blocks under
// "block" overflow mode).
func (w *ClickhouseWriter) Submit(qT *pbm.TotalQueryData) {
	if w == nil || w.events == nil {
		return
	}
	w.events.Write(qT)
}

// FlushAggregates is the entry point from the AggregatedStorage periodic
// flush. It batches the supplied buckets through AggregatedWriter and
// returns the number of rows successfully sent. Errors are logged and the
// unreachable gauge is updated accordingly so a transient CH outage shows
// up in alerts; the caller is not expected to retry.
func (w *ClickhouseWriter) FlushAggregates(ctx context.Context, buckets []AggregatedBucket) (int, error) {
	if w == nil || w.agg == nil || len(buckets) == 0 {
		return 0, nil
	}
	n, err := w.agg.FlushBuckets(ctx, buckets)
	if err != nil {
		w.logger.Warn("clickhouse aggregated_metrics flush failed",
			zap.Int("rows_sent", n),
			zap.Int("buckets", len(buckets)),
			zap.Error(err),
		)
		w.setUnreachable(true)
		return n, err
	}
	w.setUnreachable(false)
	return n, nil
}

// IsEnabled returns true when the writer was constructed with a live
// connection and at least one per-table writer. The flag is the union of
// cfg.Enabled and the schema-management outcome: a writer constructed with
// cfg.Enabled=true but schema_management=verify_only and a mismatched
// schema is reported as disabled (sink OFF) by IsEnabled.
func (w *ClickhouseWriter) IsEnabled() bool {
	if w == nil {
		return false
	}
	return w.conn != nil && (w.events != nil || w.agg != nil || w.sess != nil)
}

// Close releases the ClickHouse connection. Safe to call after Run has
// returned; calling it before Run finishes is a programming error (the
// flusher goroutine still holds a reference). Idempotent.
func (w *ClickhouseWriter) Close() error {
	if w == nil || w.conn == nil {
		return nil
	}
	err := w.conn.Close()
	w.conn = nil
	return err
}

// Stopped returns a channel that is closed once Run has finished its final
// flush and released its goroutines. Callers that defer Close should wait on
// this channel first to avoid racing the flusher goroutine that still holds
// a connection reference. The channel is also closed for disabled writers
// (Run returns immediately) and after Run is invoked with an already-cancelled
// ctx.
func (w *ClickhouseWriter) Stopped() <-chan struct{} {
	if w == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return w.stopped
}

// markStopped signals shutdown completion. Used by tests that want to
// observe the moment Run returns without depending on goroutine timing.
func (w *ClickhouseWriter) markStopped() {
	w.stopOnce.Do(func() { close(w.stopped) })
}

func (w *ClickhouseWriter) setSchemaMismatch(v bool) {
	if w.metrics != nil {
		w.metrics.SetSchemaMismatch(v)
	}
}

func (w *ClickhouseWriter) setUnreachable(v bool) {
	if w.metrics != nil {
		w.metrics.SetUnreachable(v)
	}
}

// Compile-time assertion: a real driver.Conn satisfies orchestratorConn.
// Catches accidental drift in the embedded interface set.
var _ orchestratorConn = (driver.Conn)(nil)
