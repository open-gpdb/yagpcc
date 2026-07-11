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
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	"github.com/open-gpdb/yagpcc/internal/gp"
	"github.com/open-gpdb/yagpcc/internal/sink/clickhouse"
	"go.uber.org/zap"
)

// chConn is the slice of clickhouse-go/v2 driver.Conn the writer needs. It is an
// interface so tests can supply a fake without a real ClickHouse connection;
// driver.Conn satisfies it.
type chConn interface {
	PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error)
}

// ClickHouseWriters implements ArchiveWriter by inserting each stream into its
// destination table with a single native batch per Store* call
// (PrepareBatch/Append/Send). Rows that fail JSON→column mapping are skipped and
// accounted as mapping_error; a failed batch is returned to the caller so the
// batch pipeline can drop it — no retries or blocking happen here.
type ClickHouseWriters struct {
	logger   *zap.SugaredLogger
	conn     chConn
	database string
	metrics  *clickhouse.Metrics

	sessions   *clickhouse.Mapping
	statements *clickhouse.Mapping
	segments   *clickhouse.Mapping
}

// NewClickHouseWriters builds a ClickHouseWriters over conn. database qualifies
// the destination tables (empty falls back to the connection's default). metrics
// may be nil, in which case metric recording is skipped.
func NewClickHouseWriters(logger *zap.SugaredLogger, conn chConn, database string, metrics *clickhouse.Metrics) *ClickHouseWriters {
	return &ClickHouseWriters{
		logger:     logger,
		conn:       conn,
		database:   database,
		metrics:    metrics,
		sessions:   clickhouse.SessionsMapping(),
		statements: clickhouse.StatementsMapping(),
		segments:   clickhouse.SegmentsMapping(),
	}
}

// StoreSessions inserts a batch of session data into sessions_part.
func (w *ClickHouseWriters) StoreSessions(ctx context.Context, sessions []*gp.SessionDataWrite) error {
	jsons := make([][]byte, 0, len(sessions))
	for _, val := range sessions {
		val.GpStatInfo.TmID = int(gp.DiscoveredTmID)
		val.RunningQuery.Tmid = int32(gp.DiscoveredTmID)

		js, err := val.ToJSON()
		if err != nil {
			w.logger.Errorf("fail to convert sessions data %v with error %v", val, err)
			w.incDropped(w.sessions.Table(), clickhouse.DropReasonMappingError)
			continue
		}
		jsons = append(jsons, js)
	}
	return w.insertBatch(ctx, w.sessions, jsons)
}

// StoreQuery inserts a batch of query statistics into statements_part.
func (w *ClickHouseWriters) StoreQuery(ctx context.Context, queries []*pbm.QueryStatWrite) error {
	jsons := make([][]byte, 0, len(queries))
	for _, val := range queries {
		val.QueryKey.Tmid = int32(gp.DiscoveredTmID)

		serializable := &QueryStatWriteSerializable{v: val}
		js, err := serializable.ToJSON()
		if err != nil {
			w.logger.Errorf("fail to convert query data %v with error %v", val, err)
			w.incDropped(w.statements.Table(), clickhouse.DropReasonMappingError)
			continue
		}
		jsons = append(jsons, js)
	}
	return w.insertBatch(ctx, w.statements, jsons)
}

// StoreSegmensMetrics inserts a batch of segment metrics into segments_part.
// Note: the misspelled name is preserved for backward compatibility.
func (w *ClickHouseWriters) StoreSegmensMetrics(ctx context.Context, metrics []*pbm.SegmentMetricsWrite) error {
	jsons := make([][]byte, 0, len(metrics))
	for _, val := range metrics {
		val.QueryKey.Tmid = int32(gp.DiscoveredTmID)

		serializable := &SegmentMetricsWriteSerializable{v: val}
		js, err := serializable.ToJSON()
		if err != nil {
			w.logger.Errorf("fail to convert segment metrics data %v with error %v", val, err)
			w.incDropped(w.segments.Table(), clickhouse.DropReasonMappingError)
			continue
		}
		jsons = append(jsons, js)
	}
	return w.insertBatch(ctx, w.segments, jsons)
}

// insertBatch maps every JSON row for tm, then inserts the surviving rows in one
// native batch. Mapping failures are skipped (mapping_error); an empty batch is
// a no-op; a PrepareBatch/Send failure drops the whole batch (insert_error) and
// returns the error so the pipeline can account it.
func (w *ClickHouseWriters) insertBatch(ctx context.Context, tm *clickhouse.Mapping, jsons [][]byte) error {
	if len(jsons) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	table := tm.Table()

	ts := time.Now()
	rows := make([][]any, 0, len(jsons))
	for i, js := range jsons {
		meta := clickhouse.CDCMeta{
			Timestamp: ts,
			Partition: "direct",
			Offset:    0,
			Idx:       uint32(i),
		}
		values, err := clickhouse.MapRow(tm, js, meta)
		if err != nil {
			w.logger.Warnf("clickhouse map row for %s failed, dropping row: %v", table, err)
			w.incDropped(table, clickhouse.DropReasonMappingError)
			continue
		}
		rows = append(rows, values)
	}
	if len(rows) == 0 {
		return nil
	}

	start := time.Now()
	batch, err := w.conn.PrepareBatch(ctx, w.insertQuery(tm))
	if err != nil {
		w.observeInsertError(table, start, len(rows))
		return fmt.Errorf("clickhouse prepare batch for %s: %w", table, err)
	}
	for _, values := range rows {
		if err := batch.Append(values...); err != nil {
			_ = batch.Abort()
			w.observeInsertError(table, start, len(rows))
			return fmt.Errorf("clickhouse append to %s: %w", table, err)
		}
	}
	if err := batch.Send(); err != nil {
		w.observeInsertError(table, start, len(rows))
		return fmt.Errorf("clickhouse send %s: %w", table, err)
	}

	if w.metrics != nil {
		w.metrics.ObserveInsert(table, time.Since(start))
	}
	return nil
}

// insertQuery renders the explicit-column INSERT header for tm. Columns are
// listed in ColumnNames order so batch.Append values line up regardless of the
// physical column order in the table.
func (w *ClickHouseWriters) insertQuery(tm *clickhouse.Mapping) string {
	table := tm.Table()
	if w.database != "" {
		table = w.database + "." + table
	}
	cols := tm.ColumnNames()
	return fmt.Sprintf("INSERT INTO %s (%s)", table, strings.Join(cols, ", "))
}

// observeInsertError records a failed batch and its per-row insert_error drops.
func (w *ClickHouseWriters) observeInsertError(table string, start time.Time, rows int) {
	if w.metrics == nil {
		return
	}
	w.metrics.ObserveInsertError(table, time.Since(start))
	for i := 0; i < rows; i++ {
		w.metrics.IncDropped(table, clickhouse.DropReasonInsertError)
	}
}

// incDropped bumps the dropped-rows counter when metrics are configured.
func (w *ClickHouseWriters) incDropped(table, reason string) {
	if w.metrics != nil {
		w.metrics.IncDropped(table, reason)
	}
}
