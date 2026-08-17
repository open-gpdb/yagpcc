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
	"errors"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/gp"
	"github.com/open-gpdb/yagpcc/internal/sink/clickhouse"
	"github.com/open-gpdb/yagpcc/internal/storage"
)

// fakeBatch records appended rows and lets a test inject Append/Send failures.
type fakeBatch struct {
	appendErr error
	sendErr   error
	appended  [][]any
	sent      bool
	aborted   bool
}

func (b *fakeBatch) Abort() error { b.aborted = true; return nil }
func (b *fakeBatch) Append(v ...any) error {
	if b.appendErr != nil {
		return b.appendErr
	}
	b.appended = append(b.appended, v)
	return nil
}
func (b *fakeBatch) AppendStruct(any) error        { return nil }
func (b *fakeBatch) Column(int) driver.BatchColumn { return nil }
func (b *fakeBatch) Flush() error                  { return nil }
func (b *fakeBatch) Send() error {
	if b.sendErr != nil {
		return b.sendErr
	}
	b.sent = true
	return nil
}
func (b *fakeBatch) IsSent() bool                { return b.sent }
func (b *fakeBatch) Rows() int                   { return len(b.appended) }
func (b *fakeBatch) Columns() []column.Interface { return nil }
func (b *fakeBatch) Close() error                { return nil }

// fakeConn hands out a prepared fakeBatch and records the last INSERT header.
type fakeConn struct {
	prepareErr error
	batch      *fakeBatch
	prepared   int
	lastQuery  string
}

func (c *fakeConn) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.prepared++
	c.lastQuery = query
	if c.prepareErr != nil {
		return nil, c.prepareErr
	}
	return c.batch, nil
}

func testLogger() *zap.SugaredLogger { return zap.NewNop().Sugar() }

func sampleSession() *gp.SessionDataWrite {
	return &gp.SessionDataWrite{
		ClusterID:    "c1",
		Hostname:     "h1",
		GpStatInfo:   &gp.GpStatActivity{SessID: 123, TmID: 1},
		RunningQuery: &storage.QueryKeyWrite{Ssid: 123, Ccnt: 1},
	}
}

func sampleQuery() *pbm.QueryStatWrite {
	return &pbm.QueryStatWrite{
		ClusterId: "c1",
		Hostname:  "h1",
		QueryKey:  &pbc.QueryKey{Ssid: 1, Ccnt: 2},
		QueryInfo: &pbc.QueryInfo{QueryId: 12, PlanId: 34},
	}
}

func sampleSegment() *pbm.SegmentMetricsWrite {
	return &pbm.SegmentMetricsWrite{
		ClusterId:  "c1",
		Hostname:   "h1",
		QueryKey:   &pbc.QueryKey{Ssid: 1, Ccnt: 2},
		SegmentKey: &pbc.SegmentKey{Dbid: 2, Segindex: 4},
	}
}

func TestClickHouseWriters_StoreSessions_Success(t *testing.T) {
	batch := &fakeBatch{}
	conn := &fakeConn{batch: batch}
	w := NewClickHouseWriters(testLogger(), conn, "yagpcc", nil)

	err := w.StoreSessions(context.Background(), []*gp.SessionDataWrite{sampleSession(), sampleSession()})
	require.NoError(t, err)
	assert.True(t, batch.sent)
	assert.Len(t, batch.appended, 2)
	assert.Contains(t, conn.lastQuery, "INSERT INTO yagpcc.sessions_part")
	assert.True(t, strings.Contains(conn.lastQuery, "_timestamp"))
}

func TestClickHouseWriters_StoreQuery_Success(t *testing.T) {
	batch := &fakeBatch{}
	conn := &fakeConn{batch: batch}
	w := NewClickHouseWriters(testLogger(), conn, "yagpcc", nil)

	err := w.StoreQuery(context.Background(), []*pbm.QueryStatWrite{sampleQuery()})
	require.NoError(t, err)
	assert.True(t, batch.sent)
	assert.Len(t, batch.appended, 1)
	assert.Contains(t, conn.lastQuery, "INSERT INTO yagpcc.statements_part")
}

func TestClickHouseWriters_StoreSegments_Success(t *testing.T) {
	batch := &fakeBatch{}
	conn := &fakeConn{batch: batch}
	w := NewClickHouseWriters(testLogger(), conn, "yagpcc", nil)

	err := w.StoreSegmensMetrics(context.Background(), []*pbm.SegmentMetricsWrite{sampleSegment()})
	require.NoError(t, err)
	assert.True(t, batch.sent)
	assert.Len(t, batch.appended, 1)
	assert.Contains(t, conn.lastQuery, "INSERT INTO yagpcc.segments_part")
}

func TestClickHouseWriters_NoDatabaseQualifier(t *testing.T) {
	batch := &fakeBatch{}
	conn := &fakeConn{batch: batch}
	w := NewClickHouseWriters(testLogger(), conn, "", nil)

	require.NoError(t, w.StoreQuery(context.Background(), []*pbm.QueryStatWrite{sampleQuery()}))
	assert.Contains(t, conn.lastQuery, "INSERT INTO statements_part")
	assert.NotContains(t, conn.lastQuery, "yagpcc.")
}

func TestClickHouseWriters_EmptyBatchNoOp(t *testing.T) {
	conn := &fakeConn{batch: &fakeBatch{}}
	w := NewClickHouseWriters(testLogger(), conn, "yagpcc", nil)

	require.NoError(t, w.StoreSessions(context.Background(), nil))
	require.NoError(t, w.StoreQuery(context.Background(), []*pbm.QueryStatWrite{}))
	require.NoError(t, w.StoreSegmensMetrics(context.Background(), nil))
	assert.Equal(t, 0, conn.prepared, "no batch should be prepared for an empty input")
}

func TestClickHouseWriters_SendErrorPropagates(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := clickhouse.NewMetrics(reg)
	batch := &fakeBatch{sendErr: errors.New("boom")}
	conn := &fakeConn{batch: batch}
	w := NewClickHouseWriters(testLogger(), conn, "yagpcc", m)

	err := w.StoreQuery(context.Background(), []*pbm.QueryStatWrite{sampleQuery()})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
	assert.False(t, batch.sent)

	// One row was dropped with reason insert_error, and the error insert was counted.
	assert.Equal(t, float64(1), testutil.ToFloat64(m.DroppedRows.WithLabelValues(clickhouse.TableStatements, clickhouse.DropReasonInsertError)))
	assert.Equal(t, float64(1), testutil.ToFloat64(m.Inserts.WithLabelValues(clickhouse.TableStatements, clickhouse.InsertStatusError)))
}

func TestClickHouseWriters_PrepareErrorPropagates(t *testing.T) {
	conn := &fakeConn{prepareErr: errors.New("no conn")}
	w := NewClickHouseWriters(testLogger(), conn, "yagpcc", nil)

	err := w.StoreSessions(context.Background(), []*gp.SessionDataWrite{sampleSession()})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no conn")
}

func TestClickHouseWriters_AppendErrorAbortsBatch(t *testing.T) {
	batch := &fakeBatch{appendErr: errors.New("bad row")}
	conn := &fakeConn{batch: batch}
	w := NewClickHouseWriters(testLogger(), conn, "yagpcc", nil)

	err := w.StoreQuery(context.Background(), []*pbm.QueryStatWrite{sampleQuery()})
	require.Error(t, err)
	assert.True(t, batch.aborted)
	assert.False(t, batch.sent)
}

func TestClickHouseWriters_ContextCancelled(t *testing.T) {
	conn := &fakeConn{batch: &fakeBatch{}}
	w := NewClickHouseWriters(testLogger(), conn, "yagpcc", nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := w.StoreSessions(ctx, []*gp.SessionDataWrite{sampleSession()})
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 0, conn.prepared, "cancelled ctx must not prepare a batch")
}

func TestClickHouseWriters_SuccessMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := clickhouse.NewMetrics(reg)
	conn := &fakeConn{batch: &fakeBatch{}}
	w := NewClickHouseWriters(testLogger(), conn, "yagpcc", m)

	require.NoError(t, w.StoreSegmensMetrics(context.Background(), []*pbm.SegmentMetricsWrite{sampleSegment()}))
	assert.Equal(t, float64(1), testutil.ToFloat64(m.Inserts.WithLabelValues(clickhouse.TableSegments, clickhouse.InsertStatusSuccess)))
}

// Static assertion that ClickHouseWriters satisfies the ArchiveWriter contract.
var _ ArchiveWriter = (*ClickHouseWriters)(nil)
