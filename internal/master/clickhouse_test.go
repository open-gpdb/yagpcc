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

package master_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/gp"
	"github.com/open-gpdb/yagpcc/internal/master"
	"github.com/open-gpdb/yagpcc/internal/sink/clickhouse"
	"github.com/open-gpdb/yagpcc/internal/storage"
)

// fakeClickhouseSink records every Submit / FlushAggregates call so tests can
// verify ArchiveOrAggregate's CH-side wiring without spinning up a real
// connection. Concurrent-safe so the test goroutine and ArchiveOrAggregate
// goroutine can race on it freely.
type fakeClickhouseSink struct {
	mu       sync.Mutex
	submits  []*pbm.TotalQueryData
	flushes  [][]clickhouse.AggregatedBucket
	flushErr error
}

func (f *fakeClickhouseSink) Submit(qT *pbm.TotalQueryData) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.submits = append(f.submits, qT)
}

func (f *fakeClickhouseSink) FlushAggregates(_ context.Context, buckets []clickhouse.AggregatedBucket) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]clickhouse.AggregatedBucket, len(buckets))
	copy(cp, buckets)
	f.flushes = append(f.flushes, cp)
	if f.flushErr != nil {
		return 0, f.flushErr
	}
	return len(buckets), nil
}

func (f *fakeClickhouseSink) submitsLen() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.submits)
}

func (f *fakeClickhouseSink) flushesCopy() [][]clickhouse.AggregatedBucket {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]clickhouse.AggregatedBucket, len(f.flushes))
	copy(out, f.flushes)
	return out
}

func newTestNopLogger() *zap.SugaredLogger {
	return zap.NewNop().Sugar()
}

// runArchiveOrAggregateOnce feeds a single EndedQuery through ArchiveOrAggregate
// with the supplied sink (may be nil), drains queryChan briefly, then cancels
// the worker. Returns the number of JSON-archive writes observed.
func runArchiveOrAggregateOnce(t *testing.T, fake master.ClickhouseSink) int {
	t.Helper()
	z := newTestNopLogger()
	rqStorage := storage.NewRunningQueriesStorage()
	sessStorage := gp.NewSessionsStorage(z, rqStorage, nil)
	aggStorage := storage.NewAggregatedStorage(z)
	bs := master.NewBackgroundStorage(z, sessStorage, rqStorage, aggStorage, nil, nil)
	if fake != nil {
		bs.SetClickhouseWriter(fake)
	}

	qKey := &storage.QueryKey{Ssid: 11, Ccnt: 1}
	qVal := &storage.RunningQuery{
		Completed:   true,
		QueriesData: storage.QueryMap{},
		NestedLevel: -1,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	archChan := make(chan *master.EndedQuery, 1)
	queryChan := make(chan *pbm.QueryStatWrite, 4)
	segChan := make(chan *pbm.SegmentMetricsWrite, 16)

	done := make(chan struct{})
	go func() {
		bs.ArchiveOrAggregate(ctx, time.Hour, "test-cluster", archChan, queryChan, segChan, "host")
		close(done)
	}()

	archChan <- &master.EndedQuery{QKey: qKey, QVal: qVal}

	deadline := time.After(800 * time.Millisecond)
	jsonWrites := 0
collect:
	for {
		select {
		case <-queryChan:
			jsonWrites++
		case <-segChan:
		case <-time.After(150 * time.Millisecond):
			break collect
		case <-deadline:
			break collect
		}
	}

	cancel()
	<-done
	return jsonWrites
}

func TestArchiveOrAggregate_SubmitsToClickhouseSink(t *testing.T) {
	fake := &fakeClickhouseSink{}

	_ = runArchiveOrAggregateOnce(t, fake)

	assert.Equal(t, 1, fake.submitsLen(), "ArchiveOrAggregate must forward exactly one record to the CH sink")
}

func TestArchiveOrAggregate_NilSinkDoesNotPanic(t *testing.T) {
	// no sink wired — ArchiveOrAggregate must continue without panicking.
	_ = runArchiveOrAggregateOnce(t, nil)
}

func TestAggregatedSnapshotHook_ConvertsAndForwardsToCH(t *testing.T) {
	z := newTestNopLogger()
	rqStorage := storage.NewRunningQueriesStorage()
	sessStorage := gp.NewSessionsStorage(z, rqStorage, nil)
	aggStorage := storage.NewAggregatedStorage(z, storage.WithTruncInterval(50*time.Millisecond))
	bs := master.NewBackgroundStorage(z, sessStorage, rqStorage, aggStorage, nil, nil)
	fake := &fakeClickhouseSink{}
	bs.SetClickhouseWriter(fake)

	hook := bs.AggregatedSnapshotHook()
	require.NotNil(t, hook, "hook must be non-nil when sink is configured")
	aggStorage.SetCycleHook(hook)

	startQ, endQ := pinStorageTime(t)
	require.NoError(t, aggStorage.AggQuery(&pbm.TotalQueryData{
		QueryStat: &pbm.QueryStat{
			StatKind:    pbm.StatKind_SK_PRECISE,
			Completed:   true,
			StartTime:   timestamppb.New(startQ),
			EndTime:     timestamppb.New(endQ),
			QueryStatus: pbc.QueryStatus_QUERY_STATUS_DONE,
			QueryInfo: &pbc.QueryInfo{
				QueryId:      99,
				PlanId:       3,
				UserName:     "u",
				DatabaseName: "d",
				Rsgname:      "rsg",
				QueryText:    "select 1",
			},
			QueryKey: &pbc.QueryKey{Ssid: 1, Tmid: 1, Ccnt: 1},
			TotalQueryMetrics: &pbc.GPMetrics{
				SystemStat: &pbc.SystemStat{
					UserTimeSeconds:    1,
					KernelTimeSeconds:  0.5,
					RunningTimeSeconds: 2,
					Rss:                1024,
					ReadBytes:          100,
					WriteBytes:         200,
				},
				Instrumentation: &pbc.MetricInstrumentation{Ntuples: 50},
			},
		},
	}))

	// Move the truncated interval forward so the bucket becomes mature.
	storage.CurrentTime = func() time.Time { return endQ.Add(2 * aggStorage.GetTruncInterval()) }
	// pinStorageTime registered a Cleanup that restores the original; this
	// re-assignment is rolled back when the test exits.

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	queryChan := make(chan *pbm.QueryStatWrite, 4)
	go func() { _ = aggStorage.ArchiveAggQuery(ctx, queryChan, "cluster", "host") }()

	// drain queryChan in background to keep ArchiveAggQuery from blocking
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-queryChan:
			}
		}
	}()

	require.Eventually(t, func() bool { return len(fake.flushesCopy()) > 0 }, 800*time.Millisecond, 20*time.Millisecond)

	flushes := fake.flushesCopy()
	require.NotEmpty(t, flushes)
	require.Len(t, flushes[0], 1)
	bucket := flushes[0][0]
	assert.Equal(t, uint64(99), bucket.QueryID)
	assert.Equal(t, uint64(3), bucket.PlanID)
	assert.Equal(t, "u", bucket.User)
	assert.Equal(t, "d", bucket.Database)
	assert.Equal(t, "rsg", bucket.ResourceGroup)
	assert.Equal(t, uint64(1), bucket.Executions)
	assert.InDelta(t, 1.5, bucket.TotalCPUSec, 1e-9)
	// GroupGPMetrics in storage/util.go rewrites RunningTimeSeconds to
	// User+Kernel after merge, so TotalRunningSec mirrors TotalCPUSec rather
	// than the input fixture's RunningTimeSeconds value.
	assert.InDelta(t, 1.5, bucket.TotalRunningSec, 1e-9)
	assert.Equal(t, uint64(1024), bucket.TotalRSSBytes)
	assert.Equal(t, uint64(300), bucket.TotalIOBytes)
	assert.Equal(t, uint64(50), bucket.TotalNTuples)
	// MeanTimeNs in storage = float64(time.Duration) → 1s window → ~1000ms.
	assert.InDelta(t, 1000.0, bucket.AvgDurationMs, 1)
	assert.InDelta(t, float64(1000), float64(bucket.MaxDurationMs), 1)
}

func TestAggregatedSnapshotHook_NilWhenSinkAbsent(t *testing.T) {
	z := newTestNopLogger()
	rq := storage.NewRunningQueriesStorage()
	sess := gp.NewSessionsStorage(z, rq, nil)
	agg := storage.NewAggregatedStorage(z)
	bs := master.NewBackgroundStorage(z, sess, rq, agg, nil, nil)
	assert.Nil(t, bs.AggregatedSnapshotHook(), "hook must be nil without a sink")
}

func TestClickhouseWriterGetterAndSetter(t *testing.T) {
	z := newTestNopLogger()
	rq := storage.NewRunningQueriesStorage()
	sess := gp.NewSessionsStorage(z, rq, nil)
	agg := storage.NewAggregatedStorage(z)
	bs := master.NewBackgroundStorage(z, sess, rq, agg, nil, nil)
	assert.Nil(t, bs.ClickhouseWriter())
	fake := &fakeClickhouseSink{}
	bs.SetClickhouseWriter(fake)
	assert.NotNil(t, bs.ClickhouseWriter())
}

func TestAggregatedSnapshotHook_FlushErrorSwallowed(t *testing.T) {
	z := newTestNopLogger()
	rq := storage.NewRunningQueriesStorage()
	sess := gp.NewSessionsStorage(z, rq, nil)
	agg := storage.NewAggregatedStorage(z)
	bs := master.NewBackgroundStorage(z, sess, rq, agg, nil, nil)
	fake := &fakeClickhouseSink{flushErr: errors.New("ch unreachable")}
	bs.SetClickhouseWriter(fake)
	hook := bs.AggregatedSnapshotHook()
	require.NotNil(t, hook)
	// hook must not panic when the sink fails — error is logged + swallowed.
	hook(context.Background(), []storage.AggBucketSnapshot{{Key: storage.AggKey{QueryID: 1}}})
	assert.Len(t, fake.flushesCopy(), 1)
}

// pinStorageTime overrides storage.CurrentTime for the duration of the test
// and restores the original value via t.Cleanup so subsequent tests are
// unaffected.
func pinStorageTime(t *testing.T) (time.Time, time.Time) {
	t.Helper()
	prev := storage.CurrentTime
	t.Cleanup(func() { storage.CurrentTime = prev })
	sT := time.Now()
	storage.CurrentTime = func() time.Time { return sT }
	return sT.Add(-1 * time.Second), sT
}
