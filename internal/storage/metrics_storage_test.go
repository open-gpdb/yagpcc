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

package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"

	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
)

func TestClearRunningQueries(t *testing.T) {
	s := NewRunningQueriesStorage()
	s.newQuery(&QueryKey{Ssid: 1}, 0, MeasuredQueryTimes{})
	s.ClearRunningQueries()
	assert.Equal(t, 0, len(s.runningQueries))
}

func TestDeleteQueries(t *testing.T) {
	s := NewRunningQueriesStorage()
	s.newQuery(&QueryKey{Ssid: 1}, 0, MeasuredQueryTimes{})
	assert.Equal(t, 1, len(s.runningQueries))
	s.DeleteQueries([]*QueryKey{{Ssid: 1}})
	assert.Equal(t, 0, len(s.runningQueries))
	s.DeleteQueries([]*QueryKey{{Ssid: 1}})
	assert.Equal(t, 0, len(s.runningQueries))
	s.newQuery(&QueryKey{Ssid: 1}, 0, MeasuredQueryTimes{})
	s.newQuery(&QueryKey{Ssid: 2}, 0, MeasuredQueryTimes{})
	assert.Equal(t, 2, len(s.runningQueries))
	s.DeleteQueries([]*QueryKey{{Ssid: 1}})
	assert.Equal(t, 1, len(s.runningQueries))
	s.DeleteQueries([]*QueryKey{{Ssid: 1}})
	assert.Equal(t, 1, len(s.runningQueries))
	s.DeleteQueries([]*QueryKey{{Ssid: 2}})
	assert.Equal(t, 0, len(s.runningQueries))
}

func TestGetQuery(t *testing.T) {
	s := NewRunningQueriesStorage()
	s.newQuery(&QueryKey{Ssid: 1}, 0, MeasuredQueryTimes{})
	val, ok := s.GetQuery(QueryKey{Ssid: 1})
	assert.Equal(t, ok, true)
	assert.NotNil(t, val)
	_, ok = s.GetQuery(QueryKey{Ssid: 2})
	assert.Equal(t, ok, false)
}

func TestGetQueries(t *testing.T) {
	s := NewRunningQueriesStorage()
	testQ := []*QueryKey{{Ssid: 1}, {Ssid: 2}}
	for _, tQ := range testQ {
		s.newQuery(tQ, 0, MeasuredQueryTimes{})
	}
	assert.Equal(t, 2, len(s.GetQueries()))
	for tQ := range s.GetQueries() {
		_, ok := s.GetQuery(tQ)
		assert.Equal(t, ok, true)
	}
}

func TestCanLock(t *testing.T) {
	s := NewRunningQueriesStorage()
	assert.Equal(t, true, s.CanLock())
	s.mx.Lock()
	defer s.mx.Unlock()
	assert.Equal(t, false, s.CanLock())
}

func TestQueriesCount(t *testing.T) {
	s := NewRunningQueriesStorage()
	assert.Equal(t, 0, s.QueriesCount())
	s.newQuery(&QueryKey{Ssid: 1}, 0, MeasuredQueryTimes{})
	assert.Equal(t, 1, s.QueriesCount())
}

func TestGC(t *testing.T) {
	s := NewRunningQueriesStorage()
	tStart := time.Now()
	for i := 0; i < s.maximumStoredQueries+2; i++ {
		qKey := &QueryKey{
			Ssid: int32(i),
		}
		t := tStart.Add(-time.Duration(int(time.Second) * i))
		mqTimes := MeasuredQueryTimes{
			QueryStart: timestamppb.New(t),
		}
		s.newQuery(qKey, 0, mqTimes)
	}
	assert.Equal(t, len(s.runningQueries), s.maximumStoredQueries*50/100+2)
	qKeyFirst := &QueryKey{
		Ssid: int32(s.maximumStoredQueries*50/100 - 1),
	}
	val, ok := s.runningQueries[*qKeyFirst]
	assert.Equal(t, ok, true)
	assert.WithinDuration(t, val.QueryStart, tStart.Add(-time.Duration(int(time.Second)*int(qKeyFirst.Ssid))), 0)
	qKeyNotFound := &QueryKey{
		Ssid: int32(s.maximumStoredQueries * 50 / 100),
	}
	_, ok = s.runningQueries[*qKeyNotFound]
	assert.Equal(t, ok, false)
	qKeyLast := &QueryKey{
		Ssid: int32(s.maximumStoredQueries + 1),
	}
	val, ok = s.runningQueries[*qKeyLast]
	assert.Equal(t, ok, true)
	assert.WithinDuration(t, val.QueryStart, tStart.Add(-time.Duration(int(time.Second)*int(qKeyLast.Ssid))), 0)
}

func TestGCWithArchChan(t *testing.T) {
	// Use maximumStoredQueries=1000 so that integer division
	// (1000/100*20 = 200) produces a meaningful freePercent.
	archChan := make(chan *GCQuery, 1000)
	s := NewRunningQueriesStorage(
		WithMaximumStoredQueries(1000),
		WithFreePercent(20),
		WithArchChan(archChan),
	)

	tStart := time.Now()
	// Fill storage with completed queries
	for i := 0; i < 1000; i++ {
		qKey := &QueryKey{Ssid: int32(i)}
		mqTimes := MeasuredQueryTimes{
			QueryStart: timestamppb.New(tStart.Add(-time.Duration(int(time.Second) * (1000 - i)))),
		}
		rQ := s.newQuery(qKey, int32(pbc.QueryStatus_QUERY_STATUS_DONE), mqTimes)
		rQ.Completed = true
	}
	assert.Equal(t, 1000, len(s.runningQueries))

	// Trigger GC by adding one more query (exceeds maximumStoredQueries)
	s.newQuery(&QueryKey{Ssid: 10000}, 0, MeasuredQueryTimes{
		QueryStart: timestamppb.New(tStart),
	})

	// GC frees 1000/100*20 = 200 queries, then adds 1 new
	// After GC: 1000 - 200 + 1 = 801
	assert.Equal(t, 801, len(s.runningQueries))

	// All 200 evicted queries were completed, so all should be sent to archChan
	assert.Eventually(t, func() bool { return len(archChan) == 200 }, time.Second, 10*time.Millisecond)

	// Verify the archived queries have valid and distinct keys.
	seenKeys := make(map[QueryKey]struct{}, 200)
	for i := 0; i < 200; i++ {
		gcQ := <-archChan
		assert.NotNil(t, gcQ.QKey)
		assert.NotNil(t, gcQ.QVal)
		assert.True(t, gcQ.QVal.Completed)
		seenKeys[*gcQ.QKey] = struct{}{}
	}
	assert.Len(t, seenKeys, 200)
}

func TestGCWithArchChanMixedQueries(t *testing.T) {
	// Use maximumStoredQueries=1000 so that integer division works correctly.
	// GC sort order: running queries first (oldest), then completed queries (oldest).
	// So when we have a mix, the oldest running queries are evicted first.
	// Only completed (ended) queries are sent to archChan.
	archChan := make(chan *GCQuery, 1000)
	s := NewRunningQueriesStorage(
		WithMaximumStoredQueries(1000),
		WithFreePercent(20),
		WithArchChan(archChan),
	)

	tStart := time.Now()
	// Add 900 running queries (oldest — will be evicted first by GC sort)
	for i := 0; i < 900; i++ {
		qKey := &QueryKey{Ssid: int32(i)}
		mqTimes := MeasuredQueryTimes{
			QueryStart: timestamppb.New(tStart.Add(-time.Duration(int(time.Second) * (2000 - i)))),
		}
		s.newQuery(qKey, int32(pbc.QueryStatus_QUERY_STATUS_START), mqTimes)
	}
	// Add 100 completed queries (these come after running in GC sort)
	for i := 900; i < 1000; i++ {
		qKey := &QueryKey{Ssid: int32(i)}
		mqTimes := MeasuredQueryTimes{
			QueryStart: timestamppb.New(tStart.Add(-time.Duration(int(time.Second) * (1000 - i)))),
		}
		rQ := s.newQuery(qKey, int32(pbc.QueryStatus_QUERY_STATUS_DONE), mqTimes)
		rQ.Completed = true
	}
	assert.Equal(t, 1000, len(s.runningQueries))

	// Trigger GC — evicts 200 queries.
	// GC sort: running queries first (sorted by start time), then completed.
	// The 200 oldest running queries are evicted. None are completed.
	s.newQuery(&QueryKey{Ssid: 10000}, 0, MeasuredQueryTimes{
		QueryStart: timestamppb.New(tStart),
	})

	// GC frees 200 queries, then adds 1 new → 801
	assert.Equal(t, 801, len(s.runningQueries))

	// All 200 evicted queries were running (not ended), so none should be archived.
	// Running queries are not sent to archChan to avoid double-counting.
	assert.Never(t, func() bool { return len(archChan) > 0 }, time.Second, 10*time.Millisecond)
}

func TestGCWithArchChanPartiallyCompleted(t *testing.T) {
	// Test where GC evicts a mix of completed and running queries.
	// GC sort: running first, then completed. With 200 to evict,
	// if we have 150 running and 850 completed, GC evicts all 150 running + 50 completed.
	archChan := make(chan *GCQuery, 1000)
	s := NewRunningQueriesStorage(
		WithMaximumStoredQueries(1000),
		WithFreePercent(20),
		WithArchChan(archChan),
	)

	tStart := time.Now()
	// Add 150 running queries (oldest)
	for i := 0; i < 150; i++ {
		qKey := &QueryKey{Ssid: int32(i)}
		mqTimes := MeasuredQueryTimes{
			QueryStart: timestamppb.New(tStart.Add(-time.Duration(int(time.Second) * (3000 - i)))),
		}
		s.newQuery(qKey, int32(pbc.QueryStatus_QUERY_STATUS_START), mqTimes)
	}
	// Add 850 completed queries
	for i := 150; i < 1000; i++ {
		qKey := &QueryKey{Ssid: int32(i)}
		mqTimes := MeasuredQueryTimes{
			QueryStart: timestamppb.New(tStart.Add(-time.Duration(int(time.Second) * (2000 - i)))),
		}
		rQ := s.newQuery(qKey, int32(pbc.QueryStatus_QUERY_STATUS_DONE), mqTimes)
		rQ.Completed = true
	}
	assert.Equal(t, 1000, len(s.runningQueries))

	// Trigger GC — evicts 200 queries.
	// GC sort: 150 running (oldest first), then 850 completed (oldest first).
	// Evicts: all 150 running + 50 oldest completed = 200 total.
	s.newQuery(&QueryKey{Ssid: 10000}, 0, MeasuredQueryTimes{
		QueryStart: timestamppb.New(tStart),
	})

	assert.Equal(t, 801, len(s.runningQueries))

	// Only the 50 completed queries should be sent to archChan
	assert.Eventually(t, func() bool { return len(archChan) == 50 }, time.Second, 10*time.Millisecond)

	for i := 0; i < 50; i++ {
		gcQ := <-archChan
		assert.True(t, CheckQueryEnded(gcQ.QVal.QueryStatus))
	}
}

func TestGCWithoutArchChan(t *testing.T) {
	// Without archChan, GC should still work (just delete queries)
	s := NewRunningQueriesStorage(
		WithMaximumStoredQueries(1000),
		WithFreePercent(20),
	)

	tStart := time.Now()
	for i := 0; i < 1000; i++ {
		qKey := &QueryKey{Ssid: int32(i)}
		mqTimes := MeasuredQueryTimes{
			QueryStart: timestamppb.New(tStart.Add(-time.Duration(int(time.Second) * (1000 - i)))),
		}
		rQ := s.newQuery(qKey, int32(pbc.QueryStatus_QUERY_STATUS_DONE), mqTimes)
		rQ.Completed = true
	}

	// Trigger GC
	s.newQuery(&QueryKey{Ssid: 10000}, 0, MeasuredQueryTimes{
		QueryStart: timestamppb.New(tStart),
	})

	// After GC: 1000 - 200 + 1 = 801
	assert.Equal(t, 801, len(s.runningQueries))
}

func TestMultipleSlices(t *testing.T) {
	s := NewRunningQueriesStorage()
	newQuery, err := s.StoreInfoInStorage(
		&NodeKey{QKey: QueryKey{Ssid: 1, Ccnt: 2}, SKey: SegmentKey{Segindex: 1}, SliceID: 0},
		int32(pbc.QueryStatus_QUERY_STATUS_DONE),
		MeasuredQueryTimes{
			QueryStart: timestamppb.New(time.Now().Add(time.Duration(-10) * time.Minute)),
			QueryEnd:   timestamppb.New(time.Now()),
		},
		&pbc.QueryInfo{QueryText: "SELECT 1"},
		nil,
		&pbc.GPMetrics{Instrumentation: &pbc.MetricInstrumentation{Nloops: 1}},
	)
	assert.NoError(t, err)
	assert.Equal(t, newQuery, true)
	assert.Equal(t, len(s.runningQueries[QueryKey{Ssid: 1, Ccnt: 2}].QueriesData), 1)
	newQuery, err = s.StoreInfoInStorage(
		&NodeKey{QKey: QueryKey{Ssid: 1, Ccnt: 2}, SKey: SegmentKey{Segindex: 1}, SliceID: 1},
		int32(pbc.QueryStatus_QUERY_STATUS_DONE),
		MeasuredQueryTimes{
			QueryStart: timestamppb.New(time.Now().Add(time.Duration(-10) * time.Minute)),
			QueryEnd:   timestamppb.New(time.Now()),
		},
		&pbc.QueryInfo{},
		&pbc.AdditionalQueryInfo{SliceId: 1},
		&pbc.GPMetrics{Instrumentation: &pbc.MetricInstrumentation{Nloops: 2}},
	)
	assert.NoError(t, err)
	assert.Equal(t, newQuery, false)
	runningQ := s.runningQueries[QueryKey{Ssid: 1, Ccnt: 2}]
	sliceZero := runningQ.QueriesData[NodeKey{QKey: QueryKey{Ssid: 1, Ccnt: 2}, SKey: SegmentKey{Segindex: 1}, SliceID: 0}]
	assert.Equal(t, sliceZero.QueryMetrics.Instrumentation.Nloops, uint64(1))
	sliceOne := runningQ.QueriesData[NodeKey{QKey: QueryKey{Ssid: 1, Ccnt: 2}, SKey: SegmentKey{Segindex: 1}, SliceID: 1}]
	assert.Equal(t, sliceOne.QueryMetrics.Instrumentation.Nloops, uint64(2))

	queryData, err := s.GetQueryInfo(QueryKey{Ssid: 1, Ccnt: 2}, 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), queryData.QueryStat.Slices)
}

func TestGetQueriesStartTime_Empty(t *testing.T) {
	s := NewRunningQueriesStorage()
	assert.Empty(t, s.GetQueriesStartTime())
}

func TestGetQueriesStartTime_IncludesRunningQueries(t *testing.T) {
	s := NewRunningQueriesStorage()
	tA := time.Now().Add(-30 * time.Second)
	tB := time.Now().Add(-2 * time.Minute)
	s.newQuery(&QueryKey{Ssid: 1}, int32(pbc.QueryStatus_QUERY_STATUS_START), MeasuredQueryTimes{
		QueryStart: timestamppb.New(tA),
	})
	s.newQuery(&QueryKey{Ssid: 2}, int32(pbc.QueryStatus_QUERY_STATUS_SUBMIT), MeasuredQueryTimes{
		QueryStart: timestamppb.New(tB),
	})

	got := s.GetQueriesStartTime()
	assert.Len(t, got, 2)
	assert.ElementsMatch(t, []int64{tA.Unix(), tB.Unix()}, []int64{got[0].Unix(), got[1].Unix()})
}

func TestGetQueriesStartTime_ExcludesEndedQueries(t *testing.T) {
	s := NewRunningQueriesStorage()
	tRun := time.Now().Add(-10 * time.Second)
	s.newQuery(&QueryKey{Ssid: 1}, int32(pbc.QueryStatus_QUERY_STATUS_START), MeasuredQueryTimes{
		QueryStart: timestamppb.New(tRun),
	})
	s.newQuery(&QueryKey{Ssid: 2}, int32(pbc.QueryStatus_QUERY_STATUS_START), MeasuredQueryTimes{
		QueryStart: timestamppb.New(time.Now().Add(-time.Minute)),
	})

	s.mx.Lock()
	s.runningQueries[QueryKey{Ssid: 2}].QueryStatus = int32(pbc.QueryStatus_QUERY_STATUS_DONE)
	s.mx.Unlock()

	got := s.GetQueriesStartTime()
	assert.Len(t, got, 1)
	assert.Equal(t, tRun.Unix(), got[0].Unix())
}

func TestGetQueriesStartTime_ExcludesCanceledAndError(t *testing.T) {
	s := NewRunningQueriesStorage()
	s.newQuery(&QueryKey{Ssid: 1}, int32(pbc.QueryStatus_QUERY_STATUS_START), MeasuredQueryTimes{
		QueryStart: timestamppb.New(time.Now()),
	})
	s.newQuery(&QueryKey{Ssid: 2}, int32(pbc.QueryStatus_QUERY_STATUS_CANCELED), MeasuredQueryTimes{
		QueryStart: timestamppb.New(time.Now()),
	})
	s.newQuery(&QueryKey{Ssid: 3}, int32(pbc.QueryStatus_QUERY_STATUS_ERROR), MeasuredQueryTimes{
		QueryStart: timestamppb.New(time.Now()),
	})

	assert.Len(t, s.GetQueriesStartTime(), 1)
}

func TestGetQueriesStartTime_ExcludesZeroQueryStart(t *testing.T) {
	s := NewRunningQueriesStorage()
	s.newQuery(&QueryKey{Ssid: 1}, int32(pbc.QueryStatus_QUERY_STATUS_START), MeasuredQueryTimes{
		QueryStart: timestamppb.New(time.Now()),
	})

	s.mx.Lock()
	s.runningQueries[QueryKey{Ssid: 1}].QueryStart = time.Time{}
	s.mx.Unlock()

	assert.Empty(t, s.GetQueriesStartTime())
}
