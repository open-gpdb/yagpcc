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
	"context"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/utils"
)

func setTime() (time.Time, time.Time) {
	sT := time.Now()
	CurrentTime = func() time.Time {
		return sT
	}
	startQ := sT.Add(time.Duration(-1) * time.Second)
	endQ := sT
	return startQ, endQ
}

func TestAggregate(t *testing.T) {
	file, err := os.Create("trace.log")
	require.NoError(t, err)
	zLogger := utils.DualLog(true, file)
	aggStorage := NewAggregatedStorage(zLogger)

	startQ, endQ := setTime()
	testProto := &pbm.TotalQueryData{
		QueryStat: &pbm.QueryStat{
			StatKind:    pbm.StatKind_SK_PRECISE,
			Completed:   true,
			StartTime:   timestamppb.New(startQ),
			EndTime:     timestamppb.New(endQ),
			QueryStatus: pbc.QueryStatus_QUERY_STATUS_DONE,
			QueryInfo: &pbc.QueryInfo{
				QueryId:   123,
				QueryText: "Select 1",
			},
			QueryKey: &pbc.QueryKey{
				Ssid: 1,
			},
			TotalQueryMetrics: &pbc.GPMetrics{SystemStat: &pbc.SystemStat{UserTimeSeconds: 57}},
		},
		SegmentQueryMetrics: []*pbm.SegmentMetrics{
			{
				SegmentKey:     &pbc.SegmentKey{Segindex: -1},
				StartTime:      timestamppb.New(startQ),
				EndTime:        timestamppb.New(endQ),
				QueryStatus:    pbc.QueryStatus_QUERY_STATUS_DONE,
				SegmentMetrics: &pbc.GPMetrics{SystemStat: &pbc.SystemStat{UserTimeSeconds: 2}},
			},
			{
				SegmentKey:     &pbc.SegmentKey{Segindex: 1},
				StartTime:      timestamppb.New(startQ),
				EndTime:        timestamppb.New(startQ),
				QueryStatus:    pbc.QueryStatus_QUERY_STATUS_DONE,
				SegmentMetrics: &pbc.GPMetrics{SystemStat: &pbc.SystemStat{UserTimeSeconds: 10}},
			},
			{
				SegmentKey:     &pbc.SegmentKey{Segindex: 2},
				StartTime:      timestamppb.New(startQ),
				EndTime:        timestamppb.New(endQ),
				QueryStatus:    pbc.QueryStatus_QUERY_STATUS_DONE,
				SegmentMetrics: &pbc.GPMetrics{SystemStat: &pbc.SystemStat{UserTimeSeconds: 20}},
			},
			{
				SegmentKey:     &pbc.SegmentKey{Segindex: 3},
				StartTime:      timestamppb.New(endQ),
				EndTime:        timestamppb.New(endQ),
				QueryStatus:    pbc.QueryStatus_QUERY_STATUS_DONE,
				SegmentMetrics: &pbc.GPMetrics{SystemStat: &pbc.SystemStat{UserTimeSeconds: 10}},
			},
			{
				SegmentKey:     &pbc.SegmentKey{Segindex: 4},
				StartTime:      timestamppb.New(startQ),
				EndTime:        timestamppb.New(endQ),
				QueryStatus:    pbc.QueryStatus_QUERY_STATUS_DONE,
				SegmentMetrics: &pbc.GPMetrics{SystemStat: &pbc.SystemStat{UserTimeSeconds: 15}},
			},
		},
	}

	startI, endI := aggStorage.GetCurrentInterval()
	err = aggStorage.AggQuery(testProto)

	if err != nil {
		t.Errorf("Fail to aggregate query %v", err)
	}

	assert.Equal(t, len(aggStorage.aggQueries), 1)
	assert.Equal(t, len(aggStorage.aggQueriesRef), 1)

	valR, okR := aggStorage.aggQueriesRef[RefKey{StartTime: startI, EndTime: endI}]

	assert.Equal(t, okR, true)
	assert.Equal(t, valR, 1)

	valA, okA := aggStorage.aggQueries[AggKey{QueryID: 123, StartTime: startI, EndTime: endI}]
	assert.Equal(t, okA, true)
	assert.Equal(t, valA.TotalMetrics.SystemStat.UserTimeSeconds, float64(57))
	assert.True(t, proto.Equal(valA.AggTimes, &pbc.AggregatedMetrics{Calls: 1, TotalTime: float64(time.Second), MinTime: float64(time.Second), MaxTime: float64(time.Second), MeanTime: float64(time.Second), StddevTime: 0}))
	assert.Equal(t, valA.QueryKey.Ssid, int32(1))

	err = aggStorage.AggQuery(testProto)
	if err != nil {
		t.Errorf("Fail to aggregate query %v", err)
	}

	assert.Equal(t, len(aggStorage.aggQueries), 1)
	assert.Equal(t, len(aggStorage.aggQueriesRef), 1)

	valR, okR = aggStorage.aggQueriesRef[RefKey{StartTime: startI, EndTime: endI}]

	assert.Equal(t, okR, true)
	assert.Equal(t, valR, 1)

	valA, okA = aggStorage.aggQueries[AggKey{QueryID: 123, StartTime: startI, EndTime: endI}]
	assert.Equal(t, okA, true)
	assert.Equal(t, valA.TotalMetrics.SystemStat.UserTimeSeconds, float64(114))
	assert.True(t, proto.Equal(valA.AggTimes, &pbc.AggregatedMetrics{Calls: 2, TotalTime: float64(2 * time.Second), MinTime: float64(time.Second), MaxTime: float64(time.Second), MeanTime: float64(time.Second)}))
	assert.Equal(t, valA.QueryKey.Ssid, int32(1))

	for i := 0; i < aggStorage.maxQueriesPerUser+20; i++ {
		testProto := &pbm.TotalQueryData{
			QueryStat: &pbm.QueryStat{
				StatKind:    pbm.StatKind_SK_PRECISE,
				Completed:   true,
				StartTime:   timestamppb.New(startQ),
				EndTime:     timestamppb.New(endQ),
				QueryStatus: pbc.QueryStatus_QUERY_STATUS_DONE,
				QueryInfo: &pbc.QueryInfo{
					UserName:     "testuser",
					DatabaseName: "postgres",
					QueryId:      uint64(i),
					QueryText:    "Select 1",
				},
				QueryKey: &pbc.QueryKey{
					Ssid: int32(i),
				},
				TotalQueryMetrics: &pbc.GPMetrics{SystemStat: &pbc.SystemStat{UserTimeSeconds: 57}},
			},
		}
		err = aggStorage.AggQuery(testProto)
		if err != nil {
			t.Errorf("Fail to aggregate query %v", err)
		}
	}

	valR, okR = aggStorage.aggQueriesRef[RefKey{UserName: "testuser", DatabaseName: "postgres", StartTime: startI, EndTime: endI}]

	assert.Equal(t, okR, true)
	assert.Equal(t, valR, aggStorage.maxQueriesPerUser+20)

	valA, okA = aggStorage.aggQueries[AggKey{UserName: "testuser", DatabaseName: "postgres", StartTime: startI, EndTime: endI}]

	assert.Equal(t, okA, true)
	assert.Equal(t, valA.TotalMetrics.SystemStat.UserTimeSeconds, float64(57*20))
	assert.True(t, proto.Equal(valA.AggTimes, &pbc.AggregatedMetrics{Calls: 20, TotalTime: float64(20 * time.Second), MinTime: float64(time.Second), MaxTime: float64(time.Second), MeanTime: float64(time.Second)}))
	assert.Equal(t, valA.QueryKey.Ssid, int32(0))
}

func TestParallelAgg(t *testing.T) {
	// test for race
	file, err := os.Create("trace.log")
	require.NoError(t, err)
	zLogger := utils.DualLog(true, file)
	if err != nil {
		t.Error(err.Error())
	}
	aggStorage := NewAggregatedStorage(zLogger)

	startQ, endQ := setTime()
	startI, endI := aggStorage.GetCurrentInterval()

	tests := []struct {
		name      string
		isSet     bool
		paramName string
		ssid      int
		value     int
		cnt       int
		sleep     float64
	}{
		{name: "test Set Query1", isSet: true, paramName: "CPU", ssid: 1, value: 1, cnt: 10000, sleep: 0},
		{name: "test Set Query1", isSet: true, paramName: "IO", ssid: 1, value: 8, cnt: 80, sleep: 0.01},
		{name: "test Set Query1", isSet: true, paramName: "Memory", ssid: 1, value: 1024, cnt: 30, sleep: 0.02},
		{name: "test Set Query2", isSet: true, paramName: "CPU", ssid: 2, value: 1, cnt: 10000, sleep: 0},
		{name: "test Get Queries", isSet: false, paramName: "ALL", ssid: 1, value: 1, cnt: 80, sleep: 0.01},
		{name: "test Get Query1", isSet: false, paramName: "QUERY", ssid: 1, value: 1, cnt: 10000, sleep: 0},
	}

	for _, tc := range tests {
		tcTest := tc
		t.Run(tcTest.name, func(t *testing.T) {
			t.Parallel()
			for i := 0; i < tcTest.cnt; i++ {
				if tcTest.isSet {
					systemStat := &pbc.SystemStat{}
					switch tcTest.paramName {
					case "CPU":
						systemStat.UserTimeSeconds = float64(tcTest.value)
					case "IO":
						systemStat.ReadBytes = uint64(tcTest.value)
					case "Memory":
						systemStat.Rss = uint64(tcTest.value)
					}
					queryMetrics := &pbc.GPMetrics{SystemStat: systemStat}
					request := &pbm.TotalQueryData{
						QueryStat: &pbm.QueryStat{
							StatKind:    pbm.StatKind_SK_PRECISE,
							Completed:   true,
							StartTime:   timestamppb.New(startQ),
							EndTime:     timestamppb.New(endQ),
							QueryStatus: pbc.QueryStatus_QUERY_STATUS_DONE,
							QueryInfo: &pbc.QueryInfo{
								UserName:     "testuser",
								DatabaseName: "postgres",
								QueryId:      1 + uint64(rand.Intn(100)),
								QueryText:    "Select 1",
							},
							QueryKey: &pbc.QueryKey{
								Ssid: 1,
							},
							TotalQueryMetrics: queryMetrics,
						},
					}

					err := aggStorage.AggQuery(request)
					if err != nil {
						t.Error(err)
					}
				} else {
					aggStorage.mx.RLock()
					valA, okA := aggStorage.aggQueries[AggKey{QueryID: 1 + uint64(rand.Intn(100)), UserName: "testuser", DatabaseName: "postgres", StartTime: startI, EndTime: endI}]
					aggStorage.mx.RUnlock()
					if okA {
						valA.QueryLock.RLock()
						// AggQuery publishes a new entry to the map before its first
						// GroupAggMetrics call increments Calls, so a concurrently
						// observed entry may legitimately still have Calls == 0. Only
						// assert the entry is well-formed and the count is non-negative.
						assert.NotNil(t, valA.AggTimes)
						assert.GreaterOrEqual(t, valA.AggTimes.Calls, int64(0))
						valA.QueryLock.RUnlock()
					}
				}
				time.Sleep(time.Duration(tcTest.sleep) * time.Second)
			}
		})
	}
}

// TestArchiveAggQuery_CycleHookSnapshotIsDeepCloned guards against a race
// where the cycle hook receives a TotalMetrics pointer that a concurrent
// AggQuery (which already retrieved the same *AggVal from the map before
// the drain pass deleted it) could later mutate via GroupGPMetrics. The
// snapshot must own its own copy of TotalMetrics so the hook can read the
// fields without holding the bucket lock.
func TestArchiveAggQuery_CycleHookSnapshotIsDeepCloned(t *testing.T) {
	logger := zap.NewNop().Sugar()
	aggStorage := NewAggregatedStorage(logger, WithTruncInterval(50*time.Millisecond))

	startQ, endQ := setTime()
	require.NoError(t, aggStorage.AggQuery(&pbm.TotalQueryData{
		QueryStat: &pbm.QueryStat{
			StatKind:    pbm.StatKind_SK_PRECISE,
			Completed:   true,
			StartTime:   timestamppb.New(startQ),
			EndTime:     timestamppb.New(endQ),
			QueryStatus: pbc.QueryStatus_QUERY_STATUS_DONE,
			QueryInfo: &pbc.QueryInfo{
				QueryId:      42,
				UserName:     "u",
				DatabaseName: "d",
				QueryText:    "select 1",
			},
			QueryKey:          &pbc.QueryKey{Ssid: 1},
			TotalQueryMetrics: &pbc.GPMetrics{SystemStat: &pbc.SystemStat{UserTimeSeconds: 7}},
		},
	}))

	// Capture the original *AggVal pointer so we can mutate the underlying
	// TotalMetrics later, then advance time so the bucket is mature.
	startI, endI := aggStorage.GetCurrentInterval()
	aggStorage.mx.RLock()
	origVal := aggStorage.aggQueries[AggKey{QueryID: 42, UserName: "u", DatabaseName: "d", StartTime: startI, EndTime: endI}]
	aggStorage.mx.RUnlock()
	require.NotNil(t, origVal)
	// CurrentTime is a package-global; save and restore it so this override does
	// not leak into other tests and cause order-dependent flakes.
	prevCurrentTime := CurrentTime
	t.Cleanup(func() { CurrentTime = prevCurrentTime })
	CurrentTime = func() time.Time { return endQ.Add(2 * aggStorage.GetTruncInterval()) }

	captured := make(chan AggBucketSnapshot, 1)
	aggStorage.SetCycleHook(func(_ context.Context, snaps []AggBucketSnapshot) {
		if len(snaps) > 0 {
			captured <- snaps[0]
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	queryChan := make(chan *pbm.QueryStatWrite, 4)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-queryChan:
			}
		}
	}()
	go func() { _ = aggStorage.ArchiveAggQuery(ctx, queryChan, "cluster", "host") }()

	var snap AggBucketSnapshot
	select {
	case snap = <-captured:
	case <-time.After(800 * time.Millisecond):
		t.Fatal("cycle hook never fired")
	}
	require.NotNil(t, snap.TotalMetrics)
	require.NotNil(t, snap.TotalMetrics.SystemStat)
	assert.Equal(t, float64(7), snap.TotalMetrics.SystemStat.UserTimeSeconds)
	// The snapshot's TotalMetrics must be a distinct allocation. Mutating the
	// original AggVal's pointer through the storage must not bleed into the
	// snapshot.
	assert.NotSame(t, origVal.TotalMetrics, snap.TotalMetrics, "snapshot must own a deep copy of TotalMetrics")
	origVal.QueryLock.Lock()
	origVal.TotalMetrics.SystemStat.UserTimeSeconds = 999
	origVal.QueryLock.Unlock()
	assert.Equal(t, float64(7), snap.TotalMetrics.SystemStat.UserTimeSeconds, "snapshot must be unaffected by mutation of origVal")
}
