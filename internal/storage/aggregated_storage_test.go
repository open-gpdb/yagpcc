package storage

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
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
	assert.Equal(t, proto.MarshalTextString(valA.AggTimes), proto.MarshalTextString(&pbm.AggregatedMetrics{Calls: 1, TotalTime: float64(time.Second), MinTime: float64(time.Second), MaxTime: float64(time.Second), MeanTime: float64(time.Second), StddevTime: 0}))
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
	assert.Equal(t, proto.MarshalTextString(valA.AggTimes), proto.MarshalTextString(&pbm.AggregatedMetrics{Calls: 2, TotalTime: float64(2 * time.Second), MinTime: float64(time.Second), MaxTime: float64(time.Second), MeanTime: float64(time.Second)}))
	assert.Equal(t, valA.QueryKey.Ssid, int32(0))

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
					Ssid: 1,
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
	assert.Equal(t, proto.MarshalTextString(valA.AggTimes), proto.MarshalTextString(&pbm.AggregatedMetrics{Calls: 20, TotalTime: float64(20 * time.Second), MinTime: float64(time.Second), MaxTime: float64(time.Second), MeanTime: float64(time.Second)}))
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
						assert.LessOrEqual(t, int64(1), valA.AggTimes.Calls)
						valA.QueryLock.RUnlock()
					}
				}
				time.Sleep(time.Duration(tcTest.sleep) * time.Second)
			}
		})
	}
}
