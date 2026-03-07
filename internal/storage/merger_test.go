package storage

import (
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pb "github.com/open-gpdb/yagpcc/api/proto/agent_segment"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
)

func TestQuery42(t *testing.T) {

	s := NewRunningQueriesStorage()

	// querySubmit
	newQuery, err := s.StoreInfoInStorage(
		&NodeKey{QKey: QueryKey{Ssid: 13223549, Ccnt: 6}, SKey: SegmentKey{Segindex: -1, Dbid: 9}, SliceID: -1},
		int32(pbc.QueryStatus_QUERY_STATUS_SUBMIT),
		MeasuredQueryTimes{
			QuerySubmit: timestamppb.New(time.Unix(176338810, 0)),
		},
		&pbc.QueryInfo{QueryText: "select 42;"},
		nil,
		nil,
	)
	assert.NoError(t, err)
	assert.Equal(t, newQuery, true)

	// queryStart
	newQuery, err = s.StoreInfoInStorage(
		&NodeKey{QKey: QueryKey{Ssid: 13223549, Ccnt: 6}, SKey: SegmentKey{Segindex: -1, Dbid: 9}, SliceID: -1},
		int32(pbc.QueryStatus_QUERY_STATUS_START),
		MeasuredQueryTimes{
			QuerySubmit: timestamppb.New(time.Unix(1763388103, 0)),
			QueryStart:  timestamppb.New(time.Unix(1763388103, 0)),
		},
		&pbc.QueryInfo{
			Generator:        pbc.PlanGenerator_PLAN_GENERATOR_OPTIMIZER,
			QueryId:          3844438981,
			PlanId:           1250317044,
			PlanText:         "Result  (cost=0.00..0.00 rows=1 width=4)\n  Output: 42\n  ->  Result  (cost=0.00..0.00 rows=1 width=1)\n        Output: true\n",
			TemplatePlanText: "Result  (cost=$1..$2rows=$3width=$4)\n  Output: $5->  Result  (cost=$6..$7rows=$8width=$9)\n        Output: true\n",
			UserName:         "user1",
			DatabaseName:     "db1",
		},
		nil,
		nil,
	)
	assert.NoError(t, err)
	assert.Equal(t, newQuery, false)

	// queryDone
	newQuery, err = s.StoreInfoInStorage(
		&NodeKey{QKey: QueryKey{Ssid: 13223549, Ccnt: 6}, SKey: SegmentKey{Segindex: -1, Dbid: 9}, SliceID: -1},
		int32(pbc.QueryStatus_QUERY_STATUS_DONE),
		MeasuredQueryTimes{
			QuerySubmit: timestamppb.New(time.Unix(1763388103, 0)),
			QueryStart:  timestamppb.New(time.Unix(1763388103, 0)),
			QueryEnd:    timestamppb.New(time.Unix(1763388103, 0)),
		},
		&pbc.QueryInfo{
			Generator:    pbc.PlanGenerator_PLAN_GENERATOR_OPTIMIZER,
			QueryId:      3844438981,
			PlanId:       1250317044,
			UserName:     "user1",
			DatabaseName: "db1",
		},
		nil,
		&pbc.GPMetrics{
			SystemStat: &pbc.SystemStat{
				Vsize:    3857035264,
				Rss:      8409,
				VmSizeKb: 3766636,
				VmPeakKb: 3817360,
				Rchar:    1565,
				Syscr:    4},
			Instrumentation: &pbc.MetricInstrumentation{Tuplecount: 1, Firsttuple: 1e-06, Interconnect: &pbc.InterconnectStat{}},
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, newQuery, false)

	// queryEnd
	newQuery, err = s.StoreInfoInStorage(
		&NodeKey{QKey: QueryKey{Ssid: 13223549, Ccnt: 6}, SKey: SegmentKey{Segindex: -1, Dbid: 9}, SliceID: -1},
		int32(pbc.QueryStatus_QUERY_STATUS_END),
		MeasuredQueryTimes{
			QuerySubmit: timestamppb.New(time.Unix(1763388103, 0)),
			QueryStart:  timestamppb.New(time.Unix(1763388103, 0)),
			QueryEnd:    timestamppb.New(time.Unix(1763388103, 0)),
		},
		&pbc.QueryInfo{
			Generator:    pbc.PlanGenerator_PLAN_GENERATOR_OPTIMIZER,
			QueryId:      3844438981,
			PlanId:       1250317044,
			UserName:     "user1",
			DatabaseName: "db1",
		},
		nil,
		&pbc.GPMetrics{
			SystemStat: &pbc.SystemStat{
				Vsize:    3857035264,
				Rss:      8409,
				VmSizeKb: 3766636,
				VmPeakKb: 3817360,
				Rchar:    1565,
				Syscr:    4},
			Instrumentation: &pbc.MetricInstrumentation{Tuplecount: 1, Firsttuple: 1e-06, Interconnect: &pbc.InterconnectStat{}},
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, newQuery, false)

	qKey := QueryKey{Ssid: 13223549, Ccnt: 6}
	qT, err := s.GetQueryData(qKey, 1)
	assert.NoError(t, err)

	expectedResult := &pbm.TotalQueryData{
		QueryStat: &pbm.QueryStat{
			Hostname: "-1",
			QueryKey: &pbc.QueryKey{
				Ssid: 13223549,
				Ccnt: 6,
				Tmid: int32(1),
			},
			QueryInfo: &pbc.QueryInfo{
				Generator:    pbc.PlanGenerator_PLAN_GENERATOR_OPTIMIZER,
				QueryId:      3844438981,
				PlanId:       1250317044,
				UserName:     "user1",
				DatabaseName: "db1",
				PlanText:     "Result  (cost=0.00..0.00 rows=1 width=4)\n  Output: 42\n  ->  Result  (cost=0.00..0.00 rows=1 width=1)\n        Output: true\n",
				QueryText:    "select 42;",
				StartTime:    timestamppb.New(time.Unix(1763388103, 0)),
				EndTime:      timestamppb.New(time.Unix(1763388103, 0)),
				SubmitTime:   timestamppb.New(time.Unix(1763388103, 0)),
			},
			StatKind:    pbm.StatKind_SK_PRECISE,
			QueryStatus: pbc.QueryStatus_QUERY_STATUS_DONE,
			StartTime:   timestamppb.New(time.Unix(1763388103, 0)),
			EndTime:     timestamppb.New(time.Unix(1763388103, 0)),
			Completed:   true,
			Slices:      1,
			TotalQueryMetrics: &pbc.GPMetrics{
				SystemStat: &pbc.SystemStat{

					Vsize:    3857035264,
					Rss:      8409,
					VmSizeKb: 3766636,
					VmPeakKb: 3817360,
					Rchar:    1565,
					Syscr:    4},
				Instrumentation: &pbc.MetricInstrumentation{Tuplecount: 1, Firsttuple: 1e-06, Interconnect: &pbc.InterconnectStat{}},
			},
		},

		SegmentQueryMetrics: []*pbm.SegmentMetrics{
			{
				Hostname: "-1",
				QueryKey: &pbc.QueryKey{
					Ssid: 13223549,
					Ccnt: 6,
					Tmid: int32(1),
				},
				SegmentKey: &pbc.SegmentKey{
					Dbid:     9,
					Segindex: -1,
				},
				QueryInfo: &pbc.QueryInfo{
					Generator:    pbc.PlanGenerator_PLAN_GENERATOR_OPTIMIZER,
					QueryId:      3844438981,
					PlanId:       1250317044,
					UserName:     "user1",
					DatabaseName: "db1",
					PlanText:     "Result  (cost=0.00..0.00 rows=1 width=4)\n  Output: 42\n  ->  Result  (cost=0.00..0.00 rows=1 width=1)\n        Output: true\n",
					QueryText:    "select 42;",
					StartTime:    timestamppb.New(time.Unix(1763388103, 0)),
					EndTime:      timestamppb.New(time.Unix(1763388103, 0)),
					SubmitTime:   timestamppb.New(time.Unix(1763388103, 0)),
				},
				QueryStatus: pbc.QueryStatus_QUERY_STATUS_DONE,
				StartTime:   timestamppb.New(time.Unix(1763388103, 0)),
				EndTime:     timestamppb.New(time.Unix(1763388103, 0)),
				SegmentMetrics: &pbc.GPMetrics{
					SystemStat: &pbc.SystemStat{

						Vsize:    3857035264,
						Rss:      8409,
						VmSizeKb: 3766636,
						VmPeakKb: 3817360,
						Rchar:    1565,
						Syscr:    4},
					Instrumentation: &pbc.MetricInstrumentation{Tuplecount: 1, Firsttuple: 1e-06, Interconnect: &pbc.InterconnectStat{}},
				},
			},
		},
	}
	qT.QueryStat.CollectTime = nil
	assert.Equal(t, len(qT.SegmentQueryMetrics), 1)
	qT.SegmentQueryMetrics[0].CollectTime = nil
	assert.EqualExportedValues(t, qT, expectedResult)

	qT, err = s.GetQueryInfo(qKey, 1)
	assert.NoError(t, err)

	expectedResult = &pbm.TotalQueryData{
		QueryStat: &pbm.QueryStat{
			Hostname: "-1",
			QueryKey: &pbc.QueryKey{
				Ssid: 13223549,
				Ccnt: 6,
				Tmid: int32(1),
			},
			QueryInfo: &pbc.QueryInfo{
				Generator:    pbc.PlanGenerator_PLAN_GENERATOR_OPTIMIZER,
				QueryId:      3844438981,
				PlanId:       1250317044,
				UserName:     "user1",
				DatabaseName: "db1",
				PlanText:     "Result  (cost=0.00..0.00 rows=1 width=4)\n  Output: 42\n  ->  Result  (cost=0.00..0.00 rows=1 width=1)\n        Output: true\n",
				QueryText:    "select 42;",
				StartTime:    timestamppb.New(time.Unix(1763388103, 0)),
				EndTime:      timestamppb.New(time.Unix(1763388103, 0)),
				SubmitTime:   timestamppb.New(time.Unix(1763388103, 0)),
			},
			StatKind:          pbm.StatKind_SK_PRECISE,
			QueryStatus:       pbc.QueryStatus_QUERY_STATUS_DONE,
			StartTime:         timestamppb.New(time.Unix(1763388103, 0)),
			EndTime:           timestamppb.New(time.Unix(1763388103, 0)),
			Completed:         true,
			Slices:            0,
			TotalQueryMetrics: &pbc.GPMetrics{},
		},

		SegmentQueryMetrics: []*pbm.SegmentMetrics{},
	}
	qT.QueryStat.CollectTime = nil
	assert.Equal(t, len(qT.SegmentQueryMetrics), 0)
	assert.EqualExportedValues(t, qT, expectedResult)

}

func TestMergeSegmentData(t *testing.T) {
	startQ := timestamppb.New(time.Now().Add(time.Duration(-1) * time.Hour))
	endQ := timestamppb.New(time.Now())
	s := NewRunningQueriesStorage()
	qInfo := pb.GetQueriesInfoResponse{
		QueriesData: []*pb.QueryData{
			{
				QueryStatus: pbc.QueryStatus_QUERY_STATUS_DONE,
				QueryStart:  startQ,
				QueryEnd:    endQ,
				QueryKey: &pbc.QueryKey{
					Ssid: 1,
					Ccnt: 1,
				},
				SegmentKey: &pbc.SegmentKey{
					Segindex: 0,
				},
				QueryInfo: &pbc.QueryInfo{
					QueryText: "SELECT segment",
				},
				QueryMetrics: &pbc.GPMetrics{
					SystemStat: &pbc.SystemStat{UserTimeSeconds: 2},
				},
				AdditionalStat: &pbc.AdditionalQueryStat{
					ErrorMessage: "All Ok",
				},
				SliceId: 0,
			},
		},
	}
	err := s.MergeSegmentData(&qInfo)
	assert.NoError(t, err)
	// do not store info if there are no sessions
	assert.Equal(t, len(s.runningQueries), 0)

	newQuery, err := s.StoreInfoInStorage(
		&NodeKey{QKey: QueryKey{Ssid: 1, Ccnt: 1}, SKey: SegmentKey{Segindex: -1}, SliceID: 0},
		int32(pbc.QueryStatus_QUERY_STATUS_DONE),
		MeasuredQueryTimes{
			QueryStart: timestamppb.New(time.Now().Add(time.Duration(-10) * time.Minute)),
			QueryEnd:   timestamppb.New(time.Now()),
		},
		&pbc.QueryInfo{QueryText: "SELECT master"},
		nil,
		&pbc.GPMetrics{Instrumentation: &pbc.MetricInstrumentation{Nloops: 1}},
	)
	assert.NoError(t, err)
	assert.Equal(t, newQuery, true)
	err = s.MergeSegmentData(&qInfo)
	assert.NoError(t, err)
	assert.Equal(t, len(s.runningQueries), 1)
	qData, ok := s.runningQueries[QueryKey{Ssid: 1, Ccnt: 1}]
	assert.True(t, ok)
	assert.Equal(t, qData.Completed, true)
	masterData, okMaster := qData.QueriesData[NodeKey{QKey: QueryKey{Ssid: 1, Ccnt: 1}, SKey: SegmentKey{Segindex: -1}, SliceID: 0}]
	assert.True(t, okMaster)
	segmentData, okSegment := qData.QueriesData[NodeKey{QKey: QueryKey{Ssid: 1, Ccnt: 1}, SKey: SegmentKey{Segindex: 0}, SliceID: 0}]
	assert.True(t, okSegment)
	assert.Equal(t, masterData.CurrentStatus, int32(pbc.QueryStatus_QUERY_STATUS_DONE))
	assert.Equal(t, segmentData.CurrentStatus, int32(pbc.QueryStatus_QUERY_STATUS_DONE))
	assert.Equal(t, masterData.QueryInfo.QueryText, "SELECT master")
	assert.Equal(t, segmentData.QueryInfo.QueryText, "SELECT segment")
	assert.Equal(t, masterData.QueryMetrics.Instrumentation.Nloops, uint64(1))
	assert.Equal(t, segmentData.QueryMetrics.SystemStat.UserTimeSeconds, float64(2))
	assert.Equal(t, segmentData.QueryMetrics.SystemStat.RunningTimeSeconds, float64(2))
}

func TestAggregateQueryStat(t *testing.T) {
	startQ := timestamppb.New(time.Now().Add(time.Duration(-1) * time.Hour))
	endQ := timestamppb.New(time.Now())

	s := NewRunningQueriesStorage()
	newQuery, err := s.StoreInfoInStorage(
		&NodeKey{QKey: QueryKey{Ssid: 1, Ccnt: 1}, SKey: SegmentKey{Segindex: -1}, SliceID: 0},
		int32(pbc.QueryStatus_QUERY_STATUS_DONE),
		MeasuredQueryTimes{
			QueryStart: startQ,
			QueryEnd:   endQ,
		},
		&pbc.QueryInfo{QueryText: "SELECT master"},
		nil,
		&pbc.GPMetrics{Instrumentation: &pbc.MetricInstrumentation{Nloops: 1}},
	)
	assert.NoError(t, err)
	assert.Equal(t, newQuery, true)

	qInfo := pb.GetQueriesInfoResponse{
		QueriesData: []*pb.QueryData{
			{
				QueryStatus: pbc.QueryStatus_QUERY_STATUS_DONE,
				QueryStart:  startQ,
				QueryEnd:    endQ,
				QueryKey: &pbc.QueryKey{
					Ssid: 1,
					Ccnt: 1,
				},
				SegmentKey: &pbc.SegmentKey{
					Segindex: 0,
				},
				QueryInfo: &pbc.QueryInfo{
					QueryText: "SELECT segment",
				},
				QueryMetrics: &pbc.GPMetrics{
					SystemStat: &pbc.SystemStat{UserTimeSeconds: 1, KernelTimeSeconds: 1},
				},
				AdditionalStat: &pbc.AdditionalQueryStat{
					ErrorMessage: "All Ok",
				},
				SliceId: 0,
			},
		},
	}
	err = s.MergeSegmentData(&qInfo)
	assert.NoError(t, err)

	qT := &pbm.TotalQueryData{
		QueryStat:           &pbm.QueryStat{},
		SegmentQueryMetrics: make([]*pbm.SegmentMetrics, 0),
	}
	qKey := QueryKey{Ssid: 1, Ccnt: 1}
	qData, ok := s.runningQueries[qKey]
	assert.True(t, ok)
	err = AggregateQueryStat(qT, qKey, qData, 1, AggSegmentHost)
	assert.NoError(t, err)

	assert.Equal(t, qT.QueryStat.TotalQueryMetrics.Instrumentation.Nloops, uint64(1))
	assert.Equal(t, qT.QueryStat.TotalQueryMetrics.SystemStat.RunningTimeSeconds, float64(2))
	assert.Equal(t, qT.QueryStat.TotalQueryMetrics.SystemStat.UserTimeSeconds, float64(1))
	assert.Equal(t, qT.QueryStat.TotalQueryMetrics.SystemStat.KernelTimeSeconds, float64(1))
	assert.Equal(t, qT.QueryStat.QueryInfo.QueryText, "SELECT master")
	assert.Equal(t, qT.QueryStat.Message, "All Ok")
	assert.Equal(t, len(qT.SegmentQueryMetrics), 2)

	sort.Slice(qT.SegmentQueryMetrics, func(i int, j int) bool {
		return qT.SegmentQueryMetrics[i].SegmentKey.Segindex < qT.SegmentQueryMetrics[j].SegmentKey.Segindex
	})
	assert.Equal(t, qT.SegmentQueryMetrics[0].SegmentKey.Segindex, int32(-1))
	assert.Equal(t, qT.SegmentQueryMetrics[0].SegmentMetrics.Instrumentation.Nloops, uint64(1))
	assert.Nil(t, qT.SegmentQueryMetrics[0].SegmentMetrics.SystemStat)

	assert.Equal(t, qT.SegmentQueryMetrics[1].SegmentKey.Segindex, int32(0))
	assert.Equal(t, qT.SegmentQueryMetrics[1].SegmentMetrics.SystemStat.RunningTimeSeconds, float64(2))
	assert.Equal(t, qT.SegmentQueryMetrics[1].SegmentMetrics.SystemStat.UserTimeSeconds, float64(1))
	assert.Equal(t, qT.SegmentQueryMetrics[1].SegmentMetrics.SystemStat.KernelTimeSeconds, float64(1))
	assert.Nil(t, qT.SegmentQueryMetrics[1].SegmentMetrics.Instrumentation)
}
