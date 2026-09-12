package master_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/master"
)

// TestArchiveQueryTrimsSegmentQueryInfo: statement rows keep full QueryInfo,
// segment rows drop the plan/analyze payloads.
func TestArchiveQueryTrimsSegmentQueryInfo(t *testing.T) {
	now := timestamppb.Now()
	full := &pbc.QueryInfo{
		QueryId:      12,
		PlanId:       321,
		UserName:     "bob",
		DatabaseName: "db",
		Generator:    pbc.PlanGenerator_PLAN_GENERATOR_OPTIMIZER,
		QueryText:    "select 1",
		PlanText:     "plan",
		AnalyzeText:  "analyze",
		PlanJson:     `[{"Plan": {}}]`,
		AnalyzeJson:  `[{"Plan": {"Actual Rows": 1}}]`,
	}
	qT := &pbm.TotalQueryData{
		QueryStat: &pbm.QueryStat{
			QueryKey:    &pbc.QueryKey{Ssid: 1, Tmid: 2, Ccnt: 3},
			QueryInfo:   full,
			CollectTime: now,
			StartTime:   now,
			EndTime:     now,
		},
		SegmentQueryMetrics: []*pbm.SegmentMetrics{
			{Hostname: "seg1", SegmentKey: &pbc.SegmentKey{Segindex: 0}, StartTime: now, EndTime: now},
			{Hostname: "seg2", SegmentKey: &pbc.SegmentKey{Segindex: 1}, StartTime: now, EndTime: now},
		},
	}

	queryChan := make(chan *pbm.QueryStatWrite, 1)
	segChan := make(chan *pbm.SegmentMetricsWrite, 2)
	master.ArchiveQuery(qT, "cluster", queryChan, segChan, "master")

	require.Len(t, queryChan, 1)
	require.Len(t, segChan, 2)

	stmt := <-queryChan
	assert.Same(t, full, stmt.QueryInfo, "statement row keeps the full QueryInfo")
	assert.Equal(t, `[{"Plan": {}}]`, stmt.QueryInfo.GetPlanJson())
	assert.Equal(t, `[{"Plan": {"Actual Rows": 1}}]`, stmt.QueryInfo.GetAnalyzeJson())

	for i := 0; i < 2; i++ {
		seg := <-segChan
		qi := seg.QueryInfo
		require.NotNil(t, qi)
		assert.Equal(t, uint64(12), qi.GetQueryId())
		assert.Equal(t, uint64(321), qi.GetPlanId())
		assert.Equal(t, "bob", qi.GetUserName())
		assert.Equal(t, "db", qi.GetDatabaseName())
		assert.Equal(t, pbc.PlanGenerator_PLAN_GENERATOR_OPTIMIZER, qi.GetGenerator())
		assert.Empty(t, qi.GetQueryText())
		assert.Empty(t, qi.GetPlanText())
		assert.Empty(t, qi.GetAnalyzeText())
		assert.Empty(t, qi.GetPlanJson())
		assert.Empty(t, qi.GetAnalyzeJson())
	}
}
