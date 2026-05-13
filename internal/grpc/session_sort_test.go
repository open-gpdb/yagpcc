package grpc_test

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/grpc"
)

func TestSort(t *testing.T) {
	sessState := make([]*pbc.SessionState, 0)
	sessState = append(sessState, &pbc.SessionState{
		SessionKey:   &pbc.SessionKey{SessId: 1},
		SessionInfo:  &pbc.SessionInfo{State: "active"},
		TotalMetrics: &pbc.GPMetrics{SystemStat: &pbc.SystemStat{RunningTimeSeconds: 1}},
		LastMetrics:  &pbc.GPMetrics{SystemStat: &pbc.SystemStat{RunningTimeSeconds: 1}},
	})
	sessState = append(sessState, &pbc.SessionState{
		SessionKey:   &pbc.SessionKey{SessId: 2},
		SessionInfo:  &pbc.SessionInfo{State: "active"},
		TotalMetrics: &pbc.GPMetrics{SystemStat: &pbc.SystemStat{RunningTimeSeconds: 2}},
		LastMetrics:  &pbc.GPMetrics{SystemStat: &pbc.SystemStat{RunningTimeSeconds: 2}},
	})
	sortFields := []*pbm.SessionFieldWrapper{
		{
			FieldName: pbm.SessionField_TOTAL_RUNNINGTIMESECONDS,
			Order:     pbm.SortOrder_SORT_DESC,
		},
	}
	err := grpc.SortResult(&sessState, sortFields)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, sessState[0].SessionKey.SessId, int64(2))
	assert.Equal(t, sessState[0].TotalMetrics.SystemStat.RunningTimeSeconds, float64(2))
	assert.Equal(t, sessState[1].SessionKey.SessId, int64(1))
	assert.Equal(t, sessState[1].TotalMetrics.SystemStat.RunningTimeSeconds, float64(1))
	r := rand.New(rand.NewSource(42))
	for i := 0; i < 1000; i++ {
		sessState = append(sessState, &pbc.SessionState{
			SessionKey:   &pbc.SessionKey{SessId: int64(i + 3)},
			TotalMetrics: &pbc.GPMetrics{SystemStat: &pbc.SystemStat{RunningTimeSeconds: r.Float64()}},
		})
	}
	err = grpc.SortResult(&sessState, sortFields)
	if err != nil {
		t.Error(err)
	}
	for ind, sess := range sessState {
		if ind == 0 {
			continue
		}
		assert.True(t, sess.TotalMetrics.SystemStat.RunningTimeSeconds <= sessState[ind-1].TotalMetrics.SystemStat.RunningTimeSeconds)
	}
	sortFields = []*pbm.SessionFieldWrapper{
		{
			FieldName: pbm.SessionField_LAST_RUNNINGTIMESECONDS,
			Order:     pbm.SortOrder_SORT_DESC,
		},
		{
			FieldName: pbm.SessionField_SESSION_FIELD_STATE,
			Order:     pbm.SortOrder_SORT_ASC,
		},
		{
			FieldName: pbm.SessionField_SESSION_FIELD_KEY,
			Order:     pbm.SortOrder_SORT_ASC,
		},
	}
	sessState = make([]*pbc.SessionState, 0)
	sessState = append(sessState, &pbc.SessionState{
		SessionKey:   &pbc.SessionKey{SessId: 6717846},
		SessionInfo:  &pbc.SessionInfo{State: "idle"},
		TotalMetrics: &pbc.GPMetrics{SystemStat: &pbc.SystemStat{RunningTimeSeconds: 0}},
		LastMetrics:  &pbc.GPMetrics{SystemStat: &pbc.SystemStat{RunningTimeSeconds: 1}},
	})
	sessState = append(sessState, &pbc.SessionState{
		SessionKey:   &pbc.SessionKey{SessId: 6672194},
		SessionInfo:  &pbc.SessionInfo{State: "idle in transaction"},
		TotalMetrics: &pbc.GPMetrics{SystemStat: &pbc.SystemStat{RunningTimeSeconds: 0}},
		LastMetrics:  &pbc.GPMetrics{SystemStat: &pbc.SystemStat{RunningTimeSeconds: 195}},
	})
	sessState = append(sessState, &pbc.SessionState{
		SessionKey:   &pbc.SessionKey{SessId: 6672222},
		SessionInfo:  &pbc.SessionInfo{State: "idle in transaction"},
		TotalMetrics: &pbc.GPMetrics{SystemStat: &pbc.SystemStat{RunningTimeSeconds: 0}},
		LastMetrics:  &pbc.GPMetrics{SystemStat: &pbc.SystemStat{RunningTimeSeconds: 80}},
	})

	sessState = append(sessState, &pbc.SessionState{
		SessionKey:   &pbc.SessionKey{SessId: 1},
		SessionInfo:  &pbc.SessionInfo{State: "idle in transaction"},
		TotalMetrics: &pbc.GPMetrics{},
		LastMetrics:  &pbc.GPMetrics{},
	})

	sessState = append(sessState, &pbc.SessionState{
		SessionKey:   &pbc.SessionKey{SessId: 2},
		SessionInfo:  &pbc.SessionInfo{State: "idle"},
		TotalMetrics: &pbc.GPMetrics{},
		LastMetrics:  &pbc.GPMetrics{},
	})

	err = grpc.SortResult(&sessState, sortFields)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, sessState[0].SessionKey.SessId, int64(6672194))
	assert.Equal(t, sessState[0].LastMetrics.SystemStat.RunningTimeSeconds, float64(195))
	assert.Equal(t, sessState[1].SessionKey.SessId, int64(6672222))
	assert.Equal(t, sessState[1].LastMetrics.SystemStat.RunningTimeSeconds, float64(80))
	assert.Equal(t, sessState[2].SessionKey.SessId, int64(6717846))
	assert.Equal(t, sessState[2].LastMetrics.SystemStat.RunningTimeSeconds, float64(1))
}

func TestSortAggregatedSessionStats(t *testing.T) {
	// Three sessions: distinct aggregate stats; one with nil AggregatedMetrics (treated as zeros).
	makeSessions := func() []*pbc.SessionState {
		return []*pbc.SessionState{
			{
				SessionKey: &pbc.SessionKey{SessId: 1},
				AggregatedMetrics: &pbc.AggregatedMetrics{
					Calls: 1, MinTime: 10, MaxTime: 50, MeanTime: 30, StddevTime: 5, TotalTime: 100,
				},
			},
			{
				SessionKey: &pbc.SessionKey{SessId: 2},
				AggregatedMetrics: &pbc.AggregatedMetrics{
					Calls: 5, MinTime: 1, MaxTime: 200, MeanTime: 100, StddevTime: 20, TotalTime: 500,
				},
			},
			{
				SessionKey:        &pbc.SessionKey{SessId: 3},
				AggregatedMetrics: nil,
			},
		}
	}

	t.Run("by aggregated total_time desc", func(t *testing.T) {
		sessions := makeSessions()
		fields := []*pbm.SessionFieldWrapper{
			{FieldName: pbm.SessionField_SESSION_FIELD_AGGREGATED_TOTAL_TIME, Order: pbm.SortOrder_SORT_DESC},
		}
		require.NoError(t, grpc.SortResult(&sessions, fields))
		require.Equal(t, int64(2), sessions[0].SessionKey.SessId)
		require.Equal(t, int64(1), sessions[1].SessionKey.SessId)
		require.Equal(t, int64(3), sessions[2].SessionKey.SessId)
	})

	t.Run("by aggregated calls asc", func(t *testing.T) {
		sessions := makeSessions()
		fields := []*pbm.SessionFieldWrapper{
			{FieldName: pbm.SessionField_SESSION_FIELD_AGGREGATED_CALLS, Order: pbm.SortOrder_SORT_ASC},
		}
		require.NoError(t, grpc.SortResult(&sessions, fields))
		require.Equal(t, int64(3), sessions[0].SessionKey.SessId)
		require.Equal(t, int64(1), sessions[1].SessionKey.SessId)
		require.Equal(t, int64(2), sessions[2].SessionKey.SessId)
	})

	t.Run("by aggregated max_time desc", func(t *testing.T) {
		sessions := makeSessions()
		fields := []*pbm.SessionFieldWrapper{
			{FieldName: pbm.SessionField_SESSION_FIELD_AGGREGATED_MAX_TIME, Order: pbm.SortOrder_SORT_DESC},
		}
		require.NoError(t, grpc.SortResult(&sessions, fields))
		require.Equal(t, int64(2), sessions[0].SessionKey.SessId)
		require.Equal(t, int64(1), sessions[1].SessionKey.SessId)
		require.Equal(t, int64(3), sessions[2].SessionKey.SessId)
	})

	t.Run("by aggregated min_time asc", func(t *testing.T) {
		sessions := makeSessions()
		fields := []*pbm.SessionFieldWrapper{
			{FieldName: pbm.SessionField_SESSION_FIELD_AGGREGATED_MIN_TIME, Order: pbm.SortOrder_SORT_ASC},
		}
		require.NoError(t, grpc.SortResult(&sessions, fields))
		// nil AggregatedMetrics sorts as 0 for min_time, so session 3 is first.
		require.Equal(t, int64(3), sessions[0].SessionKey.SessId)
		require.Equal(t, int64(2), sessions[1].SessionKey.SessId)
		require.Equal(t, int64(1), sessions[2].SessionKey.SessId)
	})

	t.Run("by aggregated mean_time desc", func(t *testing.T) {
		sessions := makeSessions()
		fields := []*pbm.SessionFieldWrapper{
			{FieldName: pbm.SessionField_SESSION_FIELD_AGGREGATED_MEAN_TIME, Order: pbm.SortOrder_SORT_DESC},
		}
		require.NoError(t, grpc.SortResult(&sessions, fields))
		require.Equal(t, int64(2), sessions[0].SessionKey.SessId)
		require.Equal(t, int64(1), sessions[1].SessionKey.SessId)
		require.Equal(t, int64(3), sessions[2].SessionKey.SessId)
	})

	t.Run("by aggregated stddev_time desc", func(t *testing.T) {
		sessions := makeSessions()
		fields := []*pbm.SessionFieldWrapper{
			{FieldName: pbm.SessionField_SESSION_FIELD_AGGREGATED_STDDEV_TIME, Order: pbm.SortOrder_SORT_DESC},
		}
		require.NoError(t, grpc.SortResult(&sessions, fields))
		require.Equal(t, int64(2), sessions[0].SessionKey.SessId)
		require.Equal(t, int64(1), sessions[1].SessionKey.SessId)
		require.Equal(t, int64(3), sessions[2].SessionKey.SessId)
	})
}

func TestSortAndFilterSessions(t *testing.T) {
	sessions := []*pbc.SessionState{
		{
			SessionInfo:  &pbc.SessionInfo{User: "test"},
			LastMetrics:  &pbc.GPMetrics{SystemStat: &pbc.SystemStat{CancelledWriteBytes: 10}},
			TotalMetrics: &pbc.GPMetrics{Instrumentation: &pbc.MetricInstrumentation{Sent: &pbc.NetworkStat{TotalBytes: 10}}},
		},
		{
			SessionInfo:  &pbc.SessionInfo{User: "test2"},
			LastMetrics:  &pbc.GPMetrics{SystemStat: &pbc.SystemStat{CancelledWriteBytes: 10}},
			TotalMetrics: &pbc.GPMetrics{Instrumentation: &pbc.MetricInstrumentation{Sent: &pbc.NetworkStat{TotalBytes: 5}}},
		},
		{
			SessionInfo: &pbc.SessionInfo{User: "test"},
			LastMetrics: &pbc.GPMetrics{SystemStat: &pbc.SystemStat{CancelledWriteBytes: 20}},
		},
	}

	t.Run("by user asc", func(t *testing.T) {
		fields := []*pbm.SessionFieldWrapper{
			{FieldName: pbm.SessionField_SESSION_FIELD_USER, Order: pbm.SortOrder_SORT_ASC},
		}

		err := grpc.SortResult(&sessions, fields)

		require.NoError(t, err)
		assert.Equal(t, 3, len(sessions))
		assert.Equal(t, "test", sessions[0].SessionInfo.User)
		assert.Equal(t, "test2", sessions[2].SessionInfo.User)
	})

	t.Run("by user asc and last cancelled write bytes desc", func(t *testing.T) {
		fields := []*pbm.SessionFieldWrapper{
			{FieldName: pbm.SessionField_SESSION_FIELD_USER, Order: pbm.SortOrder_SORT_ASC},
			{FieldName: pbm.SessionField_LAST_CANCELLED_WRITE_BYTES, Order: pbm.SortOrder_SORT_DESC},
		}

		err := grpc.SortResult(&sessions, fields)

		require.NoError(t, err)
		assert.Equal(t, 3, len(sessions))
		assert.Equal(t, uint64(20), sessions[0].LastMetrics.SystemStat.CancelledWriteBytes)
		assert.Equal(t, "test", sessions[0].SessionInfo.User)
		assert.Equal(t, uint64(10), sessions[1].LastMetrics.SystemStat.CancelledWriteBytes)
		assert.Equal(t, "test", sessions[1].SessionInfo.User)
	})

	t.Run("by total net sent total bytes asc", func(t *testing.T) {
		fields := []*pbm.SessionFieldWrapper{
			{FieldName: pbm.SessionField_TOTAL_NET_SENT_TOTAL_BYTES, Order: pbm.SortOrder_SORT_ASC},
		}
		err := grpc.SortResult(&sessions, fields)

		require.NoError(t, err)
		assert.Equal(t, 3, len(sessions))
		assert.Nil(t, sessions[0].TotalMetrics)
		assert.Equal(t, uint64(5), sessions[1].TotalMetrics.Instrumentation.Sent.TotalBytes)
		assert.Equal(t, uint64(10), sessions[2].TotalMetrics.Instrumentation.Sent.TotalBytes)
	})
}
