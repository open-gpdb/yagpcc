package grpc_test

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pb "github.com/open-gpdb/yagpcc/api/proto/agent_segment"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/gp"
	"github.com/open-gpdb/yagpcc/internal/grpc"
	"github.com/open-gpdb/yagpcc/internal/storage"
	"github.com/open-gpdb/yagpcc/internal/utils"
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

func TestFilterOut(t *testing.T) {
	sessionState := &pbc.SessionState{
		SessionKey: &pbc.SessionKey{SessId: 1},
		SessionInfo: &pbc.SessionInfo{
			Database: "test",
			State:    "active",
		},
	}
	sessionState2 := &pbc.SessionState{
		SessionKey: &pbc.SessionKey{SessId: 1},
		SessionInfo: &pbc.SessionInfo{
			Database: "test",
			State:    "waiting",
		},
	}
	filter1 := []*pbm.SessionFilter{
		{
			FieldName: pbm.SessionFilterEnum_SESSION_FILTER_DATABASE,
			Value:     "test",
		},
	}
	filter := grpc.FilterOutSession(filter1, sessionState)
	assert.Equal(t, filter, false)
	filter2 := []*pbm.SessionFilter{
		{
			FieldName: pbm.SessionFilterEnum_SESSION_FILTER_DATABASE,
			Value:     "test1",
		},
		{
			FieldName: pbm.SessionFilterEnum_SESSION_FILTER_STATE,
			Value:     "SESSION_STATUS_ACTIVE",
		},
	}
	filter = grpc.FilterOutSession(filter2, sessionState)
	assert.Equal(t, filter, true)
	filter3 := []*pbm.SessionFilter{
		{
			FieldName: pbm.SessionFilterEnum_SESSION_FILTER_DATABASE,
			Value:     "test",
		},
		{
			FieldName: pbm.SessionFilterEnum_SESSION_FILTER_STATE,
			Value:     "SESSION_STATUS_ACTIVE",
		},
		{
			FieldName: pbm.SessionFilterEnum_SESSION_FILTER_STATE,
			Value:     "SESSION_STATUS_WAITING",
		},
	}
	filter = grpc.FilterOutSession(filter3, sessionState)
	assert.Equal(t, filter, false)
	filter = grpc.FilterOutSession(filter3, sessionState2)
	assert.Equal(t, filter, false)
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
		assert.Equal(t, uint32(5), sessions[1].TotalMetrics.Instrumentation.Sent.TotalBytes)
		assert.Equal(t, uint32(10), sessions[2].TotalMetrics.Instrumentation.Sent.TotalBytes)
	})
}

func TestMasterMethods(t *testing.T) {
	ctrl := gomock.NewController(t)
	sessionMocker := NewMockStatActivityLister(ctrl)
	clientSet, cleanup := setupGRPCClientSet(t, sessionMocker)
	defer cleanup()

	t.Run("setup", func(t *testing.T) {
		startQuery := timestamppb.New(time.Now().Add(time.Duration(-1) * time.Hour))
		for _, request := range []*pb.SetQueryReq{
			{
				QueryStatus: pbc.QueryStatus_QUERY_STATUS_END,
				Datetime:    startQuery,
				QueryKey:    &pbc.QueryKey{Ssid: 1},
				SegmentKey:  &pbc.SegmentKey{Segindex: -1},
				QueryInfo:   &pbc.QueryInfo{UserName: "test", DatabaseName: "test"},
				QueryMetrics: &pbc.GPMetrics{
					Instrumentation: &pbc.MetricInstrumentation{
						Ntuples:      1,
						Interconnect: &pbc.InterconnectStat{Retransmits: 40},
					},
				},
			},
			{
				QueryStatus: pbc.QueryStatus_QUERY_STATUS_DONE,
				Datetime:    startQuery,
				QueryKey:    &pbc.QueryKey{Ssid: 1},
				SegmentKey:  &pbc.SegmentKey{Segindex: -1},
			},
			{
				QueryStatus:  pbc.QueryStatus_QUERY_STATUS_END,
				Datetime:     startQuery,
				QueryKey:     &pbc.QueryKey{Ssid: 3},
				SegmentKey:   &pbc.SegmentKey{Segindex: -1},
				QueryInfo:    &pbc.QueryInfo{UserName: "test", DatabaseName: "test2"},
				QueryMetrics: &pbc.GPMetrics{Instrumentation: &pbc.MetricInstrumentation{Ntuples: 2}},
			},
		} {
			_, err := clientSet.SetQueryInfoClient().SetMetricQuery(context.Background(), request)
			require.NoError(t, err)
		}
	})

	t.Run("get gp query", func(t *testing.T) {
		request := &pbm.GetGPQueryReq{QueryKey: &pbc.QueryKey{Ssid: 1}}
		response, err := clientSet.GetGetGPInfoClient().GetGPQuery(context.Background(), request)

		require.NoError(t, err)
		assert.Equal(t, int32(1), response.QueriesData.QueryStat.QueryKey.Ssid)
		assert.Equal(t, "test", response.QueriesData.QueryStat.QueryInfo.UserName)

		expectedTotalQueryMetrics := &pbc.GPMetrics{
			Instrumentation: &pbc.MetricInstrumentation{
				Ntuples:      1,
				Interconnect: &pbc.InterconnectStat{Retransmits: 40},
			},
		}
		utils.AssertProtoMessagesEqual(t, expectedTotalQueryMetrics, response.QueriesData.QueryStat.TotalQueryMetrics)
	})

	// archive query to aggregate data to session
	qKey := storage.QueryKey{Ssid: 1}
	qVal, ok := clientSet.backgroundStorage.RQStorage.GetQuery(qKey)
	require.True(t, ok)
	_, err := clientSet.backgroundStorage.AggtregateDataToQueryAndSession(qKey, qVal)
	require.NoError(t, err)

	// now we could query session data
	t.Run("get gp session by id", func(t *testing.T) {
		request := &pbm.GetGPSessionReq{SessionKey: &pbc.SessionKey{SessId: 1}}

		response, err := clientSet.GetGetGPInfoClient().GetGPSession(context.Background(), request)

		require.NoError(t, err)
		assert.Equal(t, int64(1), response.SessionsState.SessionKey.SessId)
		assert.Equal(t, "test", response.SessionsState.SessionInfo.User)

		expectedTotalMetrics := &pbc.GPMetrics{
			Instrumentation: &pbc.MetricInstrumentation{
				Ntuples:      1,
				Interconnect: &pbc.InterconnectStat{Retransmits: 40},
			},
		}
		utils.AssertProtoMessagesEqual(t, expectedTotalMetrics, response.SessionsState.TotalMetrics)
	})

	t.Run("get gp session with parameters", func(t *testing.T) {
		request := &pbm.GetGPSessionsReq{
			Field: []*pbm.SessionFieldWrapper{
				{FieldName: pbm.SessionField_SESSION_FIELD_DATABASE, Order: pbm.SortOrder_SORT_ASC},
				{FieldName: pbm.SessionField_LAST_BLK_WRITE_TIME, Order: pbm.SortOrder_SORT_DESC},
				{FieldName: pbm.SessionField_LAST_BLK_READ_TIME, Order: pbm.SortOrder_SORT_ORDER_UNSPECIFIED},
			},
			Filter: []*pbm.SessionFilter{
				{FieldName: pbm.SessionFilterEnum_SESSION_FILTER_USER, Value: "test"},
			},
			PageSize:  10,
			PageToken: "0",
		}

		clientSet.getSessionMocker.EXPECT().List(gomock.Any()).Times(1).Return([]*gp.GpStatActivity{
			{
				SessID:  1,
				Usename: "test",
				Datname: "test2",
			},
		}, nil)
		response, err := clientSet.GetGetGPInfoClient().GetGPSessions(context.Background(), request)

		require.NoError(t, err)
		assert.Equal(t, 1, len(response.SessionsState))
		assert.Equal(t, "test", response.SessionsState[0].SessionInfo.User)
		assert.Equal(t, "test2", response.SessionsState[0].SessionInfo.Database)

		// now lets get session with next token - should return empty response
		request.PageToken = "2"
		clientSet.getSessionMocker.EXPECT().List(gomock.Any()).Times(1).Return([]*gp.GpStatActivity{
			{
				SessID:  1,
				Usename: "test",
				Datname: "test2",
			},
		}, nil)
		response, err = clientSet.GetGetGPInfoClient().GetGPSessions(context.Background(), request)

		require.NoError(t, err)
		assert.Equal(t, 0, len(response.SessionsState))
	})
}
