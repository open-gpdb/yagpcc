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

package grpc_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pb "github.com/open-gpdb/yagpcc/api/proto/agent_segment"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/gp"
	"github.com/open-gpdb/yagpcc/internal/grpc"
	"github.com/open-gpdb/yagpcc/internal/storage"
	"github.com/open-gpdb/yagpcc/internal/utils"
)

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

func TestMasterMethods(t *testing.T) {
	ctrl := gomock.NewController(t)
	sessionMocker := NewMockStatActivityLister(ctrl)
	clientSet, cleanup := setupGRPCClientSet(t, sessionMocker)
	defer cleanup()

	var aggQueryResult *pbm.TotalQueryData

	t.Run("setup", func(t *testing.T) {
		startQuery := timestamppb.New(time.Now().Add(time.Duration(-1) * time.Hour))
		addTopLevel := &pbc.AdditionalQueryInfo{NestedLevel: 0}
		for _, request := range []*pb.SetQueryReq{
			{
				QueryStatus: pbc.QueryStatus_QUERY_STATUS_END,
				Datetime:    startQuery,
				QueryKey:    &pbc.QueryKey{Ssid: 1},
				SegmentKey:  &pbc.SegmentKey{Segindex: -1},
				QueryInfo:   &pbc.QueryInfo{UserName: "test", DatabaseName: "test"},
				AddInfo:     addTopLevel,
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
				AddInfo:     addTopLevel,
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
	var err error
	aggQueryResult, err = clientSet.backgroundStorage.AggtregateDataToQueryAndSession(qKey, qVal)
	require.NoError(t, err)
	require.NotNil(t, aggQueryResult)

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

		require.NotNil(t, response.SessionsState.AggregatedMetrics, "GetGPSession must return short-query aggregate stats on SessionState")
		start := utils.GetTimeForTimestamp(aggQueryResult.QueryStat.StartTime)
		end := utils.GetTimeForTimestamp(aggQueryResult.QueryStat.EndTime)
		dur := end.Sub(start)
		if dur < 0 {
			dur = 0
		}
		expectedAgg := &pbc.AggregatedMetrics{}
		require.NoError(t, storage.GroupAggMetrics(expectedAgg, dur))
		utils.AssertProtoMessagesEqual(t, expectedAgg, response.SessionsState.AggregatedMetrics)
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

		require.NotNil(t, response.SessionsState[0].AggregatedMetrics, "GetGPSessions must include aggregated_metrics on SessionState")
		start := utils.GetTimeForTimestamp(aggQueryResult.QueryStat.StartTime)
		end := utils.GetTimeForTimestamp(aggQueryResult.QueryStat.EndTime)
		dur := end.Sub(start)
		if dur < 0 {
			dur = 0
		}
		expectedAgg := &pbc.AggregatedMetrics{}
		require.NoError(t, storage.GroupAggMetrics(expectedAgg, dur))
		utils.AssertProtoMessagesEqual(t, expectedAgg, response.SessionsState[0].AggregatedMetrics)

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
