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
	"slices"
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

func TestGetGPExtensionsWithoutDatabaseName(t *testing.T) {
	ctrl := gomock.NewController(t)
	sessionMocker := NewMockStatActivityLister(ctrl)
	clientSet, cleanup := setupGRPCClientSet(t, sessionMocker)
	defer cleanup()

	oldCachedItem, hadCachedItem := gp.CachedItems[gp.ExtensionsConfig]
	gp.CachedItems[gp.ExtensionsConfig] = &gp.CacheItem{
		ItemValue: gp.AllDatabaseExtensions{
			"db1": {
				Extensions: []gp.PgExtension{
					{
						ExtName:        "ext1",
						ExtOwner:       "owner1",
						ExtNamespace:   "public",
						ExtRelocatable: true,
						ExtVersion:     "1.0",
					},
				},
			},
			"db2": {
				Extensions: []gp.PgExtension{
					{
						ExtName:        "ext2",
						ExtOwner:       "owner2",
						ExtNamespace:   "public",
						ExtRelocatable: false,
						ExtVersion:     "2.0",
					},
				},
			},
		},
		Status:      gp.CacheOk,
		RefreshDate: time.Now(),
	}
	t.Cleanup(func() {
		if hadCachedItem {
			gp.CachedItems[gp.ExtensionsConfig] = oldCachedItem
		} else {
			delete(gp.CachedItems, gp.ExtensionsConfig)
		}
	})

	response, err := clientSet.GetGetGPInfoClient().GetGPExtensions(context.Background(), &pbm.GetGPExtensionsReq{})
	require.NoError(t, err)
	require.Len(t, response.Databases, 2)

	databaseNames := make([]string, 0, len(response.Databases))
	for _, db := range response.Databases {
		databaseNames = append(databaseNames, db.DatabaseName)
	}
	slices.Sort(databaseNames)
	assert.Equal(t, []string{"db1", "db2"}, databaseNames)
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

func TestGetGPSessions_SpillStat(t *testing.T) {
	ctrl := gomock.NewController(t)
	sessionMocker := NewMockStatActivityLister(ctrl)
	clientSet, cleanup := setupGRPCClientSet(t, sessionMocker)
	defer cleanup()

	sessID := int64(42)
	pid := int64(1234)
	base := time.Now().Add(-time.Second)

	// Register two procfs snapshots for the session so that the diff
	// (get5Min) works. ProcSpill is a snapshot value (taken from last),
	// so both snapshots carry spill data; only the latest matters.
	clientSet.procfsStorage.RegisterProcfsStat(base, []*pbc.GpPidProcInfo{
		{
			GpSegmentId: 0, SessId: sessID, Pid: pid,
			ProcStat:  &pbc.ProcStat{Utime: 0},
			ProcSpill: &pbc.ProcSpill{Size: 0, Files: 0},
		},
	})
	clientSet.procfsStorage.RegisterProcfsStat(base.Add(time.Second), []*pbc.GpPidProcInfo{
		{
			GpSegmentId: 0, SessId: sessID, Pid: pid,
			ProcStat:  &pbc.ProcStat{Utime: 10},
			ProcSpill: &pbc.ProcSpill{Size: 8192, Files: 4},
		},
	})

	// Seed the session and trigger procfs recalculation.
	sessionMocker.EXPECT().List(gomock.Any()).Return([]*gp.GpStatActivity{
		{SessID: int(sessID), Pid: int(pid), Usename: "testuser", Datname: "testdb"},
	}, nil)
	sessionMocker.EXPECT().ListAllSessions(gomock.Any()).AnyTimes()

	_, err := clientSet.GetGetGPInfoClient().GetGPSessions(context.Background(), &pbm.GetGPSessionsReq{})
	require.NoError(t, err)

	// Issue a second request now that the session storage is populated.
	sessionMocker.EXPECT().List(gomock.Any()).Return([]*gp.GpStatActivity{
		{SessID: int(sessID), Pid: int(pid), Usename: "testuser", Datname: "testdb"},
	}, nil)
	sessionMocker.EXPECT().ListAllSessions(gomock.Any()).AnyTimes()

	response, err := clientSet.GetGetGPInfoClient().GetGPSessions(context.Background(), &pbm.GetGPSessionsReq{})
	require.NoError(t, err)
	require.Len(t, response.SessionsState, 1)

	lastMetrics := response.SessionsState[0].LastMetrics
	require.NotNil(t, lastMetrics, "LastMetrics should be populated after procfs recalculation")
	require.NotNil(t, lastMetrics.Spill, "SpillInfo should be populated from ProcSpill data")
	assert.Equal(t, int32(4), lastMetrics.Spill.FileCount)
	assert.Equal(t, int64(8192), lastMetrics.Spill.TotalBytes)
}

func TestGetGPQueryRunningMatrics(t *testing.T) {
	ctrl := gomock.NewController(t)
	sessionMocker := NewMockStatActivityLister(ctrl)
	clientSet, cleanup := setupGRPCClientSet(t, sessionMocker)
	defer cleanup()

	queryKey := &pbc.QueryKey{Ssid: 77, Ccnt: 5}
	base := time.Now().Add(-time.Second)
	clientSet.procfsStorage.RegisterProcfsStatWithDataQuality(base, []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 77, Pid: 10, Ccnt: 5, SliceId: 1, Cmdline: "postgres: con77 cmd5 SELECT", ProcStat: &pbc.ProcStat{Utime: 0}, ProcStatus: &pbc.ProcStatus{VmPeak: 10, VmRss: 5}, ProcIo: &pbc.ProcIO{ReadBytes: 0}},
		{GpSegmentId: 1, SessId: 77, Pid: 11, Ccnt: 5, SliceId: 1, Cmdline: "postgres: con77 cmd5 SELECT", ProcStat: &pbc.ProcStat{Utime: 0}, ProcStatus: &pbc.ProcStatus{VmPeak: 10, VmRss: 5}, ProcIo: &pbc.ProcIO{ReadBytes: 0}},
		{GpSegmentId: 2, SessId: 77, Pid: 12, Ccnt: 6, SliceId: 1, Cmdline: "postgres: con77 cmd6 SELECT", ProcStat: &pbc.ProcStat{Utime: 0}, ProcStatus: &pbc.ProcStatus{VmPeak: 10, VmRss: 5}, ProcIo: &pbc.ProcIO{ReadBytes: 0}},
	}, 3, 2)
	clientSet.procfsStorage.RegisterProcfsStatWithDataQuality(base.Add(time.Second), []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 77, Pid: 10, Ccnt: 5, SliceId: 1, Cmdline: "postgres: con77 cmd5 SELECT", ProcStat: &pbc.ProcStat{Utime: 10, Stime: 1}, ProcStatus: &pbc.ProcStatus{VmPeak: 100, VmRss: 50}, ProcIo: &pbc.ProcIO{ReadBytes: 100}, ProcSpill: &pbc.ProcSpill{Size: 1000, Files: 1}},
		{GpSegmentId: 1, SessId: 77, Pid: 11, Ccnt: 5, SliceId: 1, Cmdline: "postgres: con77 cmd5 SELECT", ProcStat: &pbc.ProcStat{Utime: 40, Stime: 2}, ProcStatus: &pbc.ProcStatus{VmPeak: 200, VmRss: 100}, ProcIo: &pbc.ProcIO{ReadBytes: 400}, ProcSpill: &pbc.ProcSpill{Size: 4000, Files: 4}},
		{GpSegmentId: 2, SessId: 77, Pid: 12, Ccnt: 6, SliceId: 1, Cmdline: "postgres: con77 cmd6 SELECT", ProcStat: &pbc.ProcStat{Utime: 10000, Stime: 10000}, ProcStatus: &pbc.ProcStatus{VmPeak: 10000, VmRss: 10000}, ProcIo: &pbc.ProcIO{ReadBytes: 10000}},
	}, 3, 2)

	response, err := clientSet.GetGetGPInfoClient().GetGPQueryRunningMatrics(context.Background(), &pbm.GetGPQueryRunningMatricsReq{QueryKey: queryKey})
	require.NoError(t, err)

	require.Len(t, response.SliceId, 2)
	require.Len(t, response.Segindex, 2)
	require.Len(t, response.CellMetrics, 2)
	assert.Equal(t, []int64{1, 1}, response.SliceId)
	assert.Equal(t, []int32{0, 1}, response.Segindex)
	assert.Equal(t, int64(10), response.CellMetrics[0].RuntimeMetrics.Utime)
	assert.Equal(t, int64(40), response.CellMetrics[1].RuntimeMetrics.Utime)
	assert.Equal(t, int64(400), response.CellMetrics[1].RuntimeMetrics.ProcIo.ReadBytes)
	assert.Equal(t, int64(4000), response.CellMetrics[1].RuntimeMetrics.ProcSpill.Size)

	require.NotNil(t, response.Skew)
	assert.InDelta(t, 0.369047619, response.Skew.Skew, 0.000001)
	assert.Equal(t, int32(1), response.Skew.Segindex)

	require.NotNil(t, response.DataQuality)
	assert.Equal(t, int64(3), response.DataQuality.SegmentsExpected)
	assert.Equal(t, int64(2), response.DataQuality.SegmentsReceived)
	assert.True(t, response.DataQuality.IsPartial)
}

func TestGetGPHostsRunningQueries(t *testing.T) {
	ctrl := gomock.NewController(t)
	sessionMocker := NewMockStatActivityLister(ctrl)
	clientSet, cleanup := setupGRPCClientSet(t, sessionMocker)
	defer cleanup()

	storage.SetHostnameForSegindex(0, "grpc-host")
	base := time.Now().Add(-time.Second)
	clientSet.procfsStorage.RegisterProcfsStatWithHostStat(base, []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 88, Pid: 10, Ccnt: 9, SliceId: 1, State: "SELECT", ProcStat: &pbc.ProcStat{Utime: 0}, ProcStatus: &pbc.ProcStatus{VmRss: 10}, ProcIo: &pbc.ProcIO{ReadBytes: 0}},
	}, storage.HostStatMap{"grpc-host": {LoadAvg: &storage.HostLoadAvg{Avg5: 1.5}, CPUUsage: 0.1}}, 1, 1)
	clientSet.procfsStorage.RegisterProcfsStatWithHostStat(base.Add(time.Second), []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 88, Pid: 10, Ccnt: 9, SliceId: 1, State: "SELECT", ProcStat: &pbc.ProcStat{Utime: 20}, ProcStatus: &pbc.ProcStatus{VmRss: 100}, ProcIo: &pbc.ProcIO{ReadBytes: 200, WriteBytes: 20}, ProcSpill: &pbc.ProcSpill{Size: 300}},
	}, storage.HostStatMap{"grpc-host": {LoadAvg: &storage.HostLoadAvg{Avg5: 2.5}, CPUUsage: 0.6}}, 1, 1)

	response, err := clientSet.GetGetGPInfoClient().GetGPHostsRunningQueries(context.Background(), &pbm.GetGPHostsRunningQueriesReq{})
	require.NoError(t, err)
	require.Len(t, response.Hosts, 1)
	host := response.Hosts[0]
	assert.Equal(t, "grpc-host", host.HostName)
	assert.Equal(t, []int32{0}, host.Segindex)
	assert.Equal(t, int64(1), host.ActiveQueries)
	assert.Equal(t, int64(1), host.ActiveSlices)
	assert.Equal(t, 0.6, host.CpuUsage)
	assert.Equal(t, 2.5, host.Avg5)
	assert.Equal(t, int64(100), host.MemoryUsage)
	assert.Equal(t, int64(200), host.DiskReads)
	assert.Equal(t, int64(20), host.DiskWrites)
	assert.Equal(t, int64(220), host.DiskUsage)
	assert.Equal(t, int64(300), host.SpillBytes)
	require.NotNil(t, host.DataQuality)
	assert.False(t, host.DataQuality.IsPartial)
}

func TestGetGPHostRunningQueries(t *testing.T) {
	ctrl := gomock.NewController(t)
	sessionMocker := NewMockStatActivityLister(ctrl)
	clientSet, cleanup := setupGRPCClientSet(t, sessionMocker)
	defer cleanup()

	storage.SetHostnameForSegindex(0, "grpc-detail-host")
	base := time.Now().Add(-time.Second)
	clientSet.procfsStorage.RegisterProcfsStatWithDataQuality(base, []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 99, Pid: 10, Ccnt: 3, SliceId: 1, State: "SELECT waiting", ProcStat: &pbc.ProcStat{Utime: 0}, ProcStatus: &pbc.ProcStatus{VmRss: 0}, ProcIo: &pbc.ProcIO{}},
		{GpSegmentId: 0, SessId: 100, Pid: 20, Ccnt: storage.UnsetSliceId, SliceId: storage.UnsetSliceId, State: "idle", ProcStat: &pbc.ProcStat{Utime: 0}, ProcStatus: &pbc.ProcStatus{VmRss: 0}, ProcIo: &pbc.ProcIO{}},
	}, 1, 1)
	clientSet.procfsStorage.RegisterProcfsStatWithDataQuality(base.Add(time.Second), []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 99, Pid: 10, Ccnt: 3, SliceId: 1, State: "SELECT waiting", ProcStat: &pbc.ProcStat{Utime: 9}, ProcStatus: &pbc.ProcStatus{VmRss: 90}, ProcIo: &pbc.ProcIO{ReadBytes: 900}},
		{GpSegmentId: 0, SessId: 100, Pid: 20, Ccnt: storage.UnsetSliceId, SliceId: storage.UnsetSliceId, State: "idle", ProcStat: &pbc.ProcStat{Utime: 1}, ProcStatus: &pbc.ProcStatus{VmRss: 10}, ProcIo: &pbc.ProcIO{ReadBytes: 100}},
	}, 1, 1)
	_, err := clientSet.SetQueryInfoClient().SetMetricQuery(context.Background(), &pb.SetQueryReq{
		QueryStatus: pbc.QueryStatus_QUERY_STATUS_START,
		Datetime:    timestamppb.New(base.Add(time.Second)),
		QueryKey:    &pbc.QueryKey{Ssid: 99, Ccnt: 3},
		SegmentKey:  &pbc.SegmentKey{Segindex: -1},
		QueryInfo: &pbc.QueryInfo{
			UserName:     "host_user",
			DatabaseName: "host_db",
			QueryText:    "select * from host_detail_source",
		},
	})
	require.NoError(t, err)

	response, err := clientSet.GetGetGPInfoClient().GetGPHostRunningQueries(context.Background(), &pbm.GetGPHostRunningQueriesReq{
		HostName: "grpc-detail-host",
		PageSize: 1,
	})
	require.NoError(t, err)
	assert.Equal(t, "1", response.NextPageToken)
	require.Equal(t, "grpc-detail-host", response.HostName)
	require.Len(t, response.Queries, 1)
	active := response.Queries[0]
	assert.False(t, active.IsIdle)
	assert.Equal(t, int32(99), active.QueryKey.Ssid)
	assert.Equal(t, int32(3), active.QueryKey.Ccnt)
	assert.Equal(t, "SELECT waiting", active.State)
	assert.Equal(t, "host_user", active.UserName)
	assert.Equal(t, "host_db", active.DbName)
	assert.Equal(t, "select * from host_detail_source", active.QueryText)
	require.NotNil(t, active.RuntimeMetrics)
	assert.Equal(t, int64(9), active.RuntimeMetrics.Utime)

	response, err = clientSet.GetGetGPInfoClient().GetGPHostRunningQueries(context.Background(), &pbm.GetGPHostRunningQueriesReq{
		HostName:  "grpc-detail-host",
		PageSize:  1,
		PageToken: response.NextPageToken,
	})
	require.NoError(t, err)
	assert.Empty(t, response.NextPageToken)
	require.Len(t, response.Queries, 1)
	idle := response.Queries[0]
	assert.True(t, idle.IsIdle)
	assert.Equal(t, int32(100), idle.QueryKey.Ssid)
	assert.Equal(t, int32(storage.UnsetSliceId), idle.QueryKey.Ccnt)
	assert.Equal(t, "idle", idle.State)

	_, err = clientSet.GetGetGPInfoClient().GetGPHostRunningQueries(context.Background(), &pbm.GetGPHostRunningQueriesReq{})
	require.Error(t, err)
}

func TestGetGPQueryRunningMatricsReturnsEmptyOnNoProcfsData(t *testing.T) {
	ctrl := gomock.NewController(t)
	sessionMocker := NewMockStatActivityLister(ctrl)
	clientSet, cleanup := setupGRPCClientSet(t, sessionMocker)
	defer cleanup()

	response, err := clientSet.GetGetGPInfoClient().GetGPQueryRunningMatrics(context.Background(), &pbm.GetGPQueryRunningMatricsReq{
		QueryKey: &pbc.QueryKey{Ssid: 77, Ccnt: 5},
	})

	require.NoError(t, err)
	require.NotNil(t, response)
	assert.Empty(t, response.SliceId)
	assert.Empty(t, response.Segindex)
	assert.Empty(t, response.CellMetrics)
	assert.Nil(t, response.Skew)
	assert.Nil(t, response.DataQuality)
}
