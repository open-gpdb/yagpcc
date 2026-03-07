package grpc_test

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/open-gpdb/yagpcc/api/proto/agent_segment"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
)

func TestSetQueryInfo_SetMetricQuery_Simple(t *testing.T) {
	clientSet, cleanup := setupGRPCClientSet(t, nil)
	defer cleanup()

	for _, tt := range []struct {
		name    string
		request *pb.SetQueryReq
	}{
		{
			name: "initial set",
			request: &pb.SetQueryReq{
				QueryStatus: pbc.QueryStatus_QUERY_STATUS_START,
				QueryKey:    &pbc.QueryKey{Ssid: 0},
				QueryInfo:   &pbc.QueryInfo{QueryText: "Hello"},
				StartTime:   &timestamppb.Timestamp{Seconds: 1740125412, Nanos: 123},
			},
		},
		{
			name: "does not change query",
			request: &pb.SetQueryReq{
				QueryStatus: pbc.QueryStatus_QUERY_STATUS_START,
				QueryKey:    &pbc.QueryKey{Ssid: 0},
				QueryInfo:   &pbc.QueryInfo{QueryText: "Hello"},
				StartTime:   &timestamppb.Timestamp{Seconds: 1740125412, Nanos: 123},
			},
		},
		{
			name: "change query",
			request: &pb.SetQueryReq{
				QueryStatus: pbc.QueryStatus_QUERY_STATUS_START,
				QueryKey:    &pbc.QueryKey{Ssid: 0},
				QueryInfo:   &pbc.QueryInfo{QueryText: "Hello2"},
				StartTime:   &timestamppb.Timestamp{Seconds: 1740125412, Nanos: 123},
			},
		},
		{
			name: "set query metrics q0",
			request: &pb.SetQueryReq{
				QueryStatus: pbc.QueryStatus_QUERY_STATUS_START,
				QueryKey:    &pbc.QueryKey{Ssid: 0},
				QueryMetrics: &pbc.GPMetrics{
					SystemStat: &pbc.SystemStat{Vsize: 10000},
					Instrumentation: &pbc.MetricInstrumentation{
						Interconnect: &pbc.InterconnectStat{TotalRecvQueueSize: 1000},
					},
				},
				StartTime: &timestamppb.Timestamp{Seconds: 1740125419, Nanos: 456},
			},
		},
		{
			name: "set query metrics q1",
			request: &pb.SetQueryReq{
				QueryStatus: pbc.QueryStatus_QUERY_STATUS_START,
				QueryKey:    &pbc.QueryKey{Ssid: 1},
				QueryMetrics: &pbc.GPMetrics{
					SystemStat: &pbc.SystemStat{Vsize: 10001},
				},
				StartTime: &timestamppb.Timestamp{Seconds: 1740125419, Nanos: 456},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			response, err := clientSet.SetQueryInfoClient().SetMetricQuery(context.Background(), tt.request)

			require.NoError(t, err)
			assertMetricResponseIsOk(t, response)
		})
	}

	t.Run("check final state", func(t *testing.T) {
		expectedResponse := &pb.GetQueriesInfoResponse{
			QueriesData: []*pb.QueryData{
				{
					AdditionalStat: &pbc.AdditionalQueryStat{},
					SliceId:        0,
					QueryInfo: &pbc.QueryInfo{
						QueryText: "Hello2",
						StartTime: &timestamppb.Timestamp{Seconds: 1740125419, Nanos: 456},
					},
					QueryKey: &pbc.QueryKey{
						Ssid: 0,
					},
					QueryMetrics: &pbc.GPMetrics{
						SystemStat: &pbc.SystemStat{Vsize: 10000},
						Instrumentation: &pbc.MetricInstrumentation{
							Interconnect: &pbc.InterconnectStat{TotalRecvQueueSize: 1000},
						},
					},
					QueryStart:  &timestamppb.Timestamp{Seconds: 1740125419, Nanos: 456},
					QueryStatus: pbc.QueryStatus_QUERY_STATUS_START,
					SegmentKey:  &pbc.SegmentKey{},
				},
				{
					AdditionalStat: &pbc.AdditionalQueryStat{},
					SliceId:        0,
					QueryKey:       &pbc.QueryKey{Ssid: 1},
					QueryMetrics:   &pbc.GPMetrics{SystemStat: &pbc.SystemStat{Vsize: 10001}},
					QueryStart:     &timestamppb.Timestamp{Seconds: 1740125419, Nanos: 456},
					QueryInfo: &pbc.QueryInfo{
						StartTime: &timestamppb.Timestamp{Seconds: 1740125419, Nanos: 456},
					},
					QueryStatus: pbc.QueryStatus_QUERY_STATUS_START,
					SegmentKey:  &pbc.SegmentKey{},
				},
			},
		}

		response, err := clientSet.GetQueryInfoClient().GetMetricQueries(context.Background(), &pb.GetQueriesInfoReq{})

		require.NoError(t, err, "error getting metric queries")
		assertQueriesInfoResponseEqual(t, expectedResponse, response)
	})
}

func TestSetQueryInfo_SetMetricQuery_MultipleSlices(t *testing.T) {
	clientSet, cleanup := setupGRPCClientSet(t, nil)
	defer cleanup()

	for _, tt := range []struct {
		name    string
		request *pb.SetQueryReq
	}{
		{
			name: "initial set q1",
			request: &pb.SetQueryReq{
				QueryStatus: pbc.QueryStatus_QUERY_STATUS_START,
				QueryKey:    &pbc.QueryKey{Ssid: 0},
				QueryInfo:   &pbc.QueryInfo{QueryText: "Hello1"},
				SegmentKey:  &pbc.SegmentKey{Segindex: 1},
				AddInfo:     &pbc.AdditionalQueryInfo{SliceId: 0},
				StartTime:   &timestamppb.Timestamp{Seconds: 1740125419, Nanos: 123},
			},
		},
		{
			name: "initial set q2",
			request: &pb.SetQueryReq{
				QueryStatus: pbc.QueryStatus_QUERY_STATUS_START,
				QueryKey:    &pbc.QueryKey{Ssid: 1},
				QueryInfo:   &pbc.QueryInfo{QueryText: "Hello2"},
				SegmentKey:  &pbc.SegmentKey{Segindex: 1},
				AddInfo:     &pbc.AdditionalQueryInfo{SliceId: 0},
				StartTime:   &timestamppb.Timestamp{Seconds: 1740125419, Nanos: 123},
			},
		},
		{
			name: "slice1 q1",
			request: &pb.SetQueryReq{
				QueryStatus:  pbc.QueryStatus_QUERY_STATUS_DONE,
				QueryKey:     &pbc.QueryKey{Ssid: 0},
				SegmentKey:   &pbc.SegmentKey{Segindex: 1},
				AddInfo:      &pbc.AdditionalQueryInfo{SliceId: 1},
				QueryMetrics: &pbc.GPMetrics{Instrumentation: &pbc.MetricInstrumentation{InheritedCalls: uint64(10000)}},
				StartTime:    &timestamppb.Timestamp{Seconds: 1740125419, Nanos: 123},
				EndTime:      &timestamppb.Timestamp{Seconds: 1740125419, Nanos: 789},
			},
		},
		{
			name: "slice2 q1",
			request: &pb.SetQueryReq{
				QueryStatus:  pbc.QueryStatus_QUERY_STATUS_DONE,
				QueryKey:     &pbc.QueryKey{Ssid: 0},
				SegmentKey:   &pbc.SegmentKey{Segindex: 1},
				AddInfo:      &pbc.AdditionalQueryInfo{SliceId: 2},
				QueryMetrics: &pbc.GPMetrics{Instrumentation: &pbc.MetricInstrumentation{InheritedCalls: uint64(10002)}},
				StartTime:    &timestamppb.Timestamp{Seconds: 1740125419, Nanos: 123},
				EndTime:      &timestamppb.Timestamp{Seconds: 1740125419, Nanos: 789},
			},
		},
		{
			name: "slice1 q2",
			request: &pb.SetQueryReq{
				QueryStatus:  pbc.QueryStatus_QUERY_STATUS_DONE,
				QueryKey:     &pbc.QueryKey{Ssid: 1},
				SegmentKey:   &pbc.SegmentKey{Segindex: 1},
				AddInfo:      &pbc.AdditionalQueryInfo{SliceId: 1},
				QueryMetrics: &pbc.GPMetrics{Instrumentation: &pbc.MetricInstrumentation{InheritedCalls: uint64(10001)}},
				StartTime:    &timestamppb.Timestamp{Seconds: 1740125419, Nanos: 123},
				EndTime:      &timestamppb.Timestamp{Seconds: 1740125419, Nanos: 789},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			response, err := clientSet.SetQueryInfoClient().SetMetricQuery(context.Background(), tt.request)

			require.NoError(t, err)
			assertMetricResponseIsOk(t, response)
		})
	}

	t.Run("check final state", func(t *testing.T) {
		response, err := clientSet.GetQueryInfoClient().GetMetricQueries(context.Background(), &pb.GetQueriesInfoReq{ClearSent: true})

		require.NoError(t, err)
		assert.Equal(t, 5, len(response.QueriesData))
		sort.Slice(response.QueriesData, func(p, q int) bool {
			if response.QueriesData[p].QueryKey.Ssid == response.QueriesData[q].QueryKey.Ssid {
				return response.QueriesData[p].SliceId < response.QueriesData[q].SliceId
			}
			return response.QueriesData[p].QueryKey.Ssid < response.QueriesData[q].QueryKey.Ssid
		})
		assert.Equal(t, int32(0), response.QueriesData[0].QueryKey.Ssid)
		assert.Equal(t, "Hello1", response.QueriesData[0].QueryInfo.QueryText)

		expectedResponse := &pb.GetQueriesInfoResponse{
			QueriesData: []*pb.QueryData{
				{
					QueryStart:     &timestamppb.Timestamp{Seconds: 1740125419, Nanos: 123},
					QueryInfo:      &pbc.QueryInfo{QueryText: "Hello1", StartTime: &timestamppb.Timestamp{Seconds: 1740125419, Nanos: 123}},
					QueryKey:       &pbc.QueryKey{Ssid: 0},
					QueryStatus:    pbc.QueryStatus_QUERY_STATUS_START,
					SegmentKey:     &pbc.SegmentKey{Segindex: 1},
					SliceId:        0,
					AdditionalStat: &pbc.AdditionalQueryStat{},
					QueryMetrics:   &pbc.GPMetrics{},
				},
				{
					QueryStart: &timestamppb.Timestamp{Seconds: 1740125419, Nanos: 123},
					QueryEnd:   &timestamppb.Timestamp{Seconds: 1740125419, Nanos: 789},
					QueryInfo: &pbc.QueryInfo{
						StartTime: &timestamppb.Timestamp{Seconds: 1740125419, Nanos: 123},
						EndTime:   &timestamppb.Timestamp{Seconds: 1740125419, Nanos: 789}},
					QueryKey:       &pbc.QueryKey{Ssid: 0},
					QueryStatus:    pbc.QueryStatus_QUERY_STATUS_DONE,
					QueryMetrics:   &pbc.GPMetrics{Instrumentation: &pbc.MetricInstrumentation{InheritedCalls: 10000}},
					SegmentKey:     &pbc.SegmentKey{Segindex: 1},
					SliceId:        1,
					AdditionalStat: &pbc.AdditionalQueryStat{},
				},
				{
					QueryStart: &timestamppb.Timestamp{Seconds: 1740125419, Nanos: 123},
					QueryEnd:   &timestamppb.Timestamp{Seconds: 1740125419, Nanos: 789},
					QueryInfo: &pbc.QueryInfo{
						StartTime: &timestamppb.Timestamp{Seconds: 1740125419, Nanos: 123},
						EndTime:   &timestamppb.Timestamp{Seconds: 1740125419, Nanos: 789}},
					QueryKey:       &pbc.QueryKey{Ssid: 0},
					QueryStatus:    pbc.QueryStatus_QUERY_STATUS_DONE,
					QueryMetrics:   &pbc.GPMetrics{Instrumentation: &pbc.MetricInstrumentation{InheritedCalls: 10002}},
					SegmentKey:     &pbc.SegmentKey{Segindex: 1},
					SliceId:        2,
					AdditionalStat: &pbc.AdditionalQueryStat{},
				},
				{
					QueryStart:     &timestamppb.Timestamp{Seconds: 1740125419, Nanos: 123},
					QueryInfo:      &pbc.QueryInfo{QueryText: "Hello2", StartTime: &timestamppb.Timestamp{Seconds: 1740125419, Nanos: 123}},
					QueryKey:       &pbc.QueryKey{Ssid: 1},
					QueryStatus:    pbc.QueryStatus_QUERY_STATUS_START,
					SegmentKey:     &pbc.SegmentKey{Segindex: 1},
					SliceId:        0,
					AdditionalStat: &pbc.AdditionalQueryStat{},
					QueryMetrics:   &pbc.GPMetrics{},
				},
				{
					QueryStart: &timestamppb.Timestamp{Seconds: 1740125419, Nanos: 123},
					QueryEnd:   &timestamppb.Timestamp{Seconds: 1740125419, Nanos: 789},
					QueryInfo: &pbc.QueryInfo{
						StartTime: &timestamppb.Timestamp{Seconds: 1740125419, Nanos: 123},
						EndTime:   &timestamppb.Timestamp{Seconds: 1740125419, Nanos: 789}},
					QueryKey:       &pbc.QueryKey{Ssid: 1},
					QueryStatus:    pbc.QueryStatus_QUERY_STATUS_DONE,
					QueryMetrics:   &pbc.GPMetrics{Instrumentation: &pbc.MetricInstrumentation{InheritedCalls: 10001}},
					SegmentKey:     &pbc.SegmentKey{Segindex: 1},
					SliceId:        1,
					AdditionalStat: &pbc.AdditionalQueryStat{},
				},
			},
		}
		assertQueriesInfoResponseEqual(t, expectedResponse, response)
	})
}
