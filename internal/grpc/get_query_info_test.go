package grpc_test

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/open-gpdb/yagpcc/api/proto/agent_segment"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
)

func TestFilterMessages(t *testing.T) {
	startQ := time.Now().Add(time.Duration(-1) * time.Hour)
	endQ := time.Now()
	startQRounded := timestamppb.New(startQ).AsTime()
	endQRounded := timestamppb.New(endQ).AsTime()
	tests := []struct {
		name   string
		status pbc.QueryStatus
		ssid   int32
		planid int32
		queryT time.Time
		res    *pb.MetricResponse
	}{
		{
			"Initial set",
			pbc.QueryStatus_QUERY_STATUS_START,
			1,
			0,
			startQ,
			&pb.MetricResponse{
				ErrorCode: pb.MetricResponseStatusCode_METRIC_RESPONSE_STATUS_CODE_SUCCESS,
				ErrorText: "Ok",
			},
		},
		{
			"Query finish",
			pbc.QueryStatus_QUERY_STATUS_DONE,
			1,
			0,
			endQ,
			&pb.MetricResponse{
				ErrorCode: pb.MetricResponseStatusCode_METRIC_RESPONSE_STATUS_CODE_SUCCESS,
				ErrorText: "Ok",
			},
		},
		{
			"Start with done state",
			pbc.QueryStatus_QUERY_STATUS_DONE,
			2,
			0,
			endQ,
			&pb.MetricResponse{
				ErrorCode: pb.MetricResponseStatusCode_METRIC_RESPONSE_STATUS_CODE_SUCCESS,
				ErrorText: "Ok",
			},
		},
		{
			"Never finish",
			pbc.QueryStatus_QUERY_STATUS_START,
			3,
			0,
			startQ,
			&pb.MetricResponse{
				ErrorCode: pb.MetricResponseStatusCode_METRIC_RESPONSE_STATUS_CODE_SUCCESS,
				ErrorText: "Ok",
			},
		},
		{
			"Cancel with error start",
			pbc.QueryStatus_QUERY_STATUS_START,
			4,
			0,
			startQ,
			&pb.MetricResponse{
				ErrorCode: pb.MetricResponseStatusCode_METRIC_RESPONSE_STATUS_CODE_SUCCESS,
				ErrorText: "Ok",
			},
		},
		{
			"Cancel with error end",
			pbc.QueryStatus_QUERY_STATUS_ERROR,
			4,
			0,
			startQ,
			&pb.MetricResponse{
				ErrorCode: pb.MetricResponseStatusCode_METRIC_RESPONSE_STATUS_CODE_SUCCESS,
				ErrorText: "Ok",
			},
		},
	}

	clientSet, cleanup := setupGRPCClientSet(t, nil)
	defer cleanup()

	for _, testTuple := range tests {
		t.Run(testTuple.name, func(t *testing.T) {

			request := &pb.SetQueryReq{
				QueryStatus: testTuple.status,
				Datetime:    timestamppb.New(testTuple.queryT),
				QueryKey: &pbc.QueryKey{
					Ssid: testTuple.ssid,
				},
			}

			response, err := clientSet.SetQueryInfoClient().SetMetricQuery(context.Background(), request)

			require.NoError(t, err)
			assertMetricResponseIsOk(t, response)
		})
	}

	// check all data
	responseGet, errGet := clientSet.GetQueryInfoClient().GetMetricQueries(context.Background(), &pb.GetQueriesInfoReq{})
	if errGet != nil {
		t.Error(errGet)
	}

	assert.Equal(t, 4, len(responseGet.QueriesData))
	sort.Slice(responseGet.QueriesData, func(p, q int) bool {
		return responseGet.QueriesData[p].QueryKey.Ssid < responseGet.QueriesData[q].QueryKey.Ssid
	})

	assert.Equal(t, pbc.QueryStatus_QUERY_STATUS_DONE, responseGet.QueriesData[0].QueryStatus)
	assert.Equal(t, startQRounded, responseGet.QueriesData[0].QueryInfo.StartTime.AsTime())
	assert.Equal(t, endQRounded, responseGet.QueriesData[0].QueryInfo.EndTime.AsTime())

	// check filter by QueryKey, one key not in storage
	reqFilterKey := &pb.GetQueriesInfoReq{
		FilterQueries: make([]*pbc.QueryKey, 3),
	}
	reqFilterKey.FilterQueries[0] = &pbc.QueryKey{Ssid: 1}
	reqFilterKey.FilterQueries[1] = &pbc.QueryKey{Ssid: 4}
	reqFilterKey.FilterQueries[2] = &pbc.QueryKey{Ssid: 5}
	responseGet, errGet = clientSet.GetQueryInfoClient().GetMetricQueries(context.Background(), reqFilterKey)
	if errGet != nil {
		t.Error(errGet)
	}
	assert.Equal(t, 2, len(responseGet.QueriesData))
	sort.Slice(responseGet.QueriesData, func(p, q int) bool {
		return responseGet.QueriesData[p].QueryKey.Ssid < responseGet.QueriesData[q].QueryKey.Ssid
	})
	assert.Equal(t, int32(1), responseGet.QueriesData[0].QueryKey.Ssid)
	assert.Equal(t, pbc.QueryStatus_QUERY_STATUS_DONE, responseGet.QueriesData[0].QueryStatus)
	assert.Equal(t, startQRounded, responseGet.QueriesData[0].QueryStart.AsTime())
	assert.Equal(t, endQRounded, responseGet.QueriesData[0].QueryEnd.AsTime())

	// checkfilter by start Time
	startF := time.Now().Add(time.Duration(-30) * time.Minute)
	reqFilterStart := &pb.GetQueriesInfoReq{
		FromTime: timestamppb.New(startF),
	}
	responseGet, errGet = clientSet.GetQueryInfoClient().GetMetricQueries(context.Background(), reqFilterStart)
	if errGet != nil {
		t.Error(errGet)
	}
	assert.Equal(t, 1, len(responseGet.QueriesData))
	sort.Slice(responseGet.QueriesData, func(p, q int) bool {
		return responseGet.QueriesData[p].QueryKey.Ssid < responseGet.QueriesData[q].QueryKey.Ssid
	})
	assert.Equal(t, int32(2), responseGet.QueriesData[0].QueryKey.Ssid)
	assert.Equal(t, pbc.QueryStatus_QUERY_STATUS_DONE, responseGet.QueriesData[0].QueryStatus)

	// check filter by End time
	endF := time.Now().Add(time.Duration(30) * time.Minute)
	reqFilterEnd := &pb.GetQueriesInfoReq{
		ToTime: timestamppb.New(endF),
	}
	responseGet, errGet = clientSet.GetQueryInfoClient().GetMetricQueries(context.Background(), reqFilterEnd)
	if errGet != nil {
		t.Error(errGet)
	}
	sort.Slice(responseGet.QueriesData, func(p, q int) bool {
		return responseGet.QueriesData[p].QueryKey.Ssid < responseGet.QueriesData[q].QueryKey.Ssid
	})
	assert.Equal(t, 4, len(responseGet.QueriesData))
}

func TestDeleteCompleted(t *testing.T) {
	clientSet, cleanup := setupGRPCClientSet(t, nil)
	defer cleanup()

	queryStart := timestamppb.New(time.Now().Add(time.Duration(-1) * time.Hour))
	queryEnd := timestamppb.New(time.Now())

	t.Run("setup", func(t *testing.T) {
		for _, tt := range []struct {
			setQueryRequest *pb.SetQueryReq
		}{
			{
				setQueryRequest: &pb.SetQueryReq{
					QueryStatus: pbc.QueryStatus_QUERY_STATUS_START,
					Datetime:    queryStart,
					QueryKey:    &pbc.QueryKey{Ssid: 1},
					SegmentKey:  &pbc.SegmentKey{Segindex: 1},
				},
			},
			{
				setQueryRequest: &pb.SetQueryReq{
					QueryStatus: pbc.QueryStatus_QUERY_STATUS_DONE,
					Datetime:    queryEnd,
					QueryKey:    &pbc.QueryKey{Ssid: 1},
					SegmentKey:  &pbc.SegmentKey{Segindex: 1},
				},
			},
			{
				setQueryRequest: &pb.SetQueryReq{
					QueryStatus: pbc.QueryStatus_QUERY_STATUS_DONE,
					Datetime:    queryEnd,
					QueryKey:    &pbc.QueryKey{Ssid: 2},
					SegmentKey:  &pbc.SegmentKey{Segindex: 1},
				},
			},
			{
				setQueryRequest: &pb.SetQueryReq{
					QueryStatus: pbc.QueryStatus_QUERY_STATUS_START,
					Datetime:    queryStart,
					QueryKey:    &pbc.QueryKey{Ssid: 3},
					SegmentKey:  &pbc.SegmentKey{Segindex: 1},
				},
			},
			{
				setQueryRequest: &pb.SetQueryReq{
					QueryStatus: pbc.QueryStatus_QUERY_STATUS_START,
					Datetime:    queryStart,
					QueryKey:    &pbc.QueryKey{Ssid: 4},
					SegmentKey:  &pbc.SegmentKey{Segindex: 1},
				},
			},
			{
				setQueryRequest: &pb.SetQueryReq{
					QueryStatus: pbc.QueryStatus_QUERY_STATUS_ERROR,
					Datetime:    queryStart,
					QueryKey:    &pbc.QueryKey{Ssid: 4},
					SegmentKey:  &pbc.SegmentKey{Segindex: 1},
				},
			},
		} {
			if tt.setQueryRequest != nil {
				response, err := clientSet.SetQueryInfoClient().SetMetricQuery(context.Background(), tt.setQueryRequest)

				require.NoError(t, err)
				assertMetricResponseIsOk(t, response)
			}
		}
	})

	t.Run("check all data", func(t *testing.T) {
		expectedResponse := &pb.GetQueriesInfoResponse{
			QueriesData: []*pb.QueryData{
				{
					QueryKey:    &pbc.QueryKey{Ssid: 1},
					SliceId:     0,
					QueryStart:  queryStart,
					QueryEnd:    queryEnd,
					QueryStatus: pbc.QueryStatus_QUERY_STATUS_DONE,
					SegmentKey:  &pbc.SegmentKey{Segindex: 1},
					QueryInfo: &pbc.QueryInfo{
						StartTime: queryStart,
						EndTime:   queryEnd,
					},
					QueryMetrics:   &pbc.GPMetrics{},
					AdditionalStat: &pbc.AdditionalQueryStat{},
				},
				{
					QueryKey:    &pbc.QueryKey{Ssid: 2},
					SliceId:     0,
					QueryStart:  queryEnd,
					QueryEnd:    queryEnd,
					QueryStatus: pbc.QueryStatus_QUERY_STATUS_DONE,
					SegmentKey:  &pbc.SegmentKey{Segindex: 1},
					QueryInfo: &pbc.QueryInfo{
						StartTime: queryEnd,
						EndTime:   queryEnd,
					},
					QueryMetrics:   &pbc.GPMetrics{},
					AdditionalStat: &pbc.AdditionalQueryStat{},
				},
				{
					QueryKey:    &pbc.QueryKey{Ssid: 3},
					SliceId:     0,
					QueryStart:  queryStart,
					QueryStatus: pbc.QueryStatus_QUERY_STATUS_START,
					SegmentKey:  &pbc.SegmentKey{Segindex: 1},
					QueryInfo: &pbc.QueryInfo{
						StartTime: queryStart,
					},
					QueryMetrics:   &pbc.GPMetrics{},
					AdditionalStat: &pbc.AdditionalQueryStat{},
				},
				{
					QueryKey:    &pbc.QueryKey{Ssid: 4},
					SliceId:     0,
					QueryStart:  queryStart,
					QueryEnd:    queryStart,
					QueryStatus: pbc.QueryStatus_QUERY_STATUS_ERROR,
					SegmentKey:  &pbc.SegmentKey{Segindex: 1},
					QueryInfo: &pbc.QueryInfo{
						StartTime: queryStart,
						EndTime:   queryStart,
					},
					QueryMetrics:   &pbc.GPMetrics{},
					AdditionalStat: &pbc.AdditionalQueryStat{},
				},
			},
		}

		response, err := clientSet.GetQueryInfoClient().GetMetricQueries(context.Background(), &pb.GetQueriesInfoReq{ClearSent: true})

		require.NoError(t, err)
		assertQueriesInfoResponseEqual(t, expectedResponse, response)
	})

	t.Run("empty response after response with clearSent=true", func(t *testing.T) {
		response, err := clientSet.GetQueryInfoClient().GetMetricQueries(context.Background(), &pb.GetQueriesInfoReq{ClearSent: true})

		require.NoError(t, err)
		assertQueriesInfoResponseEqual(t, &pb.GetQueriesInfoResponse{}, response)
	})
}
