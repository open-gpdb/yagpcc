package grpc

import (
	"context"
	"fmt"
	"time"

	pb "github.com/open-gpdb/yagpcc/api/proto/agent_segment"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/gp"
	"github.com/open-gpdb/yagpcc/internal/metrics"
	"github.com/open-gpdb/yagpcc/internal/storage"
	"go.uber.org/zap"
)

type SetQueryInfoServer struct {
	pb.UnimplementedSetQueryInfoServer
	Logger               *zap.SugaredLogger
	UpdateSessionMetrics bool
	LogWorkAmount        bool
	RQStorage            *storage.RunningQueriesStorage
	SessionsStorage      *gp.SessionsStorage
}

func (s *SetQueryInfoServer) logSpecificMetrics(in *pb.SetQueryReq) {
	if in.QueryStatus == pbc.QueryStatus_QUERY_STATUS_DONE && s.LogWorkAmount {
		// Extract values safely with nil checks
		var readBytes, writeBytes uint64
		if in.QueryMetrics != nil && in.QueryMetrics.SystemStat != nil {
			readBytes = in.QueryMetrics.SystemStat.ReadBytes
			writeBytes = in.QueryMetrics.SystemStat.WriteBytes
		}
		var recvPktNum, sndPktNum int64
		var ntuples, nloops uint64
		if in.QueryMetrics != nil && in.QueryMetrics.Instrumentation != nil {
			ntuples = in.QueryMetrics.Instrumentation.Ntuples
			nloops = in.QueryMetrics.Instrumentation.Nloops
			if in.QueryMetrics.Instrumentation.Interconnect != nil {
				recvPktNum = in.QueryMetrics.Instrumentation.Interconnect.RecvPktNum
				sndPktNum = in.QueryMetrics.Instrumentation.Interconnect.SndPktNum
			}
		}
		s.Logger.Infof("WORK AMOUNT: Segment: seg%d Query: con%d cmd%d slice%d "+
			"DiskIO: %d %d IC: %d %d NTuples: %d NLoops: %d",
			in.SegmentKey.Segindex, in.QueryKey.Ssid, in.QueryKey.Ccnt, in.AddInfo.SliceId,
			readBytes, writeBytes,
			recvPktNum, sndPktNum,
			ntuples, nloops,
		)
	}

}

func (s *SetQueryInfoServer) SetMetricQuery(ctx context.Context, in *pb.SetQueryReq) (*pb.MetricResponse, error) {
	s.Logger.Debugf("got set metric query message %v", in)
	s.logSpecificMetrics(in)
	start := time.Now()
	if in.GetQueryKey() == nil {
		return nil, fmt.Errorf("query key cannot be null")
	}
	sliceId := int64(0)
	if in.AddInfo != nil {
		sliceId = in.AddInfo.SliceId
	}
	var qKey = storage.NewQueryKey(in.GetQueryKey(), in.GetSegmentKey(), sliceId)

	measuredQueryTimes := storage.MeasuredQueryTimes{
		QueryStart:  in.StartTime,
		QuerySubmit: in.SubmitTime,
		QueryEnd:    in.EndTime,
	}
	// sanity check if we forgot to set Start/End time
	if storage.CheckQueryEnded(int32(in.QueryStatus)) && measuredQueryTimes.QueryEnd == nil {
		measuredQueryTimes.QueryEnd = in.Datetime
	}
	if storage.CheckQueryStarted(int32(in.QueryStatus)) && measuredQueryTimes.QueryStart == nil {
		measuredQueryTimes.QueryStart = in.Datetime
	}
	newQuery, err := s.RQStorage.StoreInfoInStorage(
		qKey,
		int32(in.QueryStatus),
		measuredQueryTimes,
		in.GetQueryInfo(),
		in.GetAddInfo(),
		in.GetQueryMetrics(),
	)
	if s.UpdateSessionMetrics {
		err = s.SessionsStorage.UpdateSessionQuery(
			in.GetQueryKey(),
			in.GetQueryInfo(),
			int32(in.QueryStatus),
			in.GetAddInfo(),
			newQuery,
		)
	}
	s.Logger.Debugf("set metric query message %v took %v", in.QueryKey, time.Since(start))
	if metrics.YagpccMetrics != nil {
		metrics.YagpccMetrics.HandleLatencies.With(map[string]string{"method": "SetMetricQuery"}).Observe(time.Since(start).Seconds())
		metrics.YagpccMetrics.QueryStatuses.With(map[string]string{"status": in.QueryStatus.String()}).Inc()
	}
	if err != nil {
		return &pb.MetricResponse{
			ErrorCode: pb.MetricResponseStatusCode_METRIC_RESPONSE_STATUS_CODE_ERROR,
			ErrorText: err.Error(),
		}, nil
	}
	return &pb.MetricResponse{
		ErrorCode: pb.MetricResponseStatusCode_METRIC_RESPONSE_STATUS_CODE_SUCCESS,
		ErrorText: "Ok",
	}, nil
}
