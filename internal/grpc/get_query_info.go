package grpc

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/open-gpdb/yagpcc/api/proto/agent_segment"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/gp"
	"github.com/open-gpdb/yagpcc/internal/metrics"
	"github.com/open-gpdb/yagpcc/internal/storage"
	"github.com/open-gpdb/yagpcc/internal/utils"
	"go.uber.org/zap"
)

type GetQueryInfoServer struct {
	pb.UnimplementedGetQueryInfoServer
	MaxMessageSize int
	Logger         *zap.SugaredLogger
	RQStorage      *storage.RunningQueriesStorage
}

func (s *GetQueryInfoServer) GetMetricQueries(ctx context.Context, in *pb.GetQueriesInfoReq) (*pb.GetQueriesInfoResponse, error) {
	s.Logger.Debugf("got get data request %v", in)
	start := time.Now()

	filterQueryKey := storage.QueryKey{}
	queryResponse := &pb.GetQueriesInfoResponse{QueriesData: make([]*pb.QueryData, 0)}

	fromTime := time.Time{}
	if in.FromTime != nil {
		fromTime = utils.GetTimeForTimestamp(in.FromTime)
	}
	toTime := time.Time{}
	if in.ToTime != nil {
		toTime = utils.GetTimeForTimestamp(in.ToTime)
	}

	keysToDelete := make([]*storage.QueryKey, 0)
	responseSize := proto.Size(queryResponse)

	if in.GetFilterQueries() != nil {
		// use index to get data
		for _, filterQuery := range in.GetFilterQueries() {
			filterQueryKey.Ssid = filterQuery.Ssid
			filterQueryKey.Ccnt = filterQuery.Ccnt

			val, ok := s.RQStorage.GetQuery(filterQueryKey)

			if ok {
				err := s.addQueryMessage(queryResponse, val, fromTime, toTime, in.ClearSent, &responseSize)
				if err != nil {
					s.Logger.Debugf("got error while adding message %v", err)
					return nil, fmt.Errorf("got error while adding message %w", err)
				}
				keysToDelete = append(keysToDelete, &filterQueryKey)
			}
		}
	} else {
		// iterate over all saved queries
		rQueries := s.RQStorage.GetQueries()
		for key, val := range rQueries {
			err := s.addQueryMessage(queryResponse, val, fromTime, toTime, in.ClearSent, &responseSize)
			if err != nil {
				s.Logger.Debugf("got error while adding message %v", err)
				break
			}
			keysToDelete = append(keysToDelete, &key)
		}
	}

	if in.ClearSent {
		// clear queries with empty segment info
		s.RQStorage.DeleteQueries(keysToDelete)
	}

	if metrics.YagpccMetrics != nil {
		metrics.YagpccMetrics.HandleLatencies.With(map[string]string{"method": "GetMetricQueries"}).Observe(time.Since(start).Seconds())
	}

	return queryResponse, nil
}

func (s *GetQueryInfoServer) constructQueryMessage(nodeKey *storage.NodeKey, queryData *storage.QueryData) *pb.QueryData {
	queryData.QueryDataLock.RLock()
	defer queryData.QueryDataLock.RUnlock()
	queryKey := &pbc.QueryKey{
		Tmid: int32(gp.DiscoveredTmID),
		Ssid: nodeKey.QKey.Ssid,
		Ccnt: nodeKey.QKey.Ccnt,
	}
	segmentKey := &pbc.SegmentKey{
		Dbid:     nodeKey.SKey.Dbid,
		Segindex: nodeKey.SKey.Segindex,
	}
	queryMessage := pb.QueryData{
		QueryKey:     queryKey,
		SegmentKey:   segmentKey,
		SliceId:      nodeKey.SliceID,
		QueryStatus:  pbc.QueryStatus(queryData.CurrentStatus),
		QueryInfo:    queryData.QueryInfo,
		QueryMetrics: queryData.QueryMetrics,
		AdditionalStat: &pbc.AdditionalQueryStat{
			// Query message right now contains only error, so just copy string without additional checks
			ErrorMessage: queryData.QueryMessage,
		},
	}
	if queryData.QueryInfo != nil {
		queryMessage.QueryStart = queryData.QueryInfo.StartTime
		queryMessage.QueryEnd = queryData.QueryInfo.EndTime
		// sanity check - if QueryEnd set and QueryStart not set - fix it
		if queryData.QueryInfo.EndTime != nil && queryData.QueryInfo.StartTime == nil {
			queryData.QueryInfo.StartTime = queryData.QueryInfo.EndTime
			queryMessage.QueryStart = queryData.QueryInfo.EndTime
		}
	}

	return &queryMessage
}

func filterQueryTime(startTime time.Time, endTime time.Time, queryData *storage.QueryData) bool {
	if queryData.QueryInfo == nil {
		return true
	}
	if !startTime.IsZero() && queryData.QueryInfo.StartTime != nil {
		if queryData.QueryInfo.StartTime.AsTime().Before(startTime) {
			return false
		}
	}
	if !endTime.IsZero() && queryData.QueryInfo.EndTime != nil {
		if queryData.QueryInfo.EndTime.AsTime().After(endTime) {
			return false
		}
	}
	return true
}

func (s *GetQueryInfoServer) addQueryMessage(response *pb.GetQueriesInfoResponse,
	query *storage.RunningQuery,
	fromTime time.Time,
	toTime time.Time,
	clearSent bool,
	currentSize *int,
) error {
	if clearSent {
		query.QueryLock.Lock()
		defer query.QueryLock.Unlock()
	} else {
		query.QueryLock.RLock()
		defer query.QueryLock.RUnlock()
	}

	for keyQI, valQI := range query.QueriesData {
		okFilter := filterQueryTime(fromTime, toTime, valQI)
		if okFilter {
			message := s.constructQueryMessage(&keyQI, valQI)
			msgSize := proto.Size(message)
			if *currentSize+msgSize > s.MaxMessageSize && *currentSize > 0 {
				return fmt.Errorf("current sizes %d + %d more then max %d", *currentSize, msgSize, s.MaxMessageSize)
			}
			response.QueriesData = append(response.QueriesData, message)
			*currentSize += msgSize
			if clearSent {
				delete(query.QueriesData, keyQI)
			}
		}
	}
	return nil
}
