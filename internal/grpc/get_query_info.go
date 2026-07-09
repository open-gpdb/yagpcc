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

package grpc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/procfs"
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

func cpuUsage(stat procfs.CPUStat) float64 {
	total := stat.User + stat.Nice + stat.System + stat.Idle + stat.Iowait + stat.IRQ + stat.SoftIRQ + stat.Steal
	if total <= 0 {
		return 0
	}
	idle := stat.Idle + stat.Iowait
	return (total - idle) / total
}

func (s *GetQueryInfoServer) GetHostStat(ctx context.Context, in *pb.GetHostStatReq) (*pb.GetHostStatResponse, error) {
	s.Logger.Debugf("got get host stat request %v", in)
	start := time.Now()
	fs, err := procfs.NewDefaultFS()
	if err != nil {
		return nil, fmt.Errorf("failed to open procfs: %w", err)
	}
	loadAvg, err := fs.LoadAvg()
	if err != nil {
		return nil, fmt.Errorf("failed to read loadavg: %w", err)
	}
	stat, err := fs.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to read proc stat: %w", err)
	}
	if metrics.YagpccMetrics != nil {
		metrics.YagpccMetrics.HandleLatencies.With(map[string]string{"method": "GetHostStat"}).Observe(time.Since(start).Seconds())
	}
	return &pb.GetHostStatResponse{
		LoadAvg: &pb.LoadAvg{
			Avg1:  loadAvg.Load1,
			Avg5:  loadAvg.Load5,
			Avg15: loadAvg.Load15,
		},
		CpuUsage: &pb.CpuUsage{CpuUsage: cpuUsage(stat.CPUTotal)},
	}, nil
}

func (s *GetQueryInfoServer) GetPidProcStat(ctx context.Context, in *pb.GetPidProcInfoReq) (*pb.GetPidProcInfoResponse, error) {
	s.Logger.Debugf("got get pid info request %v", in)
	start := time.Now()

	pidResponse := &pb.GetPidProcInfoResponse{}
	nErrors := 0
	lastError := error(nil)

	if in != nil && in.SegmentProcess != nil {
		for _, segProcess := range in.SegmentProcess {
			pidStat, err := utils.GetPidProcInfo(s.Logger, segProcess.Pid, segProcess.GpSegmentId, segProcess.SessId)
			if err != nil {
				if errors.Is(err, utils.ErrProcessNotFound) {
					s.Logger.Debugf("pid %d not found: %v", segProcess.Pid, err)
					continue
				}
				s.Logger.Debugf("got error while getting pid info %v for %v", err, segProcess)
				nErrors++
				lastError = err
				continue
			}
			pidResponse.PidProcData = append(pidResponse.PidProcData, pidStat)
		}
	}

	if lastError != nil {
		s.Logger.Infof("got %v errors in pid request, the last error is %v", nErrors, lastError)
	}

	if metrics.YagpccMetrics != nil {
		metrics.YagpccMetrics.HandleLatencies.With(map[string]string{"method": "GpPidProcInfo"}).Observe(time.Since(start).Seconds())
	}

	if nErrors > 0 && len(pidResponse.PidProcData) == 0 {
		// something got totally wrong
		return nil, lastError
	}

	return pidResponse, nil
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
