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
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/gp"
	"github.com/open-gpdb/yagpcc/internal/master"
	"github.com/open-gpdb/yagpcc/internal/metrics"
	"github.com/open-gpdb/yagpcc/internal/storage"
	"go.uber.org/zap"
)

type GetMasterInfoServer struct {
	pbm.UnimplementedGetGPInfoServer
	clusterID          string
	logger             *zap.SugaredLogger
	maxMessageSize     int
	backgroundStorage  *master.BackgroundStorage
	extensionsCacheTTL time.Duration
}

func NewGetMasterInfoServer(clusterID string, logger *zap.SugaredLogger, maxMessageSize int, backgroundStorage *master.BackgroundStorage, extensionsCacheTTL time.Duration) *GetMasterInfoServer {
	return &GetMasterInfoServer{
		clusterID:          clusterID,
		logger:             logger,
		maxMessageSize:     maxMessageSize,
		backgroundStorage:  backgroundStorage,
		extensionsCacheTTL: extensionsCacheTTL,
	}
}

func FilterOutSession(filters []*pbm.SessionFilter, sessionState *pbc.SessionState) bool {
	sortMap := make(map[pbm.SessionFilterEnum]bool)
	for _, filter := range filters {
		sortMap[filter.FieldName] = false
	}
	for _, filter := range filters {
		switch filter.FieldName {
		case pbm.SessionFilterEnum_SESSION_FILTER_HOST:
			if sessionState.Hostname == filter.Value {
				sortMap[pbm.SessionFilterEnum_SESSION_FILTER_HOST] = true
			}
		case pbm.SessionFilterEnum_SESSION_FILTER_USER:
			if sessionState.SessionInfo.User == filter.Value {
				sortMap[pbm.SessionFilterEnum_SESSION_FILTER_USER] = true
			}
		case pbm.SessionFilterEnum_SESSION_FILTER_DATABASE:
			if sessionState.SessionInfo.Database == filter.Value {
				sortMap[pbm.SessionFilterEnum_SESSION_FILTER_DATABASE] = true
			}
		case pbm.SessionFilterEnum_SESSION_FILTER_APPLICATION_NAME:
			if sessionState.SessionInfo.ApplicationName == filter.Value {
				sortMap[pbm.SessionFilterEnum_SESSION_FILTER_APPLICATION_NAME] = true
			}
		case pbm.SessionFilterEnum_SESSION_FILTER_CLIENT_HOSTNAME:
			if sessionState.SessionInfo.ClientHostname == filter.Value {
				sortMap[pbm.SessionFilterEnum_SESSION_FILTER_CLIENT_HOSTNAME] = true
			}
		case pbm.SessionFilterEnum_SESSION_FILTER_STATE:
			switch filterValue := filter.Value; filterValue {
			case "SESSION_STATUS_IDLE":
				if strings.EqualFold(sessionState.SessionInfo.State, "IDLE") {
					sortMap[pbm.SessionFilterEnum_SESSION_FILTER_STATE] = true
				}
			case "SESSION_STATUS_BLOCKED", "blocked":
				if sessionState.SessionInfo.BlockedBySessId != 0 {
					sortMap[pbm.SessionFilterEnum_SESSION_FILTER_STATE] = true
				}
			case "SESSION_STATUS_ACTIVE":
				if strings.EqualFold(sessionState.SessionInfo.State, "ACTIVE") {
					sortMap[pbm.SessionFilterEnum_SESSION_FILTER_STATE] = true
				}
			case "SESSION_STATUS_IDLE_TRANSACTION", "SESSION_STATUS_IDLE_IN_TRANSACTION":
				if strings.EqualFold(sessionState.SessionInfo.State, "IDLE IN TRANSACTION") {
					sortMap[pbm.SessionFilterEnum_SESSION_FILTER_STATE] = true
				}
			case "SESSION_STATUS_IDLE_TRANSACTION_ABORTED", "SESSION_STATUS_IDLE_IN_TRANSACTION_ABORTED":
				if strings.EqualFold(sessionState.SessionInfo.State, "IDLE IN TRANSACTION (ABORTED)") {
					sortMap[pbm.SessionFilterEnum_SESSION_FILTER_STATE] = true
				}
			case "SESSION_STATUS_WAITING":
				if strings.EqualFold(sessionState.SessionInfo.State, "WAITING") {
					sortMap[pbm.SessionFilterEnum_SESSION_FILTER_STATE] = true
				}
			default:
				if strings.EqualFold(sessionState.SessionInfo.State, filter.Value) {
					sortMap[pbm.SessionFilterEnum_SESSION_FILTER_STATE] = true
				}
			}
		case pbm.SessionFilterEnum_SESSION_FILTER_RSGNAME:
			if sessionState.SessionInfo.Rsgname == filter.Value {
				sortMap[pbm.SessionFilterEnum_SESSION_FILTER_RSGNAME] = true
			}
		case pbm.SessionFilterEnum_SESSION_FILTER_SESS_ID:
			i, err := strconv.ParseInt(filter.Value, 10, 64)
			if err != nil {
				return true
			}
			if sessionState.SessionKey.SessId == i {
				sortMap[pbm.SessionFilterEnum_SESSION_FILTER_SESS_ID] = true
			}
		case pbm.SessionFilterEnum_SESSION_FILTER_TM_ID:
			i, err := strconv.ParseInt(filter.Value, 10, 64)
			if err != nil {
				return true
			}
			if sessionState.SessionKey.TmId == i {
				sortMap[pbm.SessionFilterEnum_SESSION_FILTER_TM_ID] = true
			}
		}
	}
	for _, f := range sortMap {
		if !f {
			return true
		}
	}
	return false
}

func (s *GetMasterInfoServer) GetGPSessions(ctx context.Context, in *pbm.GetGPSessionsReq) (*pbm.GetGPSessionsResponse, error) {
	s.logger.Debugf("got get sessions request %v", in)
	start := time.Now()
	if in.HideEmptyQueries {
		return s.GetGPQueries(ctx, &pbm.GetGPQueriesReq{
			Field:     in.Field,
			Filter:    in.Filter,
			PageSize:  in.PageSize,
			PageToken: in.PageToken,
		})
	}
	queryType := in.ShowQueryType
	if queryType == pbm.RunningQueryType_RQT_UNSPECIFIED {
		// by default show top queries
		queryType = pbm.RunningQueryType_RQT_TOP
	}
	// refresh list of sessions
	err := s.backgroundStorage.TryRefreshSessionsFromGP(ctx, true)
	if err != nil {
		s.logger.Errorf("error while refreshing session list: %v", err)
	}
	responseI, err := s.backgroundStorage.SessionStorage.GetAllSessions(in.ShowSystem, queryType)
	s.logger.Debugf("got session list len %v", len(responseI.SessionsState))
	if err != nil {
		s.logger.Errorf("error while get session list: %v", err)
		return nil, fmt.Errorf("error while get session list: %w", err)
	}
	if len(responseI.SessionsState) > 0 {
		s.logger.Debugf("first session state data is %v len is %v", responseI.SessionsState[0], len(responseI.SessionsState))
	}
	filteredState := make([]*pbc.SessionState, 0)
	for _, sessionState := range responseI.SessionsState {
		if FilterOutSession(in.Filter, sessionState) {
			continue
		}
		filteredState = append(filteredState, sessionState)
	}
	if len(filteredState) > 0 {
		s.logger.Debugf("first filtered state data is %v len is %v", filteredState[0], len(filteredState))
	}
	s.logger.Debugf("Get session data took %v", time.Since(start))
	result := &pbm.GetGPSessionsResponse{
		SessionsState: make([]*pbc.SessionState, 0),
	}
	err = SortResult(&filteredState, in.Field)
	if err != nil {
		s.logger.Debugf("data not sorted: no sort field specified %v", err)
	}
	if len(filteredState) > 0 {
		s.logger.Debugf("first filtered state data is %v", filteredState[0])
	}
	result.SessionsState, result.NextPageToken, err = paginateProtoMessages(filteredState, in.PageSize, in.PageToken, 100, s.maxMessageSize)
	if err != nil {
		return nil, err
	}
	if len(result.SessionsState) > 0 {
		s.logger.Debugf("first result state data is %v", result.SessionsState[0])
	}
	s.logger.Debugf("Get session total processing time took %v", time.Since(start))
	if metrics.YagpccMetrics != nil {
		metrics.YagpccMetrics.HandleLatencies.With(map[string]string{"method": "GetGPSessions"}).Observe(time.Since(start).Seconds())
	}
	return result, nil
}

func (s *GetMasterInfoServer) GetGPSession(ctx context.Context, in *pbm.GetGPSessionReq) (*pbm.GetGPSessionResponse, error) {
	s.logger.Debugf("got get gp session data request %v", in)
	if in.SessionKey == nil {
		return nil, fmt.Errorf("invalid unput - session key cannot be nil")
	}
	start := time.Now()
	sKey := gp.SessionKey{
		SessID: int(in.SessionKey.SessId),
	}
	sesI, ok := s.backgroundStorage.SessionStorage.GetSession(sKey)
	res := &pbm.GetGPSessionResponse{}
	if ok {
		sessionState, err := s.backgroundStorage.SessionStorage.GetSessionDesc(
			sKey,
			sesI,
			true,
			pbm.RunningQueryType_RQT_LAST,
			-1)
		if err != nil {
			return nil, fmt.Errorf("fail to get session desc %w", err)
		}
		res.SessionsState = sessionState
	}
	if metrics.YagpccMetrics != nil {
		metrics.YagpccMetrics.HandleLatencies.With(map[string]string{"method": "GetGPSession"}).Observe(time.Since(start).Seconds())
	}
	return res, nil
}

func (s *GetMasterInfoServer) GetTotalSessionsStat(ctx context.Context, in *pbm.GetTotalSessionsReq) (*pbm.GetTotalSessionsResponse, error) {
	s.logger.Debugf("got total sessions stat request %v", in)
	start := time.Now()
	totalResponse := pbm.GetTotalSessionsResponse{}
	statMap := make(map[string]int)
	sMap := s.backgroundStorage.SessionStorage.GetSessions()
	for _, valSO := range sMap {
		valSO.SessionLock.RLock()
		state := "Unknown"
		if valSO.SessionData.GpStatInfo != nil && valSO.SessionData.GpStatInfo.State != nil {
			state = strings.ToLower(*valSO.SessionData.GpStatInfo.State)
		}
		val, ok := statMap[state]
		if ok {
			statMap[state] = val + 1
		} else {
			statMap[state] = 1
		}
		valSO.SessionLock.RUnlock()
	}
	for key, val := range statMap {
		totalResponse.SessionsStat = append(totalResponse.SessionsStat, &pbm.SessionStat{
			State: key,
			Count: float64(val),
		})
	}
	s.logger.Debugf("Get total sessions stat took %v", time.Since(start))
	if metrics.YagpccMetrics != nil {
		metrics.YagpccMetrics.HandleLatencies.With(map[string]string{"method": "GetTotalSessionsStat"}).Observe(time.Since(start).Seconds())
	}
	return &totalResponse, nil
}

func (s *GetMasterInfoServer) GetGPQueries(ctx context.Context, in *pbm.GetGPQueriesReq) (*pbm.GetGPSessionsResponse, error) {
	s.logger.Debugf("got running queries request %v", in)
	start := time.Now()
	rQueries := s.backgroundStorage.RQStorage.GetQueries()
	filteredState := make([]*pbc.SessionState, 0)
	for qKeyI := range rQueries {
		sessionState, err := s.backgroundStorage.SessionStorage.GetQueryDesc(&qKeyI, true)
		if err != nil {
			s.logger.Errorf("Fail to get queryState %v", err)
			continue
		}
		if FilterOutSession(in.Filter, sessionState) {
			continue
		}
		filteredState = append(filteredState, sessionState)
	}
	s.logger.Debugf("Get session data len %v of running queries took %v", len(filteredState), time.Since(start))
	result := &pbm.GetGPSessionsResponse{
		SessionsState: make([]*pbc.SessionState, 0),
	}
	err := SortResult(&filteredState, in.Field)
	if err != nil {
		s.logger.Debugf("data not sorted: no sort field specified")
	}
	s.logger.Debugf("Sort data len %v took %v", len(filteredState), time.Since(start))
	result.SessionsState, result.NextPageToken, err = paginateProtoMessages(filteredState, in.PageSize, in.PageToken, 100, s.maxMessageSize)
	if err != nil {
		return nil, err
	}
	s.logger.Debugf("Result dataset size is %v", len(result.SessionsState))
	s.logger.Debugf("Get running queries took %v", time.Since(start))
	if metrics.YagpccMetrics != nil {
		metrics.YagpccMetrics.HandleLatencies.With(map[string]string{"method": "GetGPQueries"}).Observe(time.Since(start).Seconds())
	}
	return result, nil
}

func (s *GetMasterInfoServer) GetGPQuery(ctx context.Context, in *pbm.GetGPQueryReq) (*pbm.GetGPQueryResponse, error) {
	s.logger.Debugf("got get query data request %v", in)
	if in.QueryKey == nil {
		return nil, fmt.Errorf("invalid inout - query key cannot be nil")
	}
	start := time.Now()
	queryResponse := pbm.GetGPQueryResponse{}
	qKey := storage.QueryKey{
		Ssid: in.QueryKey.Ssid,
		Ccnt: in.QueryKey.Ccnt,
	}

	tmID := atomic.LoadInt64(&gp.DiscoveredTmID)
	queryData, err := s.backgroundStorage.RQStorage.GetQueryData(qKey, tmID)
	if err != nil {
		return nil, err
	}
	queryResponse.QueriesData = queryData

	s.logger.Debugf("Get query data took %v", time.Since(start))
	if metrics.YagpccMetrics != nil {
		metrics.YagpccMetrics.HandleLatencies.With(map[string]string{"method": "GetGPQuery"}).Observe(time.Since(start).Seconds())
	}
	return &queryResponse, nil
}

func (s *GetMasterInfoServer) GetGPQueryRunningMatrics(ctx context.Context, in *pbm.GetGPQueryRunningMatricsReq) (*pbm.GetGPQueryRunningMatricsResponse, error) {
	s.logger.Debugf("got get query running metrics request %v", in)
	if in.QueryKey == nil {
		return nil, fmt.Errorf("invalid input - query key cannot be nil")
	}
	start := time.Now()
	response := &pbm.GetGPQueryRunningMatricsResponse{
		SliceId:     make([]int64, 0),
		Segindex:    make([]int32, 0),
		CellMetrics: make([]*pbm.CellMetrics, 0),
	}

	cellMetrics, skew, dataQuality, err := s.backgroundStorage.GetQueryRunningMetrics(in.QueryKey)
	if err != nil {
		s.logger.Errorf("failed to get query running metrics: %v", err)
		if metrics.YagpccMetrics != nil {
			metrics.YagpccMetrics.HandleLatencies.With(map[string]string{"method": "GetGPQueryRunningMatrics"}).Observe(time.Since(start).Seconds())
		}
		return response, nil
	}

	response.SliceId = make([]int64, 0, len(cellMetrics))
	response.Segindex = make([]int32, 0, len(cellMetrics))
	response.CellMetrics = make([]*pbm.CellMetrics, 0, len(cellMetrics))
	response.Skew = skew
	response.DataQuality = dataQuality
	for _, cell := range cellMetrics {
		response.SliceId = append(response.SliceId, cell.SliceID)
		response.Segindex = append(response.Segindex, cell.Segindex)
		response.CellMetrics = append(response.CellMetrics, &pbm.CellMetrics{
			SliceId:        cell.SliceID,
			Segindex:       cell.Segindex,
			RuntimeMetrics: cell.RuntimeMetrics,
		})
	}

	s.logger.Debugf("Get query running metrics took %v", time.Since(start))
	if metrics.YagpccMetrics != nil {
		metrics.YagpccMetrics.HandleLatencies.With(map[string]string{"method": "GetGPQueryRunningMatrics"}).Observe(time.Since(start).Seconds())
	}
	return response, nil
}

func (s *GetMasterInfoServer) GetGPHostsRunningQueries(ctx context.Context, in *pbm.GetGPHostsRunningQueriesReq) (*pbm.GetGPHostsRunningQueriesResponse, error) {
	s.logger.Debugf("got get hosts running queries request %v", in)
	start := time.Now()
	hosts, err := s.backgroundStorage.GetHostsRunningQueries()
	if err != nil {
		return nil, fmt.Errorf("failed to get hosts running queries: %w", err)
	}
	response := &pbm.GetGPHostsRunningQueriesResponse{Hosts: make([]*pbm.RunningHostInfo, 0, len(hosts))}
	for _, host := range hosts {
		if host == nil {
			continue
		}
		response.Hosts = append(response.Hosts, &pbm.RunningHostInfo{
			HostName:      host.HostName,
			Segindex:      host.Segindex,
			ActiveQueries: host.ActiveQueries,
			ActiveSlices:  host.ActiveSlices,
			TotalSessions: host.TotalSessions,
			CpuUsage:      host.CPUUsage,
			MemoryUsage:   host.MemoryUsage,
			DiskUsage:     host.DiskUsage,
			SpillBytes:    host.SpillBytes,
			Skew:          host.Skew,
			DataQuality:   host.DataQuality,
			Avg5:          host.Avg5,
			DiskReads:     host.DiskReads,
			DiskWrites:    host.DiskWrites,
		})
	}
	if metrics.YagpccMetrics != nil {
		metrics.YagpccMetrics.HandleLatencies.With(map[string]string{"method": "GetGPHostsRunningQueries"}).Observe(time.Since(start).Seconds())
	}
	return response, nil
}

func (s *GetMasterInfoServer) GetGPHostRunningQueries(ctx context.Context, in *pbm.GetGPHostRunningQueriesReq) (*pbm.GetGPHostRunningQueriesResponse, error) {
	s.logger.Debugf("got get host running queries request %v", in)
	start := time.Now()
	if strings.TrimSpace(in.GetHostName()) == "" {
		return nil, fmt.Errorf("invalid input - host name cannot be empty")
	}
	result, err := s.backgroundStorage.GetHostRunningQueries(in.GetHostName())
	if err != nil {
		return nil, fmt.Errorf("failed to get host running queries: %w", err)
	}
	queries, nextPageToken, err := paginateItems(result.Queries, in.GetPageSize(), in.GetPageToken(), 50)
	if err != nil {
		return nil, err
	}
	response := &pbm.GetGPHostRunningQueriesResponse{
		HostName:      result.HostName,
		Segindex:      result.Segindex,
		Queries:       make([]*pbm.RunningQueryInfo, 0, len(queries)),
		DataQuality:   result.DataQuality,
		NextPageToken: nextPageToken,
	}
	for _, query := range queries {
		if query == nil {
			continue
		}
		response.Queries = append(response.Queries, &pbm.RunningQueryInfo{
			QueryKey:       query.QueryKey,
			UserName:       query.UserName,
			DbName:         query.DbName,
			QueryText:      query.QueryText,
			RuntimeMetrics: query.RuntimeMetrics,
			Skew:           query.Skew,
			DataQuality:    query.DataQuality,
			State:          query.State,
			IsIdle:         query.IsIdle,
		})
	}
	if metrics.YagpccMetrics != nil {
		metrics.YagpccMetrics.HandleLatencies.With(map[string]string{"method": "GetGPHostRunningQueries"}).Observe(time.Since(start).Seconds())
	}
	return response, nil
}

func (s *GetMasterInfoServer) GetGpPidProcInfo(ctx context.Context, in *pbm.GetGpPidProcInfoReq) (*pbm.GpPidProcInfoResponse, error) {
	s.logger.Debugf("got get pid proc info request %v", in)
	start := time.Now()

	// Validate required fields
	if in.GetQueryKey() == nil || in.GetQueryKey().GetSsid() == 0 {
		return nil, fmt.Errorf("invalid input - query key with non-zero ssid is required")
	}
	if strings.TrimSpace(in.GetHostname()) == "" && in.Segindex == nil {
		return nil, fmt.Errorf("invalid input - either hostname or segindex must be set")
	}

	// Build storage filter
	filter := storage.ProcInfoFilter{
		QueryKey: in.GetQueryKey(),
		Hostname: in.GetHostname(),
	}
	if in.SliceId != nil {
		sliceID := in.GetSliceId()
		filter.SliceID = &sliceID
	}
	if in.Segindex != nil {
		segindex := in.GetSegindex()
		filter.Segindex = &segindex
	}

	// Fetch matching rows from storage
	rows, err := s.backgroundStorage.GetGpPidProcInfo(filter)
	if err != nil {
		return nil, fmt.Errorf("failed to get pid proc info: %w", err)
	}

	// Sort rows
	sortProcInfoRows(rows, in.GetField())

	// Paginate
	pagedRows, nextPageToken, err := paginateItems(rows, in.GetPageSize(), in.GetPageToken(), 50)
	if err != nil {
		return nil, err
	}

	if metrics.YagpccMetrics != nil {
		metrics.YagpccMetrics.HandleLatencies.With(map[string]string{"method": "GetGpPidProcInfo"}).Observe(time.Since(start).Seconds())
	}

	return &pbm.GpPidProcInfoResponse{
		PidProcData:   pagedRows,
		NextPageToken: nextPageToken,
	}, nil
}

// sortProcInfoRows sorts process rows by the requested fields, defaulting to CPU descending.
func sortProcInfoRows(rows []*pbc.GpPidProcInfo, fields []*pbm.ProcInfoFieldWrapper) {
	if len(fields) == 0 {
		// Default: CPU descending
		sort.Slice(rows, func(i, j int) bool {
			cpuI := procInfoCPU(rows[i])
			cpuJ := procInfoCPU(rows[j])
			if cpuI != cpuJ {
				return cpuI > cpuJ
			}
			return procInfoTieBreak(rows[i], rows[j])
		})
		return
	}

	sort.Slice(rows, func(i, j int) bool {
		for _, f := range fields {
			field := f.GetField()
			asc := f.GetOrder() == pbm.SortOrder_SORT_ASC
			less := procInfoFieldLess(rows[i], rows[j], field)
			if less {
				return asc
			}
			if !procInfoFieldEqual(rows[i], rows[j], field) {
				return !asc
			}
		}
		return procInfoTieBreak(rows[i], rows[j])
	})
}

func procInfoCPU(row *pbc.GpPidProcInfo) int64 {
	if row == nil || row.ProcStat == nil {
		return 0
	}
	return row.ProcStat.Utime + row.ProcStat.Stime
}

func procInfoTieBreak(a, b *pbc.GpPidProcInfo) bool {
	if a.GpSegmentId != b.GpSegmentId {
		return a.GpSegmentId < b.GpSegmentId
	}
	if a.SliceId != b.SliceId {
		return a.SliceId < b.SliceId
	}
	return a.Pid < b.Pid
}

func procInfoFieldLess(a, b *pbc.GpPidProcInfo, field pbm.ProcInfoFieldEnum) bool {
	switch field {
	case pbm.ProcInfoFieldEnum_PROC_INFO_FIELD_CPU:
		return procInfoCPU(a) < procInfoCPU(b)
	case pbm.ProcInfoFieldEnum_PROC_INFO_FIELD_MEMORY_RSS:
		rssA := int64(0)
		rssB := int64(0)
		if a != nil && a.ProcStatus != nil {
			rssA = a.ProcStatus.VmRss
		}
		if b != nil && b.ProcStatus != nil {
			rssB = b.ProcStatus.VmRss
		}
		return rssA < rssB
	case pbm.ProcInfoFieldEnum_PROC_INFO_FIELD_READ_BYTES:
		readA := int64(0)
		readB := int64(0)
		if a != nil && a.ProcIo != nil {
			readA = a.ProcIo.ReadBytes
		}
		if b != nil && b.ProcIo != nil {
			readB = b.ProcIo.ReadBytes
		}
		return readA < readB
	case pbm.ProcInfoFieldEnum_PROC_INFO_FIELD_WRITE_BYTES:
		writeA := int64(0)
		writeB := int64(0)
		if a != nil && a.ProcIo != nil {
			writeA = a.ProcIo.WriteBytes
		}
		if b != nil && b.ProcIo != nil {
			writeB = b.ProcIo.WriteBytes
		}
		return writeA < writeB
	case pbm.ProcInfoFieldEnum_PROC_INFO_FIELD_SPILL_BYTES:
		spillA := int64(0)
		spillB := int64(0)
		if a != nil && a.ProcSpill != nil {
			spillA = a.ProcSpill.Size
		}
		if b != nil && b.ProcSpill != nil {
			spillB = b.ProcSpill.Size
		}
		return spillA < spillB
	case pbm.ProcInfoFieldEnum_PROC_INFO_FIELD_SEGINDEX:
		return a.GpSegmentId < b.GpSegmentId
	case pbm.ProcInfoFieldEnum_PROC_INFO_FIELD_SLICE_ID:
		return a.SliceId < b.SliceId
	case pbm.ProcInfoFieldEnum_PROC_INFO_FIELD_PID:
		return a.Pid < b.Pid
	case pbm.ProcInfoFieldEnum_PROC_INFO_FIELD_STATE:
		return a.State < b.State
	default:
		return false
	}
}

func procInfoFieldEqual(a, b *pbc.GpPidProcInfo, field pbm.ProcInfoFieldEnum) bool {
	switch field {
	case pbm.ProcInfoFieldEnum_PROC_INFO_FIELD_CPU:
		return procInfoCPU(a) == procInfoCPU(b)
	case pbm.ProcInfoFieldEnum_PROC_INFO_FIELD_MEMORY_RSS:
		rssA := int64(0)
		rssB := int64(0)
		if a != nil && a.ProcStatus != nil {
			rssA = a.ProcStatus.VmRss
		}
		if b != nil && b.ProcStatus != nil {
			rssB = b.ProcStatus.VmRss
		}
		return rssA == rssB
	case pbm.ProcInfoFieldEnum_PROC_INFO_FIELD_READ_BYTES:
		readA := int64(0)
		readB := int64(0)
		if a != nil && a.ProcIo != nil {
			readA = a.ProcIo.ReadBytes
		}
		if b != nil && b.ProcIo != nil {
			readB = b.ProcIo.ReadBytes
		}
		return readA == readB
	case pbm.ProcInfoFieldEnum_PROC_INFO_FIELD_WRITE_BYTES:
		writeA := int64(0)
		writeB := int64(0)
		if a != nil && a.ProcIo != nil {
			writeA = a.ProcIo.WriteBytes
		}
		if b != nil && b.ProcIo != nil {
			writeB = b.ProcIo.WriteBytes
		}
		return writeA == writeB
	case pbm.ProcInfoFieldEnum_PROC_INFO_FIELD_SPILL_BYTES:
		spillA := int64(0)
		spillB := int64(0)
		if a != nil && a.ProcSpill != nil {
			spillA = a.ProcSpill.Size
		}
		if b != nil && b.ProcSpill != nil {
			spillB = b.ProcSpill.Size
		}
		return spillA == spillB
	case pbm.ProcInfoFieldEnum_PROC_INFO_FIELD_SEGINDEX:
		return a.GpSegmentId == b.GpSegmentId
	case pbm.ProcInfoFieldEnum_PROC_INFO_FIELD_SLICE_ID:
		return a.SliceId == b.SliceId
	case pbm.ProcInfoFieldEnum_PROC_INFO_FIELD_PID:
		return a.Pid == b.Pid
	case pbm.ProcInfoFieldEnum_PROC_INFO_FIELD_STATE:
		return a.State == b.State
	default:
		return true
	}
}

func (s *GetMasterInfoServer) GetGPExtensions(ctx context.Context, in *pbm.GetGPExtensionsReq) (*pbm.GetGPExtensionsResponse, error) {
	s.logger.Debugf("got get extensions request")
	start := time.Now()

	// Get extensions data from all databases
	allExtensions, err := gp.GetAllExtensions(ctx, s.extensionsCacheTTL)
	if err != nil {
		return nil, fmt.Errorf("failed to get extensions: %w", err)
	}

	// Convert to protobuf response
	response := &pbm.GetGPExtensionsResponse{
		Databases: make([]*pbm.DatabaseExtensionsInfo, 0),
	}

	addDatabaseToResponse := func(databaseName string, dbExtensions gp.DatabaseExtensions) {
		dbInfo := &pbm.DatabaseExtensionsInfo{
			DatabaseName: databaseName,
			Error:        dbExtensions.Error,
		}

		extensions := make([]*pbm.Extension, 0, len(dbExtensions.Extensions))
		for _, ext := range dbExtensions.Extensions {
			extensions = append(extensions, &pbm.Extension{
				Name:        ext.ExtName,
				Owner:       ext.ExtOwner,
				Namespace:   ext.ExtNamespace,
				Relocatable: ext.ExtRelocatable,
				Version:     ext.ExtVersion,
			})
		}
		dbInfo.Extensions = extensions
		response.Databases = append(response.Databases, dbInfo)
	}

	if in.GetDatabaseName() == "" {
		databaseNames := make([]string, 0, len(allExtensions))
		for databaseName := range allExtensions {
			databaseNames = append(databaseNames, databaseName)
		}
		sort.Strings(databaseNames)
		for _, databaseName := range databaseNames {
			addDatabaseToResponse(databaseName, allExtensions[databaseName])
		}
	} else if dbExtensions, exists := allExtensions[in.GetDatabaseName()]; exists {
		addDatabaseToResponse(in.GetDatabaseName(), dbExtensions)
	}

	s.logger.Debugf("Get extensions took %v", time.Since(start))
	if metrics.YagpccMetrics != nil {
		metrics.YagpccMetrics.HandleLatencies.With(map[string]string{"method": "GetGPExtensions"}).Observe(time.Since(start).Seconds())
	}
	return response, nil
}

func (s *GetMasterInfoServer) ListDatabases(ctx context.Context, in *pbm.ListDatabasesReq) (*pbm.ListDatabasesResponse, error) {
	s.logger.Debugf("got list databases request")
	start := time.Now()

	// Get extensions data from all databases (this includes the list of databases)
	allExtensions, err := gp.GetAllExtensions(ctx, s.extensionsCacheTTL)
	if err != nil {
		return nil, fmt.Errorf("failed to get database list: %w", err)
	}

	// Extract database names from the map keys
	databaseNames := make([]string, 0, len(allExtensions))
	for dbName := range allExtensions {
		databaseNames = append(databaseNames, dbName)
	}

	// Sort the database names for consistent output
	sort.Strings(databaseNames)

	// Create DatabaseInfo objects
	databases := make([]*pbm.DatabaseInfo, 0, len(databaseNames))
	for _, dbName := range databaseNames {
		databases = append(databases, &pbm.DatabaseInfo{
			Name: dbName,
		})
	}

	// Create response
	response := &pbm.ListDatabasesResponse{
		Databases: databases,
	}

	s.logger.Debugf("List databases took %v", time.Since(start))
	if metrics.YagpccMetrics != nil {
		metrics.YagpccMetrics.HandleLatencies.With(map[string]string{"method": "ListDatabases"}).Observe(time.Since(start).Seconds())
	}
	return response, nil
}
