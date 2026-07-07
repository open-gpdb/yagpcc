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

package httpui

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
)

// handleGetQueries handles GET /api/queries
//
// Query parameters:
//   - page_size: int (default 100)
//   - page_token: string
//   - sort: FIELD_NAME:ASC|DESC (repeatable)
//   - filter_*: same as /api/sessions
func (s *Server) handleGetQueries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.grpcServerReady(w) {
		return
	}

	q := r.URL.Query()
	filters := parseSessionFilters(r)
	sortFields := parseSortFields(r)

	pageSize := int64(0)
	if ps := q.Get("page_size"); ps != "" {
		if v, err := strconv.ParseInt(ps, 10, 64); err == nil && v > 0 {
			pageSize = v
		}
	}
	pageToken := q.Get("page_token")

	req := &pbm.GetGPQueriesReq{
		Field:     sortFields,
		Filter:    filters,
		PageSize:  pageSize,
		PageToken: pageToken,
	}

	resp, err := s.grpcServer.GetGPQueries(r.Context(), req)
	if err != nil {
		s.logger.Errorf("UI GetGPQueries error: %v", err)
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("error: %v", err))
		return
	}

	// Convert to the format expected by the frontend.
	queries := make([]map[string]interface{}, 0, len(resp.SessionsState))
	for _, sessionState := range resp.SessionsState {
		// For queries, we're interested in the running query info.
		if sessionState.RunningQueryInfo == nil || sessionState.RunningQuery == nil {
			continue
		}

		// Calculate query duration from session's query_start.
		var queryDurationSeconds float64
		var queryStartStr string
		if sessionState.SessionInfo != nil && sessionState.SessionInfo.QueryStart != nil && sessionState.SessionInfo.QueryStart.Seconds > 0 {
			queryDurationSeconds = float64(time.Now().Unix() - sessionState.SessionInfo.QueryStart.Seconds)
			queryStartStr = sessionState.SessionInfo.GetQueryStart().AsTime().Format(time.RFC3339)
		} else if sessionState.RunningQueryInfo != nil && sessionState.RunningQueryInfo.SubmitTime != nil && sessionState.RunningQueryInfo.SubmitTime.Seconds > 0 {
			queryDurationSeconds = float64(time.Now().Unix() - sessionState.RunningQueryInfo.SubmitTime.Seconds)
			queryStartStr = sessionState.RunningQueryInfo.GetSubmitTime().AsTime().Format(time.RFC3339)
		}

		query := map[string]interface{}{
			"queryKey": map[string]interface{}{
				"ssid": sessionState.RunningQuery.GetSsid(),
				"ccnt": sessionState.RunningQuery.GetCcnt(),
			},
			"queryText":            sessionState.RunningQueryInfo.GetQueryText(),
			"queryStart":           queryStartStr,
			"queryDurationSeconds": queryDurationSeconds,
			"status":               sessionState.RunningQueryStatus.String(),
			"user":                 sessionState.SessionInfo.GetUser(),
			"database":             sessionState.SessionInfo.GetDatabase(),
			"rsgName":              sessionState.SessionInfo.GetRsgname(),
			"host":                 sessionState.GetHostname(),
			"pid":                  sessionState.SessionInfo.GetPid(),
			"state":                sessionState.SessionInfo.GetState(),
			"waitEventType":        sessionState.SessionInfo.GetWaitEventType(),
			"waitEvent":            sessionState.SessionInfo.GetWaitEvent(),
			"runningQueryLevel":    sessionState.GetRunningQueryLevel(),
			"runningQuerySlices":   sessionState.GetRunningQuerySlices(),
			"runningQueryError":    sessionState.GetRunningQueryError(),
			"sessionKey": map[string]interface{}{
				"sessId": fmt.Sprintf("%d", sessionState.SessionKey.GetSessId()),
				"tmId":   fmt.Sprintf("%d", sessionState.SessionKey.GetTmId()),
			},
		}
		queries = append(queries, query)
	}

	response := map[string]interface{}{
		"queries":       queries,
		"nextPageToken": resp.GetNextPageToken(),
		"totalCount":    fmt.Sprintf("%d", len(queries)),
	}

	writeJSON(w, http.StatusOK, response)
}

// convertNetworkStat converts a proto NetworkStat to a map.
func convertNetworkStat(ns *pbc.NetworkStat) map[string]interface{} {
	if ns == nil {
		return nil
	}
	return map[string]interface{}{
		"totalBytes": ns.GetTotalBytes(),
		"tupleBytes": ns.GetTupleBytes(),
		"chunks":     ns.GetChunks(),
	}
}

// convertInterconnectStat converts a proto InterconnectStat to a map.
func convertInterconnectStat(ic *pbc.InterconnectStat) map[string]interface{} {
	if ic == nil {
		return nil
	}
	return map[string]interface{}{
		"totalRecvQueueSize":        ic.GetTotalRecvQueueSize(),
		"recvQueueSizeCountingTime": ic.GetRecvQueueSizeCountingTime(),
		"totalCapacity":             ic.GetTotalCapacity(),
		"capacityCountingTime":      ic.GetCapacityCountingTime(),
		"totalBuffers":              ic.GetTotalBuffers(),
		"bufferCountingTime":        ic.GetBufferCountingTime(),
		"activeConnectionsNum":      ic.GetActiveConnectionsNum(),
		"retransmits":               ic.GetRetransmits(),
		"startupCachedPktNum":       ic.GetStartupCachedPktNum(),
		"mismatchNum":               ic.GetMismatchNum(),
		"crcErrors":                 ic.GetCrcErrors(),
		"sndPktNum":                 ic.GetSndPktNum(),
		"recvPktNum":                ic.GetRecvPktNum(),
		"disorderedPktNum":          ic.GetDisorderedPktNum(),
		"duplicatedPktNum":          ic.GetDuplicatedPktNum(),
		"recvAckNum":                ic.GetRecvAckNum(),
		"statusQueryMsgNum":         ic.GetStatusQueryMsgNum(),
	}
}

// convertGPMetrics converts a proto GPMetrics to a map with all available fields.
func convertGPMetrics(m *pbc.GPMetrics) map[string]interface{} {
	if m == nil {
		return nil
	}

	var cpuUsage, memoryUsage float64
	var diskRead, diskWrite float64
	var networkSent, networkReceived float64

	if ss := m.GetSystemStat(); ss != nil {
		cpuUsage = ss.GetUserTimeSeconds() + ss.GetKernelTimeSeconds()
		memoryUsage = float64(ss.GetRss())
		diskRead = float64(ss.GetReadBytes())
		diskWrite = float64(ss.GetWriteBytes())
	}

	if instr := m.GetInstrumentation(); instr != nil {
		if sent := instr.GetSent(); sent != nil {
			networkSent = float64(sent.GetTotalBytes())
		}
		if recv := instr.GetReceived(); recv != nil {
			networkReceived = float64(recv.GetTotalBytes())
		}
	}

	result := map[string]interface{}{
		// Summary fields (backward compatible)
		"cpuUsage":        cpuUsage,
		"memoryUsage":     memoryUsage,
		"diskRead":        diskRead,
		"diskWrite":       diskWrite,
		"networkSent":     networkSent,
		"networkReceived": networkReceived,
	}

	// Detailed SystemStat
	if ss := m.GetSystemStat(); ss != nil {
		result["systemStat"] = map[string]interface{}{
			"runningTimeSeconds":  ss.GetRunningTimeSeconds(),
			"userTimeSeconds":     ss.GetUserTimeSeconds(),
			"kernelTimeSeconds":   ss.GetKernelTimeSeconds(),
			"vsize":               ss.GetVsize(),
			"rss":                 ss.GetRss(),
			"vmPeakKb":            ss.GetVmPeakKb(),
			"rchar":               ss.GetRchar(),
			"wchar":               ss.GetWchar(),
			"syscr":               ss.GetSyscr(),
			"syscw":               ss.GetSyscw(),
			"readBytes":           ss.GetReadBytes(),
			"writeBytes":          ss.GetWriteBytes(),
			"cancelledWriteBytes": ss.GetCancelledWriteBytes(),
		}
	}

	// Detailed Instrumentation
	if instr := m.GetInstrumentation(); instr != nil {
		result["instrumentation"] = map[string]interface{}{
			"ntuples":           instr.GetNtuples(),
			"nloops":            instr.GetNloops(),
			"tuplecount":        instr.GetTuplecount(),
			"firsttuple":        instr.GetFirsttuple(),
			"startup":           instr.GetStartup(),
			"total":             instr.GetTotal(),
			"sharedBlksHit":     instr.GetSharedBlksHit(),
			"sharedBlksRead":    instr.GetSharedBlksRead(),
			"sharedBlksDirtied": instr.GetSharedBlksDirtied(),
			"sharedBlksWritten": instr.GetSharedBlksWritten(),
			"localBlksHit":      instr.GetLocalBlksHit(),
			"localBlksRead":     instr.GetLocalBlksRead(),
			"localBlksDirtied":  instr.GetLocalBlksDirtied(),
			"localBlksWritten":  instr.GetLocalBlksWritten(),
			"tempBlksRead":      instr.GetTempBlksRead(),
			"tempBlksWritten":   instr.GetTempBlksWritten(),
			"blkReadTime":       instr.GetBlkReadTime(),
			"blkWriteTime":      instr.GetBlkWriteTime(),
			"startupTime":       instr.GetStartupTime(),
			"inheritedCalls":    instr.GetInheritedCalls(),
			"inheritedTime":     instr.GetInheritedTime(),
			"sent":              convertNetworkStat(instr.GetSent()),
			"received":          convertNetworkStat(instr.GetReceived()),
			"interconnect":      convertInterconnectStat(instr.GetInterconnect()),
		}
	}

	// Spill info
	if sp := m.GetSpill(); sp != nil {
		result["spill"] = map[string]interface{}{
			"fileCount":  sp.GetFileCount(),
			"totalBytes": sp.GetTotalBytes(),
		}
	}

	return result
}

// convertAggregatedMetrics converts a proto AggregatedMetrics to a map.
func convertAggregatedMetrics(am *pbc.AggregatedMetrics) map[string]interface{} {
	if am == nil {
		return nil
	}
	return map[string]interface{}{
		"calls":      am.GetCalls(),
		"minTime":    am.GetMinTime(),
		"maxTime":    am.GetMaxTime(),
		"meanTime":   am.GetMeanTime(),
		"stddevTime": am.GetStddevTime(),
		"totalTime":  am.GetTotalTime(),
	}
}

// convertQueryInfo converts a proto QueryInfo to a map with all fields.
func convertQueryInfo(qi *pbc.QueryInfo) map[string]interface{} {
	if qi == nil {
		return nil
	}
	return map[string]interface{}{
		"generator":    qi.GetGenerator().String(),
		"queryId":      qi.GetQueryId(),
		"planId":       qi.GetPlanId(),
		"queryText":    qi.GetQueryText(),
		"planText":     qi.GetPlanText(),
		"userName":     qi.GetUserName(),
		"databaseName": qi.GetDatabaseName(),
		"rsgname":      qi.GetRsgname(),
		"analyzeText":  qi.GetAnalyzeText(),
		"submitTime":   formatTimestamp(qi.GetSubmitTime()),
		"startTime":    formatTimestamp(qi.GetStartTime()),
		"endTime":      formatTimestamp(qi.GetEndTime()),
	}
}

// convertSegmentMetrics converts a proto SegmentMetrics to a map.
func convertSegmentMetrics(sm *pbm.SegmentMetrics) map[string]interface{} {
	if sm == nil {
		return nil
	}
	result := map[string]interface{}{
		"clusterId":   sm.GetClusterId(),
		"hostname":    sm.GetHostname(),
		"collectTime": formatTimestamp(sm.GetCollectTime()),
		"queryStatus": sm.GetQueryStatus().String(),
		"startTime":   formatTimestamp(sm.GetStartTime()),
		"endTime":     formatTimestamp(sm.GetEndTime()),
		"metrics":     convertGPMetrics(sm.GetSegmentMetrics()),
	}
	if sk := sm.GetSegmentKey(); sk != nil {
		result["segmentKey"] = map[string]interface{}{
			"dbid":     sk.GetDbid(),
			"segindex": sk.GetSegindex(),
		}
	}
	return result
}

func convertProcIO(io *pbc.ProcIO) map[string]interface{} {
	if io == nil {
		return nil
	}
	return map[string]interface{}{
		"rchar":               io.GetRchar(),
		"wchar":               io.GetWchar(),
		"syscr":               io.GetSyscr(),
		"syscw":               io.GetSyscw(),
		"readBytes":           io.GetReadBytes(),
		"writeBytes":          io.GetWriteBytes(),
		"cancelledWriteBytes": io.GetCancelledWriteBytes(),
	}
}

func convertProcSpill(spill *pbc.ProcSpill) map[string]interface{} {
	if spill == nil {
		return nil
	}
	return map[string]interface{}{
		"size":  spill.GetSize(),
		"files": spill.GetFiles(),
	}
}

func convertRuntimeMetrics(metrics *pbc.RuntimeMetrics) map[string]interface{} {
	if metrics == nil {
		return nil
	}
	return map[string]interface{}{
		"utime":     metrics.GetUtime(),
		"stime":     metrics.GetStime(),
		"vmPeak":    metrics.GetVmPeak(),
		"vmRss":     metrics.GetVmRss(),
		"state":     metrics.GetState(),
		"procIo":    convertProcIO(metrics.GetProcIo()),
		"procSpill": convertProcSpill(metrics.GetProcSpill()),
	}
}

func convertDataQuality(dq *pbc.DataQuality) map[string]interface{} {
	if dq == nil {
		return nil
	}
	return map[string]interface{}{
		"segmentsExpected": dq.GetSegmentsExpected(),
		"segmentsReceived": dq.GetSegmentsReceived(),
		"isPartial":        dq.GetIsPartial(),
		"freshnessMs":      dq.GetFreshnessMs(),
	}
}

func convertSkew(skew *pbc.Skew) map[string]interface{} {
	if skew == nil {
		return nil
	}
	return map[string]interface{}{
		"skew":     skew.GetSkew(),
		"segindex": skew.GetSegindex(),
	}
}

func (s *Server) handleGetQueryRunningMetrics(w http.ResponseWriter, r *http.Request, ssid int32, ccnt int32) {
	req := &pbm.GetGPQueryRunningMatricsReq{
		QueryKey: &pbc.QueryKey{Ssid: ssid, Ccnt: ccnt},
	}
	resp, err := s.grpcServer.GetGPQueryRunningMatrics(r.Context(), req)
	if err != nil {
		s.logger.Errorf("UI GetGPQueryRunningMatrics error: %v", err)
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("error: %v", err))
		return
	}

	cells := make([]map[string]interface{}, 0, len(resp.GetCellMetrics()))
	for _, metrics := range resp.GetCellMetrics() {
		cell := map[string]interface{}{
			"sliceId":        metrics.GetSliceId(),
			"segindex":       metrics.GetSegindex(),
			"runtimeMetrics": convertRuntimeMetrics(metrics.GetRuntimeMetrics()),
		}
		cells = append(cells, cell)
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"sliceId":        resp.GetSliceId(),
		"segindex":       resp.GetSegindex(),
		"runtimeMetrics": cells,
		"skew":           convertSkew(resp.GetSkew()),
		"dataQuality":    convertDataQuality(resp.GetDataQuality()),
	})
}

// handleGetQuery handles GET /api/query/{ssid}/{ccnt}
func (s *Server) handleGetQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.grpcServerReady(w) {
		return
	}

	// Extract ssid and ccnt from path: /api/query/42/1
	trimmed := strings.TrimPrefix(r.URL.Path, "/api/query/")
	parts := strings.Split(trimmed, "/")
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		writeJSONError(w, http.StatusBadRequest, "ssid and ccnt are required in path: /api/query/{ssid}/{ccnt}")
		return
	}

	ssid, err := strconv.ParseInt(parts[0], 10, 32)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid ssid")
		return
	}
	ccnt, err := strconv.ParseInt(parts[1], 10, 32)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid ccnt")
		return
	}
	if len(parts) >= 3 && parts[2] == "running-metrics" {
		s.handleGetQueryRunningMetrics(w, r, int32(ssid), int32(ccnt))
		return
	}

	req := &pbm.GetGPQueryReq{
		QueryKey: &pbc.QueryKey{
			Ssid: int32(ssid),
			Ccnt: int32(ccnt),
		},
	}

	resp, err := s.grpcServer.GetGPQuery(r.Context(), req)
	if err != nil {
		s.logger.Errorf("UI GetGPQuery error: %v", err)
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("error: %v", err))
		return
	}

	// Convert to the format expected by the frontend.
	qData := resp.GetQueriesData()
	if qData == nil || qData.QueryStat == nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"query": nil,
		})
		return
	}

	qs := qData.QueryStat

	// Calculate query duration.
	var queryDurationSeconds float64
	var queryStartStr string
	if qs.StartTime != nil && qs.StartTime.Seconds > 0 {
		queryDurationSeconds = float64(time.Now().Unix() - qs.StartTime.Seconds)
		queryStartStr = qs.GetStartTime().AsTime().Format(time.RFC3339)
	}

	query := map[string]interface{}{
		"queryKey": map[string]interface{}{
			"ssid": qs.QueryKey.GetSsid(),
			"ccnt": qs.QueryKey.GetCcnt(),
		},
		"queryText":            qs.QueryInfo.GetQueryText(),
		"queryStart":           queryStartStr,
		"queryDurationSeconds": queryDurationSeconds,
		"status":               qs.GetQueryStatus().String(),
		"user":                 qs.QueryInfo.GetUserName(),
		"database":             qs.QueryInfo.GetDatabaseName(),
		"rsgName":              qs.QueryInfo.GetRsgname(),
		"host":                 qs.GetHostname(),
		"sessionKey": map[string]interface{}{
			"sessId": fmt.Sprintf("%d", qs.QueryKey.GetSsid()),
			"tmId":   fmt.Sprintf("%d", qs.QueryKey.GetTmid()),
		},
		"metrics": convertGPMetrics(qs.GetTotalQueryMetrics()),

		// QueryStat fields
		"clusterId":       qs.GetClusterId(),
		"collectTime":     formatTimestamp(qs.GetCollectTime()),
		"statKind":        qs.GetStatKind().String(),
		"startTime":       formatTimestamp(qs.GetStartTime()),
		"endTime":         formatTimestamp(qs.GetEndTime()),
		"completed":       qs.GetCompleted(),
		"message":         qs.GetMessage(),
		"blockedBySessId": qs.GetBlockedBySessId(),
		"waitMode":        qs.GetWaitMode(),
		"lockedItem":      qs.GetLockedItem(),
		"lockedMode":      qs.GetLockedMode(),
		"sessionState":    qs.GetSessionState(),
		"slices":          qs.GetSlices(),

		// Detailed query info
		"queryInfo":         convertQueryInfo(qs.GetQueryInfo()),
		"aggregatedMetrics": convertAggregatedMetrics(qs.GetAggregatedMetrics()),
	}

	// Convert segment metrics
	segMetrics := make([]map[string]interface{}, 0, len(qData.SegmentQueryMetrics))
	for _, sm := range qData.SegmentQueryMetrics {
		if converted := convertSegmentMetrics(sm); converted != nil {
			segMetrics = append(segMetrics, converted)
		}
	}
	query["segmentMetrics"] = segMetrics

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"query": query,
	})
}
