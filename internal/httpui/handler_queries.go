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

// convertGPMetrics converts a proto GPMetrics to a map matching the frontend
// GPMetrics interface: { cpuUsage, memoryUsage, diskRead, diskWrite, networkSent, networkReceived }.
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

	return map[string]interface{}{
		"cpuUsage":        cpuUsage,
		"memoryUsage":     memoryUsage,
		"diskRead":        diskRead,
		"diskWrite":       diskWrite,
		"networkSent":     networkSent,
		"networkReceived": networkReceived,
	}
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
			"tmId":   "0",
		},
		"metrics": convertGPMetrics(qs.GetTotalQueryMetrics()),
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"query": query,
	})
}
