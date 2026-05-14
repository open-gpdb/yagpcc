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
	"google.golang.org/protobuf/types/known/timestamppb"
)

// formatTimestamp safely formats a protobuf timestamp to RFC3339 string.
// Returns empty string for nil or zero timestamps.
func formatTimestamp(ts *timestamppb.Timestamp) string {
	if ts == nil || (ts.Seconds == 0 && ts.Nanos == 0) {
		return ""
	}
	return ts.AsTime().Format(time.RFC3339)
}

// convertQueryDesc converts a proto QueryDesc to a map matching the frontend
// QueryDesc interface: { queryKey, queryText, queryStart, queryDurationSeconds, status }.
func convertQueryDesc(qd *pbc.QueryDesc) map[string]interface{} {
	if qd == nil {
		return nil
	}

	queryStartStr := formatTimestamp(qd.GetQueryStart())

	// Calculate duration from queryStart to now.
	var queryDurationSeconds float64
	if qd.QueryStart != nil && qd.QueryStart.Seconds > 0 {
		queryDurationSeconds = float64(time.Now().Unix() - qd.QueryStart.Seconds)
	}

	result := map[string]interface{}{
		"queryKey":             nil,
		"queryText":            "",
		"queryStart":           queryStartStr,
		"queryDurationSeconds": queryDurationSeconds,
		"status":               qd.GetQueryStatus().String(),
	}
	if qd.QueryKey != nil {
		result["queryKey"] = map[string]interface{}{
			"ssid": qd.QueryKey.GetSsid(),
			"ccnt": qd.QueryKey.GetCcnt(),
		}
	}
	if qd.QueryInfo != nil {
		result["queryText"] = qd.QueryInfo.GetQueryText()
	}
	return result
}

// convertSessionState converts a proto SessionState to a flat map with all
// available fields for the frontend.
func convertSessionState(sessionState *pbc.SessionState) map[string]interface{} {
	if sessionState == nil || sessionState.SessionInfo == nil {
		return nil
	}

	si := sessionState.SessionInfo

	// Calculate total running time.
	var totalRunningTimeSeconds float64
	if si.QueryStart != nil && si.QueryStart.Seconds > 0 {
		totalRunningTimeSeconds = float64(time.Now().Unix() - si.QueryStart.Seconds)
	}

	// Convert queries stack.
	queriesStack := make([]map[string]interface{}, 0)
	for _, qd := range sessionState.GetRunningQueriesStack() {
		if converted := convertQueryDesc(qd); converted != nil {
			queriesStack = append(queriesStack, converted)
		}
	}

	session := map[string]interface{}{
		// SessionKey
		"sessionKey": map[string]interface{}{
			"sessId": fmt.Sprintf("%d", sessionState.SessionKey.GetSessId()),
			"tmId":   fmt.Sprintf("%d", sessionState.SessionKey.GetTmId()),
		},

		// SessionState top-level fields
		"collectTime":         formatTimestamp(sessionState.GetTime()),
		"clusterId":           sessionState.GetClusterId(),
		"host":                sessionState.GetHostname(),
		"runningQueryLevel":   sessionState.GetRunningQueryLevel(),
		"blockedSessionLevel": sessionState.GetBlockedSessionLevel(),
		"runningQueryError":   sessionState.GetRunningQueryError(),
		"runningQuerySlices":  sessionState.GetRunningQuerySlices(),

		// SessionInfo fields
		"pid":                     si.GetPid(),
		"user":                    si.GetUser(),
		"database":                si.GetDatabase(),
		"applicationName":         si.GetApplicationName(),
		"clientAddr":              si.GetClientAddr(),
		"clientHostname":          si.GetClientHostname(),
		"clientPort":              si.GetClientPort(),
		"backendStart":            formatTimestamp(si.GetBackendStart()),
		"xactStart":               formatTimestamp(si.GetXactStart()),
		"queryStart":              formatTimestamp(si.GetQueryStart()),
		"stateChange":             formatTimestamp(si.GetStateChange()),
		"waitingReason":           si.GetWaitingReason(),
		"waiting":                 si.GetWaiting(),
		"state":                   si.GetState(),
		"backendXid":              si.GetBackendXid(),
		"backendXmin":             si.GetBackendXmin(),
		"rsgId":                   si.GetRsgid(),
		"rsgName":                 si.GetRsgname(),
		"rsgQueueDuration":        si.GetRsgqueueduration(),
		"blockedBySessId":         si.GetBlockedBySessId(),
		"waitMode":                si.GetWaitMode(),
		"lockedItem":              si.GetLockedItem(),
		"lockedMode":              si.GetLockedMode(),
		"waitEventType":           si.GetWaitEventType(),
		"waitEvent":               si.GetWaitEvent(),
		"totalRunningTimeSeconds": totalRunningTimeSeconds,

		// Running query info
		"runningQuery": nil,
		"queries":      queriesStack,
	}

	// Running query key
	if rq := sessionState.GetRunningQuery(); rq != nil {
		session["runningQuery"] = map[string]interface{}{
			"ssid": rq.GetSsid(),
			"ccnt": rq.GetCcnt(),
		}
	}

	// Running query status
	session["runningQueryStatus"] = sessionState.GetRunningQueryStatus().String()

	// Running query text (from RunningQueryInfo)
	if rqi := sessionState.GetRunningQueryInfo(); rqi != nil {
		session["runningQueryText"] = rqi.GetQueryText()
	} else {
		session["runningQueryText"] = ""
	}

	return session
}

// handleGetSessions handles GET /api/sessions
//
// Query parameters (same as CSV endpoint):
//   - show_system: bool (default false)
//   - show_query_type: RQT_TOP|RQT_LAST (default RQT_UNSPECIFIED)
//   - hide_empty_queries: bool (default false)
//   - page_size: int (default 100)
//   - page_token: string
//   - sort: FIELD_NAME:ASC|DESC (repeatable)
//   - filter_host, filter_user, filter_database, filter_application_name,
//     filter_client_hostname, filter_state, filter_rsgname, filter_sess_id, filter_tm_id
func (s *Server) handleGetSessions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.grpcServerReady(w) {
		return
	}

	q := r.URL.Query()

	showSystem, _ := strconv.ParseBool(q.Get("show_system"))
	hideEmptyQueries, _ := strconv.ParseBool(q.Get("hide_empty_queries"))

	filters := parseSessionFilters(r)
	sortFields := parseSortFields(r)

	pageSize := int64(0)
	if ps := q.Get("page_size"); ps != "" {
		if v, err := strconv.ParseInt(ps, 10, 64); err == nil && v > 0 {
			pageSize = v
		}
	}
	pageToken := q.Get("page_token")

	queryType := pbm.RunningQueryType_RQT_UNSPECIFIED
	if qt := q.Get("show_query_type"); qt != "" {
		if val, ok := pbm.RunningQueryType_value[qt]; ok {
			queryType = pbm.RunningQueryType(val)
		}
	}

	req := &pbm.GetGPSessionsReq{
		ShowSystem:       showSystem,
		Field:            sortFields,
		Filter:           filters,
		PageSize:         pageSize,
		PageToken:        pageToken,
		HideEmptyQueries: hideEmptyQueries,
		ShowQueryType:    queryType,
	}

	resp, err := s.grpcServer.GetGPSessions(r.Context(), req)
	if err != nil {
		s.logger.Errorf("UI GetGPSessions error: %v", err)
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("error: %v", err))
		return
	}

	// Convert to the format expected by the frontend.
	sessions := make([]map[string]interface{}, 0, len(resp.SessionsState))
	for _, sessionState := range resp.SessionsState {
		if session := convertSessionState(sessionState); session != nil {
			sessions = append(sessions, session)
		}
	}

	response := map[string]interface{}{
		"sessions":      sessions,
		"nextPageToken": resp.GetNextPageToken(),
		"totalCount":    fmt.Sprintf("%d", len(sessions)),
	}

	writeJSON(w, http.StatusOK, response)
}

// handleGetSession handles GET /api/session/{sess_id}
func (s *Server) handleGetSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.grpcServerReady(w) {
		return
	}

	// Extract sess_id from path: /api/session/123
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/session/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		writeJSONError(w, http.StatusBadRequest, "sess_id is required in path: /api/session/{sess_id}")
		return
	}
	sessID, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid sess_id")
		return
	}

	req := &pbm.GetGPSessionReq{
		SessionKey: &pbc.SessionKey{
			SessId: sessID,
		},
	}

	resp, err := s.grpcServer.GetGPSession(r.Context(), req)
	if err != nil {
		s.logger.Errorf("UI GetGPSession error: %v", err)
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("error: %v", err))
		return
	}

	session := convertSessionState(resp.GetSessionsState())
	if session == nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"session": nil,
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"session": session,
	})
}

// parseSessionFilters parses filter query parameters.
func parseSessionFilters(r *http.Request) []*pbm.SessionFilter {
	filterMap := map[string]pbm.SessionFilterEnum{
		"filter_host":             pbm.SessionFilterEnum_SESSION_FILTER_HOST,
		"filter_user":             pbm.SessionFilterEnum_SESSION_FILTER_USER,
		"filter_database":         pbm.SessionFilterEnum_SESSION_FILTER_DATABASE,
		"filter_application_name": pbm.SessionFilterEnum_SESSION_FILTER_APPLICATION_NAME,
		"filter_client_hostname":  pbm.SessionFilterEnum_SESSION_FILTER_CLIENT_HOSTNAME,
		"filter_state":            pbm.SessionFilterEnum_SESSION_FILTER_STATE,
		"filter_rsgname":          pbm.SessionFilterEnum_SESSION_FILTER_RSGNAME,
		"filter_sess_id":          pbm.SessionFilterEnum_SESSION_FILTER_SESS_ID,
		"filter_tm_id":            pbm.SessionFilterEnum_SESSION_FILTER_TM_ID,
	}

	var filters []*pbm.SessionFilter
	for param, enum := range filterMap {
		if val := r.URL.Query().Get(param); val != "" {
			filters = append(filters, &pbm.SessionFilter{
				FieldName: enum,
				Value:     val,
			})
		}
	}
	return filters
}

// parseSortFields parses sort query parameters.
// Sort is specified as: sort=FIELD_NAME:ASC or sort=FIELD_NAME:DESC
func parseSortFields(r *http.Request) []*pbm.SessionFieldWrapper {
	sortParams := r.URL.Query()["sort"]
	if len(sortParams) == 0 {
		return nil
	}

	var fields []*pbm.SessionFieldWrapper
	for _, s := range sortParams {
		parts := strings.SplitN(s, ":", 2)
		fieldName := parts[0]
		order := pbm.SortOrder_SORT_ASC
		if len(parts) == 2 && strings.EqualFold(parts[1], "DESC") {
			order = pbm.SortOrder_SORT_DESC
		}

		if val, ok := pbm.SessionField_value[fieldName]; ok {
			fields = append(fields, &pbm.SessionFieldWrapper{
				FieldName: pbm.SessionField(val),
				Order:     order,
			})
		}
	}
	return fields
}
