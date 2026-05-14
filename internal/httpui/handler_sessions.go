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

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
)

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

	writeProtoJSON(w, http.StatusOK, resp)
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

	writeProtoJSON(w, http.StatusOK, resp)
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
