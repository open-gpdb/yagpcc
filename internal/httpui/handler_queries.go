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

	writeProtoJSON(w, http.StatusOK, resp)
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

	writeProtoJSON(w, http.StatusOK, resp)
}
