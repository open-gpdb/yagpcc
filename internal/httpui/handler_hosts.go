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
	"net/http"
	"net/url"
	"strconv"
	"strings"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
)

// handleGetHostsRunningQueries handles GET /api/hosts/running-queries
//
// Returns a JSON object shaped like {"hosts": [...]} describing per-host
// running-query statistics collected from procfs snapshots.
func (s *Server) handleGetHostsRunningQueries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.grpcServerReady(w) {
		return
	}

	resp, err := s.grpcServer.GetGPHostsRunningQueries(r.Context(), &pbm.GetGPHostsRunningQueriesReq{})
	if err != nil {
		s.logger.Errorf("UI GetGPHostsRunningQueries error: %v", err)
		// Return empty response on error — graceful degradation.
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"hosts": []map[string]interface{}{},
		})
		return
	}

	writeProtoJSON(w, http.StatusOK, resp)
}

// handleGetHostRunningQueries handles GET /api/hosts/{host_name}/running-queries.
func (s *Server) handleGetHostRunningQueries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.grpcServerReady(w) {
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/hosts/")
	encodedHost, ok := strings.CutSuffix(path, "/running-queries")
	if !ok || encodedHost == "" || encodedHost == "running-queries" {
		writeJSONError(w, http.StatusBadRequest, "missing host name")
		return
	}
	hostname, err := url.PathUnescape(encodedHost)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid host name")
		return
	}

	pageSize := int64(50)
	if raw := r.URL.Query().Get("page_size"); raw != "" {
		var parsed int64
		var parseErr error
		parsed, parseErr = strconv.ParseInt(raw, 10, 64)
		if parseErr != nil || parsed <= 0 {
			writeJSONError(w, http.StatusBadRequest, "invalid page_size")
			return
		}
		pageSize = parsed
	}

	resp, err := s.grpcServer.GetGPHostRunningQueries(r.Context(), &pbm.GetGPHostRunningQueriesReq{
		HostName:  hostname,
		PageSize:  pageSize,
		PageToken: r.URL.Query().Get("page_token"),
	})
	if err != nil {
		s.logger.Errorf("UI GetGPHostRunningQueries error: %v", err)
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"hostName":      hostname,
			"segindex":      []int32{},
			"queries":       []map[string]interface{}{},
			"nextPageToken": "",
		})
		return
	}

	writeProtoJSON(w, http.StatusOK, resp)
}
