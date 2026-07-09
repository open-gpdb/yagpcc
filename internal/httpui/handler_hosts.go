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

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
)

// handleGetHostsRunningQueries handles GET /api/hosts/running-queries
//
// Returns a JSON array of RunningHostInfo objects describing per-host
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
