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

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
)

// handleGetTotalSessionsStat handles GET /api/stats/sessions
func (s *Server) handleGetTotalSessionsStat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.grpcServerReady(w) {
		return
	}

	req := &pbm.GetTotalSessionsReq{}

	resp, err := s.grpcServer.GetTotalSessionsStat(r.Context(), req)
	if err != nil {
		s.logger.Errorf("UI GetTotalSessionsStat error: %v", err)
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("error: %v", err))
		return
	}

	// Convert to the format expected by the frontend
	stats := make([]map[string]interface{}, 0, len(resp.GetSessionsStat()))
	for _, stat := range resp.GetSessionsStat() {
		stats = append(stats, map[string]interface{}{
			"state": stat.GetState(),
			"count": stat.GetCount(),
		})
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"stats": stats,
	})
}
