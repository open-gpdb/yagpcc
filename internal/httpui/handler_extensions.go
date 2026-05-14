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

// handleGetExtensions handles GET /api/extensions
func (s *Server) handleGetExtensions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.grpcServerReady(w) {
		return
	}

	databaseName := r.URL.Query().Get("database_name")

	req := &pbm.GetGPExtensionsReq{
		DatabaseName: databaseName,
	}

	resp, err := s.grpcServer.GetGPExtensions(r.Context(), req)
	if err != nil {
		s.logger.Errorf("UI GetGPExtensions error: %v", err)
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("error: %v", err))
		return
	}

	writeProtoJSON(w, http.StatusOK, resp)
}

// handleListDatabases handles GET /api/databases
func (s *Server) handleListDatabases(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.grpcServerReady(w) {
		return
	}

	req := &pbm.ListDatabasesReq{}

	resp, err := s.grpcServer.ListDatabases(r.Context(), req)
	if err != nil {
		s.logger.Errorf("UI ListDatabases error: %v", err)
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("error: %v", err))
		return
	}

	writeProtoJSON(w, http.StatusOK, resp)
}
