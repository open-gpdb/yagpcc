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
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
)

// terminateSessionRequest is the JSON request body for POST /api/action/terminate-session.
type terminateSessionRequest struct {
	SessID int64 `json:"sess_id"`
	TmID   int64 `json:"tm_id"`
}

// terminateQueryRequest is the JSON request body for POST /api/action/terminate-query.
type terminateQueryRequest struct {
	SSID int32 `json:"ssid"`
	CCNT int32 `json:"ccnt"`
}

// terminateSessionsRequest is the JSON request body for POST /api/action/terminate-sessions.
type terminateSessionsRequest struct {
	Database string `json:"database"`
	Username string `json:"username"`
	QueryID  uint64 `json:"query_id"`
}

// moveQueryRSGRequest is the JSON request body for POST /api/action/move-query-rsg.
type moveQueryRSGRequest struct {
	SSID              int32  `json:"ssid"`
	CCNT              int32  `json:"ccnt"`
	ResourceGroupName string `json:"resource_group_name"`
}

// handleTerminateSession handles POST /api/action/terminate-session
func (s *Server) handleTerminateSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.actionServerReady(w) {
		return
	}

	var body terminateSessionRequest
	if !decodeJSONBody(w, r, &body) {
		return
	}

	if body.SessID == 0 {
		writeJSONError(w, http.StatusBadRequest, "sess_id is required")
		return
	}

	req := &pbm.TerminateSessionRequest{
		SessionKey: &pbc.SessionKey{
			SessId: body.SessID,
			TmId:   body.TmID,
		},
	}

	resp, err := s.actionServer.TerminateSession(r.Context(), req)
	if err != nil {
		s.logger.Errorf("UI TerminateSession error: %v", err)
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("error: %v", err))
		return
	}

	writeProtoJSON(w, http.StatusOK, resp)
}

// handleTerminateQuery handles POST /api/action/terminate-query
func (s *Server) handleTerminateQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.actionServerReady(w) {
		return
	}

	var body terminateQueryRequest
	if !decodeJSONBody(w, r, &body) {
		return
	}

	if body.SSID == 0 {
		writeJSONError(w, http.StatusBadRequest, "ssid is required")
		return
	}

	req := &pbm.TerminateQueryRequest{
		QueryKey: &pbc.QueryKey{
			Ssid: body.SSID,
			Ccnt: body.CCNT,
		},
	}

	resp, err := s.actionServer.TerminateQuery(r.Context(), req)
	if err != nil {
		s.logger.Errorf("UI TerminateQuery error: %v", err)
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("error: %v", err))
		return
	}

	writeProtoJSON(w, http.StatusOK, resp)
}

// handleTerminateSessions handles POST /api/action/terminate-sessions
func (s *Server) handleTerminateSessions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.actionServerReady(w) {
		return
	}

	var body terminateSessionsRequest
	if !decodeJSONBody(w, r, &body) {
		return
	}

	req := &pbm.TerminateSessionsRequest{
		Database: body.Database,
		Username: body.Username,
		QueryId:  body.QueryID,
	}

	resp, err := s.actionServer.TerminateSessions(r.Context(), req)
	if err != nil {
		s.logger.Errorf("UI TerminateSessions error: %v", err)
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("error: %v", err))
		return
	}

	writeProtoJSON(w, http.StatusOK, resp)
}

// handleMoveQueryToResourceGroup handles POST /api/action/move-query-rsg
func (s *Server) handleMoveQueryToResourceGroup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.actionServerReady(w) {
		return
	}

	var body moveQueryRSGRequest
	if !decodeJSONBody(w, r, &body) {
		return
	}

	if body.SSID == 0 {
		writeJSONError(w, http.StatusBadRequest, "ssid is required")
		return
	}
	if body.ResourceGroupName == "" {
		writeJSONError(w, http.StatusBadRequest, "resource_group_name is required")
		return
	}

	req := &pbm.MoveQueryToResourceGroupRequest{
		QueryKey: &pbc.QueryKey{
			Ssid: body.SSID,
			Ccnt: body.CCNT,
		},
		ResourceGroupName: body.ResourceGroupName,
	}

	_, err := s.actionServer.MoveQueryToResourceGroup(r.Context(), req)
	if err != nil {
		s.logger.Errorf("UI MoveQueryToResourceGroup error: %v", err)
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("error: %v", err))
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
