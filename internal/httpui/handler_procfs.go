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
	"strconv"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
)

// handleGetPidProcInfo handles GET /api/procfs/pid-proc-info.
func (s *Server) handleGetPidProcInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.grpcServerReady(w) {
		return
	}

	q := r.URL.Query()
	ssidRaw := q.Get("ssid")
	if ssidRaw == "" {
		writeJSONError(w, http.StatusBadRequest, "missing ssid")
		return
	}
	ssid, err := strconv.ParseInt(ssidRaw, 10, 32)
	if err != nil || ssid == 0 {
		writeJSONError(w, http.StatusBadRequest, "invalid ssid")
		return
	}

	req := &pbm.GetGpPidProcInfoReq{
		QueryKey: &pbc.QueryKey{Ssid: int32(ssid)},
		Hostname: q.Get("hostname"),
	}
	if raw := q.Get("ccnt"); raw != "" {
		ccnt, parseErr := strconv.ParseInt(raw, 10, 32)
		if parseErr != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid ccnt")
			return
		}
		req.QueryKey.Ccnt = int32(ccnt)
	}
	if raw := q.Get("segindex"); raw != "" {
		segindex, parseErr := strconv.ParseInt(raw, 10, 32)
		if parseErr != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid segindex")
			return
		}
		value := int32(segindex)
		req.Segindex = &value
	}
	if raw := q.Get("slice_id"); raw != "" {
		sliceID, parseErr := strconv.ParseInt(raw, 10, 64)
		if parseErr != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid slice_id")
			return
		}
		req.SliceId = &sliceID
	}
	if raw := q.Get("page_size"); raw != "" {
		pageSize, parseErr := strconv.ParseInt(raw, 10, 64)
		if parseErr != nil || pageSize <= 0 {
			writeJSONError(w, http.StatusBadRequest, "invalid page_size")
			return
		}
		req.PageSize = pageSize
	}
	req.PageToken = q.Get("page_token")

	resp, err := s.grpcServer.GetGpPidProcInfo(r.Context(), req)
	if err != nil {
		s.logger.Errorf("UI GetGpPidProcInfo error: %v", err)
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeProtoJSON(w, http.StatusOK, resp)
}
