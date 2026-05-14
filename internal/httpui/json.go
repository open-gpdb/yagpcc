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
	"encoding/json"
	"net/http"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// protojsonMarshaler is the shared marshaler for converting protobuf messages
// to JSON. It uses camelCase field names and emits fields with default values
// so the frontend always receives a consistent shape.
var protojsonMarshaler = protojson.MarshalOptions{
	UseProtoNames:   false, // use camelCase
	EmitUnpopulated: true,  // include zero-value fields
}

// writeJSON writes a Go value as JSON to the response.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// Best-effort; headers already sent.
		_ = err
	}
}

// writeProtoJSON writes a protobuf message as JSON to the response using
// protojson for proper field naming and timestamp formatting.
func writeProtoJSON(w http.ResponseWriter, status int, msg proto.Message) {
	data, err := protojsonMarshaler.Marshal(msg)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to marshal response")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

// writeJSONError writes a JSON error response.
func writeJSONError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

// decodeJSONBody decodes the request body into the given value.
// Returns false and writes an error response if decoding fails.
func decodeJSONBody(w http.ResponseWriter, r *http.Request, v any) bool {
	if r.Body == nil {
		writeJSONError(w, http.StatusBadRequest, "request body is required")
		return false
	}
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return false
	}
	return true
}
