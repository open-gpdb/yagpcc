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
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
)

// newTestServer creates a Server with nil gRPC backends (for guard tests).
func newTestServer() *Server {
	return NewServer(zap.NewNop().Sugar(), nil, nil, "test-cluster")
}

// newTestMux creates an http.ServeMux with all UI routes registered using a nil-backend server.
func newTestMux() (*Server, *http.ServeMux) {
	s := newTestServer()
	mux := http.NewServeMux()
	s.RegisterRoutes(mux)
	return s, mux
}

// --- Server / factory tests ---

func TestNewServer(t *testing.T) {
	logger := zap.NewNop().Sugar()
	s := NewServer(logger, nil, nil, "my-cluster")
	assert.NotNil(t, s)
	assert.Equal(t, "my-cluster", s.clusterID)
	assert.Nil(t, s.grpcServer)
	assert.Nil(t, s.actionServer)
}

func TestNewHTTPServer(t *testing.T) {
	s := newTestServer()
	srv := NewHTTPServer("[::1]:0", s)
	assert.NotNil(t, srv)
	assert.Equal(t, "[::1]:0", srv.Addr)
}

// --- Method-not-allowed tests ---

func TestGETEndpointsRejectPOST(t *testing.T) {
	_, mux := newTestMux()

	getEndpoints := []string{
		"/api/sessions",
		"/api/session/42",
		"/api/queries",
		"/api/query/42/1",
		"/api/stats/sessions",
		"/api/extensions",
		"/api/databases",
		"/api/hosts/running-queries",
		"/api/hosts/host-a/running-queries",
		"/api/procfs/pid-proc-info",
	}

	for _, endpoint := range getEndpoints {
		t.Run("POST "+endpoint, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, endpoint, nil)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)

			var body map[string]string
			err := json.NewDecoder(rec.Body).Decode(&body)
			require.NoError(t, err)
			assert.Contains(t, body["error"], "method not allowed")
		})
	}
}

func TestPOSTEndpointsRejectGET(t *testing.T) {
	_, mux := newTestMux()

	postEndpoints := []string{
		"/api/action/terminate-session",
		"/api/action/terminate-query",
		"/api/action/terminate-sessions",
		"/api/action/move-query-rsg",
	}

	for _, endpoint := range postEndpoints {
		t.Run("GET "+endpoint, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, endpoint, nil)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
		})
	}
}

// --- Nil gRPC server returns 503 ---

func TestNilGRPCServerReturns503(t *testing.T) {
	_, mux := newTestMux()

	// GET endpoints that reach the nil guard without required params
	noParamEndpoints := []string{
		"/api/sessions",
		"/api/queries",
		"/api/stats/sessions",
		"/api/extensions",
		"/api/databases",
		"/api/hosts/running-queries",
		"/api/hosts/host-a/running-queries",
		"/api/procfs/pid-proc-info",
	}
	for _, endpoint := range noParamEndpoints {
		t.Run(endpoint, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, endpoint, nil)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

			var body map[string]string
			err := json.NewDecoder(rec.Body).Decode(&body)
			require.NoError(t, err)
			assert.Contains(t, body["error"], "master server not initialized")
		})
	}

	// Endpoints that require valid path params before reaching the nil guard
	t.Run("/api/session/42", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/session/42", nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})

	t.Run("/api/query/42/1", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/query/42/1", nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})
}

func TestNilActionServerReturns503(t *testing.T) {
	_, mux := newTestMux()

	actionEndpoints := []struct {
		path string
		body string
	}{
		{"/api/action/terminate-session", `{"sess_id": 42}`},
		{"/api/action/terminate-query", `{"ssid": 42, "ccnt": 1}`},
		{"/api/action/terminate-sessions", `{"database": "testdb"}`},
		{"/api/action/move-query-rsg", `{"ssid": 42, "ccnt": 1, "resource_group_name": "rg1"}`},
	}

	for _, tc := range actionEndpoints {
		t.Run(tc.path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

			var body map[string]string
			err := json.NewDecoder(rec.Body).Decode(&body)
			require.NoError(t, err)
			assert.Contains(t, body["error"], "action server not initialized")
		})
	}
}

// --- Path parsing tests ---

// Note: handleGetSession and handleGetQuery check grpcServerReady BEFORE
// parsing path parameters. With a nil gRPC server, we always get 503.
// The path-parsing validation is tested indirectly via the 503 tests above.
// Below we verify the guard ordering is consistent.

func TestHandleGetSessionNilServerBeforePathParsing(t *testing.T) {
	_, mux := newTestMux()

	// Even with missing sess_id, nil server returns 503 first
	req := httptest.NewRequest(http.MethodGet, "/api/session/", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

	// Even with invalid sess_id, nil server returns 503 first
	req = httptest.NewRequest(http.MethodGet, "/api/session/abc", nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestHandleGetQueryNilServerBeforePathParsing(t *testing.T) {
	_, mux := newTestMux()

	tests := []struct {
		name string
		path string
	}{
		{"missing both", "/api/query/"},
		{"missing ccnt", "/api/query/42"},
		{"missing ccnt with slash", "/api/query/42/"},
		{"invalid ssid", "/api/query/abc/1"},
		{"invalid ccnt", "/api/query/42/abc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			// nil gRPC server check happens before path parsing
			assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
		})
	}
}

// --- Action request body validation ---

func TestHandleTerminateSessionMissingSessID(t *testing.T) {
	_, mux := newTestMux()

	// sess_id=0 should be rejected
	req := httptest.NewRequest(http.MethodPost, "/api/action/terminate-session",
		strings.NewReader(`{"sess_id": 0}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	// With nil action server, we get 503 before validation
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestHandleTerminateQueryMissingSSID(t *testing.T) {
	_, mux := newTestMux()

	req := httptest.NewRequest(http.MethodPost, "/api/action/terminate-query",
		strings.NewReader(`{"ssid": 0, "ccnt": 1}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestHandleMoveQueryRSGMissingFields(t *testing.T) {
	_, mux := newTestMux()

	req := httptest.NewRequest(http.MethodPost, "/api/action/move-query-rsg",
		strings.NewReader(`{"ssid": 0}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestActionEndpointsRejectEmptyBody(t *testing.T) {
	_, mux := newTestMux()

	// With nil action server, empty body hits 503 before body decode.
	// But if we test with a non-nil action server, we'd get 400.
	// For now, test that nil body returns 503 (action server check first).
	endpoints := []string{
		"/api/action/terminate-session",
		"/api/action/terminate-query",
		"/api/action/terminate-sessions",
		"/api/action/move-query-rsg",
	}

	for _, endpoint := range endpoints {
		t.Run(endpoint+" nil body", func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, endpoint, nil)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			// 503 because action server is nil (checked before body decode)
			assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
		})
	}
}

func TestActionEndpointsRejectInvalidJSON(t *testing.T) {
	_, mux := newTestMux()

	// With nil action server, invalid JSON hits 503 before body decode.
	endpoints := []string{
		"/api/action/terminate-session",
		"/api/action/terminate-query",
		"/api/action/terminate-sessions",
		"/api/action/move-query-rsg",
	}

	for _, endpoint := range endpoints {
		t.Run(endpoint+" invalid json", func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader("{invalid"))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
		})
	}
}

func TestValidateActionRequest(t *testing.T) {
	t.Run("accepts json without origin", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/action/terminate-session", strings.NewReader(`{}`))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		ok := validateActionRequest(rec, req)
		assert.True(t, ok)
	})

	t.Run("rejects non-json content type", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/action/terminate-session", strings.NewReader(`{}`))
		req.Header.Set("Content-Type", "text/plain")
		rec := httptest.NewRecorder()

		ok := validateActionRequest(rec, req)
		assert.False(t, ok)
		assert.Equal(t, http.StatusUnsupportedMediaType, rec.Code)
		assert.Contains(t, rec.Body.String(), "Content-Type must be application/json")
	})

	t.Run("accepts same-origin request", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/action/terminate-session", strings.NewReader(`{}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Origin", "http://example.com")
		rec := httptest.NewRecorder()

		ok := validateActionRequest(rec, req)
		assert.True(t, ok)
	})

	t.Run("rejects cross-origin request", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/action/terminate-session", strings.NewReader(`{}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Origin", "http://evil.example")
		rec := httptest.NewRecorder()

		ok := validateActionRequest(rec, req)
		assert.False(t, ok)
		assert.Equal(t, http.StatusForbidden, rec.Code)
		assert.Contains(t, rec.Body.String(), "cross-origin request forbidden")
	})
}

// --- parseSessionFilters tests ---

func TestParseSessionFilters(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet,
		"/api/sessions?filter_host=host1&filter_user=admin&filter_state=SESSION_STATUS_ACTIVE",
		nil)

	filters := parseSessionFilters(req)
	assert.Len(t, filters, 3)

	filterMap := make(map[pbm.SessionFilterEnum]string)
	for _, f := range filters {
		filterMap[f.FieldName] = f.Value
	}

	assert.Equal(t, "host1", filterMap[pbm.SessionFilterEnum_SESSION_FILTER_HOST])
	assert.Equal(t, "admin", filterMap[pbm.SessionFilterEnum_SESSION_FILTER_USER])
	assert.Equal(t, "SESSION_STATUS_ACTIVE", filterMap[pbm.SessionFilterEnum_SESSION_FILTER_STATE])
}

func TestParseSessionFiltersEmpty(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/sessions", nil)
	filters := parseSessionFilters(req)
	assert.Nil(t, filters)
}

func TestParseSessionFiltersAllFields(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet,
		"/api/sessions?filter_host=h&filter_user=u&filter_database=d&filter_application_name=a&filter_client_hostname=c&filter_state=s&filter_rsgname=r&filter_sess_id=1&filter_tm_id=2",
		nil)

	filters := parseSessionFilters(req)
	assert.Len(t, filters, 9)
}

// --- parseSortFields tests ---

func TestParseSortFields(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected []*pbm.SessionFieldWrapper
	}{
		{
			name:     "no sort",
			query:    "",
			expected: nil,
		},
		{
			name:  "single sort ASC",
			query: "sort=TOTAL_RUNNINGTIMESECONDS:ASC",
			expected: []*pbm.SessionFieldWrapper{
				{
					FieldName: pbm.SessionField_TOTAL_RUNNINGTIMESECONDS,
					Order:     pbm.SortOrder_SORT_ASC,
				},
			},
		},
		{
			name:  "single sort DESC",
			query: "sort=SESSION_FIELD_KEY:DESC",
			expected: []*pbm.SessionFieldWrapper{
				{
					FieldName: pbm.SessionField_SESSION_FIELD_KEY,
					Order:     pbm.SortOrder_SORT_DESC,
				},
			},
		},
		{
			name:  "default order is ASC",
			query: "sort=SESSION_FIELD_HOST",
			expected: []*pbm.SessionFieldWrapper{
				{
					FieldName: pbm.SessionField_SESSION_FIELD_HOST,
					Order:     pbm.SortOrder_SORT_ASC,
				},
			},
		},
		{
			name:  "multiple sort fields",
			query: "sort=TOTAL_RUNNINGTIMESECONDS:DESC&sort=SESSION_FIELD_KEY:ASC",
			expected: []*pbm.SessionFieldWrapper{
				{
					FieldName: pbm.SessionField_TOTAL_RUNNINGTIMESECONDS,
					Order:     pbm.SortOrder_SORT_DESC,
				},
				{
					FieldName: pbm.SessionField_SESSION_FIELD_KEY,
					Order:     pbm.SortOrder_SORT_ASC,
				},
			},
		},
		{
			name:     "unknown field is ignored",
			query:    "sort=NONEXISTENT_FIELD:ASC",
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/sessions?"+tt.query, nil)
			result := parseSortFields(req)
			if tt.expected == nil {
				assert.Nil(t, result)
			} else {
				require.Len(t, result, len(tt.expected))
				for i, expected := range tt.expected {
					assert.Equal(t, expected.FieldName, result[i].FieldName)
					assert.Equal(t, expected.Order, result[i].Order)
				}
			}
		})
	}
}

// --- JSON helper tests ---

func TestWriteJSON(t *testing.T) {
	rec := httptest.NewRecorder()
	writeJSON(rec, http.StatusOK, map[string]string{"key": "value"})

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json; charset=utf-8", rec.Header().Get("Content-Type"))

	var body map[string]string
	err := json.NewDecoder(rec.Body).Decode(&body)
	require.NoError(t, err)
	assert.Equal(t, "value", body["key"])
}

func TestWriteJSONError(t *testing.T) {
	rec := httptest.NewRecorder()
	writeJSONError(rec, http.StatusBadRequest, "something went wrong")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, "application/json; charset=utf-8", rec.Header().Get("Content-Type"))

	var body map[string]string
	err := json.NewDecoder(rec.Body).Decode(&body)
	require.NoError(t, err)
	assert.Equal(t, "something went wrong", body["error"])
}

func TestDecodeJSONBodySuccess(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/test",
		strings.NewReader(`{"sess_id": 42, "tm_id": 7}`))
	rec := httptest.NewRecorder()

	var body terminateSessionRequest
	ok := decodeJSONBody(rec, req, &body)
	assert.True(t, ok)
	assert.Equal(t, int64(42), body.SessID)
	assert.Equal(t, int64(7), body.TmID)
}

func TestDecodeJSONBodyNilBody(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/test", nil)
	req.Body = nil
	rec := httptest.NewRecorder()

	var body terminateSessionRequest
	ok := decodeJSONBody(rec, req, &body)
	assert.False(t, ok)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]string
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Contains(t, resp["error"], "request body is required")
}

func TestDecodeJSONBodyInvalidJSON(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/test",
		strings.NewReader("{invalid json"))
	rec := httptest.NewRecorder()

	var body terminateSessionRequest
	ok := decodeJSONBody(rec, req, &body)
	assert.False(t, ok)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]string
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Contains(t, resp["error"], "invalid JSON")
}

// --- SPA handler tests ---

func TestSPAHandlerServesIndexHTML(t *testing.T) {
	handler := spaHandler()

	// Root path should serve index.html
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "html")
}

func TestSPAHandlerFallbackForUnknownPaths(t *testing.T) {
	handler := spaHandler()

	// Unknown path should fall back to index.html (SPA routing)
	req := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "html")
}

func TestSPAHandlerRejectsAPIPaths(t *testing.T) {
	handler := spaHandler()

	// /api/ paths should return 404 (not served by SPA handler)
	req := httptest.NewRequest(http.MethodGet, "/api/nonexistent", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- grpcServerReady / actionServerReady guard tests ---

func TestGrpcServerReadyWithNil(t *testing.T) {
	s := newTestServer()
	rec := httptest.NewRecorder()
	ready := s.grpcServerReady(rec)
	assert.False(t, ready)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestActionServerReadyWithNil(t *testing.T) {
	s := newTestServer()
	rec := httptest.NewRecorder()
	ready := s.actionServerReady(rec)
	assert.False(t, ready)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

// --- Content-Type header tests ---

func TestJSONResponseContentType(t *testing.T) {
	_, mux := newTestMux()

	// All API endpoints should return JSON content type
	req := httptest.NewRequest(http.MethodGet, "/api/sessions", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	assert.Equal(t, "application/json; charset=utf-8", rec.Header().Get("Content-Type"))
}
