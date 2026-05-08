package httpcsv

import (
	"encoding/csv"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
)

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
			req := httptest.NewRequest(http.MethodGet, "/csv/sessions?"+tt.query, nil)
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

func TestParseSessionFilters(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet,
		"/csv/sessions?filter_host=host1&filter_user=admin&filter_state=SESSION_STATUS_ACTIVE",
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

func TestHandleGetTotalSessionsStat(t *testing.T) {
	// This test verifies the CSV output format for total sessions stat
	stats := []*pbm.SessionStat{
		{State: "active", Count: 3},
		{State: "idle", Count: 7},
	}

	// Use WriteTotalSessionsStatCSV directly since we can't easily mock GetMasterInfoServer
	recorder := httptest.NewRecorder()
	recorder.Header().Set("Content-Type", "text/csv; charset=utf-8")
	err := WriteTotalSessionsStatCSV(recorder, stats)
	require.NoError(t, err)

	assert.Equal(t, "text/csv; charset=utf-8", recorder.Header().Get("Content-Type"))

	reader := csv.NewReader(recorder.Body)
	records, err := reader.ReadAll()
	require.NoError(t, err)
	assert.Len(t, records, 3) // header + 2 rows
}

func TestMethodNotAllowed(t *testing.T) {
	logger := zap.NewNop().Sugar()
	handler := NewHandler(logger, nil)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	// POST should be rejected (method check happens before grpcServer is used)
	endpoints := []string{
		"/csv/sessions",
		"/csv/session",
		"/csv/queries",
		"/csv/query",
		"/csv/total_sessions_stat",
	}

	for _, endpoint := range endpoints {
		t.Run("POST "+endpoint, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, endpoint, nil)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
		})
	}
}

func TestHandleGetGPSessionMissingSessID(t *testing.T) {
	logger := zap.NewNop().Sugar()
	handler := NewHandler(logger, nil)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/csv/session", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleGetGPSessionInvalidSessID(t *testing.T) {
	logger := zap.NewNop().Sugar()
	handler := NewHandler(logger, nil)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/csv/session?sess_id=abc", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleGetGPQueryMissingParams(t *testing.T) {
	logger := zap.NewNop().Sugar()
	handler := NewHandler(logger, nil)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	// Missing both
	req := httptest.NewRequest(http.MethodGet, "/csv/query", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Missing ccnt
	req = httptest.NewRequest(http.MethodGet, "/csv/query?ssid=42", nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Invalid ssid
	req = httptest.NewRequest(http.MethodGet, "/csv/query?ssid=abc&ccnt=1", nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestNewCSVServer(t *testing.T) {
	logger := zap.NewNop().Sugar()
	handler := NewHandler(logger, nil)
	srv := NewCSVServer("[::1]:0", handler)
	assert.NotNil(t, srv)
	assert.Equal(t, "[::1]:0", srv.Addr)
}

func TestNilGRPCServerReturns503(t *testing.T) {
	logger := zap.NewNop().Sugar()
	handler := NewHandler(logger, nil)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	// Endpoints that reach the nil guard without required params
	noParamEndpoints := []string{
		"/csv/sessions",
		"/csv/queries",
		"/csv/total_sessions_stat",
		"/csv/extensions",
	}
	for _, endpoint := range noParamEndpoints {
		t.Run(endpoint, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, endpoint, nil)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
		})
	}

	// Endpoints that require valid params before reaching the nil guard
	t.Run("/csv/session with valid sess_id", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/csv/session?sess_id=42", nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})

	t.Run("/csv/query with valid params", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/csv/query?ssid=42&ccnt=1", nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})
}
