// Package httpcsv provides an HTTP service that mirrors the gRPC GetGPInfo
// service but returns data in CSV format.
//
// Endpoints:
//
//	GET /csv/sessions          — same as GetGPSessions
//	GET /csv/session           — same as GetGPSession
//	GET /csv/queries           — same as GetGPQueries
//	GET /csv/query             — same as GetGPQuery
//	GET /csv/total_sessions_stat — same as GetTotalSessionsStat
package httpcsv

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	ygrpc "github.com/open-gpdb/yagpcc/internal/grpc"
	"go.uber.org/zap"
)

// Handler serves CSV responses for the GetGPInfo service endpoints.
// It delegates all business logic to the existing gRPC GetMasterInfoServer.
type Handler struct {
	logger     *zap.SugaredLogger
	grpcServer *ygrpc.GetMasterInfoServer
}

// NewHandler creates a new CSV HTTP handler that delegates to the given gRPC server.
func NewHandler(logger *zap.SugaredLogger, grpcServer *ygrpc.GetMasterInfoServer) *Handler {
	return &Handler{
		logger:     logger,
		grpcServer: grpcServer,
	}
}

// grpcServerReady reports whether the gRPC backend is available.
// It writes an HTTP 503 response and returns false when the server is nil,
// allowing callers to return immediately without panicking.
func (h *Handler) grpcServerReady(w http.ResponseWriter) bool {
	if h.grpcServer == nil {
		http.Error(w, "CSV handler is not available: master server not initialized", http.StatusServiceUnavailable)
		return false
	}
	return true
}

// RegisterRoutes registers all CSV endpoints on the given mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/csv/sessions", h.handleGetGPSessions)
	mux.HandleFunc("/csv/session", h.handleGetGPSession)
	mux.HandleFunc("/csv/queries", h.handleGetGPQueries)
	mux.HandleFunc("/csv/query", h.handleGetGPQuery)
	mux.HandleFunc("/csv/total_sessions_stat", h.handleGetTotalSessionsStat)
}

// parseSessionFilters parses filter query parameters.
// Filters are specified as: filter_host=value, filter_user=value, etc.
func parseSessionFilters(r *http.Request) []*pbm.SessionFilter {
	filterMap := map[string]pbm.SessionFilterEnum{
		"filter_host":             pbm.SessionFilterEnum_SESSION_FILTER_HOST,
		"filter_user":             pbm.SessionFilterEnum_SESSION_FILTER_USER,
		"filter_database":         pbm.SessionFilterEnum_SESSION_FILTER_DATABASE,
		"filter_application_name": pbm.SessionFilterEnum_SESSION_FILTER_APPLICATION_NAME,
		"filter_client_hostname":  pbm.SessionFilterEnum_SESSION_FILTER_CLIENT_HOSTNAME,
		"filter_state":            pbm.SessionFilterEnum_SESSION_FILTER_STATE,
		"filter_rsgname":          pbm.SessionFilterEnum_SESSION_FILTER_RSGNAME,
		"filter_sess_id":          pbm.SessionFilterEnum_SESSION_FILTER_SESS_ID,
		"filter_tm_id":            pbm.SessionFilterEnum_SESSION_FILTER_TM_ID,
	}

	var filters []*pbm.SessionFilter
	for param, enum := range filterMap {
		if val := r.URL.Query().Get(param); val != "" {
			filters = append(filters, &pbm.SessionFilter{
				FieldName: enum,
				Value:     val,
			})
		}
	}
	return filters
}

// parseSortFields parses sort query parameters.
// Sort is specified as: sort=FIELD_NAME:ASC or sort=FIELD_NAME:DESC
// Multiple sort fields can be specified by repeating the parameter.
func parseSortFields(r *http.Request) []*pbm.SessionFieldWrapper {
	sortParams := r.URL.Query()["sort"]
	if len(sortParams) == 0 {
		return nil
	}

	var fields []*pbm.SessionFieldWrapper
	for _, s := range sortParams {
		parts := strings.SplitN(s, ":", 2)
		fieldName := parts[0]
		order := pbm.SortOrder_SORT_ASC
		if len(parts) == 2 && strings.EqualFold(parts[1], "DESC") {
			order = pbm.SortOrder_SORT_DESC
		}

		// Look up the field enum value by name
		if val, ok := pbm.SessionField_value[fieldName]; ok {
			fields = append(fields, &pbm.SessionFieldWrapper{
				FieldName: pbm.SessionField(val),
				Order:     order,
			})
		}
	}
	return fields
}

// handleGetGPSessions handles GET /csv/sessions
//
// Query parameters:
//   - show_system: bool (default false)
//   - show_query_type: RQT_TOP|RQT_LAST (default RQT_TOP)
//   - hide_empty_queries: bool (default false)
//   - page_size: int (default 100)
//   - page_token: string
//   - sort: FIELD_NAME:ASC|DESC (repeatable)
//   - filter_*: string (see parseSessionFilters)
func (h *Handler) handleGetGPSessions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !h.grpcServerReady(w) {
		return
	}

	q := r.URL.Query()

	showSystem, _ := strconv.ParseBool(q.Get("show_system"))
	hideEmptyQueries, _ := strconv.ParseBool(q.Get("hide_empty_queries"))

	filters := parseSessionFilters(r)
	sortFields := parseSortFields(r)

	pageSize := int64(0)
	if ps := q.Get("page_size"); ps != "" {
		if v, err := strconv.ParseInt(ps, 10, 64); err == nil && v > 0 {
			pageSize = v
		}
	}
	pageToken := q.Get("page_token")

	queryType := pbm.RunningQueryType_RQT_UNSPECIFIED
	if qt := q.Get("show_query_type"); qt != "" {
		if val, ok := pbm.RunningQueryType_value[qt]; ok {
			queryType = pbm.RunningQueryType(val)
		}
	}

	req := &pbm.GetGPSessionsReq{
		ShowSystem:       showSystem,
		Field:            sortFields,
		Filter:           filters,
		PageSize:         pageSize,
		PageToken:        pageToken,
		HideEmptyQueries: hideEmptyQueries,
		ShowQueryType:    queryType,
	}

	resp, err := h.grpcServer.GetGPSessions(r.Context(), req)
	if err != nil {
		h.logger.Errorf("CSV GetGPSessions error: %v", err)
		http.Error(w, fmt.Sprintf("error: %v", err), http.StatusInternalServerError)
		return
	}

	h.writeSessionsCSV(w, resp.SessionsState, resp.NextPageToken, "sessions.csv")
}

// handleGetGPSession handles GET /csv/session
//
// Query parameters:
//   - sess_id: int64 (required)
func (h *Handler) handleGetGPSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()

	sessIDStr := q.Get("sess_id")
	if sessIDStr == "" {
		http.Error(w, "sess_id parameter is required", http.StatusBadRequest)
		return
	}
	sessID, err := strconv.ParseInt(sessIDStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid sess_id parameter", http.StatusBadRequest)
		return
	}
	if !h.grpcServerReady(w) {
		return
	}

	req := &pbm.GetGPSessionReq{
		SessionKey: &pbc.SessionKey{
			SessId: sessID,
		},
	}

	resp, err := h.grpcServer.GetGPSession(r.Context(), req)
	if err != nil {
		h.logger.Errorf("CSV GetGPSession error: %v", err)
		http.Error(w, fmt.Sprintf("error: %v", err), http.StatusInternalServerError)
		return
	}

	var sessions []*pbc.SessionState
	if resp.SessionsState != nil {
		sessions = []*pbc.SessionState{resp.SessionsState}
	}

	h.writeSessionsCSV(w, sessions, "", "session.csv")
}

// handleGetGPQueries handles GET /csv/queries
//
// Query parameters:
//   - page_size: int (default 100)
//   - page_token: string
//   - sort: FIELD_NAME:ASC|DESC (repeatable)
//   - filter_*: string (see parseSessionFilters)
func (h *Handler) handleGetGPQueries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !h.grpcServerReady(w) {
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

	resp, err := h.grpcServer.GetGPQueries(r.Context(), req)
	if err != nil {
		h.logger.Errorf("CSV GetGPQueries error: %v", err)
		http.Error(w, fmt.Sprintf("error: %v", err), http.StatusInternalServerError)
		return
	}

	h.writeSessionsCSV(w, resp.SessionsState, resp.NextPageToken, "queries.csv")
}

// handleGetGPQuery handles GET /csv/query
//
// Query parameters:
//   - ssid: int32 (required)
//   - ccnt: int32 (required)
func (h *Handler) handleGetGPQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()

	ssidStr := q.Get("ssid")
	ccntStr := q.Get("ccnt")
	if ssidStr == "" || ccntStr == "" {
		http.Error(w, "ssid and ccnt parameters are required", http.StatusBadRequest)
		return
	}

	ssid, err := strconv.ParseInt(ssidStr, 10, 32)
	if err != nil {
		http.Error(w, "invalid ssid parameter", http.StatusBadRequest)
		return
	}
	ccnt, err := strconv.ParseInt(ccntStr, 10, 32)
	if err != nil {
		http.Error(w, "invalid ccnt parameter", http.StatusBadRequest)
		return
	}
	if !h.grpcServerReady(w) {
		return
	}

	req := &pbm.GetGPQueryReq{
		QueryKey: &pbc.QueryKey{
			Ssid: int32(ssid),
			Ccnt: int32(ccnt),
		},
	}

	resp, err := h.grpcServer.GetGPQuery(r.Context(), req)
	if err != nil {
		h.logger.Errorf("CSV GetGPQuery error: %v", err)
		http.Error(w, fmt.Sprintf("error: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", "attachment; filename=query.csv")

	if err := WriteQueryDataCSV(w, resp.QueriesData); err != nil {
		h.logger.Errorf("error writing CSV: %v", err)
	}
}

// handleGetTotalSessionsStat handles GET /csv/total_sessions_stat
func (h *Handler) handleGetTotalSessionsStat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !h.grpcServerReady(w) {
		return
	}

	req := &pbm.GetTotalSessionsReq{}

	resp, err := h.grpcServer.GetTotalSessionsStat(r.Context(), req)
	if err != nil {
		h.logger.Errorf("CSV GetTotalSessionsStat error: %v", err)
		http.Error(w, fmt.Sprintf("error: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", "attachment; filename=total_sessions_stat.csv")

	if err := WriteTotalSessionsStatCSV(w, resp.SessionsStat); err != nil {
		h.logger.Errorf("error writing CSV: %v", err)
	}
}

// writeSessionsCSV writes the sessions as CSV with appropriate headers.
// The filename parameter sets the Content-Disposition attachment filename.
// If nextPageToken is non-empty, it is included as an X-Next-Page-Token response header.
func (h *Handler) writeSessionsCSV(w http.ResponseWriter, sessions []*pbc.SessionState, nextPageToken, filename string) {
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", "attachment; filename="+filename)
	if nextPageToken != "" {
		w.Header().Set("X-Next-Page-Token", nextPageToken)
	}

	if err := WriteSessionsCSV(w, sessions); err != nil {
		h.logger.Errorf("error writing CSV: %v", err)
	}
}

// NewCSVServer creates an http.Server with all CSV routes registered.
func NewCSVServer(addr string, handler *Handler) *http.Server {
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	return &http.Server{Addr: addr, Handler: mux}
}
