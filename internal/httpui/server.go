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

// Package httpui provides an HTTP server that serves a web-based Command Center
// UI and a JSON REST API wrapping the gRPC GetGPInfo and ActionService services.
//
// The JSON API endpoints mirror the existing CSV HTTP endpoints but return JSON
// instead of CSV, making them suitable for consumption by a browser-based SPA.
//
// Static frontend assets are served from an embedded filesystem (go:embed) with
// SPA fallback routing — any path that does not match an API route or a static
// file is served as index.html so that client-side routing works correctly.
package httpui

import (
	"net/http"

	ygrpc "github.com/open-gpdb/yagpcc/internal/grpc"
	"go.uber.org/zap"
)

// Server serves the web UI and its JSON API.
type Server struct {
	logger       *zap.SugaredLogger
	grpcServer   *ygrpc.GetMasterInfoServer
	actionServer *ygrpc.ActionsServer
	clusterID    string
}

// NewServer creates a new UI server that delegates data retrieval to the given
// gRPC servers.
func NewServer(
	logger *zap.SugaredLogger,
	grpcServer *ygrpc.GetMasterInfoServer,
	actionServer *ygrpc.ActionsServer,
	clusterID string,
) *Server {
	return &Server{
		logger:       logger,
		grpcServer:   grpcServer,
		actionServer: actionServer,
		clusterID:    clusterID,
	}
}

// RegisterRoutes registers all UI and API routes on the given mux.
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	// JSON API — read endpoints
	mux.HandleFunc("/api/sessions", s.handleGetSessions)
	mux.HandleFunc("/api/session/", s.handleGetSession)
	mux.HandleFunc("/api/queries", s.handleGetQueries)
	mux.HandleFunc("/api/query/", s.handleGetQuery)
	mux.HandleFunc("/api/stats/sessions", s.handleGetTotalSessionsStat)
	mux.HandleFunc("/api/extensions", s.handleGetExtensions)
	mux.HandleFunc("/api/databases", s.handleListDatabases)

	// JSON API — action endpoints
	mux.HandleFunc("/api/action/terminate-session", s.handleTerminateSession)
	mux.HandleFunc("/api/action/terminate-query", s.handleTerminateQuery)
	mux.HandleFunc("/api/action/terminate-sessions", s.handleTerminateSessions)
	mux.HandleFunc("/api/action/move-query-rsg", s.handleMoveQueryToResourceGroup)

	// Static files — SPA fallback
	mux.Handle("/", spaHandler())
}

// grpcServerReady reports whether the gRPC backend is available.
// It writes an HTTP 503 response and returns false when the server is nil.
func (s *Server) grpcServerReady(w http.ResponseWriter) bool {
	if s.grpcServer == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "UI handler is not available: master server not initialized")
		return false
	}
	return true
}

// actionServerReady reports whether the action gRPC backend is available.
func (s *Server) actionServerReady(w http.ResponseWriter) bool {
	if s.actionServer == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "action handler is not available: action server not initialized")
		return false
	}
	return true
}

// NewHTTPServer creates an http.Server with all UI routes registered.
func NewHTTPServer(addr string, server *Server) *http.Server {
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	return &http.Server{Addr: addr, Handler: mux}
}
