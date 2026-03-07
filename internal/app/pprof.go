package app

import (
	"context"
	"expvar"
	"fmt"
	"net/http"
	"net/http/pprof"
	"sync"
	"time"

	"go.uber.org/zap"
)

func NewDebugWebServer(httpAddr string, l *zap.SugaredLogger) *DebugWebServer {
	return &DebugWebServer{
		httpAddr: httpAddr,
		logger:   l,
	}
}

type DebugWebServer struct {
	*http.Server
	*http.ServeMux
	mu       sync.Mutex
	httpAddr string
	timer    *time.Timer
	logger   *zap.SugaredLogger
}

// ServeFor starts pprof webserver in the background goroutine.
// After Duration `t` server shuts down for security reasons.
func (dws *DebugWebServer) ServeFor(t time.Duration) error {
	dws.mu.Lock()
	defer dws.mu.Unlock()

	if dws.timer != nil {
		postponed := dws.timer.Reset(t)
		if !postponed {
			// Reset() started new countdown (because previous were executed), however we just
			// observed that `dws.timer` is not nil... it means that AfterFunc`s is running
			// concurrently (waiting for mutex)
			return fmt.Errorf("previous debug server is being shutdown, retry later")
		}
		return nil
	}

	dws.configureDebugWebServer()
	dws.timer = time.AfterFunc(t, func() {
		err := dws.Shutdown(context.Background())
		if err != nil {
			dws.logger.Errorf("Failed to shutdown debug webserver: %v", err)
		}
	})
	go func() {
		_ = dws.ListenAndServe()
	}()

	return nil
}

func (dws *DebugWebServer) configureDebugWebServer() {
	mux := http.NewServeMux()
	dws.Server = &http.Server{
		Addr:    dws.httpAddr,
		Handler: mux,
	}
	dws.ServeMux = mux

	dws.enablePprofEndpoints()
	dws.enableExpVarEndpoints()
}

// Shutdown synchronously shuts down debug server (if started).
func (dws *DebugWebServer) Shutdown(ctx context.Context) error {
	dws.mu.Lock()
	defer dws.mu.Unlock()

	if dws.timer == nil {
		return nil
	}
	dws.timer = nil
	return dws.Server.Shutdown(ctx)
}

// enablePprofEndpoints exposes pprof http endpoints.
func (dws *DebugWebServer) enablePprofEndpoints() {
	dws.HandleFunc("/debug/pprof/", pprof.Index)
	dws.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	dws.HandleFunc("/debug/pprof/profile", pprof.Profile)
	dws.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	dws.HandleFunc("/debug/pprof/trace", pprof.Trace)
}

// enableExpVarEndpoints exposes expvar http endpoints.
func (dws *DebugWebServer) enableExpVarEndpoints() {
	dws.HandleFunc("/debug/vars", expvar.Handler().ServeHTTP)
}
