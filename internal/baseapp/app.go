package baseapp

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/open-gpdb/yagpcc/internal/config"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

type App struct {
	l                     *zap.SugaredLogger
	cfg                   config.BaseConfig
	enableInstrumentation bool
	enableMetrics         bool
	shutdownCtx           context.Context
	shutdownFunc          context.CancelFunc

	insthttp *http.Server
}

const (
	// PrometheusEndpoint is where prometheus metrics can be found
	PrometheusEndpoint = "/metrics"
)

var stopSignals = []os.Signal{syscall.SIGINT, syscall.SIGTERM}

// WaitForStop waits for either SIGINT or SIGTERM
func WaitForStop() {
	<-prepareWait()
}

func prepareWait() <-chan os.Signal {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, stopSignals...)
	return sigs
}

// InstrumentationServer creates http server with pprof, prometheus and yasm endpoints
func InstrumentationServer(addr string, l *zap.SugaredLogger) *http.Server {
	mux := http.NewServeMux()
	// Redirect pprof prefix to default mux
	mux.Handle("/debug/pprof/", http.DefaultServeMux)
	mux.Handle(PrometheusEndpoint, promhttp.Handler())

	return &http.Server{Addr: addr, Handler: mux}
}

// Serve http server in separate goroutine
func Serve(srv *http.Server, l *zap.SugaredLogger) error {
	addr := srv.Addr
	if addr == "" {
		addr = ":http"
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("http server listen at %q:: %w", srv.Addr, err)
	}

	go func() {
		l.Info("serving http", zap.String("addr", srv.Addr))
		if err := srv.Serve(listener); err != nil && err != http.ErrServerClosed {
			l.Error("error while serving http", zap.String("addr", srv.Addr), zap.Error(err))
		}
	}()

	return nil
}

type AppOption = func(*App)

func DefaultOptions(cfg config.BaseConfig) []AppOption {
	return []AppOption{
		WithConfig(cfg),
	}
}

// WithInstrumentation enables instrumentation and endpoints
func WithInstrumentation() AppOption {
	return func(app *App) {
		app.enableInstrumentation = true
	}
}

// WithConfig sets default config. Config must be a pointer to struct and implement AppConfig interface.
// Config will be filled with loaded data if WithConfigLoad was provided.
func WithConfig(cfg config.BaseConfig) AppOption {
	return func(app *App) {
		app.cfg = cfg
	}
}

// WithMetrics enables metrics registry
func WithMetrics() AppOption {
	return func(app *App) {
		app.enableMetrics = true
	}
}

// New creates new application instance.
// One of these happens:
// - returns new application
// - returns an error
// - performs os.Exit(0) if one-time action was requested (like config generation)
func New(opts ...AppOption) (*App, error) {
	ctx, cancel := context.WithCancel(context.Background())
	// Create default app
	a := &App{
		cfg:          config.DefaultBaseConfig(),
		shutdownCtx:  ctx,
		shutdownFunc: cancel,
	}

	// Apply options
	for _, opt := range opts {
		opt(a)
	}

	// Create logger
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, fmt.Errorf("can't initialize zap logger: %v", err)
	}
	a.l = logger.Sugar()

	a.l.Debug("using application config", zap.Any("config", a.cfg))

	if a.enableInstrumentation {
		a.insthttp = InstrumentationServer(a.cfg.AppConfig().Instrumentation.Addr, a.l)
		if err := Serve(a.insthttp, a.l); err != nil {
			a.l.Debugf("got error in serve instrumentation %v", err)
			return nil, err
		}
	}

	return a, nil
}

func (a *App) WaitForStop() {
	WaitForStop()
	if a.L() != nil {
		a.L().Info("received shutdown signal, shutting down application")
	}
	a.Shutdown()
}

func ShutdownHttp(srv *http.Server, timeout time.Duration) error {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("error while shutting down http server %q: %w", srv.Addr, err)
	}

	return nil
}

func (a *App) Shutdown() {
	a.shutdownFunc()

	if a.enableInstrumentation {
		if err := ShutdownHttp(a.insthttp, a.cfg.AppConfig().Instrumentation.ShutdownTimeout); err != nil {
			a.l.Error("error while shutting down instrumentation server", zap.Error(err))
		}
	}
}

func (a *App) ShutdownContext() context.Context {
	return a.shutdownCtx
}

func (a *App) L() *zap.SugaredLogger {
	return a.l
}

// IsInstrumentationEnabled returns if instrumentation enabled
func (a *App) IsInstrumentationEnabled() bool {
	return a.enableInstrumentation
}

// Config returns app config
func (a *App) Config() config.BaseConfig {
	return a.cfg
}
