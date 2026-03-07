package app

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gofrs/flock"
	gogrpc "google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pb "github.com/open-gpdb/yagpcc/api/proto/agent_segment"
	"github.com/open-gpdb/yagpcc/internal/baseapp"
	"github.com/open-gpdb/yagpcc/internal/config"
	"github.com/open-gpdb/yagpcc/internal/gp"
	"github.com/open-gpdb/yagpcc/internal/gp/master_sentinel"
	"github.com/open-gpdb/yagpcc/internal/gp/stat_activity"
	"github.com/open-gpdb/yagpcc/internal/grpc"
	"github.com/open-gpdb/yagpcc/internal/master"
	"github.com/open-gpdb/yagpcc/internal/metrics"
	"github.com/open-gpdb/yagpcc/internal/storage"
	"github.com/open-gpdb/yagpcc/internal/uds"
	"go.uber.org/zap"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type AgentApp struct {
	*baseapp.App
	Config      *config.Config
	GrpcServer  *gogrpc.Server
	SetQIServer *grpc.SetQueryInfoServer
	pingHttp    *http.Server
	filelock    *flock.Flock
}

type keepAliveCheck struct {
	name      string
	checkFunc func() bool
}

type pingHandler struct {
	mu     sync.Mutex // only one ping in progress
	logger *zap.SugaredLogger

	backgroundStorage *master.BackgroundStorage
	keepAliveChecks   []keepAliveCheck
}

const (
	PING_TIMEOUT            = 1 * time.Second
	KEEPALIVE_CHECK_TIMEOUT = 16 * time.Minute
)

func NewPingHandler(logger *zap.SugaredLogger, backgroundStorage *master.BackgroundStorage) *pingHandler {
	return &pingHandler{
		keepAliveChecks:   make([]keepAliveCheck, 0),
		logger:            logger,
		backgroundStorage: backgroundStorage,
	}
}

func (h *pingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	start := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), PING_TIMEOUT)
	defer cancel()

	c := make(chan interface{})

	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if h.backgroundStorage.RQStorage.CanLock() {
					if h.backgroundStorage.SessionStorage.CanLock() {
						c <- struct{}{}
						close(c)
						return
					}
				}
				/* } */
			}
			time.Sleep(PING_TIMEOUT / 20)
		}
	}(ctx)

	for _, check := range h.keepAliveChecks {
		if !check.checkFunc() {
			h.logger.Infof("keep-alive check '%s' failed", check.name)
			w.WriteHeader(http.StatusInternalServerError)

			_, err := fmt.Fprintf(w, "2; Failed to check %s, took %s", check.name, time.Since(start))
			if err != nil {
				h.logger.Errorf("Failed to send data: %v", err)
			}
			return
		}
	}

	select {
	case <-ctx.Done():
		h.logger.Infof("lock check failed in %s", time.Since(start))
		w.WriteHeader(http.StatusInternalServerError)
		_, err := fmt.Fprintf(w, "2; Failed to lock data in %s", time.Since(start))
		if err != nil {
			h.logger.Errorf("Failed to send data: %v", err)
		}

	case <-c:
		_, err := fmt.Fprintf(w, "0; All Ok, took %s", time.Since(start))
		if err != nil {
			h.logger.Errorf("Failed to send data: %v", err)
		}
	}
}

func requestDurationBuckets() []float64 {
	return []float64{
		(1 * time.Millisecond).Seconds(),
		(10 * time.Millisecond).Seconds(),
		(20 * time.Millisecond).Seconds(),
		(50 * time.Millisecond).Seconds(),
		(100 * time.Millisecond).Seconds(),
		(200 * time.Millisecond).Seconds(),
		(500 * time.Millisecond).Seconds(),
		(1 * time.Second).Seconds(),
		(2 * time.Second).Seconds(),
		(5 * time.Second).Seconds(),
		(10 * time.Second).Seconds(),
		(20 * time.Second).Seconds(),
		(30 * time.Second).Seconds(),
	}

}

func requestHistogramLabels() []string {
	return []string{
		"method",
	}

}

func requestQueryBuckets() []float64 {
	return []float64{
		(10 * time.Millisecond).Seconds(),
		(50 * time.Millisecond).Seconds(),
		(100 * time.Millisecond).Seconds(),
		(1 * time.Second).Seconds(),
		(2 * time.Second).Seconds(),
		(5 * time.Second).Seconds(),
		(10 * time.Second).Seconds(),
		(30 * time.Second).Seconds(),
		(1 * time.Minute).Seconds(),
		(2 * time.Minute).Seconds(),
		(5 * time.Minute).Seconds(),
		(10 * time.Minute).Seconds(),
		(30 * time.Minute).Seconds(),
		(1 * time.Hour).Seconds(),
		(2 * time.Hour).Seconds(),
		(3 * time.Hour).Seconds(),
		(5 * time.Hour).Seconds(),
		(12 * time.Hour).Seconds(),
	}
}

func requestQueryLabels() []string {
	return []string{}

}

func requestQueryStatuses() []string {
	return []string{
		"status",
	}
}

func (app *AgentApp) lockFile() error {
	app.filelock = flock.New(app.Config.Lockfile)
	if locked, err := app.filelock.TryLock(); !locked {
		msg := "Possibly another instance is running."
		if err != nil {
			msg = err.Error()
		}
		return fmt.Errorf("failed to acquire lock on %s: %s", app.Config.Lockfile, msg)
	}
	return nil
}

func (app *AgentApp) unlockFile() {
	_ = app.filelock.Unlock()
}

func InitMetrics() {

	metrics.YagpccMetrics = &metrics.YagpccMetricsType{
		NewSessions:          promauto.NewCounter(prometheus.CounterOpts{Name: "new_sessions"}),
		NewQueries:           promauto.NewCounter(prometheus.CounterOpts{Name: "new_queries"}),
		DeletedSessions:      promauto.NewCounter(prometheus.CounterOpts{Name: "deleted_sessions"}),
		DeletedQueries:       promauto.NewCounter(prometheus.CounterOpts{Name: "deleted_queries"}),
		NewAggregatedQueries: promauto.NewCounter(prometheus.CounterOpts{Name: "new_aggregated_queries"}),
		DroppedQueries:       promauto.NewCounter(prometheus.CounterOpts{Name: "dropped_queries"}),

		TotalSessions:     promauto.NewGauge(prometheus.GaugeOpts{Name: "total_sessions"}),
		TotalQueries:      promauto.NewGauge(prometheus.GaugeOpts{Name: "total_queries"}),
		AggregatedQueries: promauto.NewGauge(prometheus.GaugeOpts{Name: "aggregated_queries"}),

		HandleLatencies: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "time",
			Buckets: requestDurationBuckets(),
		}, requestHistogramLabels()),
		QueryLatencies: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "query",
			Buckets: requestQueryBuckets(),
		}, requestQueryLabels()),
		SliceLatencies: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "slice",
				Buckets: requestQueryBuckets(),
			}, requestQueryLabels()),
		FailedQueries:   promauto.NewCounter(prometheus.CounterOpts{Name: "failed_queries"}),
		QueryStatuses:   promauto.NewCounterVec(prometheus.CounterOpts{Name: "query_statuses"}, requestQueryStatuses()),
		QueriesInFlight: promauto.NewGauge(prometheus.GaugeOpts{Name: "queries_in_flight"}),
	}

}

func NewApp(
	baseApp *baseapp.App,
	config *config.Config,
	statActivityLister *stat_activity.Lister,
	backgroundStorage *master.BackgroundStorage,
) (*AgentApp, error) {
	s := gogrpc.NewServer(
		gogrpc.MaxRecvMsgSize(int(config.MaxMessageSize)),
		gogrpc.MaxSendMsgSize(int(config.MaxMessageSize)),
	)
	agentApp := &AgentApp{
		App:        baseApp,
		Config:     config,
		GrpcServer: s,
	}
	if config.Role == "segment" {
		agentApp.SetQIServer = &grpc.SetQueryInfoServer{Logger: baseApp.L(), LogWorkAmount: config.SegmentLogWorkAmount, RQStorage: backgroundStorage.RQStorage, SessionsStorage: backgroundStorage.SessionStorage}
		pb.RegisterSetQueryInfoServer(s, agentApp.SetQIServer)
		pb.RegisterGetQueryInfoServer(s, &grpc.GetQueryInfoServer{Logger: baseApp.L(), MaxMessageSize: int(config.MaxMessageSize), RQStorage: backgroundStorage.RQStorage})
		pb.RegisterAgentControlServer(s, &grpc.AgentControlServer{Logger: baseApp.L(), RQStorage: backgroundStorage.RQStorage})
	}
	if config.Role == "master" {
		agentApp.SetQIServer = &grpc.SetQueryInfoServer{Logger: baseApp.L(), UpdateSessionMetrics: true, RQStorage: backgroundStorage.RQStorage, SessionsStorage: backgroundStorage.SessionStorage}
		pb.RegisterSetQueryInfoServer(s, agentApp.SetQIServer)
		pb.RegisterGetQueryInfoServer(s, &grpc.GetQueryInfoServer{Logger: baseApp.L(), MaxMessageSize: int(config.MaxOuterMessageSize), RQStorage: backgroundStorage.RQStorage})
		pb.RegisterAgentControlServer(s, &grpc.AgentControlServer{Logger: baseApp.L(), RQStorage: backgroundStorage.RQStorage})

		getMasterInfo := grpc.NewGetMasterInfoServer(config.ClusterID, baseApp.L(), statActivityLister, int(config.MaxOuterMessageSize), backgroundStorage)
		actionInfo := &grpc.ActionsServer{ClusterID: config.ClusterID, Logger: baseApp.L(), Timeout: 5 * time.Minute}

		pbm.RegisterGetGPInfoServer(s, getMasterInfo)
		pbm.RegisterActionServiceServer(s, actionInfo)

	}
	// Register reflection service on gRPC server.
	reflection.Register(s)

	return agentApp, nil
}

func OpenUDS(path string, logger *zap.SugaredLogger) (net.Listener, error) {
	if _, err := os.Stat(path); err == nil {
		// we holding the exclusive lock and may recreate UDS file
		err = os.Remove(path)
		if err != nil {
			logger.Errorf("failed to remove socket file %v", err)
		}
	}
	uds, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	// group members should also can connect to UDS
	err = os.Chmod(path, 0775)
	if err != nil {
		return nil, err
	}
	return uds, nil
}

func (app *AgentApp) RunPingHandler(backgroundStorage *master.BackgroundStorage) error {
	mux := http.NewServeMux()
	pingH := NewPingHandler(app.L(), backgroundStorage)
	// this check runs only on master
	mux.Handle("/ping", pingH)
	pingAddr := fmt.Sprintf("[::1]:%d", app.Config.PingPort)
	app.pingHttp = &http.Server{Addr: pingAddr, Handler: mux}
	if err := baseapp.Serve(app.pingHttp, app.L()); err != nil {
		app.L().Errorf("got error in serve ping %v", err)
		return err
	}
	return nil
}

func (app *AgentApp) RunDebugHandler() {
	debugAddr := fmt.Sprintf("[::1]:%d", app.Config.DebugPort)
	dws := NewDebugWebServer(debugAddr, app.L())

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGUSR2)

	go func() {
		for {
			select {
			case <-sigChan:
				app.L().Info("SIGUSR2 received")
				err := dws.ServeFor(time.Duration(app.Config.DebugMinutes) * time.Minute)
				if err != nil {
					app.L().Errorf("Error in debug server: %v", err)
				}
			case <-app.ShutdownContext().Done():
				app.L().Info("Shutting down debug server due to application shutdown")
				err := dws.Shutdown(app.ShutdownContext())
				if err != nil {
					app.L().Errorf("Error during debug server shutdown: %v", err)
				}
				return // shutdown this goroutine as well
			}
		}
	}()
}

func (app *AgentApp) Shutdown() {
	if app.pingHttp != nil {

		if err := baseapp.ShutdownHttp(app.pingHttp, PING_TIMEOUT); err != nil {
			app.L().Error("error while shutting down ping server", zap.Error(err))
		}
	}

	if app.Config.ListenPort > 0 || app.Config.SocketFile != "" {
		defer app.GrpcServer.Stop()
	}

}

func Run(ctx context.Context, configFile string) error {
	cfg, err := config.DefaultConfig()
	if err != nil {
		return err
	}
	if configFile != "" {
		if cfg, err = config.ReadFromFile(configFile); err != nil {
			return err
		}
	}
	opts := baseapp.DefaultOptions(cfg.App)
	opts = append(opts, baseapp.WithMetrics())
	opts = append(opts, baseapp.WithInstrumentation())

	baseApp, err := baseapp.New(opts...)
	if err != nil {
		return err
	}
	logger := baseApp.L()
	defer baseApp.Shutdown()
	logger.Debug("Init metrics")
	InitMetrics()
	logger.Debug("Inited metrics")

	rqStorage := storage.NewConfiguredRunningQueriesStorage(cfg)
	aggStorage := storage.NewConfiguredAggregatedStorage(logger, cfg)
	sessionsStorage := gp.NewSessionsStorage(rqStorage)
	backgroundStorage := master.NewBackgroundStorage(logger, sessionsStorage, rqStorage, aggStorage)

	masterConnection := gp.NewConnection(baseApp.L(), &cfg.MasterConnection, nil)

	masterSentinel := master_sentinel.NewSentinel(baseApp.L(), masterConnection)
	statActivityLister := stat_activity.NewLister(baseApp.L(), masterConnection)

	agentApp, err := NewApp(baseApp, cfg, statActivityLister, backgroundStorage)
	if err != nil {
		logger.Fatalf("failed to start application %v", err)
		return err
	}

	err = agentApp.lockFile()
	defer agentApp.unlockFile()
	if err != nil {
		logger.Error(err.Error())
		return err
	}

	if agentApp.Config.PingPort > 0 {
		err = agentApp.RunPingHandler(backgroundStorage)
		if err != nil {
			logger.Error(err.Error())
			return err
		}
	}
	if agentApp.Config.DebugPort > 0 {
		agentApp.RunDebugHandler()
	}
	defer agentApp.Shutdown()

	if agentApp.Config.ListenPort > 0 {
		lisTCP, err := net.Listen("tcp", fmt.Sprintf(":%d", agentApp.Config.ListenPort))
		defer func() {
			_ = lisTCP.Close()
		}()
		if err != nil {
			logger.Fatalf("failed to listen tcp %v", err)
			return err
		}
		go func() {
			logger.Infof("grpc ran on tcp protocol %v", agentApp.Config.ListenPort)
			err := agentApp.GrpcServer.Serve(lisTCP)
			if err != nil {
				logger.Fatalf("failed to serve GRPC on tcp - got %v", err)
			}
		}()
	}

	if agentApp.Config.SocketFile != "" {
		lisUDS, err := OpenUDS(agentApp.Config.SocketFile, logger)
		defer func() {
			_ = lisUDS.Close()
		}()
		if err != nil {
			logger.Fatalf("failed to listen UDS GRPC %v", err)
			return err
		}
		go func() {
			logger.Infof("grpc ran on unix socket  protocol %v", agentApp.Config.SocketFile)
			err := agentApp.GrpcServer.Serve(lisUDS)
			if err != nil {
				logger.Fatalf("failed to serve GRPC on UDS - got %v", err)
			}
		}()
	}

	if agentApp.Config.UDSFile != "" {
		// todo: MDB-38186
		lisUDSYa, err := OpenUDS(agentApp.Config.UDSFile, logger)
		if err != nil {
			logger.Fatalf("failed to listen UDS Yagpcc %v", err)
			return err
		}
		defer func() {
			_ = lisUDSYa.Close()
		}()

		clientProcessor := uds.NewProcessor(
			logger,
			agentApp.SetQIServer,
			uds.WithBufferSize(agentApp.Config.UDSBuffer),
		)

		go func() {
			logger.Infof("yagpcc uds reader ran on unix socket protocol %v", agentApp.Config.UDSFile)
			for {
				select {
				case <-ctx.Done():
					return
				default:
					connection, err := lisUDSYa.Accept()
					if err != nil {
						logger.Errorf("error accepting: %v", err.Error())
						continue
					}
					go func() {
						err := clientProcessor.Process(ctx, connection)
						if err != nil {
							logger.Error(err.Error())
						}
					}()
				}
			}
		}()
	}

	if cfg.Role == "master" {
		err := master.InitConnection(ctx, logger, cfg, true)
		if err != nil {
			logger.Fatal(err.Error())
			return err
		}
		cloudberry, err := gp.IsCloudberry(ctx)
		if err != nil {
			logger.Fatal(err.Error())
			return err
		}
		if cloudberry {
			err = statActivityLister.SetCloudberrySessionLister(ctx)
			if err != nil {
				logger.Fatal(err.Error())
				return err
			}
		}
	}

	for {
		if cfg.Role == "master" {
			logger.Infof("Starting master background tasks")
			ctxC, ctxF := context.WithCancel(ctx)
			defer ctxF()
			err = master.InitBG(ctxC, logger, masterSentinel, statActivityLister, cfg, backgroundStorage)
			if err != nil {
				logger.Fatal(err.Error())
				return err
			}
			// check if we could connect to GP
			err := master.InitConnection(ctx, logger, cfg, false)
			if err != nil {
				logger.Fatal(err.Error())
				return err
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			logger.Info("All Ok, I'm alive")
			time.Sleep(time.Second * 1)
		}

	}
}
