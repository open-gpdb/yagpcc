package stat_activity

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/open-gpdb/yagpcc/internal/gp"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	savedMetricsLatencyHandler *prometheus.HistogramVec
	metricsMutex               sync.Mutex
)

const (
	metricsLabelOperation = "operation"
	metricsLabelStatus    = "status"
)

func (l *Lister) List(context.Context) ([]*gp.GpStatActivity, error) {
	l.mx.Lock()
	defer l.mx.Unlock()

	if l.backgroundCtx == nil {
		return nil, fmt.Errorf("background collection was not started")
	}

	sessions, err := l.backgroundSessions.readStale()
	if err != nil {
		return nil, fmt.Errorf("error reading sessions: %w", err)
	}

	locks, err := l.backgroundLocks.readStale()
	if err != nil {
		l.log.Warnf("returning stat activity data without locks info due to error: %s", err.Error())
		return l.leftJoin(sessions, nil), nil
	}

	return l.leftJoin(sessions, locks), nil
}

func (l *Lister) Start(ctx context.Context) error {
	l.mx.Lock()
	defer l.mx.Unlock()

	if l.backgroundCtx != nil {
		l.log.Warnf("an attempt was made to start a background collection that is already running")
		return nil
	}

	l.log.Infof("initializing cache")

	if err := l.backgroundSessions.collectOnce(ctx); err != nil {
		return fmt.Errorf("error initializing sessions cache: %w", err)
	}

	if err := l.backgroundLocks.collectOnce(ctx); err != nil {
		return fmt.Errorf("error initializing locks cache: %w", err)
	}

	l.backgroundCtx, l.backgroundCancel = context.WithCancel(context.Background())
	go l.backgroundSessions.collectBackground(l.backgroundCtx)
	go l.backgroundLocks.collectBackground(l.backgroundCtx)

	return nil
}

func (l *Lister) Stop() {
	l.mx.Lock()
	defer l.mx.Unlock()

	if l.backgroundCtx == nil {
		l.log.Warnf("an attempt was made to stop a background collection that is not running")
		return
	}

	// signal background goroutine to stop
	l.backgroundCancel()

	l.backgroundCtx = nil
	l.backgroundCancel = nil
}

func NewLister(log log, db db, opts ...Option) *Lister {

	metricsLatencyHandler := getMetricsLatencyHandler()

	makeOperationLatencyHandler := func(operation string) latencyHandler {
		return func(status operationStatus, duration time.Duration) {
			metricsLatencyHandler.
				With(map[string]string{metricsLabelOperation: operation, metricsLabelStatus: string(status)}).
				Observe(duration.Seconds())
		}
	}

	l := &Lister{
		log:                   log,
		db:                    db,
		mx:                    &sync.Mutex{},
		backgroundCtx:         nil,
		backgroundCancel:      nil,
		backgroundSessions:    newBackgroundSessions(log, db, makeOperationLatencyHandler),
		backgroundLocks:       newBackgroundLocks(log, db, makeOperationLatencyHandler),
		metricsLatencyHandler: metricsLatencyHandler,
	}

	for _, o := range opts {
		o(l)
	}

	return l
}

func (l *Lister) SetCloudberrySessionLister(ctx context.Context) error {
	return l.setCustomSessionLister(ctx, cloudberrySessionsQuery())
}

func (l *Lister) SetGP6SessionLister(ctx context.Context) error {
	return l.setCustomSessionLister(ctx, gp6SessionsQuery())
}

func getMetricsLatencyHandler() *prometheus.HistogramVec {

	metricsMutex.Lock()
	defer metricsMutex.Unlock()
	if savedMetricsLatencyHandler == nil {
		savedMetricsLatencyHandler = promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "gp_stat_activity_lister_operations_duration",
				Buckets: []float64{
					(10 * time.Millisecond).Seconds(),
					(50 * time.Millisecond).Seconds(),
					(100 * time.Millisecond).Seconds(),
					(500 * time.Millisecond).Seconds(),
					(1 * time.Second).Seconds(),
					(2 * time.Second).Seconds(),
					(5 * time.Second).Seconds(),
					(10 * time.Second).Seconds(),
					(15 * time.Second).Seconds(),
					(20 * time.Second).Seconds(),
					(25 * time.Second).Seconds(),
					(30 * time.Second).Seconds(),
					(45 * time.Second).Seconds(),
					(60 * time.Second).Seconds(),
					(90 * time.Second).Seconds(),
					(120 * time.Second).Seconds(),
					(150 * time.Second).Seconds(),
					(180 * time.Second).Seconds(),
					(300 * time.Second).Seconds(),
				}},
			[]string{metricsLabelOperation, metricsLabelStatus},
		)
	}
	return savedMetricsLatencyHandler
}

func (l *Lister) setCustomSessionLister(ctx context.Context, query string) error {
	needStart := false
	if l.backgroundCtx != nil {
		// need stop and start again
		l.Stop()
		needStart = true

	}
	l.mx.Lock()
	WithCustomBackgroundSessionsQuery(query)(l)
	l.mx.Unlock()

	if needStart {
		return l.Start(ctx)
	}
	return nil
}

type Option func(*Lister)

func WithBackgroundSessionsCollectionInterval(interval time.Duration) Option {
	return func(l *Lister) {
		l.backgroundSessions.collectionInterval = interval
	}
}

func WithBackgroundLocksCollectionInterval(interval time.Duration) Option {
	return func(l *Lister) {
		l.backgroundLocks.collectionInterval = interval
	}
}

func WithBackgroundSessionsCacheTTL(ttl time.Duration) Option {
	return func(l *Lister) {
		l.backgroundSessions.cacheTTL = ttl
	}
}

func WithBackgroundLocksCacheTTL(ttl time.Duration) Option {
	return func(l *Lister) {
		l.backgroundLocks.cacheTTL = ttl
	}
}

func WithCustomBackgroundSessionsQuery(query string) Option {
	return func(l *Lister) {
		l.backgroundSessions.query = query
	}
}

type Lister struct {
	log                   log
	db                    db
	mx                    *sync.Mutex
	backgroundCtx         context.Context
	backgroundCancel      context.CancelFunc
	backgroundSessions    *background[Session]
	backgroundLocks       *background[SessionLock]
	metricsLatencyHandler *prometheus.HistogramVec
}

func gp6SessionsQuery() string {
	return `
			SELECT
				datid,
				datname,
				pid,
				sess_id AS SessID,
				cast(extract(epoch from pg_postmaster_start_time()) AS bigint) AS TmID,
				usesysid,
				usename,
				application_name AS ApplicationName,
				client_addr AS ClientAddr,
				client_hostname AS ClientHostname,
				client_port AS ClientPort,
				backend_start AS BackendStart,
				xact_start AS XactStart,
				query_start AS QueryStart,
				state_change AS StateChange,
				waiting,
				state,
				backend_xid AS BackendXid,
				backend_xmin AS backendXmin,
				query,
				waiting_reason AS WaitingReason,
				rsgid,
				rsgname,
				rsgqueueduration,
				'' as WaitEvent,
                '' AS WaitEventType 
			FROM pg_stat_activity
		`
}

func cloudberrySessionsQuery() string {
	return `
                        SELECT
                                COALESCE(datid, 0) as datid,
                                COALESCE(datname, 'system') as datname,
                                pid,
                                sess_id AS SessID,
                                cast(extract(epoch from pg_postmaster_start_time()) AS bigint) AS TmID,
                                COALESCE(usesysid, 0) as usesysid,
                                COALESCE(usename, 'system') as usename,
                                application_name AS ApplicationName,
                                client_addr AS ClientAddr,
                                client_hostname AS ClientHostname,
                                client_port AS ClientPort,
                                backend_start AS BackendStart,
                                xact_start AS XactStart,
                                query_start AS QueryStart,
                                state_change AS StateChange,
                                false as waiting,
                                state,
                                backend_xid AS BackendXid,
                                backend_xmin AS backendXmin,
                                query,
                                '' as WaitingReason,
                                rsgid,
                                rsgname,
                                0 as rsgqueueduration,
                                wait_event as WaitEvent,
                                wait_event_type AS WaitEventType
                        FROM pg_stat_activity
		`
}

func newBackgroundSessions(log log, db db, makeOperationLatencyHandler func(string) latencyHandler) *background[Session] {
	const (
		operationCollect   = "background_collection_sessions"
		operationStaleRead = "stale_read_sessions"

		defaultCollectionInterval = 2 * time.Second
		defaultCollectionTimeout  = 60 * time.Second
		defaultCacheTTL           = 180 * time.Second
	)

	return &background[Session]{
		log:                      log,
		query:                    gp6SessionsQuery(),
		db:                       db,
		staleReadLatencyHandler:  makeOperationLatencyHandler(operationStaleRead),
		collectionTimeout:        defaultCollectionTimeout,
		collectionLatencyHandler: makeOperationLatencyHandler(operationCollect),
		collectionInterval:       defaultCollectionInterval,
		cacheMX:                  &sync.Mutex{},
		cache:                    nil,
		cachedAt:                 time.Time{},
		cacheTTL:                 defaultCacheTTL,
	}
}

func newBackgroundLocks(log log, db db, makeOperationLatencyHandler func(string) latencyHandler) *background[SessionLock] {
	const (
		operationCollect   = "background_collection_locks"
		operationStaleRead = "stale_read_locks"

		defaultCollectionInterval = 10 * time.Second
		defaultCollectionTimeout  = 90 * time.Second
		defaultCacheTTL           = 300 * time.Second
	)

	return &background[SessionLock]{
		log: log,
		query: `
			SELECT
				w.mppsessionid AS BlockSessID,
				w.mode AS WaitMode,
				coalesce(cast(cast(l.relation AS regclass) AS text), l.locktype) AS LockedItem,
				l.mode AS LockedMode,
				l.mppsessionid AS BlockedBySessID
			FROM
				pg_locks l,
				pg_locks w
			WHERE l.transactionid = w.transactionid
				AND l.granted = true
				AND w.granted = false
				AND l.transactionid is not NULL
			UNION ALL
			SELECT
				w.mppsessionid AS BlockSessID,
				w.mode AS WaitMode,
				coalesce(cast(cast(l.relation AS regclass) AS text), l.locktype) AS LockedItem,
				l.mode AS LockedMode,
				l.mppsessionid AS BlockedBySessID
			FROM
				pg_locks l,
				pg_locks w
			WHERE l.database = w.database
				AND l.relation = w.relation
				AND l.granted = true
				AND w.granted = false
				AND l.locktype = 'relation'
				AND l.gp_segment_id = w.gp_segment_id
		`,
		db:                       db,
		staleReadLatencyHandler:  makeOperationLatencyHandler(operationStaleRead),
		collectionTimeout:        defaultCollectionTimeout,
		collectionLatencyHandler: makeOperationLatencyHandler(operationCollect),
		collectionInterval:       defaultCollectionInterval,
		cacheMX:                  &sync.Mutex{},
		cache:                    nil,
		cachedAt:                 time.Time{},
		cacheTTL:                 defaultCacheTTL,
	}
}

func (l *Lister) leftJoin(sessions []Session, locks []SessionLock) []*gp.GpStatActivity {
	if len(sessions) == 0 {
		return []*gp.GpStatActivity{}
	}

	locksBySessID := make(map[int]SessionLock, len(locks))
	for _, lock := range locks {
		locksBySessID[lock.BlockSessID] = lock
	}

	statActivity := make([]*gp.GpStatActivity, 0, len(sessions))
	for _, s := range sessions {
		sa := &gp.GpStatActivity{
			DatID:            s.DatID,
			Datname:          s.Datname,
			Pid:              s.Pid,
			SessID:           s.SessID,
			TmID:             s.TmID,
			UsesysID:         s.UsesysID,
			Usename:          s.Usename,
			ApplicationName:  s.ApplicationName,
			ClientAddr:       s.ClientAddr,
			ClientHostname:   s.ClientHostname,
			ClientPort:       s.ClientPort,
			BackendStart:     s.BackendStart,
			XactStart:        s.XactStart,
			QueryStart:       s.QueryStart,
			StateChange:      s.StateChange,
			Waiting:          s.Waiting,
			State:            s.State,
			BackendXid:       s.BackendXid,
			BackendXmin:      s.BackendXmin,
			Query:            s.Query,
			WaitingReason:    s.WaitingReason,
			Rsgid:            s.Rsgid,
			Rsgname:          s.Rsgname,
			Rsgqueueduration: s.Rsgqueueduration,
			WaitEvent:        s.WaitEvent,
			WaitEventType:    s.WaitEventType,
		}

		if lock, found := locksBySessID[s.SessID]; found {
			sa.BlockedBySessID = &lock.BlockedBySessID
			sa.WaitMode = &lock.WaitMode
			sa.LockedItem = &lock.LockedItem
			sa.LockedMode = &lock.LockedMode
		}

		statActivity = append(statActivity, sa)
	}

	return statActivity
}

type background[T any] struct {
	log                      log
	query                    string
	db                       db
	staleReadLatencyHandler  latencyHandler
	collectionTimeout        time.Duration
	collectionInterval       time.Duration
	collectionLatencyHandler latencyHandler
	cacheMX                  *sync.Mutex
	cache                    []T
	cachedAt                 time.Time
	cacheTTL                 time.Duration
}

func (b *background[T]) readStale() ([]T, error) {
	b.cacheMX.Lock()
	defer b.cacheMX.Unlock()

	staleness := time.Since(b.cachedAt)
	if staleness > b.cacheTTL {
		b.staleReadLatencyHandler(operationFailed, staleness)
		return nil, fmt.Errorf("cached value is stale")
	}

	value := make([]T, len(b.cache))
	copy(value, b.cache)

	b.staleReadLatencyHandler(operationSucceeded, staleness)
	return value, nil
}

func (b *background[T]) collectOnce(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, b.collectionTimeout)
	defer cancel()

	startedAt := time.Now()

	result := make([]T, 0)
	if err := b.db.ExecQuery(ctx, b.query, &result); err != nil {
		b.collectionLatencyHandler(operationFailed, time.Since(startedAt))
		return fmt.Errorf("error executing query: %w", err)
	}

	b.cacheMX.Lock()
	b.cache = result
	b.cachedAt = time.Now()
	b.cacheMX.Unlock()

	b.collectionLatencyHandler(operationSucceeded, time.Since(startedAt))
	return nil
}

func (b *background[T]) collectBackground(ctx context.Context) {
	var zero T
	typeName := fmt.Sprintf("%T", zero)

	b.log.Infof("background collection for %s started", typeName)

	t := time.NewTicker(b.collectionInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			b.log.Infof("background collection for %s stopped", typeName)
			return
		case <-t.C:
			if err := b.collectOnce(ctx); err != nil {
				b.log.Warnf("error during background collection %s: %s", typeName, err.Error())
			}
		}
	}
}

type latencyHandler func(operationStatus, time.Duration)

type operationStatus string

const (
	operationSucceeded operationStatus = "ok"
	operationFailed    operationStatus = "fail"
)
