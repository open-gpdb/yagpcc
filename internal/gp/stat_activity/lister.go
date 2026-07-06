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

package stat_activity

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/open-gpdb/yagpcc/internal/gp"
	"github.com/open-gpdb/yagpcc/internal/gp/lister"

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

const (
	gp6SessionsQuery = `
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

	gp6AllSessionsQuery = `
select
  pg_catalog.gp_execution_segment() as GpSegmentId,
  pid,
  sess_id as SessId,
  '' as BackendType
from
  gp_dist_random('pg_stat_activity')
union all
select
  pg_catalog.gp_execution_segment() as GpSegmentId,
  pid,
  sess_id as SessId,
  '' as BackendType
from
  pg_stat_activity;
		`

	cloudberrySessionsQuery = `
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
	cloudberryAllSessionsQuery = `
select
  pg_catalog.gp_execution_segment() as GpSegmentId,
  pid,
  sess_id as SessId,
  backend_type as BackendType
from
  gp_dist_random('pg_stat_activity')
union all
select
  pg_catalog.gp_execution_segment() as GpSegmentId,
  pid,
  sess_id as SessId,
  backend_type as BackendType
from
  pg_stat_activity;
		`

	locksQuery = `
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
		`
)

func (l *Lister) List(context.Context) ([]*gp.GpStatActivity, error) {
	l.mx.Lock()
	defer l.mx.Unlock()

	if l.backgroundCtx == nil {
		return nil, fmt.Errorf("background collection was not started")
	}

	sessions, err := l.backgroundSessions.ReadStale()
	if err != nil {
		return nil, fmt.Errorf("error reading sessions: %w", err)
	}

	locks, err := l.backgroundLocks.ReadStale()
	if err != nil {
		l.log.Warnf("returning stat activity data without locks info due to error: %s", err.Error())
		return l.leftJoin(sessions, nil), nil
	}

	return l.leftJoin(sessions, locks), nil
}

func (l *Lister) ListAllSessions(context.Context) ([]SessionPid, error) {
	l.mx.Lock()
	defer l.mx.Unlock()

	if l.backgroundCtx == nil {
		return nil, fmt.Errorf("background collection was not started")
	}

	sessions, err := l.allSessions.ReadStale()
	if err != nil {
		return nil, fmt.Errorf("error reading all sessions: %w", err)
	}

	return sessions, nil
}

func (l *Lister) Start(ctx context.Context) error {
	l.mx.Lock()
	defer l.mx.Unlock()

	if l.backgroundCtx != nil {
		l.log.Warnf("an attempt was made to start a background collection that is already running")
		return nil
	}

	l.log.Infof("initializing cache")

	if err := l.backgroundSessions.CollectOnce(ctx); err != nil {
		return fmt.Errorf("error initializing sessions cache: %w", err)
	}

	if err := l.backgroundLocks.CollectOnce(ctx); err != nil {
		l.log.Warnf("error initializing locks cache, continuing without lock data: %s", err.Error())
	}

	if err := l.allSessions.CollectOnce(ctx); err != nil {
		return fmt.Errorf("error initializing all sessions cache: %w", err)
	}

	l.backgroundCtx, l.backgroundCancel = context.WithCancel(context.Background())
	go l.backgroundSessions.CollectBackground(l.backgroundCtx)
	go l.backgroundLocks.CollectBackground(l.backgroundCtx)
	go l.allSessions.CollectBackground(l.backgroundCtx)

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

func NewLister(log lister.Log, db lister.DB, opts ...Option) *Lister {
	metricsLatencyHandler := getMetricsLatencyHandler()

	makeOperationLatencyHandler := func(operation string) lister.LatencyHandler {
		return func(status lister.OperationStatus, duration time.Duration) {
			metricsLatencyHandler.
				With(map[string]string{metricsLabelOperation: operation, metricsLabelStatus: string(status)}).
				Observe(duration.Seconds())
		}
	}

	l := &Lister{
		log:                log,
		mx:                 &sync.Mutex{},
		backgroundCtx:      nil,
		backgroundCancel:   nil,
		backgroundSessions: newBackgroundSessions(log, db, makeOperationLatencyHandler),
		backgroundLocks:    newBackgroundLocks(log, db, makeOperationLatencyHandler),
		allSessions:        newBackgroundAllSessions(log, db, makeOperationLatencyHandler),
	}

	for _, o := range opts {
		o(l)
	}

	return l
}

func (l *Lister) SetModernSessionLister(ctx context.Context) error {
	return l.setCustomSessionLister(ctx, cloudberrySessionsQuery, cloudberryAllSessionsQuery)
}

func (l *Lister) SetCloudberrySessionLister(ctx context.Context) error {
	return l.SetModernSessionLister(ctx)
}

func (l *Lister) SetGP6SessionLister(ctx context.Context) error {
	return l.setCustomSessionLister(ctx, gp6SessionsQuery, gp6AllSessionsQuery)
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

func (l *Lister) setCustomSessionLister(ctx context.Context, masterQuery string, allSessionsQuery string) error {
	needStart := false
	if l.backgroundCtx != nil {
		// need stop and start again
		l.Stop()
		needStart = true
	}
	l.mx.Lock()
	WithCustomBackgroundSessionsQuery(masterQuery)(l)
	WithCustomAllSessionsQuery(allSessionsQuery)(l)
	l.mx.Unlock()

	if needStart {
		return l.Start(ctx)
	}
	return nil
}

type Option func(*Lister)

func WithBackgroundSessionsCollectionInterval(interval time.Duration) Option {
	return func(l *Lister) {
		l.backgroundSessions.CollectionInterval = interval
	}
}

func WithBackgroundLocksCollectionInterval(interval time.Duration) Option {
	return func(l *Lister) {
		l.backgroundLocks.CollectionInterval = interval
	}
}

func WithBackgroundSessionsCacheTTL(ttl time.Duration) Option {
	return func(l *Lister) {
		l.backgroundSessions.CacheTTL = ttl
	}
}

func WithBackgroundLocksCacheTTL(ttl time.Duration) Option {
	return func(l *Lister) {
		l.backgroundLocks.CacheTTL = ttl
	}
}

func WithBackgroundAllSessionsCollectionInterval(interval time.Duration) Option {
	return func(l *Lister) {
		l.allSessions.CollectionInterval = interval
	}
}

func WithBackgroundAllSessionsCacheTTL(ttl time.Duration) Option {
	return func(l *Lister) {
		l.allSessions.CacheTTL = ttl
	}
}

func WithCustomBackgroundSessionsQuery(query string) Option {
	return func(l *Lister) {
		l.backgroundSessions.Query = query
	}
}

func WithCustomAllSessionsQuery(query string) Option {
	return func(l *Lister) {
		l.allSessions.Query = query
	}
}

type Lister struct {
	log                lister.Log
	mx                 *sync.Mutex
	backgroundCtx      context.Context
	backgroundCancel   context.CancelFunc
	backgroundSessions *lister.Background[Session]
	allSessions        *lister.Background[SessionPid]
	backgroundLocks    *lister.Background[SessionLock]
}

func newBackgroundSessions(log lister.Log, db lister.DB, makeLatency func(string) lister.LatencyHandler) *lister.Background[Session] {
	const (
		operationCollect   = "background_collection_sessions"
		operationStaleRead = "stale_read_sessions"

		defaultCollectionInterval = 2 * time.Second
		defaultCollectionTimeout  = 60 * time.Second
		defaultCacheTTL           = 180 * time.Second
	)

	bg := lister.NewBackground[Session](
		log, db,
		gp6SessionsQuery,
		makeLatency(operationCollect),
		makeLatency(operationStaleRead),
	)
	bg.CollectionInterval = defaultCollectionInterval
	bg.CollectionTimeout = defaultCollectionTimeout
	bg.CacheTTL = defaultCacheTTL
	return bg
}

func newBackgroundAllSessions(log lister.Log, db lister.DB, makeLatency func(string) lister.LatencyHandler) *lister.Background[SessionPid] {
	const (
		operationCollect   = "background_collection_all_sessions"
		operationStaleRead = "stale_read_all_sessions"

		defaultCollectionInterval = 60 * time.Second
		defaultCollectionTimeout  = 300 * time.Second
		defaultCacheTTL           = 600 * time.Second
	)

	bg := lister.NewBackground[SessionPid](
		log, db,
		gp6AllSessionsQuery,
		makeLatency(operationCollect),
		makeLatency(operationStaleRead),
	)
	bg.CollectionInterval = defaultCollectionInterval
	bg.CollectionTimeout = defaultCollectionTimeout
	bg.CacheTTL = defaultCacheTTL
	return bg
}

func newBackgroundLocks(log lister.Log, db lister.DB, makeLatency func(string) lister.LatencyHandler) *lister.Background[SessionLock] {
	const (
		operationCollect   = "background_collection_locks"
		operationStaleRead = "stale_read_locks"

		defaultCollectionInterval = 10 * time.Second
		defaultCollectionTimeout  = 90 * time.Second
		defaultCacheTTL           = 300 * time.Second
	)

	bg := lister.NewBackground[SessionLock](
		log, db,
		locksQuery,
		makeLatency(operationCollect),
		makeLatency(operationStaleRead),
	)
	bg.CollectionInterval = defaultCollectionInterval
	bg.CollectionTimeout = defaultCollectionTimeout
	bg.CacheTTL = defaultCacheTTL
	return bg
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
