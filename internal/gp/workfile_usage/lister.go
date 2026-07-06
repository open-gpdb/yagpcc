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

package workfile_usage

import (
	"context"
	"fmt"
	"sync"
	"time"

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

const workfileUsageQuery = `
	SELECT
		pid,
		sess_id    AS SessID,
		command_cnt AS CommandCnt,
		segid      AS SegID,
		size,
		numfiles   AS NumFiles
	FROM gp_toolkit.gp_workfile_usage_per_query
`

// Lister collects gp_toolkit.gp_workfile_usage_per_query rows in the background
// at the same frequency as procfs stat gathering and exposes them via List.
// The pair (SegID, Pid) acts as the key for later correlation with procfs data.
//
// The embedded *lister.Background[WorkfileUsageEntry] promotes all collection
// methods (CollectOnce, CollectBackground, ReadStale) and configuration fields
// (Query, CollectionInterval, CollectionTimeout, CacheTTL) directly onto Lister.
type Lister struct {
	*lister.Background[WorkfileUsageEntry]

	log              lister.Log
	mx               *sync.Mutex
	backgroundCtx    context.Context
	backgroundCancel context.CancelFunc
}

// NewLister creates a new Lister with optional configuration overrides.
func NewLister(log lister.Log, db lister.DB, opts ...Option) *Lister {
	metricsLatencyHandler := getMetricsLatencyHandler()

	makeOperationLatencyHandler := func(operation string) lister.LatencyHandler {
		return func(status lister.OperationStatus, duration time.Duration) {
			metricsLatencyHandler.
				With(map[string]string{metricsLabelOperation: operation, metricsLabelStatus: string(status)}).
				Observe(duration.Seconds())
		}
	}

	const (
		operationCollect   = "background_collection_workfile_usage"
		operationStaleRead = "stale_read_workfile_usage"

		// Same frequency as procfs stat gathering.
		defaultCollectionInterval = 2 * time.Second
		defaultCollectionTimeout  = 60 * time.Second
		defaultCacheTTL           = 180 * time.Second
	)

	bg := lister.NewBackground[WorkfileUsageEntry](
		log, db,
		workfileUsageQuery,
		makeOperationLatencyHandler(operationCollect),
		makeOperationLatencyHandler(operationStaleRead),
	)
	bg.CollectionInterval = defaultCollectionInterval
	bg.CollectionTimeout = defaultCollectionTimeout
	bg.CacheTTL = defaultCacheTTL

	l := &Lister{
		Background:       bg,
		log:              log,
		mx:               &sync.Mutex{},
		backgroundCtx:    nil,
		backgroundCancel: nil,
	}

	for _, o := range opts {
		o(l)
	}

	return l
}

// Start initialises the cache with an immediate query and launches the
// periodic background collection goroutine.
func (l *Lister) Start(ctx context.Context) error {
	l.mx.Lock()
	defer l.mx.Unlock()

	if l.backgroundCtx != nil {
		l.log.Warnf("an attempt was made to start a background collection that is already running")
		return nil
	}

	l.log.Infof("initializing cache")

	if err := l.CollectOnce(ctx); err != nil {
		l.log.Warnf("error initializing workfile usage cache, continuing without workfile usage data: %s", err.Error())
	}

	l.backgroundCtx, l.backgroundCancel = context.WithCancel(context.Background())
	go l.CollectBackground(l.backgroundCtx)

	return nil
}

// Stop signals the background goroutine to stop.
func (l *Lister) Stop() {
	l.mx.Lock()
	defer l.mx.Unlock()

	if l.backgroundCtx == nil {
		l.log.Warnf("an attempt was made to stop a background collection that is not running")
		return
	}

	l.backgroundCancel()

	l.backgroundCtx = nil
	l.backgroundCancel = nil
}

// List returns a snapshot of the last successfully collected workfile usage rows.
// Returns an error if the background collection was not started or the cached
// value is too stale.
func (l *Lister) List(context.Context) ([]WorkfileUsageEntry, error) {
	l.mx.Lock()
	defer l.mx.Unlock()

	if l.backgroundCtx == nil {
		return nil, fmt.Errorf("background collection was not started")
	}

	entries, err := l.ReadStale()
	if err != nil {
		return nil, fmt.Errorf("error reading workfile usage: %w", err)
	}

	return entries, nil
}

// Option is a functional option for Lister.
type Option func(*Lister)

// WithCollectionInterval overrides the periodic collection interval.
func WithCollectionInterval(interval time.Duration) Option {
	return func(l *Lister) {
		l.CollectionInterval = interval
	}
}

// WithCacheTTL overrides the maximum acceptable age of a cached result.
func WithCacheTTL(ttl time.Duration) Option {
	return func(l *Lister) {
		l.CacheTTL = ttl
	}
}

func getMetricsLatencyHandler() *prometheus.HistogramVec {
	metricsMutex.Lock()
	defer metricsMutex.Unlock()
	if savedMetricsLatencyHandler == nil {
		savedMetricsLatencyHandler = promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "gp_workfile_usage_lister_operations_duration",
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
				},
			},
			[]string{metricsLabelOperation, metricsLabelStatus},
		)
	}
	return savedMetricsLatencyHandler
}
