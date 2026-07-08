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

package lister

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// OperationStatus is the result label for a latency observation.
type OperationStatus string

const (
	OperationSucceeded OperationStatus = "ok"
	OperationFailed    OperationStatus = "fail"
)

// LatencyHandler is called after each collect or stale-read operation with its
// outcome and elapsed duration so callers can record Prometheus histograms.
type LatencyHandler func(OperationStatus, time.Duration)

// Background is a generic, self-refreshing cache for any row type T.
// It executes a SQL query periodically in a background goroutine and
// serves read requests from the in-memory cache, returning an error when
// the cached data has grown too stale.
//
// Embed *Background[T] directly into a Lister struct that only needs one
// background data source; or keep multiple named *Background[T] fields when
// a Lister needs several independent data sources.
type Background[T any] struct {
	log Log
	db  DB

	// Query is the SQL statement executed on every collection cycle.
	Query string

	// CollectionInterval controls how often the background goroutine fires.
	CollectionInterval time.Duration

	// CollectionTimeout is the per-cycle context deadline for the query.
	CollectionTimeout time.Duration

	// CacheTTL is the maximum age of a cached result before ReadStale returns
	// an error.
	CacheTTL time.Duration

	collectionLatencyHandler LatencyHandler
	staleReadLatencyHandler  LatencyHandler

	cacheMX  *sync.Mutex
	cache    []T
	cachedAt time.Time
}

// NewBackground constructs a Background[T] with the required dependencies and
// optional latency handlers.  All time-related settings default to zero; set
// CollectionInterval, CollectionTimeout, and CacheTTL directly on the returned
// value before calling CollectOnce or CollectBackground.
func NewBackground[T any](log Log, db DB, query string, collectHandler, staleHandler LatencyHandler) *Background[T] {
	return &Background[T]{
		log:                      log,
		db:                       db,
		Query:                    query,
		collectionLatencyHandler: collectHandler,
		staleReadLatencyHandler:  staleHandler,
		cacheMX:                  &sync.Mutex{},
	}
}

// ReadStale returns a copy of the last collected result set.
// It returns an error when the cached data is older than CacheTTL.
func (b *Background[T]) ReadStale() ([]T, error) {
	b.cacheMX.Lock()
	defer b.cacheMX.Unlock()

	staleness := time.Since(b.cachedAt)
	if staleness > b.CacheTTL {
		b.record(b.staleReadLatencyHandler, OperationFailed, staleness)
		return nil, fmt.Errorf("cached value is stale")
	}

	value := make([]T, len(b.cache))
	copy(value, b.cache)

	b.record(b.staleReadLatencyHandler, OperationSucceeded, staleness)
	return value, nil
}

// CollectOnce executes the query once under a fresh timeout-derived context
// and atomically replaces the cache on success.
func (b *Background[T]) CollectOnce(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, b.CollectionTimeout)
	defer cancel()

	startedAt := time.Now()

	result := make([]T, 0)
	if err := b.db.ExecQuery(ctx, b.Query, &result); err != nil {
		b.record(b.collectionLatencyHandler, OperationFailed, time.Since(startedAt))
		return fmt.Errorf("error executing query: %w", err)
	}

	b.cacheMX.Lock()
	b.cache = result
	b.cachedAt = time.Now()
	b.cacheMX.Unlock()

	b.record(b.collectionLatencyHandler, OperationSucceeded, time.Since(startedAt))
	return nil
}

// CollectBackground runs CollectOnce on every CollectionInterval tick until
// ctx is cancelled.  It is intended to be launched as a goroutine.
func (b *Background[T]) CollectBackground(ctx context.Context) {
	var zero T
	typeName := fmt.Sprintf("%T", zero)

	b.log.Infof("background collection for %s started", typeName)

	t := time.NewTicker(b.CollectionInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			b.log.Infof("background collection for %s stopped", typeName)
			return
		case <-t.C:
			if err := b.CollectOnce(ctx); err != nil {
				b.log.Warnf("error during background collection %s: %s", typeName, err.Error())
			}
		}
	}
}

func (b *Background[T]) record(h LatencyHandler, status OperationStatus, d time.Duration) {
	if h != nil {
		h(status, d)
	}
}
