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

package lister_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/open-gpdb/yagpcc/internal/gp/lister"
)

// row is a minimal row type used for generic instantiation in tests.
type row struct {
	ID    int
	Value string
}

func makeBackground(log lister.Log, db lister.DB, interval, timeout, ttl time.Duration) *lister.Background[row] {
	bg := lister.NewBackground[row](log, db, "SELECT 1", nil, nil)
	bg.CollectionInterval = interval
	bg.CollectionTimeout = timeout
	bg.CacheTTL = ttl
	return bg
}

// --------------------------------------------------------------------------
// CollectOnce
// --------------------------------------------------------------------------

func TestBackground_CollectOnce_Success(t *testing.T) {
	ctrl := gomock.NewController(t)

	dbMock := NewMockDB(ctrl)
	dbMock.EXPECT().
		ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]row{})).
		DoAndReturn(func(_ context.Context, _ string, dest *[]row) error {
			*dest = []row{{ID: 1, Value: "a"}, {ID: 2, Value: "b"}}
			return nil
		})

	bg := makeBackground(NewMockLog(ctrl), dbMock, time.Hour, time.Minute, time.Hour)

	err := bg.CollectOnce(context.Background())
	assert.NoError(t, err)

	got, err := bg.ReadStale()
	assert.NoError(t, err)
	assert.Equal(t, []row{{ID: 1, Value: "a"}, {ID: 2, Value: "b"}}, got)
}

func TestBackground_CollectOnce_DBError(t *testing.T) {
	ctrl := gomock.NewController(t)

	dbMock := NewMockDB(ctrl)
	dbMock.EXPECT().
		ExecQuery(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("db down"))

	bg := makeBackground(NewMockLog(ctrl), dbMock, time.Hour, time.Minute, time.Hour)

	err := bg.CollectOnce(context.Background())
	assert.EqualError(t, err, "error executing query: db down")
}

func TestBackground_CollectOnce_LatencyHandlers(t *testing.T) {
	ctrl := gomock.NewController(t)

	var capturedCollectStatus lister.OperationStatus
	var capturedStaleStatus lister.OperationStatus

	collectHandler := func(s lister.OperationStatus, _ time.Duration) { capturedCollectStatus = s }
	staleHandler := func(s lister.OperationStatus, _ time.Duration) { capturedStaleStatus = s }

	dbMock := NewMockDB(ctrl)
	dbMock.EXPECT().
		ExecQuery(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	bg := lister.NewBackground[row](NewMockLog(ctrl), dbMock, "SELECT 1", collectHandler, staleHandler)
	bg.CollectionTimeout = time.Minute
	bg.CacheTTL = time.Hour

	err := bg.CollectOnce(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, lister.OperationSucceeded, capturedCollectStatus)

	_, err = bg.ReadStale()
	assert.NoError(t, err)
	assert.Equal(t, lister.OperationSucceeded, capturedStaleStatus)
}

func TestBackground_CollectOnce_LatencyHandlers_OnError(t *testing.T) {
	ctrl := gomock.NewController(t)

	var capturedStatus lister.OperationStatus
	collectHandler := func(s lister.OperationStatus, _ time.Duration) { capturedStatus = s }

	dbMock := NewMockDB(ctrl)
	dbMock.EXPECT().
		ExecQuery(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("db error"))

	bg := lister.NewBackground[row](NewMockLog(ctrl), dbMock, "SELECT 1", collectHandler, nil)
	bg.CollectionTimeout = time.Minute
	bg.CacheTTL = time.Hour

	err := bg.CollectOnce(context.Background())
	assert.Error(t, err)
	assert.Equal(t, lister.OperationFailed, capturedStatus)
}

func TestBackground_CollectOnce_NilHandlers(t *testing.T) {
	// Ensure no panic when latency handlers are nil — success path.
	t.Run("success path does not panic", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		dbMock := NewMockDB(ctrl)
		dbMock.EXPECT().
			ExecQuery(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil)

		bg := lister.NewBackground[row](NewMockLog(ctrl), dbMock, "SELECT 1", nil, nil)
		bg.CollectionTimeout = time.Minute
		bg.CacheTTL = time.Hour

		assert.NotPanics(t, func() {
			_ = bg.CollectOnce(context.Background())
			_, _ = bg.ReadStale()
		})
	})

	// Ensure no panic when latency handlers are nil — error path.
	t.Run("error path does not panic", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		dbMock := NewMockDB(ctrl)
		dbMock.EXPECT().
			ExecQuery(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(fmt.Errorf("err"))

		bg := lister.NewBackground[row](NewMockLog(ctrl), dbMock, "SELECT 1", nil, nil)
		bg.CollectionTimeout = time.Minute
		bg.CacheTTL = time.Hour

		assert.NotPanics(t, func() {
			_ = bg.CollectOnce(context.Background())
		})
	})
}

// --------------------------------------------------------------------------
// ReadStale
// --------------------------------------------------------------------------

func TestBackground_ReadStale_BeforeAnyCollection(t *testing.T) {
	ctrl := gomock.NewController(t)

	bg := makeBackground(NewMockLog(ctrl), NewMockDB(ctrl), time.Hour, time.Minute, time.Millisecond)
	// TTL is very short; cache was never populated, so cachedAt is zero.
	time.Sleep(2 * time.Millisecond)

	_, err := bg.ReadStale()
	assert.EqualError(t, err, "cached value is stale")
}

func TestBackground_ReadStale_StaleAfterTTL(t *testing.T) {
	ctrl := gomock.NewController(t)

	const cacheTTL = 20 * time.Millisecond

	dbMock := NewMockDB(ctrl)
	dbMock.EXPECT().
		ExecQuery(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	bg := makeBackground(NewMockLog(ctrl), dbMock, time.Hour, time.Minute, cacheTTL)

	err := bg.CollectOnce(context.Background())
	assert.NoError(t, err)

	// Still fresh immediately after collection.
	_, err = bg.ReadStale()
	assert.NoError(t, err)

	// Wait for the cache to expire.
	time.Sleep(2 * cacheTTL)

	_, err = bg.ReadStale()
	assert.EqualError(t, err, "cached value is stale")
}

func TestBackground_ReadStale_ReturnsCopy(t *testing.T) {
	ctrl := gomock.NewController(t)

	original := []row{{ID: 1, Value: "x"}}

	dbMock := NewMockDB(ctrl)
	dbMock.EXPECT().
		ExecQuery(gomock.Any(), gomock.Any(), gomock.AssignableToTypeOf(&[]row{})).
		DoAndReturn(func(_ context.Context, _ string, dest *[]row) error {
			*dest = original
			return nil
		})

	bg := makeBackground(NewMockLog(ctrl), dbMock, time.Hour, time.Minute, time.Hour)
	_ = bg.CollectOnce(context.Background())

	got, err := bg.ReadStale()
	assert.NoError(t, err)

	// Mutate the returned slice; the internal cache must not change.
	got[0].Value = "mutated"

	got2, err := bg.ReadStale()
	assert.NoError(t, err)
	assert.Equal(t, "x", got2[0].Value, "ReadStale must return a copy, not a reference to the cache")
}

func TestBackground_ReadStale_StaleLatencyHandler(t *testing.T) {
	ctrl := gomock.NewController(t)

	const cacheTTL = 20 * time.Millisecond
	var capturedStatus lister.OperationStatus
	staleHandler := func(s lister.OperationStatus, _ time.Duration) { capturedStatus = s }

	dbMock := NewMockDB(ctrl)
	dbMock.EXPECT().
		ExecQuery(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	bg := lister.NewBackground[row](NewMockLog(ctrl), dbMock, "SELECT 1", nil, staleHandler)
	bg.CollectionTimeout = time.Minute
	bg.CacheTTL = cacheTTL

	_ = bg.CollectOnce(context.Background())
	time.Sleep(2 * cacheTTL)

	_, err := bg.ReadStale()
	assert.Error(t, err)
	assert.Equal(t, lister.OperationFailed, capturedStatus)
}

// --------------------------------------------------------------------------
// CollectBackground lifecycle
// --------------------------------------------------------------------------

func TestBackground_CollectBackground_StartsAndStops(t *testing.T) {
	ctrl := gomock.NewController(t)

	stopped := make(chan struct{})

	logMock := NewMockLog(ctrl)
	logMock.EXPECT().Infof("background collection for %s started", "lister_test.row")
	logMock.EXPECT().
		Infof("background collection for %s stopped", "lister_test.row").
		Do(func(string, ...any) { close(stopped) })

	bg := makeBackground(logMock, NewMockDB(ctrl), time.Hour, time.Minute, time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	go bg.CollectBackground(ctx)

	cancel()
	assertCompleted(t, stopped, 5*time.Second, "CollectBackground did not stop")
}

func TestBackground_CollectBackground_CollectsOnTick(t *testing.T) {
	ctrl := gomock.NewController(t)

	collected := make(chan struct{})
	collectedTimes := 0

	logMock := NewMockLog(ctrl)
	logMock.EXPECT().Infof(gomock.Any(), gomock.Any()).AnyTimes()

	dbMock := NewMockDB(ctrl)
	dbMock.EXPECT().
		ExecQuery(gomock.Any(), gomock.Any(), gomock.AssignableToTypeOf(&[]row{})).
		DoAndReturn(func(_ context.Context, _ string, dest *[]row) error {
			*dest = []row{{ID: collectedTimes + 1, Value: "v"}}
			if collectedTimes == 1 {
				close(collected)
			}
			collectedTimes++
			return nil
		}).
		AnyTimes()

	bg := makeBackground(logMock, dbMock, 1*time.Millisecond, time.Minute, time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go bg.CollectBackground(ctx)

	assertCompleted(t, collected, 5*time.Second, "background collection did not tick")

	got, err := bg.ReadStale()
	assert.NoError(t, err)
	assert.NotEmpty(t, got)
}

func TestBackground_CollectBackground_LogsWarningOnError(t *testing.T) {
	ctrl := gomock.NewController(t)

	warnLogged := make(chan struct{})

	logMock := NewMockLog(ctrl)
	logMock.EXPECT().Infof(gomock.Any(), gomock.Any()).AnyTimes()
	logMock.EXPECT().
		Warnf("error during background collection %s: %s", "lister_test.row", "error executing query: boom").
		Do(func(string, ...any) {
			select {
			case <-warnLogged:
			default:
				close(warnLogged)
			}
		}).
		AnyTimes()

	dbMock := NewMockDB(ctrl)
	dbMock.EXPECT().
		ExecQuery(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("boom")).
		AnyTimes()

	bg := makeBackground(logMock, dbMock, 1*time.Millisecond, time.Minute, time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go bg.CollectBackground(ctx)

	assertCompleted(t, warnLogged, 5*time.Second, "expected warning was not logged")
}

// --------------------------------------------------------------------------
// Configuration fields
// --------------------------------------------------------------------------

func TestBackground_ConfigurableQuery(t *testing.T) {
	ctrl := gomock.NewController(t)

	const customQuery = "SELECT id, value FROM custom_table"
	var capturedQuery string

	dbMock := NewMockDB(ctrl)
	dbMock.EXPECT().
		ExecQuery(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, q string, _ *[]row) error {
			capturedQuery = q
			return nil
		})

	bg := lister.NewBackground[row](NewMockLog(ctrl), dbMock, customQuery, nil, nil)
	bg.CollectionTimeout = time.Minute
	bg.CacheTTL = time.Hour

	_ = bg.CollectOnce(context.Background())
	assert.Equal(t, customQuery, capturedQuery)
}

func TestBackground_QueryFieldCanBeOverridden(t *testing.T) {
	ctrl := gomock.NewController(t)

	var capturedQuery string

	dbMock := NewMockDB(ctrl)
	dbMock.EXPECT().
		ExecQuery(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, q string, _ *[]row) error {
			capturedQuery = q
			return nil
		})

	bg := lister.NewBackground[row](NewMockLog(ctrl), dbMock, "original query", nil, nil)
	bg.Query = "overridden query"
	bg.CollectionTimeout = time.Minute
	bg.CacheTTL = time.Hour

	_ = bg.CollectOnce(context.Background())
	assert.Equal(t, "overridden query", capturedQuery)
}

// --------------------------------------------------------------------------
// helpers
// --------------------------------------------------------------------------

func assertCompleted(t *testing.T, c chan struct{}, timeout time.Duration, msg string) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(timeout):
		t.Fatal(msg)
	}
}
