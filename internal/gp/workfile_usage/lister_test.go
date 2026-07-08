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

package workfile_usage_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/open-gpdb/yagpcc/internal/gp/workfile_usage"
)

// sampleClusterData mirrors the example output from a real GP cluster:
//
//	select pid, sess_id, command_cnt, segid, size, numfiles
//	from gp_toolkit.gp_workfile_usage_per_query;
var sampleClusterData = []workfile_usage.WorkfileUsageEntry{
	{Pid: 766184, SessID: 2738434, CommandCnt: 129, SegID: 11, Size: 1284341760, NumFiles: 60},
	{Pid: 766184, SessID: 2738434, CommandCnt: 129, SegID: 105, Size: 1289748480, NumFiles: 60},
	{Pid: 766184, SessID: 2738434, CommandCnt: 129, SegID: 41, Size: 1543012352, NumFiles: 60},
	{Pid: 766184, SessID: 2738434, CommandCnt: 129, SegID: 127, Size: 1493860352, NumFiles: 60},
	{Pid: 766184, SessID: 2738434, CommandCnt: 129, SegID: 92, Size: 1746960384, NumFiles: 60},
	{Pid: 766184, SessID: 2738434, CommandCnt: 129, SegID: 120, Size: 1619722240, NumFiles: 60},
	{Pid: 766184, SessID: 2738434, CommandCnt: 129, SegID: 57, Size: 1725628416, NumFiles: 60},
	{Pid: 766184, SessID: 2738434, CommandCnt: 129, SegID: 78, Size: 1714421760, NumFiles: 60},
	{Pid: 766184, SessID: 2738434, CommandCnt: 129, SegID: 37, Size: 1670643712, NumFiles: 60},
	{Pid: 766184, SessID: 2738434, CommandCnt: 129, SegID: 75, Size: 1670086656, NumFiles: 60},
	{Pid: 766184, SessID: 2738434, CommandCnt: 129, SegID: 52, Size: 1764917248, NumFiles: 60},
	{Pid: 766184, SessID: 2738434, CommandCnt: 129, SegID: 19, Size: 1431601152, NumFiles: 60},
	{Pid: 778050, SessID: 2741839, CommandCnt: 111, SegID: -1, Size: 167951628, NumFiles: 26},
	{Pid: 768765, SessID: 2739242, CommandCnt: 24, SegID: 5, Size: 1245184, NumFiles: 2},
	{Pid: 766184, SessID: 2738434, CommandCnt: 129, SegID: 21, Size: 1564719664, NumFiles: 23},
}

func TestLister_Start_Negative(t *testing.T) {
	t.Run("error initializing workfile usage cache", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		collectionStopped := make(chan struct{})

		logMock := NewMockLog(ctrl)
		logMock.EXPECT().Infof("initializing cache")
		logMock.EXPECT().Infof("background collection for %s started", "workfile_usage.WorkfileUsageEntry")
		logMock.EXPECT().Warnf("error initializing workfile usage cache, continuing without workfile usage data: %s", "error executing query: test error")
		logMock.
			EXPECT().
			Infof("background collection for %s stopped", "workfile_usage.WorkfileUsageEntry").
			Do(func(string, ...any) { close(collectionStopped) })

		dbMock := NewMockDB(ctrl)
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]workfile_usage.WorkfileUsageEntry{})).
			Return(fmt.Errorf("test error"))

		sut := workfile_usage.NewLister(
			logMock,
			dbMock,
			workfile_usage.WithCollectionInterval(1*time.Hour),
		)

		err := sut.Start(context.Background())
		assert.NoError(t, err)

		sut.Stop()
		assertBackgroundOperationCompleted(t, collectionStopped, 10*time.Second, "stop collection timed out")
	})
}

func TestLister_Start_Stop(t *testing.T) {
	t.Run("simple start and stop", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		collectionStopped := make(chan struct{})

		logMock := NewMockLog(ctrl)
		logMock.EXPECT().Infof("initializing cache")
		logMock.EXPECT().Infof("background collection for %s started", "workfile_usage.WorkfileUsageEntry")
		logMock.
			EXPECT().
			Infof("background collection for %s stopped", "workfile_usage.WorkfileUsageEntry").
			Do(func(string, ...any) { close(collectionStopped) })

		dbMock := NewMockDB(ctrl)
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]workfile_usage.WorkfileUsageEntry{})).
			Return(nil).
			Times(1)

		sut := workfile_usage.NewLister(
			logMock,
			dbMock,
			workfile_usage.WithCollectionInterval(1*time.Hour),
		)

		err := sut.Start(context.Background())
		assert.NoError(t, err)

		sut.Stop()
		assertBackgroundOperationCompleted(t, collectionStopped, 10*time.Second, "stop collection timed out")
	})

	t.Run("start twice", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		collectionStopped := make(chan struct{})

		logMock := NewMockLog(ctrl)
		logMock.EXPECT().Infof("initializing cache")
		logMock.EXPECT().Infof("background collection for %s started", "workfile_usage.WorkfileUsageEntry")
		logMock.EXPECT().Warnf("an attempt was made to start a background collection that is already running")
		logMock.
			EXPECT().
			Infof("background collection for %s stopped", "workfile_usage.WorkfileUsageEntry").
			Do(func(string, ...any) { close(collectionStopped) })

		dbMock := NewMockDB(ctrl)
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]workfile_usage.WorkfileUsageEntry{})).
			Return(nil).
			Times(1)

		sut := workfile_usage.NewLister(
			logMock,
			dbMock,
			workfile_usage.WithCollectionInterval(1*time.Hour),
		)

		err := sut.Start(context.Background())
		assert.NoError(t, err)

		// second start does nothing
		err = sut.Start(context.Background())
		assert.NoError(t, err)

		sut.Stop()
		assertBackgroundOperationCompleted(t, collectionStopped, 10*time.Second, "stop collection timed out")
	})

	t.Run("stop twice", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		collectionStopped := make(chan struct{})

		logMock := NewMockLog(ctrl)
		logMock.EXPECT().Infof("initializing cache")
		logMock.EXPECT().Infof("background collection for %s started", "workfile_usage.WorkfileUsageEntry")
		logMock.
			EXPECT().
			Infof("background collection for %s stopped", "workfile_usage.WorkfileUsageEntry").
			Do(func(string, ...any) { close(collectionStopped) })
		logMock.EXPECT().Warnf("an attempt was made to stop a background collection that is not running")

		dbMock := NewMockDB(ctrl)
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]workfile_usage.WorkfileUsageEntry{})).
			Return(nil).
			Times(1)

		sut := workfile_usage.NewLister(
			logMock,
			dbMock,
			workfile_usage.WithCollectionInterval(1*time.Hour),
		)

		err := sut.Start(context.Background())
		assert.NoError(t, err)

		sut.Stop()
		assertBackgroundOperationCompleted(t, collectionStopped, 10*time.Second, "stop collection timed out")

		// second stop does nothing
		sut.Stop()
	})
}

func TestLister_List_Negative(t *testing.T) {
	t.Run("collection was not started", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		sut := workfile_usage.NewLister(
			NewMockLog(ctrl),
			NewMockDB(ctrl),
		)

		actual, err := sut.List(context.Background())

		assert.EqualError(t, err, "background collection was not started")
		assert.Nil(t, actual)
	})

	t.Run("stale read due to db errors", func(t *testing.T) {
		cacheTTL := 10 * time.Millisecond

		ctrl := gomock.NewController(t)

		collectedTimes := 0
		collected := make(chan struct{})

		logMock := NewMockLog(ctrl)
		logMock.EXPECT().Infof("initializing cache")
		logMock.EXPECT().Infof("background collection for %s started", gomock.Any()).AnyTimes()
		logMock.EXPECT().Infof("background collection for %s stopped", gomock.Any()).AnyTimes()
		logMock.
			EXPECT().
			Warnf("error during background collection %s: %s", "workfile_usage.WorkfileUsageEntry", "error executing query: test error").
			AnyTimes()

		dbMock := NewMockDB(ctrl)
		// immediate collection succeeds
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]workfile_usage.WorkfileUsageEntry{})).
			Return(nil)
		// background collection fails (causes staleness)
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]workfile_usage.WorkfileUsageEntry{})).
			DoAndReturn(func(context.Context, string, any) error {
				// sleep longer than TTL to make the cache stale
				time.Sleep(2 * cacheTTL)
				if collectedTimes == 0 {
					close(collected)
				}
				collectedTimes++
				return fmt.Errorf("test error")
			}).
			AnyTimes()

		sut := workfile_usage.NewLister(
			logMock,
			dbMock,
			workfile_usage.WithCollectionInterval(1*time.Millisecond),
			workfile_usage.WithCacheTTL(cacheTTL),
		)

		err := sut.Start(context.Background())
		assert.NoError(t, err)

		assertBackgroundOperationCompleted(t, collected, 10*time.Second, "background collection operation timed out")

		actual, err := sut.List(context.Background())

		assert.EqualError(t, err, "error reading workfile usage: cached value is stale")
		assert.Nil(t, actual)

		sut.Stop()
	})
}

func TestLister_List_Positive(t *testing.T) {
	t.Run("empty result", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logMock := NewMockLog(ctrl)
		logMock.EXPECT().Infof("initializing cache")
		logMock.EXPECT().Infof("background collection for %s started", gomock.Any()).AnyTimes()
		logMock.EXPECT().Infof("background collection for %s stopped", gomock.Any()).AnyTimes()

		dbMock := NewMockDB(ctrl)
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]workfile_usage.WorkfileUsageEntry{})).
			Return(nil).
			AnyTimes()

		sut := workfile_usage.NewLister(logMock, dbMock)

		err := sut.Start(context.Background())
		assert.NoError(t, err)

		actual, err := sut.List(context.Background())

		assert.NoError(t, err)
		assert.Empty(t, actual)

		sut.Stop()
	})

	t.Run("non-empty result with sample cluster data", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logMock := NewMockLog(ctrl)
		logMock.EXPECT().Infof("initializing cache")
		logMock.EXPECT().Infof("background collection for %s started", gomock.Any()).AnyTimes()
		logMock.EXPECT().Infof("background collection for %s stopped", gomock.Any()).AnyTimes()

		dbMock := NewMockDB(ctrl)
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]workfile_usage.WorkfileUsageEntry{})).
			DoAndReturn(func(_ context.Context, _ string, destination *[]workfile_usage.WorkfileUsageEntry) error {
				*destination = sampleClusterData
				return nil
			}).
			AnyTimes()

		sut := workfile_usage.NewLister(logMock, dbMock)

		err := sut.Start(context.Background())
		assert.NoError(t, err)

		actual, err := sut.List(context.Background())

		assert.NoError(t, err)
		assert.ElementsMatch(t, sampleClusterData, actual)

		sut.Stop()
	})

	t.Run("segment -1 (master) entry is included", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logMock := NewMockLog(ctrl)
		logMock.EXPECT().Infof("initializing cache")
		logMock.EXPECT().Infof("background collection for %s started", gomock.Any()).AnyTimes()
		logMock.EXPECT().Infof("background collection for %s stopped", gomock.Any()).AnyTimes()

		masterEntry := workfile_usage.WorkfileUsageEntry{
			Pid: 778050, SessID: 2741839, CommandCnt: 111, SegID: -1, Size: 167951628, NumFiles: 26,
		}

		dbMock := NewMockDB(ctrl)
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]workfile_usage.WorkfileUsageEntry{})).
			DoAndReturn(func(_ context.Context, _ string, destination *[]workfile_usage.WorkfileUsageEntry) error {
				*destination = []workfile_usage.WorkfileUsageEntry{masterEntry}
				return nil
			}).
			AnyTimes()

		sut := workfile_usage.NewLister(logMock, dbMock)

		err := sut.Start(context.Background())
		assert.NoError(t, err)

		actual, err := sut.List(context.Background())

		assert.NoError(t, err)
		assert.Equal(t, []workfile_usage.WorkfileUsageEntry{masterEntry}, actual)

		sut.Stop()
	})

	t.Run("list from background collected data", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		collected := make(chan struct{})
		collectedTimes := 0

		logMock := NewMockLog(ctrl)
		logMock.EXPECT().Infof("initializing cache")
		logMock.EXPECT().Infof("background collection for %s started", gomock.Any()).AnyTimes()
		logMock.EXPECT().Infof("background collection for %s stopped", gomock.Any()).AnyTimes()

		dbMock := NewMockDB(ctrl)
		// immediate collection returns empty
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]workfile_usage.WorkfileUsageEntry{})).
			Return(nil)
		// background collections return sample data
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]workfile_usage.WorkfileUsageEntry{})).
			DoAndReturn(func(_ context.Context, _ string, destination *[]workfile_usage.WorkfileUsageEntry) error {
				*destination = sampleClusterData

				if collectedTimes == 1 {
					close(collected)
				}
				collectedTimes++

				return nil
			}).
			AnyTimes()

		sut := workfile_usage.NewLister(
			logMock,
			dbMock,
			workfile_usage.WithCollectionInterval(1*time.Millisecond),
		)

		err := sut.Start(context.Background())
		assert.NoError(t, err)

		assertBackgroundOperationCompleted(t, collected, 10*time.Second, "background collection operation timed out")

		actual, err := sut.List(context.Background())

		assert.NoError(t, err)
		assert.ElementsMatch(t, sampleClusterData, actual)

		sut.Stop()
	})

	t.Run("multiple pids same session different segments", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logMock := NewMockLog(ctrl)
		logMock.EXPECT().Infof("initializing cache")
		logMock.EXPECT().Infof("background collection for %s started", gomock.Any()).AnyTimes()
		logMock.EXPECT().Infof("background collection for %s stopped", gomock.Any()).AnyTimes()

		entries := []workfile_usage.WorkfileUsageEntry{
			{Pid: 766184, SessID: 2738434, CommandCnt: 129, SegID: 0, Size: 1238794240, NumFiles: 60},
			{Pid: 766184, SessID: 2738434, CommandCnt: 129, SegID: 1, Size: 1463779328, NumFiles: 60},
			{Pid: 766184, SessID: 2738434, CommandCnt: 129, SegID: 2, Size: 1227522048, NumFiles: 60},
			{Pid: 766184, SessID: 2738434, CommandCnt: 129, SegID: 3, Size: 1449590784, NumFiles: 60},
		}

		dbMock := NewMockDB(ctrl)
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]workfile_usage.WorkfileUsageEntry{})).
			DoAndReturn(func(_ context.Context, _ string, destination *[]workfile_usage.WorkfileUsageEntry) error {
				*destination = entries
				return nil
			}).
			AnyTimes()

		sut := workfile_usage.NewLister(logMock, dbMock)

		err := sut.Start(context.Background())
		assert.NoError(t, err)

		actual, err := sut.List(context.Background())

		assert.NoError(t, err)
		assert.Len(t, actual, 4)

		// verify the (segID, pid) key pairs are all present
		type key struct{ segID, pid int }
		expectedKeys := map[key]bool{
			{0, 766184}: true,
			{1, 766184}: true,
			{2, 766184}: true,
			{3, 766184}: true,
		}
		for _, e := range actual {
			k := key{e.SegID, e.Pid}
			assert.True(t, expectedKeys[k], "unexpected (segID=%d, pid=%d)", e.SegID, e.Pid)
		}

		sut.Stop()
	})
}

func assertBackgroundOperationCompleted(t *testing.T, c chan struct{}, timeout time.Duration, format string, args ...any) {
	t.Helper()
	select {
	case <-c:
		return
	case <-time.After(timeout):
		t.Errorf(format, args...)
	}
}
