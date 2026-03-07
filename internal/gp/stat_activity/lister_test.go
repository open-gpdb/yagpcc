package stat_activity_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/open-gpdb/yagpcc/internal/gp"
	"github.com/open-gpdb/yagpcc/internal/gp/stat_activity"
	"github.com/open-gpdb/yagpcc/internal/utils"
)

func TestLister_Start_Negative(t *testing.T) {
	t.Run("error initializing sessions cache", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logMock := NewMockLog(ctrl)
		logMock.EXPECT().Infof("initializing cache")

		dbMock := NewMockDB(ctrl)
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]stat_activity.Session{})).
			Return(fmt.Errorf("test error"))

		sut := stat_activity.NewLister(
			logMock,
			dbMock,
			// delay periodic collection after immediate one
			stat_activity.WithBackgroundSessionsCollectionInterval(1*time.Hour),
			stat_activity.WithBackgroundLocksCollectionInterval(1*time.Hour),
		)

		err := sut.Start(context.Background())
		assert.EqualError(t, err, "error initializing sessions cache: error executing query: test error")
	})

	t.Run("error initializing locks cache", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logMock := NewMockLog(ctrl)
		logMock.EXPECT().Infof("initializing cache")

		dbMock := NewMockDB(ctrl)
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]stat_activity.Session{})).
			Return(nil)
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]stat_activity.SessionLock{})).
			Return(fmt.Errorf("test error"))

		sut := stat_activity.NewLister(
			logMock,
			dbMock,
			// delay periodic collection after immediate one
			stat_activity.WithBackgroundSessionsCollectionInterval(1*time.Hour),
			stat_activity.WithBackgroundLocksCollectionInterval(1*time.Hour),
		)

		err := sut.Start(context.Background())
		assert.EqualError(t, err, "error initializing locks cache: error executing query: test error")
	})
}

func TestLister_Start_Stop(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		sessionsCollectionStopped := make(chan struct{})
		locksCollectionStopped := make(chan struct{})

		logMock := NewMockLog(ctrl)
		logMock.EXPECT().Infof("initializing cache")
		logMock.EXPECT().Infof("background collection for %s started", "stat_activity.Session")
		logMock.EXPECT().Infof("background collection for %s started", "stat_activity.SessionLock")
		logMock.
			EXPECT().
			Infof("background collection for %s stopped", "stat_activity.Session").
			Do(func(string, ...any) { close(sessionsCollectionStopped) })
		logMock.
			EXPECT().
			Infof("background collection for %s stopped", "stat_activity.SessionLock").
			Do(func(string, ...any) { close(locksCollectionStopped) })

		dbMock := NewMockDB(ctrl)
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.Any()).
			DoAndReturn(func(context.Context, string, any) error {
				return nil
			}).
			Times(2)

		sut := stat_activity.NewLister(
			logMock,
			dbMock,
			// delay periodic collection after immediate one
			stat_activity.WithBackgroundSessionsCollectionInterval(1*time.Hour),
			stat_activity.WithBackgroundLocksCollectionInterval(1*time.Hour),
		)

		err := sut.Start(context.Background())
		assert.NoError(t, err)

		sut.Stop()
		assertBackgroundOperationCompleted(t, sessionsCollectionStopped, 10*time.Second, "stop sessions collection timed out")
		assertBackgroundOperationCompleted(t, locksCollectionStopped, 10*time.Second, "stop locks collection timed out")
	})

	t.Run("start twice", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		sessionsCollectionStopped := make(chan struct{})
		locksCollectionStopped := make(chan struct{})

		logMock := NewMockLog(ctrl)
		logMock.EXPECT().Infof("initializing cache")
		logMock.EXPECT().Infof("background collection for %s started", "stat_activity.Session")
		logMock.EXPECT().Infof("background collection for %s started", "stat_activity.SessionLock")
		logMock.EXPECT().Warnf("an attempt was made to start a background collection that is already running")
		logMock.
			EXPECT().
			Infof("background collection for %s stopped", "stat_activity.Session").
			Do(func(string, ...any) { close(sessionsCollectionStopped) })
		logMock.
			EXPECT().
			Infof("background collection for %s stopped", "stat_activity.SessionLock").
			Do(func(string, ...any) { close(locksCollectionStopped) })

		dbMock := NewMockDB(ctrl)
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]stat_activity.Session{})).
			DoAndReturn(func(context.Context, string, any) error {
				return nil
			})
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]stat_activity.SessionLock{})).
			Return(nil)

		sut := stat_activity.NewLister(
			logMock,
			dbMock,
			// delay periodic collection after immediate one
			stat_activity.WithBackgroundSessionsCollectionInterval(1*time.Hour),
			stat_activity.WithBackgroundLocksCollectionInterval(1*time.Hour),
		)

		err := sut.Start(context.Background())
		assert.NoError(t, err)

		// second start call does nothing
		err = sut.Start(context.Background())
		assert.NoError(t, err)

		sut.Stop()
		assertBackgroundOperationCompleted(t, sessionsCollectionStopped, 10*time.Second, "stop sessions collection timed out")
		assertBackgroundOperationCompleted(t, locksCollectionStopped, 10*time.Second, "stop locks collection timed out")
	})

	t.Run("stop twice", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		sessionsCollectionStopped := make(chan struct{})
		locksCollectionStopped := make(chan struct{})

		logMock := NewMockLog(ctrl)
		logMock.EXPECT().Infof("initializing cache")
		logMock.EXPECT().Infof("background collection for %s started", "stat_activity.Session")
		logMock.EXPECT().Infof("background collection for %s started", "stat_activity.SessionLock")
		logMock.
			EXPECT().
			Infof("background collection for %s stopped", "stat_activity.Session").
			Do(func(string, ...any) { close(sessionsCollectionStopped) })
		logMock.
			EXPECT().
			Infof("background collection for %s stopped", "stat_activity.SessionLock").
			Do(func(string, ...any) { close(locksCollectionStopped) })
		logMock.EXPECT().Warnf("an attempt was made to stop a background collection that is not running")

		dbMock := NewMockDB(ctrl)
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]stat_activity.Session{})).
			DoAndReturn(func(context.Context, string, any) error {
				return nil
			})
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]stat_activity.SessionLock{})).
			Return(nil)

		sut := stat_activity.NewLister(
			logMock,
			dbMock,
			// delay periodic collection after immediate one
			stat_activity.WithBackgroundSessionsCollectionInterval(1*time.Hour),
			stat_activity.WithBackgroundLocksCollectionInterval(1*time.Hour),
		)

		err := sut.Start(context.Background())
		assert.NoError(t, err)

		sut.Stop()
		assertBackgroundOperationCompleted(t, sessionsCollectionStopped, 10*time.Second, "stop sessions collection timed out")
		assertBackgroundOperationCompleted(t, locksCollectionStopped, 10*time.Second, "stop locks collection timed out")

		// repeated stop does nothing
		sut.Stop()
	})
}

func TestLister_List_Positive(t *testing.T) {
	t.Run("empty sessions and empty session locks", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logMock := NewMockLog(ctrl)
		logMock.EXPECT().Infof("initializing cache")
		logMock.EXPECT().Infof("background collection for %s started", gomock.Any()).AnyTimes()
		logMock.EXPECT().Infof("background collection for %s stopped", gomock.Any()).AnyTimes()

		dbMock := NewMockDB(ctrl)
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]stat_activity.Session{})).
			Return(nil).
			AnyTimes()
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]stat_activity.SessionLock{})).
			Return(nil).
			AnyTimes()

		sut := stat_activity.NewLister(
			logMock,
			dbMock,
		)

		err := sut.Start(context.Background())
		assert.NoError(t, err)

		actual, err := sut.List(context.Background())

		assert.NoError(t, err)
		assert.Empty(t, actual)

		sut.Stop()
	})

	t.Run("empty sessions and non empty session locks", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logMock := NewMockLog(ctrl)
		logMock.EXPECT().Infof("initializing cache")
		logMock.EXPECT().Infof("background collection for %s started", gomock.Any()).AnyTimes()
		logMock.EXPECT().Infof("background collection for %s stopped", gomock.Any()).AnyTimes()

		dbMock := NewMockDB(ctrl)
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]stat_activity.Session{})).
			Return(nil).
			AnyTimes()
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]stat_activity.SessionLock{})).
			DoAndReturn(func(_ context.Context, _ string, destination *[]stat_activity.SessionLock) error {
				*destination = []stat_activity.SessionLock{
					{BlockSessID: 1, BlockedBySessID: 2, WaitMode: "test-wait-mode", LockedItem: "test-item", LockedMode: "test-lock-mode"},
					{BlockSessID: 2, BlockedBySessID: 3, WaitMode: "test-wait-mode2", LockedItem: "test-item2", LockedMode: "test-lock-mode2"},
				}
				return nil
			}).
			AnyTimes()

		sut := stat_activity.NewLister(
			logMock,
			dbMock,
		)

		err := sut.Start(context.Background())
		assert.NoError(t, err)

		actual, err := sut.List(context.Background())

		assert.NoError(t, err)
		assert.Empty(t, actual)

		sut.Stop()
	})

	t.Run("non empty sessions and empty session locks", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logMock := NewMockLog(ctrl)
		logMock.EXPECT().Infof("initializing cache")
		logMock.EXPECT().Infof("background collection for %s started", gomock.Any()).AnyTimes()
		logMock.EXPECT().Infof("background collection for %s stopped", gomock.Any()).AnyTimes()

		dbMock := NewMockDB(ctrl)
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]stat_activity.Session{})).
			DoAndReturn(func(_ context.Context, _ string, destination *[]stat_activity.Session) error {
				*destination = []stat_activity.Session{
					{SessID: 1, DatID: 2, Datname: "db3", Pid: 4, TmID: 5, UsesysID: 6, Usename: "user7"},
					{SessID: 2, DatID: 3, Datname: "db4", Pid: 5, TmID: 6, UsesysID: 7, Usename: "user8"},
				}

				return nil
			}).AnyTimes()
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]stat_activity.SessionLock{})).
			Return(nil).
			AnyTimes()

		sut := stat_activity.NewLister(
			logMock,
			dbMock,
		)

		err := sut.Start(context.Background())
		assert.NoError(t, err)

		actual, err := sut.List(context.Background())

		assert.NoError(t, err)
		assert.ElementsMatch(
			t,
			[]*gp.GpStatActivity{
				{
					SessID:   1,
					DatID:    2,
					Datname:  "db3",
					Pid:      4,
					TmID:     5,
					UsesysID: 6,
					Usename:  "user7",
				},
				{
					SessID:   2,
					DatID:    3,
					Datname:  "db4",
					Pid:      5,
					TmID:     6,
					UsesysID: 7,
					Usename:  "user8",
				},
			},
			actual,
		)

		sut.Stop()
	})

	t.Run("non empty sessions and non empty locks", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logMock := NewMockLog(ctrl)
		logMock.EXPECT().Infof("initializing cache")
		logMock.EXPECT().Infof("background collection for %s started", gomock.Any()).AnyTimes()
		logMock.EXPECT().Infof("background collection for %s stopped", gomock.Any()).AnyTimes()

		dbMock := NewMockDB(ctrl)
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]stat_activity.Session{})).
			DoAndReturn(func(ctx context.Context, _ string, destination *[]stat_activity.Session) error {
				*destination = []stat_activity.Session{
					{SessID: 1, DatID: 2, Datname: "db3", Pid: 4, TmID: 5, UsesysID: 6, Usename: "user7"},
					{SessID: 2, DatID: 3, Datname: "db4", Pid: 5, TmID: 6, UsesysID: 7, Usename: "user8"},
				}

				return nil
			}).
			AnyTimes()
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]stat_activity.SessionLock{})).
			DoAndReturn(func(ctx context.Context, _ string, destination *[]stat_activity.SessionLock) error {
				*destination = []stat_activity.SessionLock{
					{BlockSessID: 1, BlockedBySessID: 2, WaitMode: "test-wait-mode", LockedItem: "test-item", LockedMode: "test-lock-mode"},
					{BlockSessID: 3, BlockedBySessID: 4, WaitMode: "test-wait-mode3", LockedItem: "test-item3", LockedMode: "test-lock-mode3"},
				}

				return nil
			}).
			AnyTimes()

		sut := stat_activity.NewLister(
			logMock,
			dbMock,
		)

		err := sut.Start(context.Background())
		assert.NoError(t, err)

		actual, err := sut.List(context.Background())

		assert.NoError(t, err)
		assert.ElementsMatch(
			t,
			[]*gp.GpStatActivity{
				{
					SessID:          1,
					DatID:           2,
					Datname:         "db3",
					Pid:             4,
					TmID:            5,
					UsesysID:        6,
					Usename:         "user7",
					BlockedBySessID: utils.P(2),
					WaitMode:        utils.P("test-wait-mode"),
					LockedItem:      utils.P("test-item"),
					LockedMode:      utils.P("test-lock-mode"),
				},
				{
					SessID:   2,
					DatID:    3,
					Datname:  "db4",
					Pid:      5,
					TmID:     6,
					UsesysID: 7,
					Usename:  "user8",
				},
			},
			actual,
		)

		sut.Stop()
	})

	t.Run("list from background collected", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		sessionsCollected := make(chan struct{})
		sessionsCollectedTimes := 0

		locksCollected := make(chan struct{})
		locksCollectedTimes := 0

		logMock := NewMockLog(ctrl)
		logMock.EXPECT().Infof("initializing cache")
		logMock.EXPECT().Infof("background collection for %s started", gomock.Any()).AnyTimes()
		logMock.EXPECT().Infof("background collection for %s stopped", gomock.Any()).AnyTimes()

		dbMock := NewMockDB(ctrl)
		// immediate collection
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]stat_activity.Session{})).
			Return(nil)
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]stat_activity.SessionLock{})).
			Return(nil)
		// background collection
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]stat_activity.Session{})).
			DoAndReturn(func(ctx context.Context, _ string, destination *[]stat_activity.Session) error {
				*destination = []stat_activity.Session{
					{SessID: 1, DatID: 2, Datname: "db3", Pid: 4, TmID: 5, UsesysID: 6, Usename: "user7"},
					{SessID: 2, DatID: 3, Datname: "db4", Pid: 5, TmID: 6, UsesysID: 7, Usename: "user8"},
				}

				if sessionsCollectedTimes == 1 {
					close(sessionsCollected)
				}
				sessionsCollectedTimes++

				return nil
			}).
			AnyTimes()
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]stat_activity.SessionLock{})).
			DoAndReturn(func(ctx context.Context, _ string, destination *[]stat_activity.SessionLock) error {
				*destination = []stat_activity.SessionLock{
					{BlockSessID: 1, BlockedBySessID: 2, WaitMode: "test-wait-mode", LockedItem: "test-item", LockedMode: "test-lock-mode"},
					{BlockSessID: 3, BlockedBySessID: 4, WaitMode: "test-wait-mode3", LockedItem: "test-item3", LockedMode: "test-lock-mode3"},
				}

				if locksCollectedTimes == 1 {
					close(locksCollected)
				}
				locksCollectedTimes++

				return nil
			}).
			AnyTimes()

		sut := stat_activity.NewLister(
			logMock,
			dbMock,
			stat_activity.WithBackgroundSessionsCollectionInterval(1*time.Millisecond),
			stat_activity.WithBackgroundLocksCollectionInterval(1*time.Millisecond),
		)

		err := sut.Start(context.Background())
		assert.NoError(t, err)

		assertBackgroundOperationCompleted(t, sessionsCollected, 10*time.Second, "sessions collection operation timed out")
		assertBackgroundOperationCompleted(t, locksCollected, 10*time.Second, "locks collection operation timed out")

		actual, err := sut.List(context.Background())

		assert.NoError(t, err)
		assert.ElementsMatch(
			t,
			[]*gp.GpStatActivity{
				{
					SessID:          1,
					DatID:           2,
					Datname:         "db3",
					Pid:             4,
					TmID:            5,
					UsesysID:        6,
					Usename:         "user7",
					BlockedBySessID: utils.P(2),
					WaitMode:        utils.P("test-wait-mode"),
					LockedItem:      utils.P("test-item"),
					LockedMode:      utils.P("test-lock-mode"),
				},
				{
					SessID:   2,
					DatID:    3,
					Datname:  "db4",
					Pid:      5,
					TmID:     6,
					UsesysID: 7,
					Usename:  "user8",
				},
			},
			actual,
		)

		sut.Stop()
	})

	t.Run("without locks info due to db errors", func(t *testing.T) {
		cacheTTL := 10 * time.Millisecond

		ctrl := gomock.NewController(t)

		sessionsCollected := make(chan struct{})
		sessionsCollectedTimes := 0

		locksCollected := make(chan struct{})
		locksCollectedTimes := 0

		logMock := NewMockLog(ctrl)
		logMock.EXPECT().Infof("initializing cache")
		logMock.EXPECT().Infof("background collection for %s started", gomock.Any()).AnyTimes()
		logMock.EXPECT().Infof("background collection for %s stopped", gomock.Any()).AnyTimes()
		logMock.EXPECT().Warnf("error during background collection %s: %s", "stat_activity.SessionLock", "error executing query: test error").AnyTimes()
		logMock.EXPECT().Warnf("returning stat activity data without locks info due to error: %s", "cached value is stale")

		dbMock := NewMockDB(ctrl)
		// immediate collection
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]stat_activity.Session{})).
			Return(nil)
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]stat_activity.SessionLock{})).
			Return(nil)
		// background collection
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]stat_activity.Session{})).
			DoAndReturn(func(ctx context.Context, _ string, destination *[]stat_activity.Session) error {
				*destination = []stat_activity.Session{
					{SessID: 1, DatID: 2, Datname: "db3", Pid: 4, TmID: 5, UsesysID: 6, Usename: "user7"},
					{SessID: 2, DatID: 3, Datname: "db4", Pid: 5, TmID: 6, UsesysID: 7, Usename: "user8"},
				}

				if sessionsCollectedTimes == 1 {
					close(sessionsCollected)
				}
				sessionsCollectedTimes++

				return nil
			}).
			AnyTimes()
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]stat_activity.SessionLock{})).
			DoAndReturn(func(context.Context, string, any) error {
				// to simulate staleness
				time.Sleep(2 * cacheTTL)
				if locksCollectedTimes == 0 {
					close(locksCollected)
				}
				locksCollectedTimes++

				return fmt.Errorf("test error")
			}).
			AnyTimes()

		sut := stat_activity.NewLister(
			logMock,
			dbMock,
			stat_activity.WithBackgroundSessionsCollectionInterval(1*time.Millisecond),
			stat_activity.WithBackgroundLocksCollectionInterval(1*time.Millisecond),
			stat_activity.WithBackgroundLocksCacheTTL(cacheTTL),
		)

		err := sut.Start(context.Background())
		assert.NoError(t, err)

		assertBackgroundOperationCompleted(t, sessionsCollected, 10*time.Second, "sessions collection operation timed out")
		assertBackgroundOperationCompleted(t, locksCollected, 10*time.Second, "locks collection operation timed out")

		actual, err := sut.List(context.Background())

		assert.NoError(t, err)
		assert.Equal(
			t,
			[]*gp.GpStatActivity{
				{
					SessID:   1,
					DatID:    2,
					Datname:  "db3",
					Pid:      4,
					TmID:     5,
					UsesysID: 6,
					Usename:  "user7",
				},
				{
					SessID:   2,
					DatID:    3,
					Datname:  "db4",
					Pid:      5,
					TmID:     6,
					UsesysID: 7,
					Usename:  "user8",
				},
			},
			actual,
		)

		sut.Stop()
	})
}

func TestLister_List_Negative(t *testing.T) {
	t.Run("collection was not started", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		sut := stat_activity.NewLister(
			NewMockLog(ctrl),
			NewMockDB(ctrl),
		)

		actual, err := sut.List(context.Background())

		assert.EqualError(t, err, "background collection was not started")
		assert.Nil(t, actual)
	})

	t.Run("stale read due to sessions db errors", func(t *testing.T) {
		cacheTTL := 10 * time.Millisecond

		ctrl := gomock.NewController(t)

		sessionsCollectedTimes := 0
		sessionsCollected := make(chan struct{})

		logMock := NewMockLog(ctrl)
		logMock.EXPECT().Infof("initializing cache")
		logMock.EXPECT().Infof("background collection for %s started", gomock.Any()).AnyTimes()
		logMock.EXPECT().Infof("background collection for %s stopped", gomock.Any()).AnyTimes()
		logMock.
			EXPECT().
			Warnf("error during background collection %s: %s", "stat_activity.Session", "error executing query: test error").
			AnyTimes()

		dbMock := NewMockDB(ctrl)
		// immediate collection
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]stat_activity.Session{})).
			Return(nil)
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]stat_activity.SessionLock{})).
			Return(nil)
		// background collection
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]stat_activity.Session{})).
			DoAndReturn(func(context.Context, string, any) error {
				// to simulate staleness
				time.Sleep(2 * cacheTTL)
				if sessionsCollectedTimes == 0 {
					close(sessionsCollected)
				}
				sessionsCollectedTimes++

				return fmt.Errorf("test error")
			}).
			AnyTimes()
		dbMock.
			EXPECT().
			ExecQuery(gomock.Any(), gomock.AssignableToTypeOf(""), gomock.AssignableToTypeOf(&[]stat_activity.SessionLock{})).
			Return(nil).
			AnyTimes()

		sut := stat_activity.NewLister(
			logMock,
			dbMock,
			stat_activity.WithBackgroundSessionsCollectionInterval(1*time.Millisecond),
			stat_activity.WithBackgroundLocksCollectionInterval(1*time.Millisecond),
			stat_activity.WithBackgroundSessionsCacheTTL(cacheTTL),
		)

		err := sut.Start(context.Background())
		assert.NoError(t, err)

		assertBackgroundOperationCompleted(t, sessionsCollected, 10*time.Second, "background collection operation timed out")

		actual, err := sut.List(context.Background())

		assert.EqualError(t, err, "error reading sessions: cached value is stale")
		assert.Nil(t, actual)

		sut.Stop()
	})
}

func assertBackgroundOperationCompleted(t *testing.T, c chan struct{}, timeout time.Duration, format string, args ...any) {
	select {
	case <-c:
		return
	case <-time.After(timeout):
		t.Errorf(format, args...)
	}
}
