package master_sentinel_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/open-gpdb/yagpcc/internal/gp/master_sentinel"
)

func TestSentinel_RunUntilIsMaster(t *testing.T) {
	t.Run("stopped by context without error", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		sut := master_sentinel.NewSentinel(NewMockLog(ctrl), NewMockDB(ctrl))

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := sut.RunUntilIsMaster(ctx)
		assert.NoError(t, err)
	})

	t.Run("switchover", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		checkAttemptN := 0
		dbMock := NewMockDB(ctrl)
		dbMock.
			EXPECT().
			ExecQueryNoRetry(gomock.Any(), `select pg_is_in_recovery();`, gomock.AssignableToTypeOf(&[]bool{})).
			DoAndReturn(func(_ context.Context, _ string, destination *[]bool) error {
				checkAttemptN++
				if checkAttemptN == 1 {
					*destination = []bool{false}
					return nil
				}

				*destination = []bool{true}
				return nil
			}).
			AnyTimes()

		sut := master_sentinel.NewSentinel(NewMockLog(ctrl), dbMock, master_sentinel.WithCheckInterval(time.Millisecond))

		err := sut.RunUntilIsMaster(context.Background())
		assert.EqualError(t, err, "current instance is in recovery mode")
	})

	t.Run("switchover (based on errors)", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		checkAttemptN := 0
		dbMock := NewMockDB(ctrl)
		dbMock.
			EXPECT().
			ExecQueryNoRetry(gomock.Any(), `select pg_is_in_recovery();`, gomock.AssignableToTypeOf(&[]bool{})).
			DoAndReturn(func(_ context.Context, _ string, destination *[]bool) error {
				checkAttemptN++
				if checkAttemptN == 1 {
					*destination = []bool{false}
					return nil
				}

				return fmt.Errorf("server error (FATAL: the database system is in recovery mode (SQLSTATE 57M02)")
			}).
			AnyTimes()

		sut := master_sentinel.NewSentinel(
			NewMockLog(ctrl),
			dbMock,
			master_sentinel.WithCheckInterval(time.Millisecond),
		)

		err := sut.RunUntilIsMaster(context.Background())
		assert.EqualError(t, err, "current instance is in recovery mode")
	})

	t.Run("temporary unavailable", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		checkAttemptN := 0

		logMock := NewMockLog(ctrl)
		logMock.
			EXPECT().
			Warnf("check is in recovery error suppressed (subsequentCheckErrorsN=%d): %s", 1, "test error").
			AnyTimes()

		dbMock := NewMockDB(ctrl)
		dbMock.
			EXPECT().
			ExecQueryNoRetry(gomock.Any(), `select pg_is_in_recovery();`, gomock.AssignableToTypeOf(&[]bool{})).
			DoAndReturn(func(_ context.Context, _ string, destination *[]bool) error {
				checkAttemptN++
				if checkAttemptN == 10 {
					// cancel context to stop test
					cancel()
				}

				if checkAttemptN%2 == 0 {
					return fmt.Errorf("test error")
				}

				*destination = []bool{false}
				return nil
			}).
			AnyTimes()

		sut := master_sentinel.NewSentinel(
			logMock,
			dbMock,
			master_sentinel.WithCheckInterval(time.Millisecond),
			master_sentinel.WithMaxSubsequentCheckErrorsN(2),
		)

		err := sut.RunUntilIsMaster(ctx)
		assert.NoError(t, err)
	})

	t.Run("unavailable for too long", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		logMock := NewMockLog(ctrl)
		logMock.EXPECT().Warnf("check is in recovery error suppressed (subsequentCheckErrorsN=%d): %s", 1, "test error")
		logMock.EXPECT().Warnf("check is in recovery error suppressed (subsequentCheckErrorsN=%d): %s", 2, "test error")

		dbMock := NewMockDB(ctrl)
		dbMock.
			EXPECT().
			ExecQueryNoRetry(gomock.Any(), `select pg_is_in_recovery();`, gomock.AssignableToTypeOf(&[]bool{})).
			DoAndReturn(func(context.Context, string, *[]bool) error {
				return fmt.Errorf("test error")
			}).
			Times(3)

		sut := master_sentinel.NewSentinel(
			logMock,
			dbMock,
			master_sentinel.WithCheckInterval(time.Millisecond),
			master_sentinel.WithMaxSubsequentCheckErrorsN(3),
		)

		err := sut.RunUntilIsMaster(context.Background())
		assert.EqualError(
			t,
			err,
			"exceeded number of max subsequent check errors (maxSubsequentCheckErrorsN=3), "+
				"last error was: test error",
		)
	})
}
