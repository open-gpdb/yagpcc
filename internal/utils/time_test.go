package utils_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/open-gpdb/yagpcc/internal/utils"
)

func TestDelay(t *testing.T) {
	t.Run("not interrupted", func(t *testing.T) {
		const delay = 100 * time.Millisecond

		startedAt := time.Now()
		err := utils.Delay(context.Background(), delay)
		actualDelay := time.Since(startedAt)

		require.NoError(t, err)
		assert.Less(t, delay, actualDelay)
	})

	t.Run("interrupted", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
		defer cancel()

		err := utils.Delay(ctx, 1*time.Minute)
		assert.EqualError(t, err, "Interrupted")
	})
}

func TestGetTimestampFromTime(t *testing.T) {
	assert.NotEqual(t, utils.GetTimestampFromTime(time.Now()), &timestamppb.Timestamp{})
	testT := time.Now()
	assert.Equal(t, utils.GetTimestampFromTime(testT), timestamppb.New(testT))
	assert.Equal(t, utils.GetTimestampFromTime(time.Time{}), &timestamppb.Timestamp{})
}

func TestGetTimeForTimestamp(t *testing.T) {
	assert.Equal(t, time.Unix(0, 0), utils.GetTimeForTimestamp(nil))

	testTWithoutNanos := time.Now().Truncate(time.Nanosecond)
	assert.Equal(t, testTWithoutNanos, utils.GetTimeForTimestamp(timestamppb.New(testTWithoutNanos)))

	testTWithNanos := testTWithoutNanos.Add(124 * time.Millisecond)
	assert.Equal(t, testTWithNanos, utils.GetTimeForTimestamp(timestamppb.New(testTWithNanos)))
}

func TestGetTimeAsString(t *testing.T) {
	assert.Equal(t, "0001-01-01T00:00:00+00:00", utils.GetTimeAsString(time.Time{}))
	assert.Equal(t, "2025-02-21T16:07:02+00:00", utils.GetTimeAsString(time.Date(2025, 02, 21, 16, 07, 02, 123, time.UTC)))
}
