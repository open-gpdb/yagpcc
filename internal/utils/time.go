package utils

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// Delay returns nil after the specified duration or error if interrupted.
func Delay(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	select {
	case <-ctx.Done():
		t.Stop()
		return fmt.Errorf("Interrupted")
	case <-t.C:
	}
	return nil
}

func GetTimestampFromTime(in time.Time) *timestamppb.Timestamp {
	if in.Unix() <= 0 {
		return &timestamppb.Timestamp{}
	}
	return timestamppb.New(in)
}

func GetTimeForTimestamp(x *timestamppb.Timestamp) time.Time {
	return time.Unix(x.GetSeconds(), int64(x.GetNanos()))
}

func GetTimeAsString(t time.Time) string {
	return t.Format("2006-01-02T15:04:05-07:00")
}

// MinTime returns the earlier of a and b. If one argument is nil, the other is returned.
// If both are nil, MinTime returns nil.
func MinTime(a, b *timestamppb.Timestamp) *timestamppb.Timestamp {
	switch {
	case a == nil && b == nil:
		return nil
	case a == nil:
		return b
	case b == nil:
		return a
	}
	ta := GetTimeForTimestamp(a)
	tb := GetTimeForTimestamp(b)
	if ta.Before(tb) {
		return a
	}
	return b
}

// MaxTime returns the later of a and b. If one argument is nil, the other is returned.
// If both are nil, MaxTime returns nil.
func MaxTime(a, b *timestamppb.Timestamp) *timestamppb.Timestamp {
	switch {
	case a == nil && b == nil:
		return nil
	case a == nil:
		return b
	case b == nil:
		return a
	}
	ta := GetTimeForTimestamp(a)
	tb := GetTimeForTimestamp(b)
	if ta.After(tb) {
		return a
	}
	return b
}
