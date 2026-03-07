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
