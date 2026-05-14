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
