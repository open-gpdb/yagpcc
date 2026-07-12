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

package master

import (
	"context"

	"github.com/open-gpdb/yagpcc/internal/metrics"
)

// fanOut reads messages from source and forwards each one to every target
// channel. Sends are non-blocking: when a target channel is full the message is
// dropped for that target alone (accounted as a WriterDroppedMessages with the
// target's label), so a slow or stalled target cannot back up the others.
// names[i] labels targets[i] and must have the same length as targets.
func fanOut[T any](ctx context.Context, source <-chan T, targets []chan T, stream string, names []string) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-source:
			if !ok {
				return
			}
			for i, tc := range targets {
				select {
				case tc <- msg:
				default:
					if metrics.YagpccMetrics != nil {
						metrics.YagpccMetrics.WriterDroppedMessages.WithLabelValues(stream, names[i]).Inc()
					}
				}
			}
		}
	}
}
