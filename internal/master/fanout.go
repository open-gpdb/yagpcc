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

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	"github.com/open-gpdb/yagpcc/internal/gp"
	"github.com/open-gpdb/yagpcc/internal/metrics"
)

// fanOut reads messages from source and forwards each one to every target
// channel. stamp, when non-nil, is applied once per message before it is
// distributed; because it runs in this single goroutine and downstream writers
// only read the message, per-message normalisation stays race-free even when
// the same pointer is teed to several targets.
//
// With a single target the send blocks, so the source keeps its original
// backpressure. With several targets the sends are non-blocking: a full target
// channel drops the message for that target alone (accounted as a
// WriterDroppedMessages with the target's label), so a slow or stalled target
// cannot back up the others. names[i] labels targets[i] and must have the same
// length as targets.
func fanOut[T any](ctx context.Context, source <-chan T, targets []chan T, stream string, names []string, stamp func(T)) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-source:
			if !ok {
				return
			}
			if stamp != nil {
				stamp(msg)
			}
			if len(targets) == 1 {
				select {
				case targets[0] <- msg:
				case <-ctx.Done():
					return
				}
				continue
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

// DiscoveredTmID normalisation. The transport master fills TmID from the
// discovered value on every archived message; doing it here — once, before the
// message fans out — keeps the writers read-only so a teed pointer is never
// mutated concurrently.
func stampSessionTmID(s *gp.SessionDataWrite) {
	if s == nil {
		return
	}
	if s.GpStatInfo != nil {
		s.GpStatInfo.TmID = int(gp.DiscoveredTmID)
	}
	if s.RunningQuery != nil {
		s.RunningQuery.Tmid = int32(gp.DiscoveredTmID)
	}
}

func stampQueryTmID(q *pbm.QueryStatWrite) {
	if q != nil && q.QueryKey != nil {
		q.QueryKey.Tmid = int32(gp.DiscoveredTmID)
	}
}

func stampSegmentTmID(m *pbm.SegmentMetricsWrite) {
	if m != nil && m.QueryKey != nil {
		m.QueryKey.Tmid = int32(gp.DiscoveredTmID)
	}
}
