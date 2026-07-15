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
	"io"
	"time"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	"github.com/open-gpdb/yagpcc/internal/gp"
	"go.uber.org/zap"
)

// BatchSize is the number of items to collect before flushing a batch.
const BatchSize = 1000

// batchTimeout is the maximum time to wait for a batch before flushing.
const batchTimeout = time.Second * 1 / 2

// SessionChanRead is a read-only channel for session data.
type SessionChanRead <-chan *gp.SessionDataWrite

// QueryStatChanRead is a read-only channel for query statistics.
type QueryStatChanRead <-chan *pbm.QueryStatWrite

// SegmentStatChanRead is a read-only channel for segment metrics.
type SegmentStatChanRead <-chan *pbm.SegmentMetricsWrite

// SessionChanWrite is a write-only channel for session data.
type SessionChanWrite chan<- *gp.SessionDataWrite

// QueryStatChanWrite is a write-only channel for query statistics.
type QueryStatChanWrite chan<- *pbm.QueryStatWrite

// SegmentStatChanWrite is a write-only channel for segment metrics.
type SegmentStatChanWrite chan<- *pbm.SegmentMetricsWrite

func StoreSerializableData(ctx context.Context, l *zap.SugaredLogger, sChan SerializableChan, writer io.Writer) {
	cachedItems := 0
	tick := time.NewTicker(batchTimeout)
	writeJS := make([]byte, 0)
	for {
		select {
		case val := <-sChan:
			myJS, err := val.ToJSON()
			if err != nil {
				l.Errorf("fail to convert sessions data %v with error %v", val, err)
				break
			}
			writeJS = append(writeJS, myJS...)
			writeJS = append(writeJS, '\n')
			cachedItems++
		case <-tick.C:
			cachedItems = BatchSize
		case <-ctx.Done():
			// here, we lost all the data, but that's expected behavior. We don't wait and exit immediately.
			return
		}
		if cachedItems >= BatchSize {
			tick.Stop()
			cnt, err := writer.Write(writeJS)
			if err != nil {
				l.Errorf("fail to write sessions data, error %v, %v bytes written", err, cnt)
			}
			cachedItems = 0
			writeJS = make([]byte, 0)
			tick.Reset(batchTimeout)
		}
	}
}

func StoreSessions(ctx context.Context, l *zap.SugaredLogger, sessionChan SessionChanRead, writer io.Writer) {
	serChan := make(SerializableChan, cap(sessionChan))
	defer close(serChan)
	go StoreSerializableData(ctx, l, serChan, writer)
	for {
		select {
		case val := <-sessionChan:
			val.GpStatInfo.TmID = int(gp.DiscoveredTmID)
			val.RunningQuery.Tmid = int32(gp.DiscoveredTmID)
			serChan <- val
		case <-ctx.Done():
			l.Warn("done StoreSessions")
			return
		}
	}
}

func StoreQuery(ctx context.Context, l *zap.SugaredLogger, queryChan QueryStatChanRead, writer io.Writer) {
	serChan := make(SerializableChan, cap(queryChan))
	defer close(serChan)
	go StoreSerializableData(ctx, l, serChan, writer)
	for {
		select {
		case val := <-queryChan:
			val.QueryKey.Tmid = int32(gp.DiscoveredTmID)
			serChan <- &QueryStatWriteSerializable{v: val}
		case <-ctx.Done():
			l.Warn("done StoreQuery")
			return
		}
	}
}

func StoreSegmensMetrics(ctx context.Context, l *zap.SugaredLogger, segChan SegmentStatChanRead, writer io.Writer) {
	serChan := make(SerializableChan, cap(segChan))
	defer close(serChan)
	go StoreSerializableData(ctx, l, serChan, writer)
	for {
		select {
		case val := <-segChan:
			val.QueryKey.Tmid = int32(gp.DiscoveredTmID)
			serChan <- &SegmentMetricsWriteSerializable{v: val}
		case <-ctx.Done():
			l.Warn("done StoreSegmensMetrics")
			return
		}
	}
}
