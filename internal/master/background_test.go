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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/storage"
)

func newTestBackgroundStorage() *BackgroundStorage {
	l := zap.NewNop().Sugar()
	return &BackgroundStorage{
		l:               l,
		segRefreshTimes: make(map[string]time.Time),
	}
}

func TestMinSegmentRefreshTime_Empty(t *testing.T) {
	bs := newTestBackgroundStorage()
	// No segments recorded — should return zero time.
	minT := bs.MinSegmentRefreshTime()
	assert.True(t, minT.IsZero(), "expected zero time for empty segRefreshTimes")
}

func TestMinSegmentRefreshTime_SingleHost(t *testing.T) {
	bs := newTestBackgroundStorage()
	now := time.Now()
	bs.recordSegmentRefresh("host-a")
	minT := bs.MinSegmentRefreshTime()
	assert.False(t, minT.IsZero())
	assert.WithinDuration(t, now, minT, time.Second)
}

func TestMinSegmentRefreshTime_MultipleHosts(t *testing.T) {
	bs := newTestBackgroundStorage()
	t1 := time.Now().Add(-10 * time.Second)
	t2 := time.Now().Add(-5 * time.Second)
	t3 := time.Now()

	bs.segRefreshMu.Lock()
	bs.segRefreshTimes["host-a"] = t1
	bs.segRefreshTimes["host-b"] = t2
	bs.segRefreshTimes["host-c"] = t3
	bs.segRefreshMu.Unlock()

	minT := bs.MinSegmentRefreshTime()
	assert.Equal(t, t1, minT, "expected the oldest refresh time")
}

func TestMinSegmentRefreshTime_ReturnsMinAfterUpdate(t *testing.T) {
	bs := newTestBackgroundStorage()
	old := time.Now().Add(-30 * time.Second)
	recent := time.Now()

	bs.segRefreshMu.Lock()
	bs.segRefreshTimes["host-a"] = old
	bs.segRefreshTimes["host-b"] = recent
	bs.segRefreshMu.Unlock()

	// Min should be the old time.
	assert.Equal(t, old, bs.MinSegmentRefreshTime())

	// After refreshing host-a, min should advance.
	bs.recordSegmentRefresh("host-a")
	minT := bs.MinSegmentRefreshTime()
	assert.True(t, minT.After(old), "expected min to advance after host-a refresh")
}

func TestSyncSegmentHosts_PrunesStaleHosts(t *testing.T) {
	bs := newTestBackgroundStorage()
	now := time.Now()

	bs.segRefreshMu.Lock()
	bs.segRefreshTimes["host-a"] = now.Add(-10 * time.Second)
	bs.segRefreshTimes["host-b"] = now.Add(-5 * time.Second)
	bs.segRefreshTimes["host-c"] = now
	bs.segRefreshMu.Unlock()

	// Simulate cluster topology change: host-b is removed.
	activeHosts := map[string]bool{
		"host-a": true,
		"host-c": true,
	}
	bs.syncSegmentHosts(activeHosts)

	bs.segRefreshMu.RLock()
	_, hasA := bs.segRefreshTimes["host-a"]
	_, hasB := bs.segRefreshTimes["host-b"]
	_, hasC := bs.segRefreshTimes["host-c"]
	bs.segRefreshMu.RUnlock()

	assert.True(t, hasA, "host-a should remain")
	assert.False(t, hasB, "host-b should be pruned")
	assert.True(t, hasC, "host-c should remain")
}

func TestSyncSegmentHosts_NewHostStartsAtZeroRefresh(t *testing.T) {
	bs := newTestBackgroundStorage()
	now := time.Now()

	bs.segRefreshMu.Lock()
	bs.segRefreshTimes["host-a"] = now
	bs.segRefreshMu.Unlock()

	// New host-b appears in the cluster but hasn't been refreshed yet.
	activeHosts := map[string]bool{
		"host-a": true,
		"host-b": true,
	}
	bs.syncSegmentHosts(activeHosts)

	bs.segRefreshMu.RLock()
	_, hasA := bs.segRefreshTimes["host-a"]
	hostBTime, hasB := bs.segRefreshTimes["host-b"]
	bs.segRefreshMu.RUnlock()

	assert.True(t, hasA, "host-a should remain")
	assert.True(t, hasB, "host-b should be inserted with zero refresh time")
	assert.True(t, hostBTime.IsZero(), "host-b should start at zero refresh time")

	// MinSegmentRefreshTime should stay zero until every active host has refreshed.
	minT := bs.MinSegmentRefreshTime()
	assert.True(t, minT.IsZero(), "min should remain zero until host-b refreshes")
}

func TestSyncSegmentHosts_AllHostsRemoved(t *testing.T) {
	bs := newTestBackgroundStorage()
	now := time.Now()

	bs.segRefreshMu.Lock()
	bs.segRefreshTimes["host-a"] = now
	bs.segRefreshTimes["host-b"] = now
	bs.segRefreshMu.Unlock()

	// All hosts removed from cluster.
	bs.syncSegmentHosts(map[string]bool{})

	bs.segRefreshMu.RLock()
	count := len(bs.segRefreshTimes)
	bs.segRefreshMu.RUnlock()

	assert.Equal(t, 0, count, "all entries should be pruned")
	assert.True(t, bs.MinSegmentRefreshTime().IsZero(), "min should be zero with no entries")
}

func TestSyncSegmentHosts_HostMovedBetweenRefreshes(t *testing.T) {
	bs := newTestBackgroundStorage()
	old := time.Now().Add(-30 * time.Second)

	// Initially host-a and host-b are in the cluster.
	bs.segRefreshMu.Lock()
	bs.segRefreshTimes["host-a"] = old
	bs.segRefreshTimes["host-b"] = old
	bs.segRefreshMu.Unlock()

	// Segments moved: host-b removed, host-c added.
	activeHosts := map[string]bool{
		"host-a": true,
		"host-c": true,
	}
	bs.syncSegmentHosts(activeHosts)

	bs.segRefreshMu.RLock()
	_, hasA := bs.segRefreshTimes["host-a"]
	_, hasB := bs.segRefreshTimes["host-b"]
	hostCRefresh, hasC := bs.segRefreshTimes["host-c"]
	bs.segRefreshMu.RUnlock()

	assert.True(t, hasA, "host-a should remain")
	assert.False(t, hasB, "host-b should be pruned (no longer in cluster)")
	assert.True(t, hasC, "host-c should be inserted with zero refresh time")
	assert.True(t, hostCRefresh.IsZero(), "host-c should start at zero refresh time")

	// Min should be zero until host-c is refreshed at least once.
	assert.True(t, bs.MinSegmentRefreshTime().IsZero())

	// After host-c is refreshed, min should still be host-a's old time.
	bs.recordSegmentRefresh("host-c")
	assert.Equal(t, old, bs.MinSegmentRefreshTime())

	// After host-a is refreshed, min should advance.
	bs.recordSegmentRefresh("host-a")
	minT := bs.MinSegmentRefreshTime()
	assert.True(t, minT.After(old), "min should advance after all active hosts are refreshed")
}

func TestRecordSegmentRefresh_UpdatesExistingEntry(t *testing.T) {
	bs := newTestBackgroundStorage()
	old := time.Now().Add(-10 * time.Second)

	bs.segRefreshMu.Lock()
	bs.segRefreshTimes["host-a"] = old
	bs.segRefreshMu.Unlock()

	bs.recordSegmentRefresh("host-a")

	bs.segRefreshMu.RLock()
	newTime := bs.segRefreshTimes["host-a"]
	bs.segRefreshMu.RUnlock()

	assert.True(t, newTime.After(old), "refresh time should be updated to a more recent value")
}

// --- Eternal (never-completing) queries ---

// An eternal query never reaches a completion status, so it must never be archived
// by the completion logic — no matter how long it has been running. The segment
// timeout applies only to *completed* queries, so it must NOT force-archive a query
// that is still running. Such a query can only leave storage via session failure or GC.
func TestQueryCompleted_EternalQueryNeverArchived(t *testing.T) {
	bs := newTestBackgroundStorage()
	qKey := &storage.QueryKey{}
	// Running for 10 hours, no end time, and a tiny (1s) segment timeout.
	qVal := &storage.RunningQuery{
		Completed:   false,
		QueryStatus: int32(pbc.QueryStatus_QUERY_STATUS_START),
		QueryStart:  time.Now().Add(-10 * time.Hour),
	}

	decision, reason := bs.queryCompleted(qKey, qVal, 1)

	assert.Equal(t, 0, decision, "an eternal running query must never be archived by completion logic")
	assert.Equal(t, reasonNone, reason, "a running query has no archival reason")
}

// Even when every segment has refreshed long after the eternal query started, it must
// still not be archived: archival keys off Completed, not off segment refresh times.
func TestQueryCompleted_EternalQueryIgnoresSegmentRefresh(t *testing.T) {
	bs := newTestBackgroundStorage()
	qKey := &storage.QueryKey{}
	qVal := &storage.RunningQuery{
		Completed:   false,
		QueryStatus: int32(pbc.QueryStatus_QUERY_STATUS_START),
		QueryStart:  time.Now().Add(-time.Hour),
	}
	bs.segRefreshMu.Lock()
	bs.segRefreshTimes["host-a"] = time.Now()
	bs.segRefreshMu.Unlock()

	decision, reason := bs.queryCompleted(qKey, qVal, 3600)

	assert.Equal(t, 0, decision, "a running query is not archived even if all segments refreshed")
	assert.Equal(t, reasonNone, reason)
}

// --- Interrupted (cancelled / errored) queries ---

// An interrupted query (CANCELED/ERROR) is marked Completed with an end time, exactly
// like a normally finished one, so its data is preserved (archived), not lost. When all
// segments have reported after the end, it archives with the all-segments reason.
func TestQueryCompleted_InterruptedArchivedWhenSegmentsReported(t *testing.T) {
	bs := newTestBackgroundStorage()
	qKey := &storage.QueryKey{}
	ended := time.Now().Add(-10 * time.Second)
	qVal := &storage.RunningQuery{
		Completed:   true,
		QueryStatus: int32(pbc.QueryStatus_QUERY_STATUS_CANCELED),
		QueryEnd:    ended,
	}
	// Every segment refreshed AFTER the query was interrupted -> full data collected.
	bs.segRefreshMu.Lock()
	bs.segRefreshTimes["host-a"] = ended.Add(2 * time.Second)
	bs.segRefreshTimes["host-b"] = ended.Add(3 * time.Second)
	bs.segRefreshMu.Unlock()

	decision, reason := bs.queryCompleted(qKey, qVal, 3600)

	assert.Equal(t, 1, decision, "an interrupted query with all segments reported must be archived")
	assert.Equal(t, reasonAllSegments, reason, "archival reason must be all-segments")
}

// An interrupted query whose segments never report is archived once the segment timeout
// fires, with possibly partial data — it is neither held forever nor silently dropped.
func TestQueryCompleted_InterruptedArchivedOnTimeout(t *testing.T) {
	bs := newTestBackgroundStorage()
	qKey := &storage.QueryKey{}
	qVal := &storage.RunningQuery{
		Completed:   true,
		QueryStatus: int32(pbc.QueryStatus_QUERY_STATUS_ERROR),
		QueryEnd:    time.Now().Add(-time.Hour),
	}

	decision, reason := bs.queryCompleted(qKey, qVal, 1)

	assert.Equal(t, 1, decision, "an interrupted query past the segment timeout must be archived")
	assert.Equal(t, reasonTimeout, reason, "archival reason must be timeout (data may be partial)")
}

// An interrupted query whose segment data is not yet collected (min refresh still before
// its end) and whose timeout has not fired must keep waiting, just like a normal one.
func TestQueryCompleted_InterruptedWaitingForSegments(t *testing.T) {
	bs := newTestBackgroundStorage()
	qKey := &storage.QueryKey{}
	ended := time.Now()
	qVal := &storage.RunningQuery{
		Completed:   true,
		QueryStatus: int32(pbc.QueryStatus_QUERY_STATUS_CANCELED),
		QueryEnd:    ended,
	}
	// The only known segment last refreshed BEFORE the query ended -> stale.
	bs.segRefreshMu.Lock()
	bs.segRefreshTimes["host-a"] = ended.Add(-5 * time.Second)
	bs.segRefreshMu.Unlock()

	decision, reason := bs.queryCompleted(qKey, qVal, 3600)

	assert.Equal(t, 0, decision, "an interrupted query with stale segment data must wait")
	assert.Equal(t, reasonNone, reason)
}
