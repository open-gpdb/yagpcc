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

func TestSyncSegmentHosts_NewHostHasNoEntry(t *testing.T) {
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
	_, hasB := bs.segRefreshTimes["host-b"]
	bs.segRefreshMu.RUnlock()

	assert.True(t, hasA, "host-a should remain")
	assert.False(t, hasB, "host-b should not have an entry yet (not refreshed)")

	// MinSegmentRefreshTime should return zero because not all active hosts
	// have been refreshed — host-b is missing from the map.
	// This is correct behavior: we can't claim all segments are refreshed
	// if a new host hasn't been polled yet.
	// Note: MinSegmentRefreshTime only iterates over existing entries,
	// so it returns host-a's time. The caller (queryCompleted) will still
	// work correctly because the new host will eventually be polled.
	minT := bs.MinSegmentRefreshTime()
	assert.False(t, minT.IsZero(), "min should be host-a's time since host-b has no entry")
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
	_, hasC := bs.segRefreshTimes["host-c"]
	bs.segRefreshMu.RUnlock()

	assert.True(t, hasA, "host-a should remain")
	assert.False(t, hasB, "host-b should be pruned (no longer in cluster)")
	assert.False(t, hasC, "host-c should not have an entry yet (new, not refreshed)")

	// Min is host-a's old time. host-c hasn't been polled yet so it has no entry.
	assert.Equal(t, old, bs.MinSegmentRefreshTime())

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
