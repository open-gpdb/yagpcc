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

package gp

import (
	"sync"
	"testing"
	"time"

	"github.com/open-gpdb/yagpcc/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func clearSegmentMap() {
	storage.SegmentConfigLock.Lock()
	defer storage.SegmentConfigLock.Unlock()
	for k := range storage.SegmentMap {
		delete(storage.SegmentMap, k)
	}
}

// TestPopulateHostnameMapOnlyPrimary verifies that only segments with Role=="p"
// (primary) are stored in the hostname map, and mirrors (Role=="m") are ignored.
func TestPopulateHostnameMapOnlyPrimary(t *testing.T) {
	clearSegmentMap()

	// Simulate gp_segment_configuration with primary and mirror for each content
	segConfig := GpSegmentsConfiguration{
		{DBID: 10, Content: -1, Role: "p", PreferredRole: "p", Hostname: "master-primary.db.yandex.net"},
		{DBID: 11, Content: -1, Role: "m", PreferredRole: "m", Hostname: "master-mirror.db.yandex.net"},
		{DBID: 4, Content: 0, Role: "p", PreferredRole: "p", Hostname: "seg0-primary.db.yandex.net"},
		{DBID: 2, Content: 0, Role: "m", PreferredRole: "m", Hostname: "seg0-mirror.db.yandex.net"},
		{DBID: 5, Content: 1, Role: "p", PreferredRole: "p", Hostname: "seg1-primary.db.yandex.net"},
		{DBID: 3, Content: 1, Role: "m", PreferredRole: "m", Hostname: "seg1-mirror.db.yandex.net"},
	}

	populateHostnameMap(segConfig)

	// Primary hostnames should be stored
	assert.Equal(t, "master-primary.db.yandex.net", storage.GetHostnameForSegindex(-1))
	assert.Equal(t, "seg0-primary.db.yandex.net", storage.GetHostnameForSegindex(0))
	assert.Equal(t, "seg1-primary.db.yandex.net", storage.GetHostnameForSegindex(1))

	// Unknown segindex should return the segindex as string
	assert.Equal(t, "99", storage.GetHostnameForSegindex(99))
}

// TestPopulateHostnameMapMirrorDoesNotOverwritePrimary verifies that when mirror
// rows appear after primary rows, the primary hostname is preserved.
func TestPopulateHostnameMapMirrorDoesNotOverwritePrimary(t *testing.T) {
	clearSegmentMap()

	// Primary first, then mirror — mirror must NOT overwrite
	segConfig := GpSegmentsConfiguration{
		{DBID: 4, Content: 0, Role: "p", PreferredRole: "p", Hostname: "primary-host.db.yandex.net"},
		{DBID: 2, Content: 0, Role: "m", PreferredRole: "m", Hostname: "mirror-host.db.yandex.net"},
	}

	populateHostnameMap(segConfig)

	assert.Equal(t, "primary-host.db.yandex.net", storage.GetHostnameForSegindex(0))
}

// TestPopulateHostnameMapMirrorBeforePrimary verifies that when mirror rows
// appear before primary rows, the primary hostname still wins.
func TestPopulateHostnameMapMirrorBeforePrimary(t *testing.T) {
	clearSegmentMap()

	// Mirror first, then primary — primary must still win
	segConfig := GpSegmentsConfiguration{
		{DBID: 2, Content: 0, Role: "m", PreferredRole: "m", Hostname: "mirror-host.db.yandex.net"},
		{DBID: 4, Content: 0, Role: "p", PreferredRole: "p", Hostname: "primary-host.db.yandex.net"},
	}

	populateHostnameMap(segConfig)

	assert.Equal(t, "primary-host.db.yandex.net", storage.GetHostnameForSegindex(0))
}

// TestPopulateHostnameMapEmptyConfig verifies that an empty config does not
// cause errors and does not populate the map.
func TestPopulateHostnameMapEmptyConfig(t *testing.T) {
	clearSegmentMap()

	segConfig := GpSegmentsConfiguration{}

	populateHostnameMap(segConfig)

	// No entries — should return segindex as string
	assert.Equal(t, "0", storage.GetHostnameForSegindex(0))
	assert.Equal(t, "-1", storage.GetHostnameForSegindex(-1))
}

// TestPopulateHostnameMapOnlyMirrors verifies that if only mirror segments
// are present, no hostnames are stored.
func TestPopulateHostnameMapOnlyMirrors(t *testing.T) {
	clearSegmentMap()

	segConfig := GpSegmentsConfiguration{
		{DBID: 2, Content: 0, Role: "m", PreferredRole: "m", Hostname: "mirror-host.db.yandex.net"},
		{DBID: 3, Content: 1, Role: "m", PreferredRole: "m", Hostname: "mirror-host2.db.yandex.net"},
	}

	populateHostnameMap(segConfig)

	assert.Equal(t, "0", storage.GetHostnameForSegindex(0))
	assert.Equal(t, "1", storage.GetHostnameForSegindex(1))
}

// TestPopulateHostnameMapUpdatesOnRefresh verifies that calling PopulateHostnameMap
// again with new data updates the hostnames (simulating config refresh).
func TestPopulateHostnameMapUpdatesOnRefresh(t *testing.T) {
	clearSegmentMap()

	// Initial config
	segConfig := GpSegmentsConfiguration{
		{DBID: 4, Content: 0, Role: "p", PreferredRole: "p", Hostname: "old-primary.db.yandex.net"},
		{DBID: 2, Content: 0, Role: "m", PreferredRole: "m", Hostname: "old-mirror.db.yandex.net"},
	}
	populateHostnameMap(segConfig)
	assert.Equal(t, "old-primary.db.yandex.net", storage.GetHostnameForSegindex(0))

	// Refreshed config — primary hostname changed (e.g., failover happened)
	segConfig2 := GpSegmentsConfiguration{
		{DBID: 4, Content: 0, Role: "p", PreferredRole: "p", Hostname: "new-primary.db.yandex.net"},
		{DBID: 2, Content: 0, Role: "m", PreferredRole: "m", Hostname: "new-mirror.db.yandex.net"},
	}
	populateHostnameMap(segConfig2)
	assert.Equal(t, "new-primary.db.yandex.net", storage.GetHostnameForSegindex(0))
}

// testCacheKey is a dedicated key for cache accessor tests, so they never
// interfere with the real cache keys used by other tests.
const testCacheKey = "TestCachedItemsAccessors"

func newTestCacheItem(version string) *CacheItem {
	return &CacheItem{
		ItemValue:   VersionConfiguration{Version: version},
		Status:      CacheOk,
		RefreshDate: time.Now(),
	}
}

func TestSetAndGetCachedItem(t *testing.T) {
	t.Cleanup(func() { DeleteCachedItem(testCacheKey) })

	item := newTestCacheItem("1.0")
	SetCachedItem(testCacheKey, item)

	got, ok := GetCachedItem(testCacheKey)
	require.True(t, ok)
	assert.Equal(t, item, got)
	assert.Equal(t, "1.0", got.ItemValue.(VersionConfiguration).Version)
}

func TestGetCachedItemMissingKey(t *testing.T) {
	got, ok := GetCachedItem("TestCachedItemsAccessorsMissing")

	assert.False(t, ok)
	assert.Nil(t, got)
}

func TestSetCachedItemOverwrites(t *testing.T) {
	t.Cleanup(func() { DeleteCachedItem(testCacheKey) })

	SetCachedItem(testCacheKey, newTestCacheItem("1.0"))
	SetCachedItem(testCacheKey, newTestCacheItem("2.0"))

	got, ok := GetCachedItem(testCacheKey)
	require.True(t, ok)
	assert.Equal(t, "2.0", got.ItemValue.(VersionConfiguration).Version)
}

func TestDeleteCachedItem(t *testing.T) {
	SetCachedItem(testCacheKey, newTestCacheItem("1.0"))

	DeleteCachedItem(testCacheKey)

	_, ok := GetCachedItem(testCacheKey)
	assert.False(t, ok)
}

func TestDeleteCachedItemMissingKey(t *testing.T) {
	// Deleting a key that does not exist must not panic.
	DeleteCachedItem("TestCachedItemsAccessorsMissing")
}

// TestCachedItemsAccessorsConcurrent hammers all three accessors from
// concurrent goroutines; run with -race to verify the internal locking.
func TestCachedItemsAccessorsConcurrent(t *testing.T) {
	t.Cleanup(func() { DeleteCachedItem(testCacheKey) })

	const (
		workers    = 8
		iterations = 200
	)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				SetCachedItem(testCacheKey, newTestCacheItem("1.0"))
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_, _ = GetCachedItem(testCacheKey)
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				DeleteCachedItem(testCacheKey)
			}
		}()
	}
	wg.Wait()
}
