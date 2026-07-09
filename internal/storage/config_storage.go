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

package storage

import (
	"fmt"
	"sort"
	"sync"
)

type (
	SegmentConfig = struct {
		fqdn string
	}
)

var (
	SegmentConfigLock sync.RWMutex
	SegmentMap        = make(map[SegmentKey]*SegmentConfig, 0)
)

func GetHostnameForSegindex(segindex int32) string {
	SegmentConfigLock.RLock()
	defer SegmentConfigLock.RUnlock()
	segConfig, ok := SegmentMap[SegmentKey{Segindex: segindex}]
	if ok {
		return segConfig.fqdn
	}
	return fmt.Sprintf("%v", segindex)
}

func SetHostnameForSegindex(segindex int32, fqdn string) {
	SegmentConfigLock.Lock()
	defer SegmentConfigLock.Unlock()
	SegmentMap[SegmentKey{Segindex: segindex}] = &SegmentConfig{fqdn: fqdn}
}

func GetConfiguredHostnames() []string {
	hostSegments := GetConfiguredHostSegments()
	hostnames := make([]string, 0, len(hostSegments))
	for hostname := range hostSegments {
		hostnames = append(hostnames, hostname)
	}
	sort.Strings(hostnames)
	return hostnames
}

func GetConfiguredHostSegments() map[string][]int32 {
	SegmentConfigLock.RLock()
	defer SegmentConfigLock.RUnlock()
	hostSegments := make(map[string][]int32, len(SegmentMap))
	seen := make(map[string]map[int32]struct{}, len(SegmentMap))
	for key, segConfig := range SegmentMap {
		if segConfig == nil || segConfig.fqdn == "" {
			continue
		}
		if _, ok := seen[segConfig.fqdn]; !ok {
			seen[segConfig.fqdn] = make(map[int32]struct{})
		}
		if _, ok := seen[segConfig.fqdn][key.Segindex]; ok {
			continue
		}
		seen[segConfig.fqdn][key.Segindex] = struct{}{}
		hostSegments[segConfig.fqdn] = append(hostSegments[segConfig.fqdn], key.Segindex)
	}
	for hostname := range hostSegments {
		sort.Slice(hostSegments[hostname], func(i, j int) bool { return hostSegments[hostname][i] < hostSegments[hostname][j] })
	}
	return hostSegments
}
