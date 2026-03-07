package storage

import (
	"fmt"
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
