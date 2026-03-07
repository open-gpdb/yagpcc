package gp

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/open-gpdb/yagpcc/internal/config"
	"github.com/open-gpdb/yagpcc/internal/storage"
	"go.uber.org/zap"
)

type CacheStatus int

const (
	CacheOk CacheStatus = iota
	CacheError
)

type (
	GpStatActivity struct {
		DatID            int
		Datname          string
		Pid              int
		SessID           int
		TmID             int
		UsesysID         int
		Usename          string
		ApplicationName  *string    `json:",omitempty"`
		ClientAddr       *string    `json:",omitempty"`
		ClientHostname   *string    `json:",omitempty"`
		ClientPort       *int       `json:",omitempty"`
		BackendStart     *time.Time `json:",omitempty"`
		XactStart        *time.Time `json:",omitempty"`
		QueryStart       *time.Time `json:",omitempty"`
		StateChange      *time.Time `json:",omitempty"`
		Waiting          *bool      `json:",omitempty"`
		State            *string    `json:",omitempty"`
		BackendXid       *string    `json:",omitempty"`
		BackendXmin      *string    `json:",omitempty"`
		Query            *string    `json:",omitempty"`
		WaitingReason    *string    `json:",omitempty"`
		Rsgid            *int       `json:",omitempty"`
		Rsgname          *string    `json:",omitempty"`
		Rsgqueueduration *string    `json:",omitempty"`
		BlockedBySessID  *int       `json:",omitempty"`
		WaitMode         *string    `json:",omitempty"`
		LockedItem       *string    `json:",omitempty"`
		LockedMode       *string    `json:",omitempty"`
		WaitEvent        *string    `json:",omitempty"`
		WaitEventType    *string    `json:",omitempty"`
	}
	GpSegmentConfiguration struct {
		DBID          int
		Content       int
		Role          string
		PreferredRole string
		Mode          string
		Status        string
		Port          int
		Hostname      string
		Address       string
		Datadir       string
	}
	GpSegmentsConfiguration []*GpSegmentConfiguration
	VersionConfiguration    struct {
		Version string
	}
	CacheItem struct {
		ItemValue   interface{}
		Status      CacheStatus
		RefreshDate time.Time
	}
	Cache map[string]*CacheItem
)

const (
	SegmentConfig = "SegmentConfig"
	VersionConfig = "VersionConfig"
)

const (
	SegmentQ = "SELECT dbid, content, role, preferred_role AS PreferredRole, mode, status, port, hostname, address, datadir FROM gp_segment_configuration"
	VersionQ = "select version();"
)

var (
	db          *Connection
	dbMutex     sync.Mutex
	CachedItems Cache = make(Cache, 0)
)

func checkCacheItem(cacheItem *CacheItem, durability time.Duration) bool {
	if cacheItem == nil {
		return false
	}
	if cacheItem.ItemValue == nil {
		return false
	}
	if cacheItem.Status != CacheOk {
		return false
	}

	if cacheItem.RefreshDate.Add(durability).Before(time.Now()) {
		return false
	}
	return true
}

func Init(ctx context.Context, log *zap.SugaredLogger, config *config.PGConfig, maxRetries int) error {
	const maxInitRetryDelay = 6
	var err error
	tries := 0
	for {
		db, err = GetAliveConnection(ctx, log, config)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return err
		}
		if tries >= maxRetries {
			return err
		}
		tries += 1
		if tries <= maxInitRetryDelay {
			time.Sleep(time.Duration(float64(time.Second) * math.Pow(2.0, float64(tries))))
			continue
		}
		time.Sleep(time.Duration(float64(time.Second) * math.Pow(2.0, float64(maxInitRetryDelay))))
	}
}

func GetSegmentConfig(ctx context.Context, durability time.Duration) (GpSegmentsConfiguration, error) {
	cachedItem, ok := CachedItems[SegmentConfig]
	if ok && checkCacheItem(cachedItem, durability) {
		return cachedItem.ItemValue.(GpSegmentsConfiguration), nil
	}
	segmentConfig := make(GpSegmentsConfiguration, 0)
	if db == nil {
		return segmentConfig, fmt.Errorf("internal - DB not initialized")
	}
	err := db.ExecQuery(ctx, SegmentQ, &segmentConfig)
	if err != nil {
		return nil, err
	}
	CachedItems[SegmentConfig] = &CacheItem{
		ItemValue:   segmentConfig,
		Status:      CacheOk,
		RefreshDate: time.Now(),
	}
	for _, segmentNode := range segmentConfig {
		storage.SetHostnameForSegindex(int32(segmentNode.Content), segmentNode.Hostname)
	}
	return segmentConfig, nil
}

func GetVersion(ctx context.Context) (VersionConfiguration, error) {
	cachedItem, ok := CachedItems[VersionConfig]
	if ok {
		return cachedItem.ItemValue.(VersionConfiguration), nil
	}
	if db == nil {
		return VersionConfiguration{}, fmt.Errorf("internal - DB not initialized")
	}
	versionConfig := make([]VersionConfiguration, 0)
	err := db.ExecQuery(ctx, VersionQ, &versionConfig)
	if err != nil {
		return VersionConfiguration{}, err
	}
	if len(versionConfig) == 0 {
		return VersionConfiguration{}, fmt.Errorf("internal - empty version query result")
	}
	CachedItems[VersionConfig] = &CacheItem{
		ItemValue:   versionConfig[0],
		Status:      CacheOk,
		RefreshDate: time.Now(),
	}
	return versionConfig[0], nil
}
