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
	"context"
	"fmt"
	"sync"
	"time"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/config"
	"github.com/open-gpdb/yagpcc/internal/metrics"
	"github.com/open-gpdb/yagpcc/internal/utils"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type (
	RefKey struct {
		UserName     string
		DatabaseName string
		StartTime    time.Time
		EndTime      time.Time
	}

	AggKey struct {
		QueryID      uint64
		PlanID       uint64
		UserName     string
		DatabaseName string
		Rsgname      string
		StartTime    time.Time
		EndTime      time.Time
	}

	AggVal struct {
		QueryText         string
		PlanText          string
		TemplateQueryText string
		TemplatePlanText  string
		TotalMetrics      *pbc.GPMetrics
		QueryKey          *pbc.QueryKey
		AggTimes          *pbc.AggregatedMetrics
		QueryLock         sync.RWMutex
	}
	AggMapRef map[RefKey]int

	AggMap map[AggKey]*AggVal
)

type (
	// AggBucketSnapshot is the per-bucket payload handed to AggCycleHook on
	// every drain pass. Field semantics match AggregatedMetrics in the proto:
	// time fields are float64(time.Duration) values (nanoseconds). TotalMetrics
	// is a deep clone of the proto the storage held — owned exclusively by the
	// snapshot so the hook can read it without holding the storage lock even
	// when a concurrent AggQuery is mutating the original.
	AggBucketSnapshot struct {
		Key          AggKey
		Calls        int64
		TotalTimeNs  float64
		MaxTimeNs    float64
		MinTimeNs    float64
		MeanTimeNs   float64
		TotalMetrics *pbc.GPMetrics
	}

	// AggCycleHook is invoked once per ArchiveAggQuery cycle with the snapshot
	// of buckets that were just drained. It runs after the storage lock is
	// released, so the hook may perform slow IO without blocking the
	// aggregator. Errors raised by the hook are the hook's responsibility —
	// the storage neither retries nor undoes the drain.
	AggCycleHook func(ctx context.Context, snapshots []AggBucketSnapshot)

	AggregatedStorage struct {
		mx                *sync.RWMutex
		aggQueries        AggMap
		aggQueriesRef     AggMapRef
		truncInterval     time.Duration
		maxQueriesPerUser int
		log               *zap.SugaredLogger
		cycleHook         AggCycleHook
	}
)

type AOption func(*AggregatedStorage)

func NewConfiguredAggregatedStorage(log *zap.SugaredLogger, cfg *config.Config) *AggregatedStorage {
	return NewAggregatedStorage(log, WithTruncInterval(cfg.ShortAggInterval), WithMaxQueriesPerUSer(int(cfg.MaxShortQueriesPerUser)))
}

func NewAggregatedStorage(log *zap.SugaredLogger, opts ...AOption) *AggregatedStorage {
	const (
		defaultTruncInterval     = 10 * time.Minute
		defaultMaxQueriesPerUser = 500
	)

	a := &AggregatedStorage{
		mx:                &sync.RWMutex{},
		aggQueries:        make(AggMap),
		aggQueriesRef:     make(AggMapRef),
		log:               log,
		truncInterval:     defaultTruncInterval,
		maxQueriesPerUser: defaultMaxQueriesPerUser,
	}

	for _, o := range opts {
		o(a)
	}

	return a
}

func WithTruncInterval(interval time.Duration) AOption {
	return func(s *AggregatedStorage) {
		s.truncInterval = interval
	}
}

func WithMaxQueriesPerUSer(maxQueriesPerUser int) AOption {
	return func(s *AggregatedStorage) {
		s.maxQueriesPerUser = maxQueriesPerUser
	}
}

// WithCycleHook installs a callback that fires after every ArchiveAggQuery
// drain pass with the snapshot of buckets that were just removed from the
// map. Used by the master orchestrator to forward aggregated rows to the
// ClickHouse sink alongside the JSON archiver. nil hooks are silently
// ignored.
func WithCycleHook(hook AggCycleHook) AOption {
	return func(s *AggregatedStorage) {
		s.cycleHook = hook
	}
}

// SetCycleHook is the post-construction setter equivalent of WithCycleHook.
// Used by app.go where the storage and the sink are built independently and
// wired together later. Passing nil disables the hook.
func (a *AggregatedStorage) SetCycleHook(hook AggCycleHook) {
	a.mx.Lock()
	defer a.mx.Unlock()
	a.cycleHook = hook
}

var CurrentTime = time.Now

func (a *AggregatedStorage) GetCurrentInterval() (time.Time, time.Time) {
	currTime := CurrentTime()
	return currTime.Truncate(a.truncInterval), currTime.Truncate(a.truncInterval).Add(a.truncInterval)
}

func (a *AggregatedStorage) ArchiveAggQuery(ctx context.Context, queryChan chan *pbm.QueryStatWrite, clusterID string, hostname string) error {
	for {
		start := time.Now()
		select {
		case <-ctx.Done():
			return fmt.Errorf("done context with %w", ctx.Err())

		default:
			startI, _ := a.GetCurrentInterval()
			var snapshots []AggBucketSnapshot
			var hook AggCycleHook
			a.mx.Lock()
			for key, val := range a.aggQueries {
				if key.EndTime.Before(startI) && startI.Sub(key.EndTime) >= a.truncInterval {
					val.QueryLock.RLock()
					collectTime := start
					if !key.EndTime.IsZero() {
						collectTime = key.EndTime
					}
					stat := &pbm.QueryStatWrite{QueryKey: val.QueryKey,
						ClusterId:   clusterID,
						CollectTime: utils.GetTimeAsString(collectTime),
						Hostname:    hostname,
						StatKind:    pbm.StatKind_SK_AGGREGATED,
						QueryInfo: &pbc.QueryInfo{
							QueryId:           key.QueryID,
							PlanId:            key.PlanID,
							UserName:          key.UserName,
							DatabaseName:      key.DatabaseName,
							QueryText:         val.QueryText,
							PlanText:          val.PlanText,
							TemplateQueryText: val.TemplateQueryText,
							TemplatePlanText:  val.TemplatePlanText,
							Rsgname:           key.Rsgname,
						},
						StartTime:         utils.GetTimeAsString(key.StartTime),
						EndTime:           utils.GetTimeAsString(key.EndTime),
						QueryStatus:       pbc.QueryStatus_QUERY_STATUS_DONE,
						TotalQueryMetrics: val.TotalMetrics,
						AggregatedMetrics: val.AggTimes}
					a.log.Debugf("Send and delete aggregate query %v", key)
					// once again check done context
					select {
					case <-ctx.Done():
						val.QueryLock.RUnlock()
						a.mx.Unlock()
						return fmt.Errorf("done context with %w", ctx.Err())
					default:
						queryChan <- stat
					}
					if a.cycleHook != nil {
						// Deep-clone TotalMetrics under val.QueryLock.RLock so the
						// snapshot owns its own copy. A concurrent AggQuery that
						// already retrieved the same *AggVal from the map (before
						// the surrounding a.mx.Lock blocked it) can later acquire
						// val.QueryLock.Lock and mutate val.TotalMetrics via
						// GroupGPMetrics — racing the hook's reads if we kept the
						// shared pointer.
						var totalCopy *pbc.GPMetrics
						if val.TotalMetrics != nil {
							totalCopy = proto.Clone(val.TotalMetrics).(*pbc.GPMetrics)
						}
						snap := AggBucketSnapshot{
							Key:          key,
							TotalMetrics: totalCopy,
						}
						if val.AggTimes != nil {
							snap.Calls = val.AggTimes.Calls
							snap.TotalTimeNs = val.AggTimes.TotalTime
							snap.MaxTimeNs = val.AggTimes.MaxTime
							snap.MinTimeNs = val.AggTimes.MinTime
							snap.MeanTimeNs = val.AggTimes.MeanTime
						}
						snapshots = append(snapshots, snap)
					}
					val.QueryLock.RUnlock()
					delete(a.aggQueries, key)
					refKey := RefKey{UserName: key.UserName, DatabaseName: key.DatabaseName, StartTime: key.StartTime, EndTime: key.EndTime}
					_, okR := a.aggQueriesRef[refKey]
					if okR {
						delete(a.aggQueriesRef, refKey)
					}
				}
			}
			hook = a.cycleHook
			a.mx.Unlock()
			if hook != nil && len(snapshots) > 0 {
				hook(ctx, snapshots)
			}
		}
		err := utils.Delay(ctx, a.truncInterval-time.Since(start))
		if err != nil {
			return err
		}
	}
}

func (a *AggregatedStorage) AggQuery(qT *pbm.TotalQueryData) error {
	startI, endI := a.GetCurrentInterval()
	if qT.QueryStat == nil {
		return fmt.Errorf("queryStat is nil")
	}
	if qT.QueryStat.QueryInfo == nil {
		return fmt.Errorf("empty QueryInfo for %v", qT.QueryStat.QueryKey)
	}
	sKey := AggKey{
		QueryID:      qT.QueryStat.QueryInfo.QueryId,
		PlanID:       qT.QueryStat.QueryInfo.PlanId,
		UserName:     qT.QueryStat.QueryInfo.UserName,
		DatabaseName: qT.QueryStat.QueryInfo.DatabaseName,
		Rsgname:      qT.QueryStat.QueryInfo.Rsgname,
		StartTime:    startI,
		EndTime:      endI,
	}
	rKey := RefKey{
		UserName:     qT.QueryStat.QueryInfo.UserName,
		DatabaseName: qT.QueryStat.QueryInfo.DatabaseName,
		StartTime:    startI,
		EndTime:      endI,
	}
	a.mx.RLock()
	aVal, okAgg := a.aggQueries[sKey]
	aggR, okR := a.aggQueriesRef[rKey]
	a.mx.RUnlock()

	timeToCheck := startI.Add(-a.truncInterval)
	endTime := utils.GetTimeForTimestamp(qT.QueryStat.EndTime)
	startTime := utils.GetTimeForTimestamp(qT.QueryStat.StartTime)
	if !okAgg && endTime.Before(timeToCheck) {
		// report that we aggregate old data, but do not drop records - go further
		a.log.Infof("interval for %v most probably was deleted and we aggregate in current interval", endTime)
	}
	if !okAgg {
		aValNew := &AggVal{
			QueryText:    qT.QueryStat.QueryInfo.QueryText,
			PlanText:     qT.QueryStat.QueryInfo.PlanText,
			TotalMetrics: &pbc.GPMetrics{},
			AggTimes:     &pbc.AggregatedMetrics{},
			QueryKey:     qT.QueryStat.QueryKey,
		}
		a.mx.Lock()
		// check once again
		aVal, okAgg = a.aggQueries[sKey]
		// if still not inserted - insert
		if !okAgg {
			if !okR {
				aggR = 0
				a.aggQueriesRef[rKey] = aggR
			}
			if aggR > a.maxQueriesPerUser {
				sKey.QueryID = 0
				sKey.PlanID = 0
				aVal, okAgg = a.aggQueries[sKey]
				if !okAgg {
					aVal = &AggVal{
						QueryText:         "Other queries",
						PlanText:          "Other queries",
						TemplateQueryText: "Other queries",
						TemplatePlanText:  "Other queries",
						TotalMetrics:      &pbc.GPMetrics{},
						AggTimes:          &pbc.AggregatedMetrics{},
						QueryKey:          qT.QueryStat.QueryKey,
					}
					a.aggQueries[sKey] = aVal
				}
			} else {
				aVal = aValNew
				a.aggQueries[sKey] = aVal
			}
			a.aggQueriesRef[rKey] += 1
		}
		a.mx.Unlock()
	}
	aVal.QueryLock.Lock()
	defer aVal.QueryLock.Unlock()
	intermediateResults := make(map[MapAggregateKey]uint64, 0)
	err := GroupGPMetrics(aVal.TotalMetrics, qT.QueryStat.TotalQueryMetrics, AggMax, "hostname", intermediateResults)
	if err != nil {
		return err
	}
	err = GroupAggMetrics(aVal.AggTimes, endTime.Sub(startTime))
	if err != nil {
		return err
	}
	if aVal.AggTimes.Calls > 1 && aVal.QueryKey != nil && qT.QueryStat.QueryKey != nil {
		if aVal.QueryKey.Ssid != qT.QueryStat.QueryKey.Ssid || aVal.QueryKey.Ccnt != qT.QueryStat.QueryKey.Ccnt {
			aVal.QueryKey = &pbc.QueryKey{}
		}
	}
	if metrics.YagpccMetrics != nil {
		metrics.YagpccMetrics.NewAggregatedQueries.Inc()
		a.mx.RLock()
		defer a.mx.RUnlock()
		metrics.YagpccMetrics.AggregatedQueries.Set(float64(len(a.aggQueries)))
	}
	return nil
}

func (a *AggregatedStorage) All() AggMap {
	return a.aggQueries
}

func (a *AggregatedStorage) GetTruncInterval() time.Duration {
	return a.truncInterval
}
