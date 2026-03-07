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
		AggTimes          *pbm.AggregatedMetrics
		QueryLock         sync.RWMutex
	}
	AggMapRef map[RefKey]int

	AggMap map[AggKey]*AggVal
)

type (
	AggregatedStorage struct {
		mx                *sync.RWMutex
		aggQueries        AggMap
		aggQueriesRef     AggMapRef
		truncInterval     time.Duration
		maxQueriesPerUser int
		log               *zap.SugaredLogger
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

var CurrentTime = func() time.Time {
	return time.Now()
}

func (a *AggregatedStorage) GetCurrentInterval() (time.Time, time.Time) {
	currTime := CurrentTime()
	return currTime.Truncate(a.truncInterval), currTime.Truncate(a.truncInterval).Add(a.truncInterval)
}

func (a *AggregatedStorage) ArchiveAggQuery(ctx context.Context, queryChan chan *pbm.QueryStatWrite, clusterID string, hostname string) error {
	for {
		start := time.Now()
		select {
		case <-ctx.Done():
			return fmt.Errorf("done context with %v", ctx.Err())

		default:
			startI, _ := a.GetCurrentInterval()
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
						return fmt.Errorf("done context with %v", ctx.Err())
					default:
						queryChan <- stat
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
			a.mx.Unlock()
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
			QueryText:         qT.QueryStat.QueryInfo.QueryText,
			PlanText:          qT.QueryStat.QueryInfo.PlanText,
			TemplateQueryText: qT.QueryStat.QueryInfo.TemplateQueryText,
			TemplatePlanText:  qT.QueryStat.QueryInfo.TemplatePlanText,
			TotalMetrics:      &pbc.GPMetrics{},
			AggTimes:          &pbm.AggregatedMetrics{},
			QueryKey:          qT.QueryStat.QueryKey,
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
						AggTimes:          &pbm.AggregatedMetrics{},
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
	if aVal.AggTimes.Calls > 1 {
		aVal.QueryKey = &pbc.QueryKey{}
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
