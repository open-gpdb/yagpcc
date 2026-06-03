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
	"maps"
	"sort"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/config"
	"github.com/open-gpdb/yagpcc/internal/metrics"
	"github.com/open-gpdb/yagpcc/internal/utils"
)

type (
	QueryKey struct {
		Ssid int32
		Ccnt int32
	}

	QueryKeyWrite struct {
		Tmid int32
		Ssid int32
		Ccnt int32
	}

	SegmentKey struct {
		Dbid     int32
		Segindex int32
	}
	NodeKey struct {
		QKey    QueryKey
		SKey    SegmentKey
		SliceID int64
	}
	SlicesMap map[int64]bool
	QueryData struct {
		CurrentStatus int32
		QueryInfo     *pbc.QueryInfo
		QueryMetrics  *pbc.GPMetrics
		QueryMessage  string
		// internal fields
		QueryDataLock sync.RWMutex
	}
	QueryMap          map[NodeKey]*QueryData
	QueryIndexNodes   map[NodeKey]bool
	SegmentIndexNodes map[uint32]interface{}
	RunningQuery      struct {
		QueriesData     QueryMap
		QueryStatus     int32
		Completed       bool
		MarkSessionSent bool
		QueryStart      time.Time
		QuerySubmit     time.Time
		QueryEnd        time.Time
		QueryMessage    string // Query status, in most cases we store here Error Message
		// NestedLevel from AdditionalQueryInfo on the coordinator/master slice (-1 if unknown).
		NestedLevel  int64
		SegmentNodes SegmentIndexNodes
		QueryLock    sync.RWMutex
	}
	RunningQueryType map[QueryKey]*RunningQuery

	RunningQueryInfo struct {
		TotalQueryMetrics *pbc.GPMetrics
		TotalQueryInfo    *pbc.QueryInfo
		QueryStatus       int32
		QueryMessage      string
		QueryKey          *QueryKey
		QueryStart        time.Time
		QuerySubmit       time.Time
		QueryEnd          time.Time
		Slices            int64
	}

	StorageStat struct {
		ResetTime       time.Time
		QueriesCount    uint64
		PlanNodesCount  uint64
		TextSize        uint64
		QueriesWipedOut uint64
		PlansWipedOut   uint64
		NumGC           uint64
	}

	MeasuredQueryTimes struct {
		QueryStart  *timestamppb.Timestamp
		QuerySubmit *timestamppb.Timestamp
		QueryEnd    *timestamppb.Timestamp
	}

	SliceAggregator struct {
	}

	// GCQuery represents a completed query evicted by garbage collection
	// that should be sent to the archive channel for processing.
	GCQuery struct {
		QKey *QueryKey
		QVal *RunningQuery
	}

	RunningQueriesStorage struct {
		mx                   *sync.RWMutex
		runningQueries       RunningQueryType
		stat                 StorageStat
		maximumStoredQueries int
		freePercent          int
		archChan             chan *GCQuery
		gcDone               chan struct{} // closed when archive reader shuts down; GC senders select on it
	}
)

type Option func(*RunningQueriesStorage)

func NewConfiguredRunningQueriesStorage(cfg *config.Config) *RunningQueriesStorage {
	return NewRunningQueriesStorage(WithMaximumStoredQueries(int(cfg.MaximumStoredQueries)))
}

func NewRunningQueriesStorage(opts ...Option) *RunningQueriesStorage {
	const (
		defaultMaximumStoredQueries = 50 * 1000
		defaultFreePercent          = 50
	)

	s := &RunningQueriesStorage{
		mx:                   &sync.RWMutex{},
		runningQueries:       make(RunningQueryType, 0),
		stat:                 StorageStat{ResetTime: time.Now()},
		maximumStoredQueries: defaultMaximumStoredQueries,
		freePercent:          defaultFreePercent,
	}

	for _, o := range opts {
		o(s)
	}

	return s
}

func WithMaximumStoredQueries(maximumStoredQueries int) Option {
	return func(s *RunningQueriesStorage) {
		s.maximumStoredQueries = maximumStoredQueries
	}
}

func WithFreePercent(freePercent int) Option {
	return func(s *RunningQueriesStorage) {
		s.freePercent = freePercent
	}
}

func WithArchChan(archChan chan *GCQuery) Option {
	return func(s *RunningQueriesStorage) {
		s.archChan = archChan
		s.gcDone = make(chan struct{})
	}
}

// SetArchChan sets the archive channel for GC-evicted completed queries.
// This can be called after construction when the channel is not available at creation time.
func (s *RunningQueriesStorage) SetArchChan(archChan chan *GCQuery) {
	s.mx.Lock()
	defer s.mx.Unlock()
	s.archChan = archChan
	s.gcDone = make(chan struct{})
}

// CloseArchChan signals GC sender goroutines to stop and drains the archive channel.
// Must be called when the archive reader (forwardGCQueries) shuts down to prevent
// GC goroutines from blocking on a channel that nobody reads.
func (s *RunningQueriesStorage) CloseArchChan() {
	s.mx.Lock()
	ch := s.archChan
	done := s.gcDone
	s.archChan = nil
	s.gcDone = nil
	s.mx.Unlock()

	// Signal all in-flight GC sender goroutines to stop.
	if done != nil {
		close(done)
	}
	// Drain remaining items so no goroutine stays blocked on send.
	if ch != nil {
		for {
			select {
			case <-ch:
			default:
				return
			}
		}
	}
}

func NewQueryKey(qKey *pbc.QueryKey, sKey *pbc.SegmentKey, sliceId int64) *NodeKey {
	queryKey := QueryKey{
		Ssid: qKey.Ssid,
		Ccnt: qKey.Ccnt,
	}
	segmentKey := SegmentKey{}
	if sKey != nil {
		segmentKey.Dbid = sKey.Dbid
		segmentKey.Segindex = sKey.Segindex
	}
	newKey := &NodeKey{
		QKey:    queryKey,
		SKey:    segmentKey,
		SliceID: sliceId,
	}
	return newKey
}

func CheckQueryStarted(status int32) bool {
	return status == int32(pbc.QueryStatus_QUERY_STATUS_START)
}

func CheckQueryEnded(status int32) bool {
	return status == int32(pbc.QueryStatus_QUERY_STATUS_DONE) || status == int32(pbc.QueryStatus_QUERY_STATUS_CANCELED) || status == int32(pbc.QueryStatus_QUERY_STATUS_ERROR)
}

func CheckQueryErrored(status int32) bool {
	return status == int32(pbc.QueryStatus_QUERY_STATUS_CANCELED) || status == int32(pbc.QueryStatus_QUERY_STATUS_ERROR)
}

func setMetricsForEndedQuery(qKey *NodeKey, rQ *RunningQuery, status int32, startTime *timestamppb.Timestamp, endTime *timestamppb.Timestamp, submitTime *timestamppb.Timestamp) {
	// global read lock should be taken
	qEnd := time.Now()
	if endTime != nil {
		qEnd = endTime.AsTime()
	}
	qStart := qEnd
	if startTime != nil {
		qStart = startTime.AsTime()
	}
	if metrics.YagpccMetrics != nil {
		metrics.YagpccMetrics.SliceLatencies.With(map[string]string{}).Observe(qEnd.Sub(qStart).Seconds())
		metrics.YagpccMetrics.QueriesInFlight.Add(-1)
	}
	rQ.QueryLock.Lock()
	defer rQ.QueryLock.Unlock()
	if qKey.SliceID == MainSliceId || qKey.SliceID == UnsetSliceId {
		// set completed only for initilal slice
		rQ.Completed = true
	}
	rQ.QueryStatus = status
	if startTime != nil {
		rQ.QueryStart = startTime.AsTime()
	}
	if endTime != nil {
		rQ.QueryEnd = endTime.AsTime()
	}
	if submitTime != nil {
		rQ.QuerySubmit = submitTime.AsTime()
	}
}

func (s *RunningQueriesStorage) deleteQuery(qKey QueryKey) {
	delete(s.runningQueries, qKey)
	if metrics.YagpccMetrics != nil {
		metrics.YagpccMetrics.DroppedQueries.Inc()
	}
	s.stat.QueriesWipedOut += 1
}

func (s *RunningQueriesStorage) garbageCollect() {
	// should be called under exclusive lock
	type kv struct {
		Key   QueryKey
		Value *RunningQuery
	}

	ss := make([]kv, 0, len(s.runningQueries))
	for k, v := range s.runningQueries {
		ss = append(ss, kv{k, v})
	}

	sort.Slice(ss, func(i, j int) bool {
		endedI := CheckQueryEnded(ss[i].Value.QueryStatus)
		endedJ := CheckQueryEnded(ss[j].Value.QueryStatus)
		if endedI == endedJ { // if both finished or unfinished sort by start time
			return ss[i].Value.QueryStart.Before(ss[j].Value.QueryStart)
		}
		// True should be if we have finished effect in J and unfinished in I
		return endedJ
	})

	toDelete := s.maximumStoredQueries * s.freePercent / 100
	if toDelete < 1 && len(ss) > 0 {
		toDelete = 1
	}
	if toDelete > len(ss) {
		toDelete = len(ss)
	}

	var toArchive []GCQuery
	for i := 0; i < toDelete; i++ {
		ss[i].Value.QueryLock.RLock()
		completed := ss[i].Value.Completed
		ss[i].Value.QueryLock.RUnlock()
		if completed && s.archChan != nil {
			// Completed query — send to archive channel instead of losing data.
			key := ss[i].Key
			toArchive = append(toArchive, GCQuery{QKey: &key, QVal: ss[i].Value})
		}
		s.deleteQuery(ss[i].Key)
	}

	if metrics.YagpccMetrics != nil {
		metrics.YagpccMetrics.GCRuns.Inc()
		metrics.YagpccMetrics.GCDeletedQueries.Add(float64(toDelete))
		metrics.YagpccMetrics.GCArchivedQueries.Add(float64(len(toArchive)))
	}

	// Send completed queries to archive channel in a separate goroutine
	// to avoid blocking while holding the storage lock.
	// Use select with gcDone to prevent hanging when the reader has shut down.
	if len(toArchive) > 0 {
		archChan := s.archChan
		gcDone := s.gcDone
		go func() {
			for i := range toArchive {
				if gcDone != nil {
					select {
					case <-gcDone:
						return
					default:
					}
					select {
					case archChan <- &toArchive[i]:
					case <-gcDone:
						return
					}
					continue
				}
				select {
				case archChan <- &toArchive[i]:
				}
			}
		}()
	}

	s.stat.NumGC += 1
}

func (s *RunningQueriesStorage) newQuery(qKey *QueryKey, status int32, mQTimes MeasuredQueryTimes) *RunningQuery {
	// should be called under exclusive lock
	if len(s.runningQueries) >= s.maximumStoredQueries {
		s.garbageCollect()
	}
	qNow := time.Now()
	rQ := &RunningQuery{
		QueriesData:     make(QueryMap),
		Completed:       false,
		MarkSessionSent: false,
		QueryStatus:     status,
		QueryStart:      qNow,
		QueryEnd:        qNow,
		QuerySubmit:     qNow,
		NestedLevel:     -1,
		SegmentNodes:    make(SegmentIndexNodes),
	}

	if mQTimes.QueryStart != nil {
		rQ.QueryStart = utils.GetTimeForTimestamp(mQTimes.QueryStart)
	}
	if mQTimes.QueryEnd != nil {
		rQ.QueryEnd = utils.GetTimeForTimestamp(mQTimes.QueryEnd)
	}
	if mQTimes.QuerySubmit != nil {
		rQ.QuerySubmit = utils.GetTimeForTimestamp(mQTimes.QuerySubmit)
	}
	s.runningQueries[*qKey] = rQ
	s.stat.QueriesCount += 1
	if metrics.YagpccMetrics != nil {
		metrics.YagpccMetrics.NewQueries.Inc()
		metrics.YagpccMetrics.TotalQueries.Set(float64(len(s.runningQueries)))
	}
	return rQ
}

func chooseTime(dst *timestamppb.Timestamp, src *timestamppb.Timestamp) *timestamppb.Timestamp {
	if dst == nil {
		return src
	}
	if src == nil {
		return dst
	}
	if src.GetSeconds() > dst.GetSeconds() {
		return src
	}
	return dst
}

func updateMeasuredTimes(qData *QueryData, mQTimes MeasuredQueryTimes) {
	qData.QueryInfo.StartTime = chooseTime(qData.QueryInfo.StartTime, mQTimes.QueryStart)
	qData.QueryInfo.EndTime = chooseTime(qData.QueryInfo.EndTime, mQTimes.QueryEnd)
	qData.QueryInfo.SubmitTime = chooseTime(qData.QueryInfo.SubmitTime, mQTimes.QuerySubmit)
}

func updateQueryStat(qData *QueryData, status int32, mQTimes MeasuredQueryTimes, qInfo *pbc.QueryInfo, qMetrics *pbc.GPMetrics) error {
	qData.QueryDataLock.Lock()
	defer qData.QueryDataLock.Unlock()

	updateMeasuredTimes(qData, mQTimes)

	// sanity check - if query is done do not change status,
	// for example query_end could be sent after query done
	// but we should ignore it
	if !CheckQueryEnded(qData.CurrentStatus) {
		qData.CurrentStatus = status
	}

	// set queryInfo
	err := MergeQueryInfo(qData.QueryInfo, qInfo)
	if err != nil {
		return fmt.Errorf("internal query info nil error")
	}

	// set GPMetrics
	err = MergeGPMetrics(qData.QueryMetrics, qMetrics)
	if err != nil {
		return fmt.Errorf("internal merge nil error")
	}

	return nil
}

func (s *RunningQueriesStorage) DeleteQueries(keysToDelete []*QueryKey) int {
	s.mx.Lock()
	defer s.mx.Unlock()
	for _, qKey := range keysToDelete {
		s.deleteQuery(*qKey)
	}
	return len(s.runningQueries)
}

func (s *RunningQueriesStorage) GetQueries() RunningQueryType {
	s.mx.RLock()
	defer s.mx.RUnlock()
	return maps.Clone(s.runningQueries)
}

func (s *RunningQueriesStorage) GetQuery(key QueryKey) (*RunningQuery, bool) {
	s.mx.RLock()
	defer s.mx.RUnlock()
	val, ok := s.runningQueries[key]
	return val, ok
}

func (s *RunningQueriesStorage) ClearRunningQueries() {
	s.mx.Lock()
	defer s.mx.Unlock()
	s.runningQueries = make(RunningQueryType, 0)
	s.stat = StorageStat{ResetTime: time.Now()}
}

func (s *RunningQueriesStorage) GetStorageStat() StorageStat {
	s.mx.RLock()
	defer s.mx.RUnlock()
	return s.stat
}

func (s *RunningQueriesStorage) QueriesCount() int {
	s.mx.RLock()
	defer s.mx.RUnlock()
	return len(s.runningQueries)
}

func (s *RunningQueriesStorage) CanLock() bool {
	if s.mx.TryRLock() {
		s.mx.RUnlock()
		return true
	}
	return false
}

func (s *RunningQueriesStorage) StoreInfoInStorage(
	nKey *NodeKey,
	status int32,
	mQTimes MeasuredQueryTimes,
	queryInfoSet *pbc.QueryInfo,
	addInfo *pbc.AdditionalQueryInfo,
	gpMetrics *pbc.GPMetrics,
) (bool, error) {
	queryEnded := CheckQueryEnded(status)
	// sanity check

	// copy query info
	queryInfo := &pbc.QueryInfo{}
	if queryInfoSet != nil {
		queryInfo = proto.Clone(queryInfoSet).(*pbc.QueryInfo)
	}

	newQuery := false
	// create new query key if needed
	s.mx.RLock()
	rQ, okQ := s.runningQueries[nKey.QKey]
	s.mx.RUnlock()
	if !okQ {
		// do not create new item for completed empty query info in master in order to avoid double-conting
		if nKey.SKey.Segindex == -1 && queryEnded && queryInfoSet == nil {
			return false, nil
		}
		s.mx.Lock()
		rQ, okQ = s.runningQueries[nKey.QKey]
		if !okQ {
			newQuery = true
			rQ = s.newQuery(&nKey.QKey, status, mQTimes)
		}
		s.mx.Unlock()
	}

	if addInfo != nil && (nKey.SliceID == MainSliceId || nKey.SliceID == UnsetSliceId) {
		rQ.QueryLock.Lock()
		rQ.NestedLevel = addInfo.GetNestedLevel()
		rQ.QueryLock.Unlock()
	}

	rQ.QueryLock.RLock()
	val, ok := rQ.QueriesData[*nKey]
	rQ.QueryLock.RUnlock()
	if !ok {
		// could update data without lock
		val = &QueryData{
			CurrentStatus: status,
			QueryInfo:     queryInfo,
			QueryMetrics:  proto.Clone(gpMetrics).(*pbc.GPMetrics),
		}
		if val.QueryMetrics == nil {
			val.QueryMetrics = &pbc.GPMetrics{}
		}
		updateMeasuredTimes(val, mQTimes)
		rQ.QueryLock.Lock()
		rQ.QueriesData[*nKey] = val
		rQ.QueryLock.Unlock()
		if metrics.YagpccMetrics != nil {
			metrics.YagpccMetrics.QueriesInFlight.Add(1)
		}
	} else {
		if val == nil {
			return newQuery, fmt.Errorf("internal nil error")
		}
		// lock needed - set it in function
		err := updateQueryStat(val, status, mQTimes, queryInfo, gpMetrics)
		if err != nil {
			return newQuery, err
		}
	}
	if queryEnded {
		val.QueryDataLock.RLock()
		startTime := val.QueryInfo.StartTime
		endTime := val.QueryInfo.EndTime
		submitTime := val.QueryInfo.SubmitTime
		val.QueryDataLock.RUnlock()
		setMetricsForEndedQuery(nKey, rQ, status, startTime, endTime, submitTime)
	}
	return newQuery, nil
}

func (s *RunningQueriesStorage) GetQueriesStartTime() []time.Time {
	s.mx.RLock()
	defer s.mx.RUnlock()

	times := make([]time.Time, 0, len(s.runningQueries))
	for _, q := range s.runningQueries {
		if q.QueryStart.IsZero() || CheckQueryEnded(q.QueryStatus) {
			continue
		}
		times = append(times, q.QueryStart)
	}
	return times
}
