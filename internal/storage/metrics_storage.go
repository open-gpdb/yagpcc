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
		SegmentNodes    SegmentIndexNodes
		QueryLock       sync.RWMutex
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

	RunningQueriesStorage struct {
		mx                   *sync.RWMutex
		runningQueries       RunningQueryType
		stat                 StorageStat
		maximumStoredQueries int
		freePercent          int
	}
)

type Option func(*RunningQueriesStorage)

func NewConfiguredRunningQueriesStorage(cfg *config.Config) *RunningQueriesStorage {
	return NewRunningQueriesStorage(WithMaximumStoredQueries(int(cfg.MaximumStoredQueries)))
}

func NewRunningQueriesStorage(opts ...Option) *RunningQueriesStorage {

	const (
		defaultMaximumStoredQueries = 50 * 1000
		defaultFreePercent          = 20
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

func setMetricsForEndedQuery(qKey *NodeKey, rQ *RunningQuery, status int32, StartTime *timestamppb.Timestamp, EndTime *timestamppb.Timestamp, SubmitTime *timestamppb.Timestamp) {
	// global read lock should be taken
	qEnd := time.Now()
	if EndTime != nil {
		qEnd = EndTime.AsTime()
	}
	qStart := qEnd
	if StartTime != nil {
		qStart = StartTime.AsTime()
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
	if StartTime != nil {
		rQ.QueryStart = StartTime.AsTime()
	}
	if EndTime != nil {
		rQ.QueryEnd = EndTime.AsTime()
	}
	if SubmitTime != nil {
		rQ.QuerySubmit = SubmitTime.AsTime()
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
		return ss[i].Value.QueryStart.Before(ss[j].Value.QueryStart)
	})

	for i := 0; i < (s.maximumStoredQueries/100*s.freePercent) && i < len(ss); i++ {
		s.deleteQuery(ss[i].Key)
	}
	s.stat.NumGC += 1
}

func (s *RunningQueriesStorage) newQuery(QKey *QueryKey, status int32, mQTimes MeasuredQueryTimes) *RunningQuery {
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
	s.runningQueries[*QKey] = rQ
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
