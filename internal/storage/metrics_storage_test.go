package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"

	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
)

func TestClearRunningQueries(t *testing.T) {
	s := NewRunningQueriesStorage()
	s.newQuery(&QueryKey{Ssid: 1}, 0, MeasuredQueryTimes{})
	s.ClearRunningQueries()
	assert.Equal(t, 0, len(s.runningQueries))
}

func TestDeleteQueries(t *testing.T) {
	s := NewRunningQueriesStorage()
	s.newQuery(&QueryKey{Ssid: 1}, 0, MeasuredQueryTimes{})
	assert.Equal(t, 1, len(s.runningQueries))
	s.DeleteQueries([]*QueryKey{{Ssid: 1}})
	assert.Equal(t, 0, len(s.runningQueries))
	s.DeleteQueries([]*QueryKey{{Ssid: 1}})
	assert.Equal(t, 0, len(s.runningQueries))
	s.newQuery(&QueryKey{Ssid: 1}, 0, MeasuredQueryTimes{})
	s.newQuery(&QueryKey{Ssid: 2}, 0, MeasuredQueryTimes{})
	assert.Equal(t, 2, len(s.runningQueries))
	s.DeleteQueries([]*QueryKey{{Ssid: 1}})
	assert.Equal(t, 1, len(s.runningQueries))
	s.DeleteQueries([]*QueryKey{{Ssid: 1}})
	assert.Equal(t, 1, len(s.runningQueries))
	s.DeleteQueries([]*QueryKey{{Ssid: 2}})
	assert.Equal(t, 0, len(s.runningQueries))
}

func TestGetQuery(t *testing.T) {
	s := NewRunningQueriesStorage()
	s.newQuery(&QueryKey{Ssid: 1}, 0, MeasuredQueryTimes{})
	val, ok := s.GetQuery(QueryKey{Ssid: 1})
	assert.Equal(t, ok, true)
	assert.NotNil(t, val)
	_, ok = s.GetQuery(QueryKey{Ssid: 2})
	assert.Equal(t, ok, false)
}

func TestGetQueries(t *testing.T) {
	s := NewRunningQueriesStorage()
	testQ := []*QueryKey{{Ssid: 1}, {Ssid: 2}}
	for _, tQ := range testQ {
		s.newQuery(tQ, 0, MeasuredQueryTimes{})
	}
	assert.Equal(t, 2, len(s.GetQueries()))
	for tQ := range s.GetQueries() {
		_, ok := s.GetQuery(tQ)
		assert.Equal(t, ok, true)
	}
}

func TestCanLock(t *testing.T) {
	s := NewRunningQueriesStorage()
	assert.Equal(t, true, s.CanLock())
	s.mx.Lock()
	defer s.mx.Unlock()
	assert.Equal(t, false, s.CanLock())
}

func TestQueriesCount(t *testing.T) {
	s := NewRunningQueriesStorage()
	assert.Equal(t, 0, s.QueriesCount())
	s.newQuery(&QueryKey{Ssid: 1}, 0, MeasuredQueryTimes{})
	assert.Equal(t, 1, s.QueriesCount())
}

func TestGC(t *testing.T) {
	s := NewRunningQueriesStorage()
	tStart := time.Now()
	for i := 0; i < s.maximumStoredQueries+2; i++ {
		qKey := &QueryKey{
			Ssid: int32(i),
		}
		t := tStart.Add(-time.Duration(int(time.Second) * i))
		mqTimes := MeasuredQueryTimes{
			QueryStart: timestamppb.New(t),
		}
		s.newQuery(qKey, 0, mqTimes)
	}
	assert.Equal(t, len(s.runningQueries), s.maximumStoredQueries/100*80+2)
	qKeyFirst := &QueryKey{
		Ssid: int32(s.maximumStoredQueries/100*80 - 1),
	}
	val, ok := s.runningQueries[*qKeyFirst]
	assert.Equal(t, ok, true)
	assert.WithinDuration(t, val.QueryStart, tStart.Add(-time.Duration(int(time.Second)*int(qKeyFirst.Ssid))), 0)
	qKeyNotFound := &QueryKey{
		Ssid: int32(s.maximumStoredQueries / 100 * 80),
	}
	_, ok = s.runningQueries[*qKeyNotFound]
	assert.Equal(t, ok, false)
	qKeyLast := &QueryKey{
		Ssid: int32(s.maximumStoredQueries + 1),
	}
	val, ok = s.runningQueries[*qKeyLast]
	assert.Equal(t, ok, true)
	assert.WithinDuration(t, val.QueryStart, tStart.Add(-time.Duration(int(time.Second)*int(qKeyLast.Ssid))), 0)
}

func TestMultipleSlices(t *testing.T) {
	s := NewRunningQueriesStorage()
	newQuery, err := s.StoreInfoInStorage(
		&NodeKey{QKey: QueryKey{Ssid: 1, Ccnt: 2}, SKey: SegmentKey{Segindex: 1}, SliceID: 0},
		int32(pbc.QueryStatus_QUERY_STATUS_DONE),
		MeasuredQueryTimes{
			QueryStart: timestamppb.New(time.Now().Add(time.Duration(-10) * time.Minute)),
			QueryEnd:   timestamppb.New(time.Now()),
		},
		&pbc.QueryInfo{QueryText: "SELECT 1"},
		nil,
		&pbc.GPMetrics{Instrumentation: &pbc.MetricInstrumentation{Nloops: 1}},
	)
	assert.NoError(t, err)
	assert.Equal(t, newQuery, true)
	assert.Equal(t, len(s.runningQueries[QueryKey{Ssid: 1, Ccnt: 2}].QueriesData), 1)
	newQuery, err = s.StoreInfoInStorage(
		&NodeKey{QKey: QueryKey{Ssid: 1, Ccnt: 2}, SKey: SegmentKey{Segindex: 1}, SliceID: 1},
		int32(pbc.QueryStatus_QUERY_STATUS_DONE),
		MeasuredQueryTimes{
			QueryStart: timestamppb.New(time.Now().Add(time.Duration(-10) * time.Minute)),
			QueryEnd:   timestamppb.New(time.Now()),
		},
		&pbc.QueryInfo{},
		&pbc.AdditionalQueryInfo{SliceId: 1},
		&pbc.GPMetrics{Instrumentation: &pbc.MetricInstrumentation{Nloops: 2}},
	)
	assert.NoError(t, err)
	assert.Equal(t, newQuery, false)
	runningQ := s.runningQueries[QueryKey{Ssid: 1, Ccnt: 2}]
	sliceZero := runningQ.QueriesData[NodeKey{QKey: QueryKey{Ssid: 1, Ccnt: 2}, SKey: SegmentKey{Segindex: 1}, SliceID: 0}]
	assert.Equal(t, sliceZero.QueryMetrics.Instrumentation.Nloops, uint64(1))
	sliceOne := runningQ.QueriesData[NodeKey{QKey: QueryKey{Ssid: 1, Ccnt: 2}, SKey: SegmentKey{Segindex: 1}, SliceID: 1}]
	assert.Equal(t, sliceOne.QueryMetrics.Instrumentation.Nloops, uint64(2))

}
