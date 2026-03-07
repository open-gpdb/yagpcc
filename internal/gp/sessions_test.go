package gp

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/utils"
)

func TestRunningQueriesLevelSet(t *testing.T) {
	rq := NewRunningQueriesInfo()
	assert.Equal(t, rq.GetCurrentQuery(), int32(-1))

	// store current query in current_cnt
	rq.SetCurrentQuery(1, 0)
	assert.Equal(t, rq.GetCurrentQuery(), int32(1))
	assert.Equal(t, len(rq.Ccnts), 1)

	// revert length back
	rq.SetCurrentQuery(2, 1)
	rq.SetCurrentQuery(1, 0)
	assert.Equal(t, rq.GetCurrentQuery(), int32(1))
	assert.Equal(t, len(rq.Ccnts), 1)

	// store current query in current_cnt
	rq.SetCurrentQuery(2, 10)
	assert.Equal(t, rq.GetCurrentQuery(), int32(2))
	assert.Equal(t, len(rq.Ccnts), 11)

	// revert to stored ccnt
	rq.EndCurrentQuery(2, 10)
	assert.Equal(t, rq.GetCurrentQuery(), int32(1))
	assert.Equal(t, len(rq.Ccnts), 1)

	// store current query in current_cnt
	rq.SetCurrentQuery(2, 10)
	assert.Equal(t, rq.GetCurrentQuery(), int32(2))
	assert.Equal(t, len(rq.Ccnts), 11)

	rq.EndCurrentQuery(3, 5)
	assert.Equal(t, rq.GetCurrentQuery(), int32(1))
	assert.Equal(t, len(rq.Ccnts), 1)

	rq.SetCurrentQuery(5, 5)
	assert.Equal(t, rq.GetCurrentQuery(), int32(5))
	assert.Equal(t, len(rq.Ccnts), 6)

	rq.SetCurrentQuery(8, MaxRecurseDepth+10)
	assert.Equal(t, rq.GetCurrentQuery(), int32(8))
	assert.Equal(t, len(rq.Ccnts), MaxRecurseDepth)

	rq.SetCurrentQuery(10, MaxRecurseDepth+10)
	assert.Equal(t, rq.GetCurrentQuery(), int32(10))
	assert.Equal(t, len(rq.Ccnts), MaxRecurseDepth)

	rq.EndCurrentQuery(3, MaxRecurseDepth+10)
	assert.Equal(t, rq.GetCurrentQuery(), int32(5))
	assert.Equal(t, len(rq.Ccnts), 6)
}

func TestRunningQueriesLevelNotSet(t *testing.T) {
	rq := NewRunningQueriesInfo()
	assert.Equal(t, rq.GetCurrentQuery(), int32(-1))

	// store current query in current_cnt
	rq.SetCurrentQuery(1, -1)
	assert.Equal(t, rq.GetCurrentQuery(), int32(1))
	assert.Equal(t, len(rq.Ccnts), 1)

	// save old ccnt
	rq.SetCurrentQuery(2, -1)
	assert.Equal(t, rq.GetCurrentQuery(), int32(2))
	assert.Equal(t, len(rq.Ccnts), 2)
	assert.Equal(t, rq.Ccnts[0], int32(1))

	// do not save ccnt once again
	rq.SetCurrentQuery(2, -1)
	assert.Equal(t, rq.GetCurrentQuery(), int32(2))
	assert.Equal(t, len(rq.Ccnts), 2)
	assert.Equal(t, rq.Ccnts[0], int32(1))

	// revert to stored ccnt
	rq.EndCurrentQuery(2, -1)
	assert.Equal(t, rq.GetCurrentQuery(), int32(1))
	assert.Equal(t, len(rq.Ccnts), 1)

	// revert to ccnt in the middle
	for i := 0; i < 10; i++ {
		rq.SetCurrentQuery(int32(i+3), -1)
	}
	assert.Equal(t, rq.GetCurrentQuery(), int32(12))
	assert.Equal(t, len(rq.Ccnts), 11)
	rq.EndCurrentQuery(8, -1)
	assert.Equal(t, rq.GetCurrentQuery(), int32(7))
	assert.Equal(t, len(rq.Ccnts), 6)

	// rever to to non-existing ccnt
	rq.EndCurrentQuery(8, -1)
	assert.Equal(t, rq.GetCurrentQuery(), int32(-1))
	assert.Equal(t, len(rq.Ccnts), 0)

	//revert last ccnt
	rq.EndCurrentQuery(8, -1)
	assert.Equal(t, rq.GetCurrentQuery(), int32(-1))
	assert.Equal(t, len(rq.Ccnts), 0)

	//store more then MaxRecurseDepth items
	for i := 0; i < MaxRecurseDepth+18; i++ {
		rq.SetCurrentQuery(int32(i+1), -1)
	}
	assert.Equal(t, rq.GetCurrentQuery(), int32(MaxRecurseDepth+18))
	assert.Equal(t, len(rq.Ccnts), MaxRecurseDepth)
}

func TestRefreshSessionList(t *testing.T) {
	file, err := os.Create("trace.log")
	require.NoError(t, err)
	zLogger := utils.DualLog(true, file)
	s := NewSessionsStorage(nil)

	activityList1 := []*GpStatActivity{
		{
			DatID:   1,
			Datname: "test",
			Pid:     12,
			SessID:  1,
			TmID:    100,
		},
		{
			DatID:   1,
			Datname: "test",
			Pid:     13,
			SessID:  2,
			TmID:    100,
		},
	}

	err = s.RefreshSessionList(zLogger, activityList1, true)
	if err != nil {
		t.Errorf("%v", err)
	}
	assert.Equal(t, len(s.sessMap), 2)

	activityList2 := []*GpStatActivity{
		{
			DatID:   1,
			Datname: "test",
			Pid:     12,
			SessID:  1,
			TmID:    100,
		},
		{
			DatID:   1,
			Datname: "test",
			Pid:     14,
			SessID:  3,
			TmID:    100,
		},
	}

	err = s.RefreshSessionList(zLogger, activityList2, true)
	if err != nil {
		t.Errorf("%v", err)
	}
	assert.Equal(t, len(s.sessMap), 2)

	_, okS := s.sessMap[SessionKey{SessID: 2}]

	assert.Equal(t, okS, false)

	valS, okS := s.sessMap[SessionKey{SessID: 3}]

	assert.Equal(t, okS, true)
	assert.Equal(t, valS.SessionData.GpStatInfo.Pid, 14)

	valS, okS = s.sessMap[SessionKey{SessID: 1}]

	assert.Equal(t, okS, true)
	assert.Equal(t, valS.SessionData.GpStatInfo.Pid, 12)
}

func TestRegisterNewSessionQuery(t *testing.T) {
	file, err := os.Create("trace.log")
	require.NoError(t, err)
	zLogger := utils.DualLog(true, file)
	if err != nil {
		t.Errorf("%v", err)
	}
	s := NewSessionsStorage(nil)
	qKey := pbc.QueryKey{Ssid: 1, Tmid: 100, Ccnt: 1}
	qInfo := pbc.QueryInfo{QueryId: 1234, QueryText: "Select 1"}

	valS := s.RegisterNewSessionQuery(nil, false, &qKey, &qInfo, 0)

	assert.Equal(t, len(s.sessMap), 1)
	assert.Equal(t, valS.RefCounter, 1)
	assert.Equal(t, valS.SessionData.RunningQueries.GetCurrentQuery(), int32(1))
	valS, okS := s.sessMap[SessionKey{SessID: 1}]
	assert.Equal(t, okS, true)
	assert.Equal(t, valS.SessionData.RunningQueries.GetCurrentQuery(), int32(1))

	activityList := []*GpStatActivity{
		{
			DatID:   1,
			Datname: "test",
			Pid:     13,
			SessID:  2,
			TmID:    100,
		},
		{
			DatID:   1,
			Datname: "test",
			Pid:     14,
			SessID:  1,
			TmID:    100,
		},
	}

	err = s.RefreshSessionList(zLogger, activityList, true)
	if err != nil {
		t.Errorf("%v", err)
	}
	assert.Equal(t, len(s.sessMap), 2)

	valS, okS = s.sessMap[SessionKey{SessID: 1}]
	assert.Equal(t, okS, true)
	assert.Equal(t, valS.RefCounter, 1)
	assert.Equal(t, valS.SessionData.RunningQueries.GetCurrentQuery(), int32(1))

	valS = s.RegisterNewSessionQuery(valS, okS, &qKey, nil, 0)

	assert.Equal(t, len(s.sessMap), 2)
	assert.Equal(t, valS.RefCounter, 2)
	assert.Equal(t, valS.SessionData.RunningQueries.GetCurrentQuery(), int32(1))

	err = s.RefreshSessionList(zLogger, activityList, true)
	if err != nil {
		t.Errorf("%v", err)
	}
	assert.Equal(t, len(s.sessMap), 2)

	valS, okS = s.sessMap[SessionKey{SessID: 1}]
	assert.Equal(t, okS, true)
	assert.Equal(t, valS.RefCounter, 2)

	qKey = pbc.QueryKey{Ssid: 2, Tmid: 100, Ccnt: 2}
	qInfo = pbc.QueryInfo{QueryId: 1235, QueryText: "Select 1"}

	valS, okS = s.sessMap[SessionKey{SessID: 2}]
	valS = s.RegisterNewSessionQuery(valS, okS, &qKey, &qInfo, 1)

	assert.Equal(t, len(s.sessMap), 2)
	assert.Equal(t, valS.RefCounter, 1)
	assert.Equal(t, valS.SessionData.RunningQueries.GetCurrentQuery(), int32(2))

	valS, okS = s.sessMap[SessionKey{SessID: 1}]
	assert.Equal(t, okS, true)
	assert.Equal(t, valS.RefCounter, 2)
	assert.Equal(t, valS.SessionData.RunningQueries.GetCurrentQuery(), int32(1))

	valS, okS = s.sessMap[SessionKey{SessID: 2}]
	assert.Equal(t, okS, true)
	assert.Equal(t, valS.RefCounter, 1)
	assert.Equal(t, valS.SessionData.RunningQueries.GetCurrentQuery(), int32(2))
}

func TestAggregateSessMetrics(t *testing.T) {
	s := NewSessionsStorage(nil)
	qKey := pbc.QueryKey{Ssid: 1, Tmid: 100, Ccnt: 1}
	qInfo := pbc.QueryInfo{QueryId: 1234, QueryText: "Select 1"}
	err := s.UpdateSessionQuery(&qKey, &qInfo, 0, &pbc.AdditionalQueryInfo{NestedLevel: 0}, false)
	if err != nil {
		t.Errorf("%v", err)
	}

	valS, okS := s.sessMap[SessionKey{SessID: 1}]
	assert.Equal(t, okS, true)
	assert.Equal(t, valS.RefCounter, 1)
	assert.Equal(t, valS.SessionData.RunningQueries.GetCurrentQuery(), int32(1))
	assert.Nil(t, valS.SessionData.TotalGPMetrics.SystemStat)
	assert.Nil(t, valS.SessionData.LongRunningGPMetrics.SystemStat)

	qKey2 := pbc.QueryKey{Ssid: 1, Tmid: 100, Ccnt: 2}
	qInfo2 := pbc.QueryInfo{QueryId: 2234, QueryText: "Select 2"}

	err = s.UpdateSessionQuery(&qKey2, &qInfo2, 1, &pbc.AdditionalQueryInfo{NestedLevel: 0}, true)
	if err != nil {
		t.Errorf("%v", err)
	}

	valS, okS = s.sessMap[SessionKey{SessID: 1}]
	assert.Equal(t, okS, true)
	assert.Equal(t, valS.RefCounter, 2)
	assert.Equal(t, valS.SessionData.RunningQueries.GetCurrentQuery(), int32(2))
	assert.Nil(t, valS.SessionData.TotalGPMetrics.SystemStat)
	assert.Nil(t, valS.SessionData.LongRunningGPMetrics.SystemStat)

	err = s.UpdateSessionQuery(&qKey, &qInfo, 0, &pbc.AdditionalQueryInfo{NestedLevel: 0}, false)
	if err != nil {
		t.Errorf("%v", err)
	}

	valS, okS = s.sessMap[SessionKey{SessID: 1}]
	assert.Equal(t, okS, true)
	assert.Equal(t, valS.RefCounter, 2)
	assert.Equal(t, valS.SessionData.RunningQueries.GetCurrentQuery(), int32(2))
	assert.Nil(t, valS.SessionData.TotalGPMetrics.SystemStat)
	assert.Nil(t, valS.SessionData.LongRunningGPMetrics.SystemStat)

}

func TestCanLock(t *testing.T) {
	s := NewSessionsStorage(nil)
	assert.Equal(t, true, s.CanLock())
	s.mx.Lock()
	defer s.mx.Unlock()
	assert.Equal(t, false, s.CanLock())
}

func TestQueriesCount(t *testing.T) {
	s := NewSessionsStorage(nil)
	assert.Equal(t, 0, s.SessionsCount())
	s.RegisterNewSessionQuery(nil, false, &pbc.QueryKey{Ssid: 1}, nil, 0)
	assert.Equal(t, 1, s.SessionsCount())
}

func TestGetSession(t *testing.T) {
	s := NewSessionsStorage(nil)
	s.RegisterNewSessionQuery(nil, false, &pbc.QueryKey{Ssid: 1}, nil, 0)
	val, ok := s.GetSession(SessionKey{SessID: 1})
	assert.Equal(t, ok, true)
	assert.NotNil(t, val)
	_, ok = s.GetSession(SessionKey{SessID: 2})
	assert.Equal(t, ok, false)
}

func TestGetSessions(t *testing.T) {
	s := NewSessionsStorage(nil)
	testQ := []*pbc.QueryKey{{Ssid: 1}, {Ssid: 2}}
	for _, tQ := range testQ {
		s.RegisterNewSessionQuery(nil, false, tQ, nil, 0)
	}
	assert.Equal(t, 2, len(s.GetSessions()))
	for tQ := range s.GetSessions() {
		_, ok := s.GetSession(tQ)
		assert.Equal(t, ok, true)
	}
}
