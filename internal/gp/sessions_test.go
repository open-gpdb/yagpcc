package gp

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/storage"
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
	s := NewSessionsStorage(zLogger, nil, nil)

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

	err = s.RefreshSessionList(activityList1, true)
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

	err = s.RefreshSessionList(activityList2, true)
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

// TestRefreshSessionListNegativeSessID verifies that in cloudberry, where multiple
// system background processes share sess_id == -1, each backend gets its own
// session entry keyed by -pid instead of all collapsing onto a single SessID=-1
// entry (and thrashing through clearDeletedSessions on every refresh).
func TestRefreshSessionListNegativeSessID(t *testing.T) {
	file, err := os.Create("trace.log")
	require.NoError(t, err)
	zLogger := utils.DualLog(true, file)
	s := NewSessionsStorage(zLogger, nil, nil)

	// One real client backend + three cloudberry system processes
	// all reporting sess_id == -1 but with distinct pids.
	activityList1 := []*GpStatActivity{
		{DatID: 1, Datname: "test", Pid: 100, SessID: 42, TmID: 100},
		{DatID: 1, Datname: "test", Pid: 19702, SessID: -1, TmID: 100}, // background writer
		{DatID: 1, Datname: "test", Pid: 19703, SessID: -1, TmID: 100}, // walwriter
		{DatID: 1, Datname: "test", Pid: 19705, SessID: -1, TmID: 100}, // archiver
	}

	require.NoError(t, s.RefreshSessionList(activityList1, true))
	assert.Equal(t, 4, len(s.sessMap), "every -1 session must get its own -pid key")

	// real session is keyed by sess_id
	if valS, okS := s.sessMap[SessionKey{SessID: 42}]; assert.True(t, okS) {
		assert.Equal(t, 100, valS.SessionData.GpStatInfo.Pid)
	}

	// system processes are keyed by -pid
	for _, pid := range []int{19702, 19703, 19705} {
		valS, okS := s.sessMap[SessionKey{SessID: -pid}]
		if assert.Truef(t, okS, "expected entry for system process pid=%d at key SessID=%d", pid, -pid) {
			assert.Equal(t, -1, valS.SessionData.GpStatInfo.SessID)
			assert.Equal(t, pid, valS.SessionData.GpStatInfo.Pid)
		}
	}
	// nothing must be stored under the literal SessID=-1
	_, okS := s.sessMap[SessionKey{SessID: -1}]
	assert.False(t, okS, "no entry must collide on the literal SessID=-1 key")

	// A second refresh with the SAME system processes must keep them
	// (i.e. clearDeletedSessions must not thrash entries because the
	// refresh-key derivation matches the insert-key derivation).
	require.NoError(t, s.RefreshSessionList(activityList1, true))
	assert.Equal(t, 4, len(s.sessMap), "stable refresh must not delete and recreate -pid entries")

	// Drop one system process (pid=19703 / walwriter) and verify only that
	// entry is removed, the other -pid entries are preserved, and the
	// remaining sessions are still keyed by -pid.
	activityList2 := []*GpStatActivity{
		{DatID: 1, Datname: "test", Pid: 100, SessID: 42, TmID: 100},
		{DatID: 1, Datname: "test", Pid: 19702, SessID: -1, TmID: 100},
		{DatID: 1, Datname: "test", Pid: 19705, SessID: -1, TmID: 100},
	}

	require.NoError(t, s.RefreshSessionList(activityList2, true))
	assert.Equal(t, 3, len(s.sessMap))

	_, okS = s.sessMap[SessionKey{SessID: -19703}]
	assert.False(t, okS, "removed system process must be evicted by clearDeletedSessions")
	_, okS = s.sessMap[SessionKey{SessID: -19702}]
	assert.True(t, okS)
	_, okS = s.sessMap[SessionKey{SessID: -19705}]
	assert.True(t, okS)
	_, okS = s.sessMap[SessionKey{SessID: 42}]
	assert.True(t, okS)
}

func TestRegisterNewSessionQuery(t *testing.T) {
	file, err := os.Create("trace.log")
	require.NoError(t, err)
	zLogger := utils.DualLog(true, file)
	if err != nil {
		t.Errorf("%v", err)
	}
	s := NewSessionsStorage(zLogger, nil, nil)
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

	err = s.RefreshSessionList(activityList, true)
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

	err = s.RefreshSessionList(activityList, true)
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
	file, err := os.Create("trace.log")
	require.NoError(t, err)
	zLogger := utils.DualLog(true, file)
	if err != nil {
		t.Errorf("%v", err)
	}
	s := NewSessionsStorage(zLogger, nil, nil)
	qKey := pbc.QueryKey{Ssid: 1, Tmid: 100, Ccnt: 1}
	qInfo := pbc.QueryInfo{QueryId: 1234, QueryText: "Select 1"}
	err = s.UpdateSessionQuery(&qKey, &qInfo, 0, &pbc.AdditionalQueryInfo{NestedLevel: 0}, false)
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

func TestUpdateSessionStatAggregatedMetricsOnlyNestingLevelZero(t *testing.T) {
	file, err := os.Create("trace.log")
	require.NoError(t, err)
	zLogger := utils.DualLog(true, file)
	if err != nil {
		t.Errorf("%v", err)
	}
	s := NewSessionsStorage(zLogger, nil, nil)
	qKey := &pbc.QueryKey{Ssid: 1, Tmid: 100, Ccnt: 1}
	qInfo := &pbc.QueryInfo{QueryId: 1234, QueryText: "Select 1"}
	require.NoError(t, s.UpdateSessionQuery(qKey, qInfo, 0, &pbc.AdditionalQueryInfo{NestedLevel: 0}, false))

	t0 := time.Unix(1000, 0)
	qStat := func(d time.Duration) *pbm.QueryStat {
		return &pbm.QueryStat{
			QueryKey:          qKey,
			StartTime:         timestamppb.New(t0),
			EndTime:           timestamppb.New(t0.Add(d)),
			TotalQueryMetrics: &pbc.GPMetrics{},
		}
	}

	require.NoError(t, s.UpdateSessionStat(qStat(5*time.Second), 1))
	valS, ok := s.GetSession(SessionKey{SessID: 1})
	require.True(t, ok)
	require.NotNil(t, valS.SessionData.AggregatedMetrics)
	assert.Equal(t, int64(0), valS.SessionData.AggregatedMetrics.Calls,
		"nested_level!=0 must not contribute to session AggregatedMetrics")

	require.NoError(t, s.UpdateSessionStat(qStat(2*time.Second), 0))
	valS, _ = s.GetSession(SessionKey{SessID: 1})
	assert.Equal(t, int64(1), valS.SessionData.AggregatedMetrics.Calls)
	assert.Equal(t, float64(2*time.Second), valS.SessionData.AggregatedMetrics.TotalTime)

	require.NoError(t, s.UpdateSessionStat(qStat(3*time.Second), 2))
	valS, _ = s.GetSession(SessionKey{SessID: 1})
	assert.Equal(t, int64(1), valS.SessionData.AggregatedMetrics.Calls,
		"higher nesting_level must not add another aggregated call")
	assert.Equal(t, float64(2*time.Second), valS.SessionData.AggregatedMetrics.TotalTime)

	require.NoError(t, s.UpdateSessionStat(qStat(time.Second), 0))
	valS, _ = s.GetSession(SessionKey{SessID: 1})
	assert.Equal(t, int64(2), valS.SessionData.AggregatedMetrics.Calls)
	assert.Equal(t, float64(2*time.Second+time.Second), valS.SessionData.AggregatedMetrics.TotalTime)
}

func TestCanLock(t *testing.T) {
	file, err := os.Create("trace.log")
	require.NoError(t, err)
	zLogger := utils.DualLog(true, file)
	if err != nil {
		t.Errorf("%v", err)
	}
	s := NewSessionsStorage(zLogger, nil, nil)
	assert.Equal(t, true, s.CanLock())
	s.mx.Lock()
	defer s.mx.Unlock()
	assert.Equal(t, false, s.CanLock())
}

func TestQueriesCount(t *testing.T) {
	file, err := os.Create("trace.log")
	require.NoError(t, err)
	zLogger := utils.DualLog(true, file)
	if err != nil {
		t.Errorf("%v", err)
	}
	s := NewSessionsStorage(zLogger, nil, nil)
	assert.Equal(t, 0, s.SessionsCount())
	s.RegisterNewSessionQuery(nil, false, &pbc.QueryKey{Ssid: 1}, nil, 0)
	assert.Equal(t, 1, s.SessionsCount())
}

func TestGetSession(t *testing.T) {
	file, err := os.Create("trace.log")
	require.NoError(t, err)
	zLogger := utils.DualLog(true, file)
	if err != nil {
		t.Errorf("%v", err)
	}
	s := NewSessionsStorage(zLogger, nil, nil)
	s.RegisterNewSessionQuery(nil, false, &pbc.QueryKey{Ssid: 1}, nil, 0)
	val, ok := s.GetSession(SessionKey{SessID: 1})
	assert.Equal(t, ok, true)
	assert.NotNil(t, val)
	_, ok = s.GetSession(SessionKey{SessID: 2})
	assert.Equal(t, ok, false)
}

func TestGetSessions(t *testing.T) {
	file, err := os.Create("trace.log")
	require.NoError(t, err)
	zLogger := utils.DualLog(true, file)
	if err != nil {
		t.Errorf("%v", err)
	}
	s := NewSessionsStorage(zLogger, nil, nil)
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

// --- procfsStatToLastStat ---

func TestProcfsStatToLastStat_NilInput(t *testing.T) {
	result, err := procfsStatToLastStat(nil)
	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "Empty procfs info")
}

func TestProcfsStatToLastStat_EmptyProcInfo(t *testing.T) {
	// GpPidProcInfo with no ProcStat and no ProcIo
	info := &pbc.GpPidProcInfo{}
	result, err := procfsStatToLastStat(info)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.SystemStat)
	// All fields should be zero
	assert.Equal(t, float64(0), result.SystemStat.UserTimeSeconds)
	assert.Equal(t, float64(0), result.SystemStat.KernelTimeSeconds)
	assert.Equal(t, uint64(0), result.SystemStat.Vsize)
	assert.Equal(t, uint64(0), result.SystemStat.Rss)
}

func TestProcfsStatToLastStat_WithProcStat(t *testing.T) {
	info := &pbc.GpPidProcInfo{
		ProcStat: &pbc.ProcStat{
			Utime: 100,
			Stime: 50,
			Vsize: 2048,
			Rss:   1024,
		},
	}
	result, err := procfsStatToLastStat(info)
	require.NoError(t, err)
	require.NotNil(t, result.SystemStat)
	assert.Equal(t, float64(100), result.SystemStat.UserTimeSeconds)
	assert.Equal(t, float64(50), result.SystemStat.KernelTimeSeconds)
	assert.Equal(t, uint64(2048), result.SystemStat.Vsize)
	assert.Equal(t, uint64(2048*1024), result.SystemStat.VmSizeKb)
	assert.Equal(t, uint64(1024), result.SystemStat.Rss)
}

func TestProcfsStatToLastStat_WithProcIO(t *testing.T) {
	info := &pbc.GpPidProcInfo{
		ProcIo: &pbc.ProcIO{
			Rchar:               500,
			Wchar:               300,
			Syscr:               10,
			Syscw:               8,
			ReadBytes:           1000,
			WriteBytes:          800,
			CancelledWriteBytes: 50,
		},
	}
	result, err := procfsStatToLastStat(info)
	require.NoError(t, err)
	require.NotNil(t, result.SystemStat)
	assert.Equal(t, uint64(500), result.SystemStat.Rchar)
	assert.Equal(t, uint64(300), result.SystemStat.Wchar)
	assert.Equal(t, uint64(10), result.SystemStat.Syscr)
	assert.Equal(t, uint64(8), result.SystemStat.Syscw)
	assert.Equal(t, uint64(1000), result.SystemStat.ReadBytes)
	assert.Equal(t, uint64(800), result.SystemStat.WriteBytes)
	assert.Equal(t, uint64(50), result.SystemStat.CancelledWriteBytes)
}

func TestProcfsStatToLastStat_WithBothProcStatAndProcIO(t *testing.T) {
	info := &pbc.GpPidProcInfo{
		ProcStat: &pbc.ProcStat{Utime: 42, Stime: 21, Vsize: 100, Rss: 50},
		ProcIo:   &pbc.ProcIO{ReadBytes: 999, WriteBytes: 888},
	}
	result, err := procfsStatToLastStat(info)
	require.NoError(t, err)
	require.NotNil(t, result.SystemStat)
	assert.Equal(t, float64(42), result.SystemStat.UserTimeSeconds)
	assert.Equal(t, float64(21), result.SystemStat.KernelTimeSeconds)
	assert.Equal(t, uint64(999), result.SystemStat.ReadBytes)
	assert.Equal(t, uint64(888), result.SystemStat.WriteBytes)
}

// --- RecalculateProcfsUsage ---

func newTestSessionsStorage(t *testing.T, procfsStorage *storage.ProcfsStorage) *SessionsStorage {
	t.Helper()
	file, err := os.Create("trace.log")
	require.NoError(t, err)
	zLogger := utils.DualLog(true, file)
	return NewSessionsStorage(zLogger, nil, procfsStorage)
}

func TestRecalculateProcfsUsage_EmptyProcfsStorage(t *testing.T) {
	// ProcfsStorage has no data — RecalculateProcfsUsage should return error
	procfsStore := storage.NewProcfsStorage()
	s := newTestSessionsStorage(t, procfsStore)

	// Add a session
	activityList := []*GpStatActivity{
		{DatID: 1, Datname: "test", Pid: 10, SessID: 1, TmID: 100},
	}
	require.NoError(t, s.RefreshSessionList(activityList, true))

	err := s.RecalculateProcfsUsage()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fail in get 5 min")
}

func TestRecalculateProcfsUsage_NoSessions(t *testing.T) {
	// No sessions in storage — should succeed with no updates
	procfsStore := storage.NewProcfsStorage()
	procfsStore.RegisterProcfsStat(time.Now(), []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 1, Pid: 10, ProcStat: &pbc.ProcStat{Utime: 100}},
	})
	s := newTestSessionsStorage(t, procfsStore)

	err := s.RecalculateProcfsUsage()
	require.NoError(t, err)
}

func TestRecalculateProcfsUsage_UpdatesLongRunningGPMetrics(t *testing.T) {
	procfsStore := storage.NewProcfsStorage()
	// Register two snapshots so get5Min works
	base := time.Now().Add(-time.Second)
	procfsStore.RegisterProcfsStat(base, []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 1, Pid: 10,
			ProcStat: &pbc.ProcStat{Utime: 10, Stime: 5, Vsize: 100, Rss: 50},
			ProcIo:   &pbc.ProcIO{ReadBytes: 100, WriteBytes: 50}},
	})
	procfsStore.RegisterProcfsStat(base.Add(time.Second), []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 1, Pid: 10,
			ProcStat: &pbc.ProcStat{Utime: 30, Stime: 15, Vsize: 200, Rss: 100},
			ProcIo:   &pbc.ProcIO{ReadBytes: 500, WriteBytes: 250}},
	})

	s := newTestSessionsStorage(t, procfsStore)
	activityList := []*GpStatActivity{
		{DatID: 1, Datname: "test", Pid: 10, SessID: 1, TmID: 100},
	}
	require.NoError(t, s.RefreshSessionList(activityList, true))

	// Verify LongRunningGPMetrics is initially empty
	valS, ok := s.GetSession(SessionKey{SessID: 1})
	require.True(t, ok)
	valS.SessionLock.RLock()
	assert.Nil(t, valS.SessionData.LongRunningGPMetrics.SystemStat)
	valS.SessionLock.RUnlock()

	// Run RecalculateProcfsUsage
	err := s.RecalculateProcfsUsage()
	require.NoError(t, err)

	// Verify LongRunningGPMetrics was updated
	valS, ok = s.GetSession(SessionKey{SessID: 1})
	require.True(t, ok)
	valS.SessionLock.RLock()
	defer valS.SessionLock.RUnlock()
	require.NotNil(t, valS.SessionData.LongRunningGPMetrics)
	require.NotNil(t, valS.SessionData.LongRunningGPMetrics.SystemStat)
	// Diff: Utime=30-10=20, Stime=15-5=10
	assert.Equal(t, float64(20), valS.SessionData.LongRunningGPMetrics.SystemStat.UserTimeSeconds)
	assert.Equal(t, float64(10), valS.SessionData.LongRunningGPMetrics.SystemStat.KernelTimeSeconds)
	// Diff: ReadBytes=500-100=400, WriteBytes=250-50=200
	assert.Equal(t, uint64(400), valS.SessionData.LongRunningGPMetrics.SystemStat.ReadBytes)
	assert.Equal(t, uint64(200), valS.SessionData.LongRunningGPMetrics.SystemStat.WriteBytes)
	// Vsize and Rss are snapshot values from the diff (last snapshot values)
	assert.Equal(t, uint64(200), valS.SessionData.LongRunningGPMetrics.SystemStat.Vsize)
	assert.Equal(t, uint64(100), valS.SessionData.LongRunningGPMetrics.SystemStat.Rss)
}

func TestRecalculateProcfsUsage_MultipleSessions(t *testing.T) {
	procfsStore := storage.NewProcfsStorage()
	base := time.Now().Add(-time.Second)
	procfsStore.RegisterProcfsStat(base, []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 1, Pid: 10,
			ProcStat: &pbc.ProcStat{Utime: 0}, ProcIo: &pbc.ProcIO{ReadBytes: 0}},
		{GpSegmentId: 0, SessId: 2, Pid: 20,
			ProcStat: &pbc.ProcStat{Utime: 0}, ProcIo: &pbc.ProcIO{ReadBytes: 0}},
	})
	procfsStore.RegisterProcfsStat(base.Add(time.Second), []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 1, Pid: 10,
			ProcStat: &pbc.ProcStat{Utime: 100}, ProcIo: &pbc.ProcIO{ReadBytes: 500}},
		{GpSegmentId: 0, SessId: 2, Pid: 20,
			ProcStat: &pbc.ProcStat{Utime: 200}, ProcIo: &pbc.ProcIO{ReadBytes: 600}},
	})

	s := newTestSessionsStorage(t, procfsStore)
	activityList := []*GpStatActivity{
		{DatID: 1, Datname: "test", Pid: 10, SessID: 1, TmID: 100},
		{DatID: 1, Datname: "test", Pid: 20, SessID: 2, TmID: 100},
	}
	require.NoError(t, s.RefreshSessionList(activityList, true))

	require.NoError(t, s.RecalculateProcfsUsage())

	// Check session 1
	valS1, ok := s.GetSession(SessionKey{SessID: 1})
	require.True(t, ok)
	valS1.SessionLock.RLock()
	require.NotNil(t, valS1.SessionData.LongRunningGPMetrics.SystemStat)
	assert.Equal(t, float64(100), valS1.SessionData.LongRunningGPMetrics.SystemStat.UserTimeSeconds)
	assert.Equal(t, uint64(500), valS1.SessionData.LongRunningGPMetrics.SystemStat.ReadBytes)
	valS1.SessionLock.RUnlock()

	// Check session 2
	valS2, ok := s.GetSession(SessionKey{SessID: 2})
	require.True(t, ok)
	valS2.SessionLock.RLock()
	require.NotNil(t, valS2.SessionData.LongRunningGPMetrics.SystemStat)
	assert.Equal(t, float64(200), valS2.SessionData.LongRunningGPMetrics.SystemStat.UserTimeSeconds)
	assert.Equal(t, uint64(600), valS2.SessionData.LongRunningGPMetrics.SystemStat.ReadBytes)
	valS2.SessionLock.RUnlock()
}

func TestRecalculateProcfsUsage_SessionWithoutProcfsData(t *testing.T) {
	// Session 1 has procfs data, session 2 does not
	procfsStore := storage.NewProcfsStorage()
	base := time.Now().Add(-time.Second)
	procfsStore.RegisterProcfsStat(base, []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 1, Pid: 10,
			ProcStat: &pbc.ProcStat{Utime: 0}},
	})
	procfsStore.RegisterProcfsStat(base.Add(time.Second), []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 1, Pid: 10,
			ProcStat: &pbc.ProcStat{Utime: 50}},
	})

	s := newTestSessionsStorage(t, procfsStore)
	activityList := []*GpStatActivity{
		{DatID: 1, Datname: "test", Pid: 10, SessID: 1, TmID: 100},
		{DatID: 1, Datname: "test", Pid: 20, SessID: 2, TmID: 100},
	}
	require.NoError(t, s.RefreshSessionList(activityList, true))

	require.NoError(t, s.RecalculateProcfsUsage())

	// Session 1 should be updated
	valS1, ok := s.GetSession(SessionKey{SessID: 1})
	require.True(t, ok)
	valS1.SessionLock.RLock()
	require.NotNil(t, valS1.SessionData.LongRunningGPMetrics.SystemStat)
	assert.Equal(t, float64(50), valS1.SessionData.LongRunningGPMetrics.SystemStat.UserTimeSeconds)
	valS1.SessionLock.RUnlock()

	// Session 2 should NOT be updated (no procfs data)
	valS2, ok := s.GetSession(SessionKey{SessID: 2})
	require.True(t, ok)
	valS2.SessionLock.RLock()
	assert.Nil(t, valS2.SessionData.LongRunningGPMetrics.SystemStat)
	valS2.SessionLock.RUnlock()
}

func TestRecalculateProcfsUsage_OverwritesPreviousMetrics(t *testing.T) {
	procfsStore := storage.NewProcfsStorage()
	base := time.Now().Add(-2 * time.Second)
	procfsStore.RegisterProcfsStat(base, []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 1, Pid: 10,
			ProcStat: &pbc.ProcStat{Utime: 0}, ProcIo: &pbc.ProcIO{ReadBytes: 0}},
	})
	procfsStore.RegisterProcfsStat(base.Add(time.Second), []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 1, Pid: 10,
			ProcStat: &pbc.ProcStat{Utime: 100}, ProcIo: &pbc.ProcIO{ReadBytes: 500}},
	})

	s := newTestSessionsStorage(t, procfsStore)
	activityList := []*GpStatActivity{
		{DatID: 1, Datname: "test", Pid: 10, SessID: 1, TmID: 100},
	}
	require.NoError(t, s.RefreshSessionList(activityList, true))

	// First recalculation
	require.NoError(t, s.RecalculateProcfsUsage())
	valS, ok := s.GetSession(SessionKey{SessID: 1})
	require.True(t, ok)
	valS.SessionLock.RLock()
	assert.Equal(t, float64(100), valS.SessionData.LongRunningGPMetrics.SystemStat.UserTimeSeconds)
	valS.SessionLock.RUnlock()

	// Register new snapshot with higher values
	procfsStore.RegisterProcfsStat(base.Add(2*time.Second), []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 1, Pid: 10,
			ProcStat: &pbc.ProcStat{Utime: 300}, ProcIo: &pbc.ProcIO{ReadBytes: 1500}},
	})

	// Second recalculation should overwrite previous metrics
	require.NoError(t, s.RecalculateProcfsUsage())
	valS, ok = s.GetSession(SessionKey{SessID: 1})
	require.True(t, ok)
	valS.SessionLock.RLock()
	// get5Min picks first=base (nearest to 5min ago), last=base+2s (latest)
	// New diff: Utime=300-0=300, ReadBytes=1500-0=1500
	assert.Equal(t, float64(300), valS.SessionData.LongRunningGPMetrics.SystemStat.UserTimeSeconds)
	assert.Equal(t, uint64(1500), valS.SessionData.LongRunningGPMetrics.SystemStat.ReadBytes)
	valS.SessionLock.RUnlock()
}
