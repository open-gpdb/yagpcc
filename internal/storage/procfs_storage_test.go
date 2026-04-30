package storage

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
)

func buildProcfsStorage(seconds ...float64) *ProcfsStorage {
	ps := &ProcfsStorage{
		mx:                  &sync.RWMutex{},
		maximumStoredPoints: defaultStoredPoints,
		procfsStat:          make([]ProcfsStatType, 0, len(seconds)),
	}
	for i, s := range seconds {
		pm := ProcMap{
			ProcKey{GpSegmentId: int64(i)}: &ProcStat{Cmdline: "cmd"},
		}
		ps.procfsStat = append(ps.procfsStat, ProcfsStatType{
			statTime:    time.Unix(0, int64(s*float64(time.Second))),
			pidProcData: pm,
		})
	}
	return ps
}

func identifyIndex(ps *ProcfsStorage, pm ProcMap) int {
	for i, s := range ps.procfsStat {
		if len(s.pidProcData) == len(pm) {
			match := true
			for k, v := range s.pidProcData {
				if pm[k] != v {
					match = false
					break
				}
			}
			if match {
				return i
			}
		}
	}
	return -1
}

func TestGetNearestNTime_EmptyStorage(t *testing.T) {
	ps := NewProcfsStorage()
	_, err := ps.GetNearestNTime(time.Second)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no data in procfsStat")
}

func TestGetNearestNTime_SingleElement(t *testing.T) {
	ps := buildProcfsStorage(5.0)
	pm, err := ps.GetNearestNTime(0)
	require.NoError(t, err)
	assert.Equal(t, 0, identifyIndex(ps, pm))
	pm, err = ps.GetNearestNTime(time.Second)
	require.NoError(t, err)
	assert.Equal(t, 0, identifyIndex(ps, pm))
	pm, err = ps.GetNearestNTime(10 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, 0, identifyIndex(ps, pm))
}

func TestGetNearestNTime_ExampleFromDescription(t *testing.T) {
	ps := buildProcfsStorage(1, 2, 3, 4.1, 4.2, 5)
	pm, err := ps.GetNearestNTime(time.Second)
	require.NoError(t, err)
	assert.Equal(t, 3, identifyIndex(ps, pm))
}

func TestGetNearestNTime_ExactMatch(t *testing.T) {
	ps := buildProcfsStorage(0, 1, 2, 3, 4, 5)
	pm, err := ps.GetNearestNTime(3 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, 2, identifyIndex(ps, pm))
}

func TestGetNearestNTime_ZeroDuration(t *testing.T) {
	ps := buildProcfsStorage(1, 2, 3, 4, 5)
	pm, err := ps.GetNearestNTime(0)
	require.NoError(t, err)
	assert.Equal(t, 4, identifyIndex(ps, pm))
}

func TestGetNearestNTime_DurationLargerThanRange(t *testing.T) {
	ps := buildProcfsStorage(10, 11, 12)
	pm, err := ps.GetNearestNTime(100 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, 0, identifyIndex(ps, pm))
}

func TestGetNearestNTime_TwoElements(t *testing.T) {
	ps := buildProcfsStorage(0, 10)
	pm, err := ps.GetNearestNTime(3 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, 1, identifyIndex(ps, pm))
	pm, err = ps.GetNearestNTime(7 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, 0, identifyIndex(ps, pm))
	pm, err = ps.GetNearestNTime(5 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, 0, identifyIndex(ps, pm))
}

func TestGetNearestNTime_CloselySpacedTimes(t *testing.T) {
	ps := buildProcfsStorage(10.0, 10.1, 10.2, 10.3, 10.4, 10.5)
	pm, err := ps.GetNearestNTime(250 * time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, 2, identifyIndex(ps, pm))
}

func TestGetNearestNTime_AllSameTime(t *testing.T) {
	ps := buildProcfsStorage(5, 5, 5, 5)
	pm, err := ps.GetNearestNTime(time.Second)
	require.NoError(t, err)
	assert.Equal(t, 0, identifyIndex(ps, pm))
}

func TestGetNearestNTime_ReturnsCorrectProcMap(t *testing.T) {
	ps := buildProcfsStorage(1, 2, 3, 4, 5)
	pm, err := ps.GetNearestNTime(2 * time.Second)
	require.NoError(t, err)
	_, ok := pm[ProcKey{GpSegmentId: 2}]
	assert.True(t, ok)
}

func TestGetNearestNTime_FirstElementIsNearest(t *testing.T) {
	ps := buildProcfsStorage(0, 1, 2)
	pm, err := ps.GetNearestNTime(2 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, 0, identifyIndex(ps, pm))
}

func TestGetNearestNTime_LastElementIsNearest(t *testing.T) {
	ps := buildProcfsStorage(0, 1, 2)
	pm, err := ps.GetNearestNTime(0)
	require.NoError(t, err)
	assert.Equal(t, 2, identifyIndex(ps, pm))
}

func TestGetNearestNTime_MiddleElementIsNearest(t *testing.T) {
	ps := buildProcfsStorage(0, 5, 10)
	pm, err := ps.GetNearestNTime(5 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, 1, identifyIndex(ps, pm))
}

func TestGetNearestNTime_LargeSlice(t *testing.T) {
	seconds := make([]float64, 100)
	for i := range seconds {
		seconds[i] = float64(i)
	}
	ps := buildProcfsStorage(seconds...)
	pm, err := ps.GetNearestNTime(50 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, 49, identifyIndex(ps, pm))
	pm, err = ps.GetNearestNTime(0)
	require.NoError(t, err)
	assert.Equal(t, 99, identifyIndex(ps, pm))
	pm, err = ps.GetNearestNTime(200 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, 0, identifyIndex(ps, pm))
}

func TestGetNearestNTime_NonUniformSpacing(t *testing.T) {
	ps := buildProcfsStorage(0, 1, 2, 8, 9, 10)
	pm, err := ps.GetNearestNTime(5 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, 2, identifyIndex(ps, pm))
}

func TestGetNearestNTime_SubSecondPrecision(t *testing.T) {
	ps := buildProcfsStorage(0, 0.5, 1.0, 1.5, 2.0)
	pm, err := ps.GetNearestNTime(700 * time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, 3, identifyIndex(ps, pm))
}

func TestAbsDuration(t *testing.T) {
	assert.Equal(t, time.Second, absDuration(time.Second))
	assert.Equal(t, time.Second, absDuration(-time.Second))
	assert.Equal(t, time.Duration(0), absDuration(0))
	assert.Equal(t, 500*time.Millisecond, absDuration(-500*time.Millisecond))
}

func TestNewProcfsStorage_MutexInitialized(t *testing.T) {
	ps := NewProcfsStorage()
	assert.NotNil(t, ps.mx)
	ps.mx.RLock()
	defer ps.mx.RUnlock()
}

// --- RegisterProcfsStat ---

func TestRegisterProcfsStat_SingleEntry(t *testing.T) {
	ps := NewProcfsStorage()
	now := time.Now()
	procs := []*pbc.GpPidProcInfo{{
		GpSegmentId: 1, SessId: 100, Pid: 42,
		Cmdline: "SELECT 1", State: "R",
		ProcStat: &pbc.ProcStat{Utime: 10}, ProcStatus: &pbc.ProcStatus{VmRss: 1024},
		ProcIo: &pbc.ProcIO{ReadBytes: 512},
	}}
	ps.RegisterProcfsStat(now, procs)
	ps.mx.RLock()
	defer ps.mx.RUnlock()
	require.Len(t, ps.procfsStat, 1)
	assert.Equal(t, now, ps.procfsStat[0].statTime)
	key := ProcKey{GpSegmentId: 1, SessId: 100, Pid: 42}
	stat, ok := ps.procfsStat[0].pidProcData[key]
	require.True(t, ok)
	assert.Equal(t, "SELECT 1", stat.Cmdline)
	assert.Equal(t, "R", stat.State)
	assert.Equal(t, int64(10), stat.ProcStat.Utime)
	assert.Equal(t, int64(1024), stat.ProcStatus.VmRss)
	assert.Equal(t, int64(512), stat.ProcIO.ReadBytes)
}

func TestRegisterProcfsStat_MultipleProcesses(t *testing.T) {
	ps := NewProcfsStorage()
	now := time.Now()
	procs := []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 1, Pid: 10, Cmdline: "cmd1", State: "S"},
		{GpSegmentId: 0, SessId: 1, Pid: 20, Cmdline: "cmd2", State: "R"},
		{GpSegmentId: 1, SessId: 2, Pid: 30, Cmdline: "cmd3", State: "D"},
	}
	ps.RegisterProcfsStat(now, procs)
	ps.mx.RLock()
	defer ps.mx.RUnlock()
	require.Len(t, ps.procfsStat, 1)
	pm := ps.procfsStat[0].pidProcData
	assert.Len(t, pm, 3)
	assert.Equal(t, "cmd1", pm[ProcKey{GpSegmentId: 0, SessId: 1, Pid: 10}].Cmdline)
	assert.Equal(t, "cmd2", pm[ProcKey{GpSegmentId: 0, SessId: 1, Pid: 20}].Cmdline)
	assert.Equal(t, "cmd3", pm[ProcKey{GpSegmentId: 1, SessId: 2, Pid: 30}].Cmdline)
}

func TestRegisterProcfsStat_EmptyProcList(t *testing.T) {
	ps := NewProcfsStorage()
	now := time.Now()
	ps.RegisterProcfsStat(now, []*pbc.GpPidProcInfo{})
	ps.mx.RLock()
	defer ps.mx.RUnlock()
	require.Len(t, ps.procfsStat, 1)
	assert.Empty(t, ps.procfsStat[0].pidProcData)
	assert.Equal(t, now, ps.procfsStat[0].statTime)
}

func TestRegisterProcfsStat_NilProcList(t *testing.T) {
	ps := NewProcfsStorage()
	now := time.Now()
	ps.RegisterProcfsStat(now, nil)
	ps.mx.RLock()
	defer ps.mx.RUnlock()
	require.Len(t, ps.procfsStat, 1)
	assert.Empty(t, ps.procfsStat[0].pidProcData)
}

func TestRegisterProcfsStat_MultipleRegistrations(t *testing.T) {
	ps := NewProcfsStorage()
	t1 := time.Now()
	t2 := t1.Add(time.Minute)
	t3 := t2.Add(time.Minute)
	ps.RegisterProcfsStat(t1, []*pbc.GpPidProcInfo{{GpSegmentId: 0, SessId: 1, Pid: 10, Cmdline: "first"}})
	ps.RegisterProcfsStat(t2, []*pbc.GpPidProcInfo{{GpSegmentId: 0, SessId: 1, Pid: 10, Cmdline: "second"}})
	ps.RegisterProcfsStat(t3, []*pbc.GpPidProcInfo{{GpSegmentId: 0, SessId: 1, Pid: 10, Cmdline: "third"}})
	ps.mx.RLock()
	defer ps.mx.RUnlock()
	require.Len(t, ps.procfsStat, 3)
	assert.Equal(t, t1, ps.procfsStat[0].statTime)
	assert.Equal(t, t2, ps.procfsStat[1].statTime)
	assert.Equal(t, t3, ps.procfsStat[2].statTime)
	key := ProcKey{GpSegmentId: 0, SessId: 1, Pid: 10}
	assert.Equal(t, "first", ps.procfsStat[0].pidProcData[key].Cmdline)
	assert.Equal(t, "second", ps.procfsStat[1].pidProcData[key].Cmdline)
	assert.Equal(t, "third", ps.procfsStat[2].pidProcData[key].Cmdline)
}

func TestRegisterProcfsStat_DuplicateKeysInSameBatch(t *testing.T) {
	ps := NewProcfsStorage()
	now := time.Now()
	procs := []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 1, Pid: 10, Cmdline: "first"},
		{GpSegmentId: 0, SessId: 1, Pid: 10, Cmdline: "second"},
	}
	ps.RegisterProcfsStat(now, procs)
	ps.mx.RLock()
	defer ps.mx.RUnlock()
	pm := ps.procfsStat[0].pidProcData
	assert.Len(t, pm, 1)
	assert.Equal(t, "second", pm[ProcKey{GpSegmentId: 0, SessId: 1, Pid: 10}].Cmdline)
}

func TestRegisterProcfsStat_NilSubFields(t *testing.T) {
	ps := NewProcfsStorage()
	now := time.Now()
	procs := []*pbc.GpPidProcInfo{{GpSegmentId: 0, SessId: 1, Pid: 10, Cmdline: "cmd", State: "S"}}
	ps.RegisterProcfsStat(now, procs)
	ps.mx.RLock()
	defer ps.mx.RUnlock()
	stat := ps.procfsStat[0].pidProcData[ProcKey{GpSegmentId: 0, SessId: 1, Pid: 10}]
	require.NotNil(t, stat)
	assert.Nil(t, stat.ProcStat)
	assert.Nil(t, stat.ProcStatus)
	assert.Nil(t, stat.ProcIO)
}

// --- TidyUpProcfsStat ---

func TestTidyUpProcfsStat_UnderLimit(t *testing.T) {
	ps := &ProcfsStorage{mx: &sync.RWMutex{}, maximumStoredPoints: 5, procfsStat: make([]ProcfsStatType, 0, 5)}
	for i := 0; i < 3; i++ {
		ps.procfsStat = append(ps.procfsStat, ProcfsStatType{statTime: time.Unix(int64(i), 0)})
	}
	ps.TidyUpProcfsStat()
	ps.mx.RLock()
	defer ps.mx.RUnlock()
	assert.Len(t, ps.procfsStat, 3)
}

func TestTidyUpProcfsStat_AtLimit(t *testing.T) {
	ps := &ProcfsStorage{mx: &sync.RWMutex{}, maximumStoredPoints: 3, procfsStat: make([]ProcfsStatType, 0, 3)}
	for i := 0; i < 3; i++ {
		ps.procfsStat = append(ps.procfsStat, ProcfsStatType{statTime: time.Unix(int64(i), 0)})
	}
	ps.TidyUpProcfsStat()
	ps.mx.RLock()
	defer ps.mx.RUnlock()
	assert.Len(t, ps.procfsStat, 3)
}

func TestTidyUpProcfsStat_OverLimit(t *testing.T) {
	ps := &ProcfsStorage{mx: &sync.RWMutex{}, maximumStoredPoints: 3, procfsStat: make([]ProcfsStatType, 0, 6)}
	for i := 0; i < 6; i++ {
		ps.procfsStat = append(ps.procfsStat, ProcfsStatType{statTime: time.Unix(int64(i), 0)})
	}
	ps.TidyUpProcfsStat()
	ps.mx.RLock()
	defer ps.mx.RUnlock()
	require.Len(t, ps.procfsStat, 3)
	assert.Equal(t, time.Unix(3, 0), ps.procfsStat[0].statTime)
	assert.Equal(t, time.Unix(4, 0), ps.procfsStat[1].statTime)
	assert.Equal(t, time.Unix(5, 0), ps.procfsStat[2].statTime)
}

func TestRegisterProcfsStat_TriggersCleanup(t *testing.T) {
	ps := &ProcfsStorage{mx: &sync.RWMutex{}, maximumStoredPoints: 3, procfsStat: make([]ProcfsStatType, 0, 5)}
	base := time.Now()
	for i := 0; i < 5; i++ {
		ps.RegisterProcfsStat(base.Add(time.Duration(i)*time.Minute), []*pbc.GpPidProcInfo{
			{GpSegmentId: int64(i), SessId: 1, Pid: 1, Cmdline: "cmd"},
		})
	}
	ps.mx.RLock()
	defer ps.mx.RUnlock()
	assert.Len(t, ps.procfsStat, 3)
	assert.Equal(t, base.Add(2*time.Minute), ps.procfsStat[0].statTime)
	assert.Equal(t, base.Add(3*time.Minute), ps.procfsStat[1].statTime)
	assert.Equal(t, base.Add(4*time.Minute), ps.procfsStat[2].statTime)
}

func TestTidyUpProcfsStat_EmptySlice(t *testing.T) {
	ps := NewProcfsStorage()
	ps.TidyUpProcfsStat()
	ps.mx.RLock()
	defer ps.mx.RUnlock()
	assert.Empty(t, ps.procfsStat)
}

// --- getNMin, Get5Min, Get15Min, Get30Min ---

func buildWithUniqueData(seconds ...float64) *ProcfsStorage {
	ps := &ProcfsStorage{
		mx:                  &sync.RWMutex{},
		maximumStoredPoints: defaultStoredPoints,
		procfsStat:          make([]ProcfsStatType, 0, len(seconds)),
	}
	for i, s := range seconds {
		pm := ProcMap{
			ProcKey{GpSegmentId: int64(i), SessId: int64(i), Pid: int64(i)}: &ProcStat{
				Cmdline: fmt.Sprintf("cmd-%d", i),
			},
		}
		ps.procfsStat = append(ps.procfsStat, ProcfsStatType{
			statTime:    time.Unix(0, int64(s*float64(time.Second))),
			pidProcData: pm,
		})
	}
	return ps
}

func findIdx(ps *ProcfsStorage, pm ProcMap) int {
	for i, s := range ps.procfsStat {
		if len(s.pidProcData) == len(pm) {
			match := true
			for k, v := range s.pidProcData {
				if pm[k] != v {
					match = false
					break
				}
			}
			if match {
				return i
			}
		}
	}
	return -1
}

func TestGetNMin_EmptyStorage(t *testing.T) {
	ps := NewProcfsStorage()
	nearest, latest, err := ps.getNMin(5 * time.Minute)
	require.Error(t, err)
	assert.Nil(t, nearest)
	assert.Nil(t, latest)
	assert.Contains(t, err.Error(), "fail in get 5 minutes interval")
}

func TestGetNMin_SingleElement(t *testing.T) {
	ps := buildWithUniqueData(100)
	nearest, latest, err := ps.getNMin(5 * time.Minute)
	require.NoError(t, err)
	assert.Equal(t, 0, findIdx(ps, nearest))
	assert.Equal(t, 0, findIdx(ps, latest))
}

func TestGetNMin_ReturnsNearestAndLatest(t *testing.T) {
	// 0,60,120,180,240,300; d=120s: last=300, nearest at diff=120 -> idx 3
	ps := buildWithUniqueData(0, 60, 120, 180, 240, 300)
	nearest, latest, err := ps.getNMin(120 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, 3, findIdx(ps, nearest))
	assert.Equal(t, 5, findIdx(ps, latest))
}

func TestGetNMin_ZeroDuration(t *testing.T) {
	ps := buildWithUniqueData(0, 10, 20)
	nearest, latest, err := ps.getNMin(0)
	require.NoError(t, err)
	assert.Equal(t, 2, findIdx(ps, nearest))
	assert.Equal(t, 2, findIdx(ps, latest))
}

func TestGetNMin_DurationExceedsRange(t *testing.T) {
	ps := buildWithUniqueData(0, 1, 2)
	nearest, latest, err := ps.getNMin(1000 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, 0, findIdx(ps, nearest))
	assert.Equal(t, 2, findIdx(ps, latest))
}

func TestGetNMin_DifferentMaps(t *testing.T) {
	ps := buildWithUniqueData(0, 60, 120, 180, 240, 300)
	nearest, latest, err := ps.getNMin(120 * time.Second)
	require.NoError(t, err)
	assert.NotEqual(t, findIdx(ps, nearest), findIdx(ps, latest))
}

func TestGet5Min_EmptyStorage(t *testing.T) {
	ps := NewProcfsStorage()
	n, l, err := ps.Get5Min()
	require.Error(t, err)
	assert.Nil(t, n)
	assert.Nil(t, l)
}

func TestGet5Min_ReturnsCorrectInterval(t *testing.T) {
	// 0,60,120,180,240,300,360,420; d=300s, last=420 -> nearest at 120 -> idx 2
	ps := buildWithUniqueData(0, 60, 120, 180, 240, 300, 360, 420)
	nearest, latest, err := ps.Get5Min()
	require.NoError(t, err)
	assert.Equal(t, 2, findIdx(ps, nearest))
	assert.Equal(t, 7, findIdx(ps, latest))
}

func TestGet5Min_SingleElement(t *testing.T) {
	ps := buildWithUniqueData(100)
	nearest, latest, err := ps.Get5Min()
	require.NoError(t, err)
	assert.Equal(t, 0, findIdx(ps, nearest))
	assert.Equal(t, 0, findIdx(ps, latest))
}

func TestGet15Min_EmptyStorage(t *testing.T) {
	ps := NewProcfsStorage()
	n, l, err := ps.Get15Min()
	require.Error(t, err)
	assert.Nil(t, n)
	assert.Nil(t, l)
}

func TestGet15Min_ReturnsCorrectInterval(t *testing.T) {
	// 0,300,600,900,1200,1500; d=900s, last=1500 -> nearest at 600 -> idx 2
	ps := buildWithUniqueData(0, 300, 600, 900, 1200, 1500)
	nearest, latest, err := ps.Get15Min()
	require.NoError(t, err)
	assert.Equal(t, 2, findIdx(ps, nearest))
	assert.Equal(t, 5, findIdx(ps, latest))
}

func TestGet15Min_RangeSmaller(t *testing.T) {
	ps := buildWithUniqueData(0, 60, 120)
	nearest, latest, err := ps.Get15Min()
	require.NoError(t, err)
	assert.Equal(t, 0, findIdx(ps, nearest))
	assert.Equal(t, 2, findIdx(ps, latest))
}

func TestGet30Min_EmptyStorage(t *testing.T) {
	ps := NewProcfsStorage()
	n, l, err := ps.Get30Min()
	require.Error(t, err)
	assert.Nil(t, n)
	assert.Nil(t, l)
}

func TestGet30Min_ReturnsCorrectInterval(t *testing.T) {
	// 0,600,1200,1800,2400,3000; d=1800s, last=3000 -> nearest at 1200 -> idx 2
	ps := buildWithUniqueData(0, 600, 1200, 1800, 2400, 3000)
	nearest, latest, err := ps.Get30Min()
	require.NoError(t, err)
	assert.Equal(t, 2, findIdx(ps, nearest))
	assert.Equal(t, 5, findIdx(ps, latest))
}

func TestGet30Min_RangeSmaller(t *testing.T) {
	ps := buildWithUniqueData(0, 60, 120, 180, 240, 300)
	nearest, latest, err := ps.Get30Min()
	require.NoError(t, err)
	assert.Equal(t, 0, findIdx(ps, nearest))
	assert.Equal(t, 5, findIdx(ps, latest))
}

func TestGet30Min_SingleElement(t *testing.T) {
	ps := buildWithUniqueData(100)
	nearest, latest, err := ps.Get30Min()
	require.NoError(t, err)
	assert.Equal(t, 0, findIdx(ps, nearest))
	assert.Equal(t, 0, findIdx(ps, latest))
}
