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
	_, err := ps.getNearestNTime(time.Second)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no data in procfsStat")
}

func TestGetNearestNTime_SingleElement(t *testing.T) {
	ps := buildProcfsStorage(5.0)
	st, err := ps.getNearestNTime(0)
	require.NoError(t, err)
	assert.Equal(t, 0, identifyIndex(ps, st.pidProcData))
	st, err = ps.getNearestNTime(time.Second)
	require.NoError(t, err)
	assert.Equal(t, 0, identifyIndex(ps, st.pidProcData))
	st, err = ps.getNearestNTime(10 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, 0, identifyIndex(ps, st.pidProcData))
}

func TestGetNearestNTime_ExampleFromDescription(t *testing.T) {
	ps := buildProcfsStorage(1, 2, 3, 4.1, 4.2, 5)
	st, err := ps.getNearestNTime(time.Second)
	require.NoError(t, err)
	assert.Equal(t, 3, identifyIndex(ps, st.pidProcData))
}

func TestGetNearestNTime_ExactMatch(t *testing.T) {
	ps := buildProcfsStorage(0, 1, 2, 3, 4, 5)
	st, err := ps.getNearestNTime(3 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, 2, identifyIndex(ps, st.pidProcData))
}

func TestGetNearestNTime_ZeroDuration(t *testing.T) {
	ps := buildProcfsStorage(1, 2, 3, 4, 5)
	st, err := ps.getNearestNTime(0)
	require.NoError(t, err)
	assert.Equal(t, 4, identifyIndex(ps, st.pidProcData))
}

func TestGetNearestNTime_DurationLargerThanRange(t *testing.T) {
	ps := buildProcfsStorage(10, 11, 12)
	st, err := ps.getNearestNTime(100 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, 0, identifyIndex(ps, st.pidProcData))
}

func TestGetNearestNTime_TwoElements(t *testing.T) {
	ps := buildProcfsStorage(0, 10)
	st, err := ps.getNearestNTime(3 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, 1, identifyIndex(ps, st.pidProcData))
	st, err = ps.getNearestNTime(7 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, 0, identifyIndex(ps, st.pidProcData))
	st, err = ps.getNearestNTime(5 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, 0, identifyIndex(ps, st.pidProcData))
}

func TestGetNearestNTime_CloselySpacedTimes(t *testing.T) {
	ps := buildProcfsStorage(10.0, 10.1, 10.2, 10.3, 10.4, 10.5)
	st, err := ps.getNearestNTime(250 * time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, 2, identifyIndex(ps, st.pidProcData))
}

func TestGetNearestNTime_AllSameTime(t *testing.T) {
	ps := buildProcfsStorage(5, 5, 5, 5)
	st, err := ps.getNearestNTime(time.Second)
	require.NoError(t, err)
	assert.Equal(t, 0, identifyIndex(ps, st.pidProcData))
}

func TestGetNearestNTime_ReturnsCorrectProcMap(t *testing.T) {
	ps := buildProcfsStorage(1, 2, 3, 4, 5)
	st, err := ps.getNearestNTime(2 * time.Second)
	require.NoError(t, err)
	_, ok := st.pidProcData[ProcKey{GpSegmentId: 2}]
	assert.True(t, ok)
}

func TestGetNearestNTime_FirstElementIsNearest(t *testing.T) {
	ps := buildProcfsStorage(0, 1, 2)
	st, err := ps.getNearestNTime(2 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, 0, identifyIndex(ps, st.pidProcData))
}

func TestGetNearestNTime_LastElementIsNearest(t *testing.T) {
	ps := buildProcfsStorage(0, 1, 2)
	st, err := ps.getNearestNTime(0)
	require.NoError(t, err)
	assert.Equal(t, 2, identifyIndex(ps, st.pidProcData))
}

func TestGetNearestNTime_MiddleElementIsNearest(t *testing.T) {
	ps := buildProcfsStorage(0, 5, 10)
	st, err := ps.getNearestNTime(5 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, 1, identifyIndex(ps, st.pidProcData))
}

func TestGetNearestNTime_LargeSlice(t *testing.T) {
	seconds := make([]float64, 100)
	for i := range seconds {
		seconds[i] = float64(i)
	}
	ps := buildProcfsStorage(seconds...)
	st, err := ps.getNearestNTime(50 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, 49, identifyIndex(ps, st.pidProcData))
	st, err = ps.getNearestNTime(0)
	require.NoError(t, err)
	assert.Equal(t, 99, identifyIndex(ps, st.pidProcData))
	st, err = ps.getNearestNTime(200 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, 0, identifyIndex(ps, st.pidProcData))
}

func TestGetNearestNTime_NonUniformSpacing(t *testing.T) {
	ps := buildProcfsStorage(0, 1, 2, 8, 9, 10)
	st, err := ps.getNearestNTime(5 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, 2, identifyIndex(ps, st.pidProcData))
}

func TestGetNearestNTime_SubSecondPrecision(t *testing.T) {
	ps := buildProcfsStorage(0, 0.5, 1.0, 1.5, 2.0)
	st, err := ps.getNearestNTime(700 * time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, 3, identifyIndex(ps, st.pidProcData))
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
	assert.Nil(t, stat.ProcSpill)
}

func TestRegisterProcfsStat_ProcSpillStoredAndReturned(t *testing.T) {
	ps := NewProcfsStorage()
	now := time.Now()
	procs := []*pbc.GpPidProcInfo{{
		GpSegmentId: 2, SessId: 50, Pid: 99,
		Cmdline: "spilly", State: "R",
		ProcIo:    &pbc.ProcIO{ReadBytes: 1024},
		ProcSpill: &pbc.ProcSpill{Size: 5_000_000, Files: 12},
	}}
	ps.RegisterProcfsStat(now, procs)

	ps.mx.RLock()
	defer ps.mx.RUnlock()
	key := ProcKey{GpSegmentId: 2, SessId: 50, Pid: 99}
	stat, ok := ps.procfsStat[0].pidProcData[key]
	require.True(t, ok)
	require.NotNil(t, stat.ProcSpill, "ProcSpill should be stored in the internal ProcStat")
	assert.Equal(t, int64(5_000_000), stat.ProcSpill.Size)
	assert.Equal(t, int64(12), stat.ProcSpill.Files)
}

func TestToGpPidProcInfo_ProcSpillRoundTrip(t *testing.T) {
	spill := &pbc.ProcSpill{Size: 8_000_000, Files: 7}
	ps := &ProcStat{
		Cmdline:    "cmd",
		State:      "R",
		ProcStat:   &pbc.ProcStat{Utime: 42},
		ProcStatus: &pbc.ProcStatus{VmRss: 256},
		ProcIO:     &pbc.ProcIO{WriteBytes: 512},
		ProcSpill:  spill,
	}
	key := ProcKey{GpSegmentId: 3, SessId: 7, Pid: 111}
	info := ps.ToGpPidProcInfo(key)

	require.NotNil(t, info.ProcSpill, "ToGpPidProcInfo must carry ProcSpill into the proto message")
	assert.Equal(t, int64(8_000_000), info.ProcSpill.Size)
	assert.Equal(t, int64(7), info.ProcSpill.Files)
}

func TestToGpPidProcInfo_NilProcSpillPreserved(t *testing.T) {
	ps := &ProcStat{Cmdline: "cmd", State: "S", ProcSpill: nil}
	info := ps.ToGpPidProcInfo(ProcKey{GpSegmentId: 0, SessId: 1, Pid: 2})
	assert.Nil(t, info.ProcSpill, "nil ProcSpill must remain nil after conversion")
}

func TestRegisterProcfsStat_ProcSpillNilWhenAbsent(t *testing.T) {
	ps := NewProcfsStorage()
	now := time.Now()
	// No ProcSpill field set — it should be nil in storage.
	procs := []*pbc.GpPidProcInfo{{
		GpSegmentId: 1, SessId: 10, Pid: 20,
		ProcIo: &pbc.ProcIO{ReadBytes: 100},
	}}
	ps.RegisterProcfsStat(now, procs)

	ps.mx.RLock()
	defer ps.mx.RUnlock()
	stat := ps.procfsStat[0].pidProcData[ProcKey{GpSegmentId: 1, SessId: 10, Pid: 20}]
	require.NotNil(t, stat)
	assert.Nil(t, stat.ProcSpill)
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

func TestWithMaximumStoredPoints_Clamping(t *testing.T) {
	// zero should be clamped to 1
	ps := NewProcfsStorage(WithMaximumStoredPoints(0))
	assert.Equal(t, 1, ps.maximumStoredPoints)

	// negative should be clamped to 1
	ps = NewProcfsStorage(WithMaximumStoredPoints(-5))
	assert.Equal(t, 1, ps.maximumStoredPoints)

	// positive value should be kept as-is
	ps = NewProcfsStorage(WithMaximumStoredPoints(10))
	assert.Equal(t, 10, ps.maximumStoredPoints)
}

func TestTidyUpProcfsStat_WithMinimumStoredPoints(t *testing.T) {
	// maximumStoredPoints of 1 (the minimum after clamping) must work correctly
	ps := NewProcfsStorage(WithMaximumStoredPoints(1))
	for i := 0; i < 5; i++ {
		ps.RegisterProcfsStat(time.Unix(int64(i), 0), []*pbc.GpPidProcInfo{
			{GpSegmentId: int64(i), SessId: 1, Pid: 1, Cmdline: "cmd"},
		})
	}
	ps.mx.RLock()
	defer ps.mx.RUnlock()
	assert.Len(t, ps.procfsStat, 1)
}

// --- getNMin, get5Min, get15Min, get30Min ---

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

func findIdx(ps *ProcfsStorage, st *ProcfsStatType) int {
	for i, s := range ps.procfsStat {
		if len(s.pidProcData) == len(st.pidProcData) {
			match := true
			for k, v := range s.pidProcData {
				if st.pidProcData[k] != v {
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
	assert.Contains(t, err.Error(), "fail in get 5m0s interval")
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
	n, l, err := ps.get5Min()
	require.Error(t, err)
	assert.Nil(t, n)
	assert.Nil(t, l)
}

func TestGet5Min_ReturnsCorrectInterval(t *testing.T) {
	// 0,60,120,180,240,300,360,420; d=300s, last=420 -> nearest at 120 -> idx 2
	ps := buildWithUniqueData(0, 60, 120, 180, 240, 300, 360, 420)
	nearest, latest, err := ps.get5Min()
	require.NoError(t, err)
	assert.Equal(t, 2, findIdx(ps, nearest))
	assert.Equal(t, 7, findIdx(ps, latest))
}

func TestGet5Min_SingleElement(t *testing.T) {
	ps := buildWithUniqueData(100)
	nearest, latest, err := ps.get5Min()
	require.NoError(t, err)
	assert.Equal(t, 0, findIdx(ps, nearest))
	assert.Equal(t, 0, findIdx(ps, latest))
}

func TestGet15Min_EmptyStorage(t *testing.T) {
	ps := NewProcfsStorage()
	n, l, err := ps.get15Min()
	require.Error(t, err)
	assert.Nil(t, n)
	assert.Nil(t, l)
}

func TestGet15Min_ReturnsCorrectInterval(t *testing.T) {
	// 0,300,600,900,1200,1500; d=900s, last=1500 -> nearest at 600 -> idx 2
	ps := buildWithUniqueData(0, 300, 600, 900, 1200, 1500)
	nearest, latest, err := ps.get15Min()
	require.NoError(t, err)
	assert.Equal(t, 2, findIdx(ps, nearest))
	assert.Equal(t, 5, findIdx(ps, latest))
}

func TestGet15Min_RangeSmaller(t *testing.T) {
	ps := buildWithUniqueData(0, 60, 120)
	nearest, latest, err := ps.get15Min()
	require.NoError(t, err)
	assert.Equal(t, 0, findIdx(ps, nearest))
	assert.Equal(t, 2, findIdx(ps, latest))
}

func TestGet30Min_EmptyStorage(t *testing.T) {
	ps := NewProcfsStorage()
	n, l, err := ps.get30Min()
	require.Error(t, err)
	assert.Nil(t, n)
	assert.Nil(t, l)
}

func TestGet30Min_ReturnsCorrectInterval(t *testing.T) {
	// 0,600,1200,1800,2400,3000; d=1800s, last=3000 -> nearest at 1200 -> idx 2
	ps := buildWithUniqueData(0, 600, 1200, 1800, 2400, 3000)
	nearest, latest, err := ps.get30Min()
	require.NoError(t, err)
	assert.Equal(t, 2, findIdx(ps, nearest))
	assert.Equal(t, 5, findIdx(ps, latest))
}

func TestGet30Min_RangeSmaller(t *testing.T) {
	ps := buildWithUniqueData(0, 60, 120, 180, 240, 300)
	nearest, latest, err := ps.get30Min()
	require.NoError(t, err)
	assert.Equal(t, 0, findIdx(ps, nearest))
	assert.Equal(t, 5, findIdx(ps, latest))
}

func TestGet30Min_SingleElement(t *testing.T) {
	ps := buildWithUniqueData(100)
	nearest, latest, err := ps.get30Min()
	require.NoError(t, err)
	assert.Equal(t, 0, findIdx(ps, nearest))
	assert.Equal(t, 0, findIdx(ps, latest))
}

// --- getProcfsSession & GetProcfsSessions ---

// buildProcfsStorageWithSessions creates a ProcfsStorage with two snapshots
// separated by the given duration, each containing the given proc info entries.
// The "first" snapshot uses firstProcs and the "last" snapshot uses lastProcs.
func buildProcfsStorageWithSessions(interval time.Duration, firstProcs, lastProcs []*pbc.GpPidProcInfo) *ProcfsStorage {
	ps := NewProcfsStorage()
	base := time.Now().Add(-interval)
	ps.RegisterProcfsStat(base, firstProcs)
	ps.RegisterProcfsStat(base.Add(interval), lastProcs)
	return ps
}

func TestGetProcfsSessions_EmptyStorage(t *testing.T) {
	ps := NewProcfsStorage()
	result, err := ps.GetProcfsSessions([]int64{1, 2, 3})
	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "fail in get 5 min")
}

func TestGetProcfsSessions_EmptySessIds(t *testing.T) {
	ps := buildProcfsStorageWithSessions(time.Second, nil, nil)
	result, err := ps.GetProcfsSessions([]int64{})
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestGetProcfsSessions_NilSessIds(t *testing.T) {
	ps := buildProcfsStorageWithSessions(time.Second, nil, nil)
	result, err := ps.GetProcfsSessions(nil)
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestGetProcfsSessions_SessionNotFound(t *testing.T) {
	procs := []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 100, Pid: 1, Cmdline: "cmd", State: "R"},
	}
	ps := buildProcfsStorageWithSessions(time.Second, procs, procs)
	result, err := ps.GetProcfsSessions([]int64{999})
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestGetProcfsSessions_SingleSessionSingleSegment(t *testing.T) {
	procs := []*pbc.GpPidProcInfo{
		{
			GpSegmentId: 0, SessId: 100, Pid: 42,
			Cmdline: "SELECT 1", State: "R",
			ProcStat:   &pbc.ProcStat{Utime: 10, Stime: 5, Vsize: 1000, Rss: 500},
			ProcStatus: &pbc.ProcStatus{VmRss: 2048},
			ProcIo:     &pbc.ProcIO{ReadBytes: 100, WriteBytes: 50},
		},
	}
	lastProcs := []*pbc.GpPidProcInfo{
		{
			GpSegmentId: 0, SessId: 100, Pid: 42,
			Cmdline: "SELECT 1", State: "R",
			ProcStat:   &pbc.ProcStat{Utime: 20, Stime: 10, Vsize: 1200, Rss: 600},
			ProcStatus: &pbc.ProcStatus{VmRss: 2048},
			ProcIo:     &pbc.ProcIO{ReadBytes: 300, WriteBytes: 150},
		},
	}
	ps := buildProcfsStorageWithSessions(time.Second, procs, lastProcs)
	result, err := ps.GetProcfsSessions([]int64{100})
	require.NoError(t, err)
	require.Len(t, result, 1)
	sess := result[100]
	require.NotNil(t, sess)
	// Diff: Utime=20-10=10, Stime=10-5=5
	assert.NotNil(t, sess.ProcStat)
	assert.Equal(t, int64(10), sess.ProcStat.Utime)
	assert.Equal(t, int64(5), sess.ProcStat.Stime)
	// Diff: ReadBytes=300-100=200, WriteBytes=150-50=100
	assert.NotNil(t, sess.ProcIo)
	assert.Equal(t, int64(200), sess.ProcIo.ReadBytes)
	assert.Equal(t, int64(100), sess.ProcIo.WriteBytes)
}

func TestGetProcfsSessions_MultipleSessionsRequested(t *testing.T) {
	procs := []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 1, Pid: 10, Cmdline: "q1", State: "R",
			ProcStat: &pbc.ProcStat{Utime: 0}, ProcIo: &pbc.ProcIO{ReadBytes: 0}},
		{GpSegmentId: 0, SessId: 2, Pid: 20, Cmdline: "q2", State: "S",
			ProcStat: &pbc.ProcStat{Utime: 0}, ProcIo: &pbc.ProcIO{ReadBytes: 0}},
		{GpSegmentId: 0, SessId: 3, Pid: 30, Cmdline: "q3", State: "D",
			ProcStat: &pbc.ProcStat{Utime: 0}, ProcIo: &pbc.ProcIO{ReadBytes: 0}},
	}
	lastProcs := []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 1, Pid: 10, Cmdline: "q1", State: "R",
			ProcStat: &pbc.ProcStat{Utime: 100}, ProcIo: &pbc.ProcIO{ReadBytes: 500}},
		{GpSegmentId: 0, SessId: 2, Pid: 20, Cmdline: "q2", State: "S",
			ProcStat: &pbc.ProcStat{Utime: 200}, ProcIo: &pbc.ProcIO{ReadBytes: 600}},
		{GpSegmentId: 0, SessId: 3, Pid: 30, Cmdline: "q3", State: "D",
			ProcStat: &pbc.ProcStat{Utime: 300}, ProcIo: &pbc.ProcIO{ReadBytes: 700}},
	}
	ps := buildProcfsStorageWithSessions(time.Second, procs, lastProcs)
	result, err := ps.GetProcfsSessions([]int64{1, 2, 3})
	require.NoError(t, err)
	require.Len(t, result, 3)
	// Verify diffs for each session by key
	assert.Equal(t, int64(100), result[1].ProcStat.Utime)
	assert.Equal(t, int64(200), result[2].ProcStat.Utime)
	assert.Equal(t, int64(300), result[3].ProcStat.Utime)
}

func TestGetProcfsSessions_PartialMatch(t *testing.T) {
	// Request sessions 1, 2, 3 but only 1 and 3 exist
	procs := []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 1, Pid: 10, Cmdline: "q1", State: "R",
			ProcStat: &pbc.ProcStat{Utime: 0}},
		{GpSegmentId: 0, SessId: 3, Pid: 30, Cmdline: "q3", State: "D",
			ProcStat: &pbc.ProcStat{Utime: 0}},
	}
	lastProcs := []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 1, Pid: 10, Cmdline: "q1", State: "R",
			ProcStat: &pbc.ProcStat{Utime: 50}},
		{GpSegmentId: 0, SessId: 3, Pid: 30, Cmdline: "q3", State: "D",
			ProcStat: &pbc.ProcStat{Utime: 150}},
	}
	ps := buildProcfsStorageWithSessions(time.Second, procs, lastProcs)
	result, err := ps.GetProcfsSessions([]int64{1, 2, 3})
	require.NoError(t, err)
	// Session 2 is missing, so only 2 results
	assert.Len(t, result, 2)
}

func TestGetProcfsSessions_SessionOnlyInLast(t *testing.T) {
	// Session 100 exists only in the last snapshot (new session appeared)
	firstProcs := []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 1, Pid: 10, Cmdline: "old", State: "R",
			ProcStat: &pbc.ProcStat{Utime: 10}},
	}
	lastProcs := []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 1, Pid: 10, Cmdline: "old", State: "R",
			ProcStat: &pbc.ProcStat{Utime: 20}},
		{GpSegmentId: 0, SessId: 100, Pid: 42, Cmdline: "new", State: "R",
			ProcStat: &pbc.ProcStat{Utime: 5, Stime: 3},
			ProcIo:   &pbc.ProcIO{ReadBytes: 100}},
	}
	ps := buildProcfsStorageWithSessions(time.Second, firstProcs, lastProcs)
	result, err := ps.GetProcfsSessions([]int64{100})
	require.NoError(t, err)
	require.Len(t, result, 1)
	sess := result[100]
	require.NotNil(t, sess)
	// No first snapshot for this session, so diff is just the last values
	assert.Equal(t, int64(5), sess.ProcStat.Utime)
	assert.Equal(t, int64(3), sess.ProcStat.Stime)
	assert.Equal(t, int64(100), sess.ProcIo.ReadBytes)
}

func TestGetProcfsSessions_MultipleSegmentsPerSession(t *testing.T) {
	// Session 100 has processes on segment 0 and segment 1
	firstProcs := []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 100, Pid: 10, Cmdline: "cmd", State: "R",
			ProcStat: &pbc.ProcStat{Utime: 10, Stime: 5, Vsize: 100, Rss: 50},
			ProcIo:   &pbc.ProcIO{ReadBytes: 100}},
		{GpSegmentId: 1, SessId: 100, Pid: 20, Cmdline: "cmd", State: "R",
			ProcStat: &pbc.ProcStat{Utime: 20, Stime: 10, Vsize: 200, Rss: 100},
			ProcIo:   &pbc.ProcIO{ReadBytes: 200}},
	}
	lastProcs := []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 100, Pid: 10, Cmdline: "cmd", State: "R",
			ProcStat: &pbc.ProcStat{Utime: 30, Stime: 15, Vsize: 150, Rss: 75},
			ProcIo:   &pbc.ProcIO{ReadBytes: 400}},
		{GpSegmentId: 1, SessId: 100, Pid: 20, Cmdline: "cmd", State: "R",
			ProcStat: &pbc.ProcStat{Utime: 50, Stime: 25, Vsize: 250, Rss: 125},
			ProcIo:   &pbc.ProcIO{ReadBytes: 600}},
	}
	ps := buildProcfsStorageWithSessions(time.Second, firstProcs, lastProcs)
	result, err := ps.GetProcfsSessions([]int64{100})
	require.NoError(t, err)
	require.Len(t, result, 1)
	sess := result[100]
	require.NotNil(t, sess)
	// Diffs: seg0 Utime=20, seg1 Utime=30 -> grouped sum = 50
	// Diffs: seg0 Stime=10, seg1 Stime=15 -> grouped sum = 25
	assert.Equal(t, int64(50), sess.ProcStat.Utime)
	assert.Equal(t, int64(25), sess.ProcStat.Stime)
	// Diffs: seg0 ReadBytes=300, seg1 ReadBytes=400 -> grouped sum = 700
	assert.Equal(t, int64(700), sess.ProcIo.ReadBytes)
}

func TestGetProcfsSessions_NilProcStatAndProcIO(t *testing.T) {
	// Processes with no ProcStat/ProcIO sub-fields
	procs := []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 100, Pid: 42, Cmdline: "cmd", State: "S"},
	}
	ps := buildProcfsStorageWithSessions(time.Second, procs, procs)
	result, err := ps.GetProcfsSessions([]int64{100})
	require.NoError(t, err)
	require.Len(t, result, 1)
	sess := result[100]
	require.NotNil(t, sess)
	// ProcStat and ProcIo should be nil since both first and last have nil
	assert.Nil(t, sess.ProcStat)
	assert.Nil(t, sess.ProcIo)
}

func TestGetProcfsSessions_SingleSnapshot(t *testing.T) {
	// Only one snapshot registered — get5Min returns same pointer for first and last
	ps := NewProcfsStorage()
	procs := []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 100, Pid: 42, Cmdline: "cmd", State: "R",
			ProcStat: &pbc.ProcStat{Utime: 50, Stime: 25},
			ProcIo:   &pbc.ProcIO{ReadBytes: 1000}},
	}
	ps.RegisterProcfsStat(time.Now(), procs)
	result, err := ps.GetProcfsSessions([]int64{100})
	require.NoError(t, err)
	require.Len(t, result, 1)
	sess := result[100]
	require.NotNil(t, sess)
	// When first == last, diff produces zeros for cumulative counters
	assert.Equal(t, int64(0), sess.ProcStat.Utime)
	assert.Equal(t, int64(0), sess.ProcStat.Stime)
	assert.Equal(t, int64(0), sess.ProcIo.ReadBytes)
}

func TestGetProcfsSessions_ProcessDisappearedFromLastSnapshot(t *testing.T) {
	// A process exists in first but not in last for the same session
	// Another process for the same session exists in both
	firstProcs := []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 100, Pid: 10, Cmdline: "cmd1", State: "R",
			ProcStat: &pbc.ProcStat{Utime: 10}, ProcIo: &pbc.ProcIO{ReadBytes: 100}},
		{GpSegmentId: 1, SessId: 100, Pid: 20, Cmdline: "cmd2", State: "R",
			ProcStat: &pbc.ProcStat{Utime: 20}, ProcIo: &pbc.ProcIO{ReadBytes: 200}},
	}
	lastProcs := []*pbc.GpPidProcInfo{
		// Only segment 0 remains; segment 1 process disappeared
		{GpSegmentId: 0, SessId: 100, Pid: 10, Cmdline: "cmd1", State: "R",
			ProcStat: &pbc.ProcStat{Utime: 30}, ProcIo: &pbc.ProcIO{ReadBytes: 400}},
	}
	ps := buildProcfsStorageWithSessions(time.Second, firstProcs, lastProcs)
	result, err := ps.GetProcfsSessions([]int64{100})
	require.NoError(t, err)
	require.Len(t, result, 1)
	sess := result[100]
	require.NotNil(t, sess)
	// Only seg0 is in last, so only seg0 diff is computed: Utime=30-10=20
	assert.Equal(t, int64(20), sess.ProcStat.Utime)
	assert.Equal(t, int64(300), sess.ProcIo.ReadBytes)
}

func TestGetProcfsSessions_SessionOnlyInFirst(t *testing.T) {
	// Session exists in first snapshot but not in last — should not be returned
	firstProcs := []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 100, Pid: 42, Cmdline: "cmd", State: "R",
			ProcStat: &pbc.ProcStat{Utime: 10}},
	}
	lastProcs := []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 200, Pid: 43, Cmdline: "other", State: "R",
			ProcStat: &pbc.ProcStat{Utime: 10}},
	}
	ps := buildProcfsStorageWithSessions(time.Second, firstProcs, lastProcs)
	result, err := ps.GetProcfsSessions([]int64{100})
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestGetProcfsSessions_MemoryAggregation_MultipleSegmentsOnSameHost(t *testing.T) {
	// Configure segments 0, 1, and 2 to all reside on the same host "host-a".
	// This tests that Vsize and Rss are correctly summed per-host when
	// multiple GpSegmentId instances share a single physical host.
	SetHostnameForSegindex(0, "host-a")
	SetHostnameForSegindex(1, "host-a")
	SetHostnameForSegindex(2, "host-a")
	defer func() {
		// Clean up global state
		SegmentConfigLock.Lock()
		delete(SegmentMap, SegmentKey{Segindex: 0})
		delete(SegmentMap, SegmentKey{Segindex: 1})
		delete(SegmentMap, SegmentKey{Segindex: 2})
		SegmentConfigLock.Unlock()
	}()

	// Session 100 has 3 processes, one per segment, all on "host-a".
	// First snapshot: each segment has some Vsize/Rss and cumulative counters.
	firstProcs := []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 100, Pid: 10, Cmdline: "cmd", State: "R",
			ProcStat: &pbc.ProcStat{Utime: 10, Stime: 5, Vsize: 1000, Rss: 500},
			ProcIo:   &pbc.ProcIO{ReadBytes: 100}},
		{GpSegmentId: 1, SessId: 100, Pid: 20, Cmdline: "cmd", State: "R",
			ProcStat: &pbc.ProcStat{Utime: 20, Stime: 10, Vsize: 2000, Rss: 1000},
			ProcIo:   &pbc.ProcIO{ReadBytes: 200}},
		{GpSegmentId: 2, SessId: 100, Pid: 30, Cmdline: "cmd", State: "R",
			ProcStat: &pbc.ProcStat{Utime: 30, Stime: 15, Vsize: 3000, Rss: 1500},
			ProcIo:   &pbc.ProcIO{ReadBytes: 300}},
	}
	// Last snapshot: counters have increased; Vsize/Rss are snapshot values.
	lastProcs := []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 100, Pid: 10, Cmdline: "cmd", State: "R",
			ProcStat: &pbc.ProcStat{Utime: 50, Stime: 25, Vsize: 1100, Rss: 550},
			ProcIo:   &pbc.ProcIO{ReadBytes: 500}},
		{GpSegmentId: 1, SessId: 100, Pid: 20, Cmdline: "cmd", State: "R",
			ProcStat: &pbc.ProcStat{Utime: 60, Stime: 30, Vsize: 2200, Rss: 1100},
			ProcIo:   &pbc.ProcIO{ReadBytes: 600}},
		{GpSegmentId: 2, SessId: 100, Pid: 30, Cmdline: "cmd", State: "R",
			ProcStat: &pbc.ProcStat{Utime: 70, Stime: 35, Vsize: 3300, Rss: 1650},
			ProcIo:   &pbc.ProcIO{ReadBytes: 700}},
	}
	ps := buildProcfsStorageWithSessions(time.Second, firstProcs, lastProcs)
	result, err := ps.GetProcfsSessions([]int64{100})
	require.NoError(t, err)
	require.Len(t, result, 1)
	sess := result[100]
	require.NotNil(t, sess)

	// Cumulative counters (Utime, Stime) are always summed across all segments:
	// Diff seg0: Utime=50-10=40, Stime=25-5=20
	// Diff seg1: Utime=60-20=40, Stime=30-10=20
	// Diff seg2: Utime=70-30=40, Stime=35-15=20
	// Total: Utime=120, Stime=60
	assert.Equal(t, int64(120), sess.ProcStat.Utime)
	assert.Equal(t, int64(60), sess.ProcStat.Stime)

	// IO counters are summed across all segments:
	// Diff seg0: ReadBytes=500-100=400
	// Diff seg1: ReadBytes=600-200=400
	// Diff seg2: ReadBytes=700-300=400
	// Total: ReadBytes=1200
	assert.Equal(t, int64(1200), sess.ProcIo.ReadBytes)

	// Memory (Vsize, Rss) with AggSegmentHost: since all 3 segments are on
	// the same host "host-a", their diff Vsize/Rss values are summed into
	// a single per-host intermediate entry.
	// Diff Vsize: seg0=1100, seg1=2200, seg2=3300 (snapshot from last)
	// All on "host-a" -> intermediateResults[{"Vsize","host-a"}] = 1100+2200+3300 = 6600
	// Diff Rss: seg0=550, seg1=1100, seg2=1650
	// All on "host-a" -> intermediateResults[{"Rss","host-a"}] = 550+1100+1650 = 3300
	assert.Equal(t, int64(6600), sess.ProcStat.Vsize)
	assert.Equal(t, int64(3300), sess.ProcStat.Rss)
}

func TestGetProcfsSessions_MemoryAggregation_SegmentsOnDifferentHosts(t *testing.T) {
	// Configure segments on different hosts to verify per-host memory tracking.
	// Segment 10 on "host-x", segments 11 and 12 on "host-y".
	SetHostnameForSegindex(10, "host-x")
	SetHostnameForSegindex(11, "host-y")
	SetHostnameForSegindex(12, "host-y")
	defer func() {
		SegmentConfigLock.Lock()
		delete(SegmentMap, SegmentKey{Segindex: 10})
		delete(SegmentMap, SegmentKey{Segindex: 11})
		delete(SegmentMap, SegmentKey{Segindex: 12})
		SegmentConfigLock.Unlock()
	}()

	// Only use "last" snapshot (no first) so diff = raw values.
	lastProcs := []*pbc.GpPidProcInfo{
		{GpSegmentId: 10, SessId: 200, Pid: 10, Cmdline: "cmd", State: "R",
			ProcStat: &pbc.ProcStat{Utime: 100, Vsize: 1000, Rss: 500},
			ProcIo:   &pbc.ProcIO{ReadBytes: 100}},
		{GpSegmentId: 11, SessId: 200, Pid: 20, Cmdline: "cmd", State: "R",
			ProcStat: &pbc.ProcStat{Utime: 200, Vsize: 2000, Rss: 1000},
			ProcIo:   &pbc.ProcIO{ReadBytes: 200}},
		{GpSegmentId: 12, SessId: 200, Pid: 30, Cmdline: "cmd", State: "R",
			ProcStat: &pbc.ProcStat{Utime: 300, Vsize: 3000, Rss: 1500},
			ProcIo:   &pbc.ProcIO{ReadBytes: 300}},
	}
	ps := buildProcfsStorageWithSessions(time.Second, nil, lastProcs)
	result, err := ps.GetProcfsSessions([]int64{200})
	require.NoError(t, err)
	require.Len(t, result, 1)
	sess := result[200]
	require.NotNil(t, sess)

	// Cumulative counters are always summed: Utime=100+200+300=600
	assert.Equal(t, int64(600), sess.ProcStat.Utime)
	// IO: ReadBytes=100+200+300=600
	assert.Equal(t, int64(600), sess.ProcIo.ReadBytes)

	// Memory with AggSegmentHost and different hosts:
	// After processing seg10 (host-x): intermediate[Vsize,host-x]=1000, dest.Vsize=1000
	// After processing seg11 (host-y): intermediate[Vsize,host-y]=2000, dest.Vsize=2000
	// After processing seg12 (host-y): intermediate[Vsize,host-y]=5000, dest.Vsize=5000
	// Final Vsize = last host's accumulated value = 5000 (host-y: 2000+3000)
	// Final Rss = last host's accumulated value = 2500 (host-y: 1000+1500)
	assert.Equal(t, int64(5000), sess.ProcStat.Vsize)
	assert.Equal(t, int64(2500), sess.ProcStat.Rss)
}
