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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/utils"
)

func TestCalculateSkew(t *testing.T) {
	skew := CalculateSkew(map[int32]float64{0: 10, 1: 10, 2: 40})

	require.NotNil(t, skew)
	assert.InDelta(t, 0.5, skew.Skew, 0.000001)
	assert.Equal(t, int32(2), skew.Segindex)
}

func TestCalculateSkew_EmptyAndZeroMax(t *testing.T) {
	assert.Equal(t, &pbc.Skew{}, CalculateSkew(nil))
	assert.Equal(t, &pbc.Skew{Segindex: 0}, CalculateSkew(map[int32]float64{0: 0, 1: 0}))
}

func TestCalculateSkew_TieChoosesLowestSegindex(t *testing.T) {
	skew := CalculateSkew(map[int32]float64{2: 10, 1: 10})

	assert.Equal(t, int32(1), skew.Segindex)
	assert.Equal(t, 0.0, skew.Skew)
}

func TestCalculateDataQuality(t *testing.T) {
	statTime := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	now := statTime.Add(1500 * time.Millisecond)

	quality := CalculateDataQuality(3, 2, statTime, now)

	require.NotNil(t, quality)
	assert.Equal(t, int64(3), quality.SegmentsExpected)
	assert.Equal(t, int64(2), quality.SegmentsReceived)
	assert.True(t, quality.IsPartial)
	assert.Equal(t, int64(1500), quality.FreshnessMs)
}

func TestGetQueryRunningMetrics_UsesOnlyProcfsDataAndFiltersCommandCount(t *testing.T) {
	base := time.Now().Add(-time.Second)
	ps := NewProcfsStorage()
	ps.RegisterProcfsStatWithDataQuality(base, []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 100, Pid: 10, Ccnt: 7, SliceId: 1, Cmdline: "postgres: con100 cmd7 SELECT", ProcStat: &pbc.ProcStat{Utime: 0, Stime: 0}, ProcStatus: &pbc.ProcStatus{VmPeak: 10, VmRss: 5}, ProcIo: &pbc.ProcIO{ReadBytes: 0}},
		{GpSegmentId: 1, SessId: 100, Pid: 11, Ccnt: 7, SliceId: 1, Cmdline: "postgres: con100 cmd7 SELECT", ProcStat: &pbc.ProcStat{Utime: 0, Stime: 0}, ProcStatus: &pbc.ProcStatus{VmPeak: 10, VmRss: 5}, ProcIo: &pbc.ProcIO{ReadBytes: 0}},
		{GpSegmentId: 2, SessId: 100, Pid: 13, Ccnt: 8, SliceId: 1, Cmdline: "postgres: con100 cmd8 SELECT", ProcStat: &pbc.ProcStat{Utime: 0, Stime: 0}, ProcStatus: &pbc.ProcStatus{VmPeak: 10, VmRss: 5}, ProcIo: &pbc.ProcIO{ReadBytes: 0}},
		{GpSegmentId: -1, SessId: 100, Pid: 12, Ccnt: 7, SliceId: 0, Cmdline: "postgres: con100 cmd7 SELECT", ProcStat: &pbc.ProcStat{Utime: 0, Stime: 0}, ProcStatus: &pbc.ProcStatus{VmPeak: 10, VmRss: 5}, ProcIo: &pbc.ProcIO{ReadBytes: 0}},
	}, 3, 2)
	ps.RegisterProcfsStatWithDataQuality(base.Add(time.Second), []*pbc.GpPidProcInfo{
		{GpSegmentId: 0, SessId: 100, Pid: 10, Ccnt: 7, SliceId: 1, Cmdline: "postgres: con100 cmd7 SELECT", ProcStat: &pbc.ProcStat{Utime: 10, Stime: 1}, ProcStatus: &pbc.ProcStatus{VmPeak: 100, VmRss: 50}, ProcIo: &pbc.ProcIO{ReadBytes: 100}, ProcSpill: &pbc.ProcSpill{Size: 1000, Files: 1}},
		{GpSegmentId: 1, SessId: 100, Pid: 11, Ccnt: 7, SliceId: 1, Cmdline: "postgres: con100 cmd7 SELECT", ProcStat: &pbc.ProcStat{Utime: 40, Stime: 2}, ProcStatus: &pbc.ProcStatus{VmPeak: 200, VmRss: 100}, ProcIo: &pbc.ProcIO{ReadBytes: 400}, ProcSpill: &pbc.ProcSpill{Size: 4000, Files: 4}},
		{GpSegmentId: 2, SessId: 100, Pid: 13, Ccnt: 8, SliceId: 1, Cmdline: "postgres: con100 cmd8 SELECT", ProcStat: &pbc.ProcStat{Utime: 10000, Stime: 10000}, ProcStatus: &pbc.ProcStatus{VmPeak: 10000, VmRss: 10000}, ProcIo: &pbc.ProcIO{ReadBytes: 10000}},
		{GpSegmentId: -1, SessId: 100, Pid: 12, Ccnt: 7, SliceId: 0, Cmdline: "postgres: con100 cmd7 SELECT", ProcStat: &pbc.ProcStat{Utime: 1000, Stime: 1000}, ProcStatus: &pbc.ProcStatus{VmPeak: 1000, VmRss: 1000}, ProcIo: &pbc.ProcIO{ReadBytes: 1000}},
	}, 3, 2)

	metrics, skew, quality, err := ps.GetQueryRunningMetrics(&pbc.QueryKey{Ssid: 100, Ccnt: 7})

	require.NoError(t, err)
	require.Len(t, metrics, 3)
	assert.Equal(t, QueryCellKey{SliceID: 0, Segindex: -1}, metrics[0].QueryCellKey)
	assert.Equal(t, QueryCellKey{SliceID: 1, Segindex: 0}, metrics[1].QueryCellKey)
	assert.Equal(t, QueryCellKey{SliceID: 1, Segindex: 1}, metrics[2].QueryCellKey)

	assert.Equal(t, int64(10), metrics[1].RuntimeMetrics.Utime)
	assert.Equal(t, int64(40), metrics[2].RuntimeMetrics.Utime)
	assert.Equal(t, int64(400), metrics[2].RuntimeMetrics.ProcIo.ReadBytes)
	assert.Equal(t, int64(4000), metrics[2].RuntimeMetrics.ProcSpill.Size)

	require.NotNil(t, skew)
	assert.InDelta(t, 0.369047619, skew.Skew, 0.000001)
	assert.Equal(t, int32(1), skew.Segindex)

	require.NotNil(t, quality)
	assert.Equal(t, int64(3), quality.SegmentsExpected)
	assert.Equal(t, int64(2), quality.SegmentsReceived)
	assert.True(t, quality.IsPartial)
	assert.GreaterOrEqual(t, quality.FreshnessMs, int64(0))
}

func TestParseCmdLineCommandCount(t *testing.T) {
	tests := []struct {
		name    string
		cmdline string
		want    int32
		wantOK  bool
	}{
		{name: "standard postgres cmdline", cmdline: "postgres: con100 cmd7 SELECT", want: 7, wantOK: true},
		{name: "last cmd token wins", cmdline: "prefix cmd1 middle cmd42 SELECT", want: 42, wantOK: true},
		{name: "no cmd token", cmdline: "postgres: checkpointer process", wantOK: false},
		{name: "cmd without digits", cmdline: "postgres: con100 cmd SELECT", wantOK: false},
		{name: "cmd overflow", cmdline: "postgres: con100 cmd999999999999999999999 SELECT", wantOK: false},
		{name: "short string", cmdline: "cmd", wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := utils.ParseCmdLineCommandCount(tt.cmdline)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestProcfsMatchesQuerySkipsIdleBackendWithoutSlice(t *testing.T) {
	assert.True(t, procfsMatchesQuery(&ProcStat{Ccnt: 7, SliceId: 2, State: "SELECT"}, 7))
	assert.True(t, procfsMatchesQuery(&ProcStat{Ccnt: 7, SliceId: UnsetSliceId, State: "SELECT"}, 7))
	assert.False(t, procfsMatchesQuery(&ProcStat{Ccnt: 7, SliceId: UnsetSliceId, State: "idle"}, 7))
	assert.False(t, procfsMatchesQuery(&ProcStat{Ccnt: 8, SliceId: 2, State: "SELECT"}, 7))
}

func TestProcfsCellForProcUsesParsedSliceID(t *testing.T) {
	assert.Equal(t, QueryCellKey{SliceID: 2, Segindex: 1}, procfsCellForProc(1, 2))
	assert.Equal(t, QueryCellKey{SliceID: procfsQuerySegmentSliceID, Segindex: 1}, procfsCellForProc(1, UnsetSliceId))
	assert.Equal(t, QueryCellKey{SliceID: procfsQuerySegmentSliceID, Segindex: -1}, procfsCellForProc(-1, UnsetSliceId))
}

func TestAddProcIO(t *testing.T) {
	assert.Nil(t, addProcIO(nil, nil))

	created := addProcIO(nil, &pbc.ProcIO{Rchar: 1, Wchar: 2, Syscr: 3, Syscw: 4, ReadBytes: 5, WriteBytes: 6, CancelledWriteBytes: 7})
	require.NotNil(t, created)
	assert.Equal(t, int64(1), created.Rchar)
	assert.Equal(t, int64(7), created.CancelledWriteBytes)

	updated := addProcIO(created, &pbc.ProcIO{Rchar: 10, Wchar: 20, Syscr: 30, Syscw: 40, ReadBytes: 50, WriteBytes: 60, CancelledWriteBytes: 70})
	assert.Same(t, created, updated)
	assert.Equal(t, int64(11), updated.Rchar)
	assert.Equal(t, int64(22), updated.Wchar)
	assert.Equal(t, int64(33), updated.Syscr)
	assert.Equal(t, int64(44), updated.Syscw)
	assert.Equal(t, int64(55), updated.ReadBytes)
	assert.Equal(t, int64(66), updated.WriteBytes)
	assert.Equal(t, int64(77), updated.CancelledWriteBytes)

	unchanged := addProcIO(updated, nil)
	assert.Same(t, updated, unchanged)
}

func TestAddProcSpill(t *testing.T) {
	assert.Nil(t, addProcSpill(nil, nil))

	created := addProcSpill(nil, &pbc.ProcSpill{Size: 10, Files: 1})
	require.NotNil(t, created)
	assert.Equal(t, int64(10), created.Size)
	assert.Equal(t, int64(1), created.Files)

	updated := addProcSpill(created, &pbc.ProcSpill{Size: 20, Files: 2})
	assert.Same(t, created, updated)
	assert.Equal(t, int64(30), updated.Size)
	assert.Equal(t, int64(3), updated.Files)

	unchanged := addProcSpill(updated, nil)
	assert.Same(t, updated, unchanged)
}

func TestAddProcfsRuntimeMetrics(t *testing.T) {
	assert.Nil(t, addProcfsRuntimeMetrics(nil, nil))

	source := &pbc.GpPidProcInfo{
		ProcStat:   &pbc.ProcStat{Utime: 10, Stime: 20},
		ProcStatus: &pbc.ProcStatus{VmPeak: 30, VmRss: 40},
		ProcIo:     &pbc.ProcIO{ReadBytes: 50},
		ProcSpill:  &pbc.ProcSpill{Size: 60, Files: 2},
	}
	created := addProcfsRuntimeMetrics(nil, source)
	require.NotNil(t, created)
	assert.Equal(t, int64(10), created.Utime)
	assert.Equal(t, int64(20), created.Stime)
	assert.Equal(t, int64(30), created.VmPeak)
	assert.Equal(t, int64(40), created.VmRss)
	assert.Equal(t, int64(50), created.ProcIo.ReadBytes)
	assert.Equal(t, int64(60), created.ProcSpill.Size)
	assert.Equal(t, int64(2), created.ProcSpill.Files)

	updated := addProcfsRuntimeMetrics(created, source)
	assert.Same(t, created, updated)
	assert.Equal(t, int64(20), updated.Utime)
	assert.Equal(t, int64(40), updated.Stime)
	assert.Equal(t, int64(60), updated.VmPeak)
	assert.Equal(t, int64(80), updated.VmRss)
	assert.Equal(t, int64(100), updated.ProcIo.ReadBytes)
	assert.Equal(t, int64(120), updated.ProcSpill.Size)
	assert.Equal(t, int64(4), updated.ProcSpill.Files)
}

func TestCPUTimeSkewAccumulatorAddAndSkew(t *testing.T) {
	var nilAcc *cpuTimeSkewAccumulator
	assert.Equal(t, &pbc.Skew{}, nilAcc.Skew())

	acc := &cpuTimeSkewAccumulator{}
	assert.Equal(t, &pbc.Skew{}, acc.Skew())

	acc.Add(0, 10)
	acc.Add(1, 40)
	acc.Add(2, 10)

	skew := acc.Skew()
	require.NotNil(t, skew)
	assert.InDelta(t, 0.5, skew.Skew, 0.000001)
	assert.Equal(t, int32(1), skew.Segindex)
}

func TestCPUTimeSkewAccumulatorSkewZeroMax(t *testing.T) {
	acc := &cpuTimeSkewAccumulator{}
	acc.Add(0, 0)
	acc.Add(1, 0)

	skew := acc.Skew()
	require.NotNil(t, skew)
	assert.Equal(t, float64(0), skew.Skew)
	assert.Equal(t, int32(0), skew.Segindex)
}

func TestCalculateCPUTimeSkew(t *testing.T) {
	cells := []*ProcfsCellMetric{
		nil,
		{QueryCellKey: QueryCellKey{SliceID: 1, Segindex: 0}, HasData: false, RuntimeMetrics: &pbc.RuntimeMetrics{Utime: 1000}},
		{QueryCellKey: QueryCellKey{SliceID: 0, Segindex: -1}, HasData: true, RuntimeMetrics: &pbc.RuntimeMetrics{Utime: 1, Stime: 9999}},
		{QueryCellKey: QueryCellKey{SliceID: 1, Segindex: 0}, HasData: true, RuntimeMetrics: &pbc.RuntimeMetrics{Utime: 10, Stime: 0}},
		{QueryCellKey: QueryCellKey{SliceID: 1, Segindex: 1}, HasData: true, RuntimeMetrics: &pbc.RuntimeMetrics{Utime: 40, Stime: 0}},
		{QueryCellKey: QueryCellKey{SliceID: 2, Segindex: 3}, HasData: true, RuntimeMetrics: &pbc.RuntimeMetrics{Utime: 1, Stime: 0}},
		{QueryCellKey: QueryCellKey{SliceID: 2, Segindex: 4}, HasData: true, RuntimeMetrics: &pbc.RuntimeMetrics{Utime: 99, Stime: 0}},
		// Positive segindex on slice 0 is included; only negative segindex is skipped.
		{QueryCellKey: QueryCellKey{SliceID: 0, Segindex: 5}, HasData: true, RuntimeMetrics: &pbc.RuntimeMetrics{Utime: 1, Stime: 0}},
		{QueryCellKey: QueryCellKey{SliceID: 0, Segindex: 6}, HasData: true, RuntimeMetrics: &pbc.RuntimeMetrics{Utime: 199, Stime: 0}},
	}

	skew := CalculateCPUTimeSkew(cells)

	require.NotNil(t, skew)
	assert.InDelta(t, 0.49748744, skew.Skew, 0.000001)
	assert.Equal(t, int32(6), skew.Segindex)
}

func TestGetProcfsQueryRuntimeMetrics(t *testing.T) {
	ps := NewProcfsStorage()
	base := time.Now().Add(-time.Second)
	ps.RegisterProcfsStatWithDataQuality(base, []*pbc.GpPidProcInfo{
		{GpSegmentId: -1, SessId: 200, Pid: 1, Ccnt: 9, SliceId: 0, Cmdline: "postgres: con200 cmd9 SELECT", ProcStat: &pbc.ProcStat{Utime: 0, Stime: 0}, ProcIo: &pbc.ProcIO{ReadBytes: 0}},
		{GpSegmentId: 0, SessId: 200, Pid: 2, Ccnt: 9, SliceId: 1, Cmdline: "postgres: con200 cmd9 SELECT", ProcStat: &pbc.ProcStat{Utime: 1, Stime: 1}, ProcIo: &pbc.ProcIO{ReadBytes: 10}},
		{GpSegmentId: 0, SessId: 200, Pid: 3, Ccnt: 9, SliceId: 1, Cmdline: "postgres: con200 cmd9 SELECT", ProcStat: &pbc.ProcStat{Utime: 2, Stime: 2}, ProcIo: &pbc.ProcIO{ReadBytes: 20}},
		{GpSegmentId: 1, SessId: 200, Pid: 4, Ccnt: 10, SliceId: 1, Cmdline: "postgres: con200 cmd10 SELECT", ProcStat: &pbc.ProcStat{Utime: 100, Stime: 100}, ProcIo: &pbc.ProcIO{ReadBytes: 1000}},
	}, 2, 2)
	ps.RegisterProcfsStatWithDataQuality(base.Add(time.Second), []*pbc.GpPidProcInfo{
		{GpSegmentId: -1, SessId: 200, Pid: 1, Ccnt: 9, SliceId: 0, Cmdline: "postgres: con200 cmd9 SELECT", ProcStat: &pbc.ProcStat{Utime: 10, Stime: 5}, ProcIo: &pbc.ProcIO{ReadBytes: 100}},
		{GpSegmentId: 0, SessId: 200, Pid: 2, Ccnt: 9, SliceId: 1, Cmdline: "postgres: con200 cmd9 SELECT", ProcStat: &pbc.ProcStat{Utime: 11, Stime: 6}, ProcIo: &pbc.ProcIO{ReadBytes: 110}, ProcSpill: &pbc.ProcSpill{Size: 1000, Files: 1}},
		{GpSegmentId: 0, SessId: 200, Pid: 3, Ccnt: 9, SliceId: 1, Cmdline: "postgres: con200 cmd9 SELECT", ProcStat: &pbc.ProcStat{Utime: 12, Stime: 7}, ProcIo: &pbc.ProcIO{ReadBytes: 120}, ProcSpill: &pbc.ProcSpill{Size: 2000, Files: 2}},
		{GpSegmentId: 1, SessId: 200, Pid: 4, Ccnt: 10, SliceId: 1, Cmdline: "postgres: con200 cmd10 SELECT", ProcStat: &pbc.ProcStat{Utime: 1000, Stime: 1000}, ProcIo: &pbc.ProcIO{ReadBytes: 10000}},
	}, 2, 2)
	first, last, err := ps.get5Min()
	require.NoError(t, err)

	metrics, err := ps.getProcfsQueryRuntimeMetrics(first, last, &pbc.QueryKey{Ssid: 200, Ccnt: 9})

	require.NoError(t, err)
	require.Len(t, metrics, 2)
	assert.Equal(t, QueryCellKey{SliceID: 0, Segindex: -1}, metrics[0].QueryCellKey)
	assert.Equal(t, QueryCellKey{SliceID: 1, Segindex: 0}, metrics[1].QueryCellKey)
	assert.Equal(t, int64(10), metrics[0].RuntimeMetrics.Utime)
	assert.Equal(t, int64(5), metrics[0].RuntimeMetrics.Stime)
	assert.Equal(t, int64(20), metrics[1].RuntimeMetrics.Utime)
	assert.Equal(t, int64(10), metrics[1].RuntimeMetrics.Stime)
	assert.Equal(t, int64(200), metrics[1].RuntimeMetrics.ProcIo.ReadBytes)
	assert.Equal(t, int64(3000), metrics[1].RuntimeMetrics.ProcSpill.Size)
	assert.Equal(t, int64(3), metrics[1].RuntimeMetrics.ProcSpill.Files)
}

func TestGetProcfsQueryRuntimeMetrics_UsesParsedSliceAndSkipsIdleWithoutSlice(t *testing.T) {
	ps := NewProcfsStorage()
	base := time.Now().Add(-time.Second)
	ps.RegisterProcfsStatWithDataQuality(base, []*pbc.GpPidProcInfo{
		{GpSegmentId: -1, SessId: 300, Pid: 1, Ccnt: 11, SliceId: 0, State: "SELECT", Cmdline: "postgres: con300 cmd11 SELECT", ProcStat: &pbc.ProcStat{Utime: 0, Stime: 0}},
		{GpSegmentId: 0, SessId: 300, Pid: 2, Ccnt: 11, SliceId: 1, State: "SELECT", Cmdline: "postgres: con300 cmd11 slice1 SELECT", ProcStat: &pbc.ProcStat{Utime: 10, Stime: 1}},
		{GpSegmentId: 0, SessId: 300, Pid: 3, Ccnt: 11, SliceId: 2, State: "SELECT", Cmdline: "postgres: con300 cmd11 slice2 SELECT", ProcStat: &pbc.ProcStat{Utime: 20, Stime: 2}},
		{GpSegmentId: 1, SessId: 300, Pid: 4, Ccnt: 11, SliceId: UnsetSliceId, State: "idle", Cmdline: "postgres: con300 cmd11 idle", ProcStat: &pbc.ProcStat{Utime: 1000, Stime: 1000}},
		{GpSegmentId: 1, SessId: 300, Pid: 5, Ccnt: 12, SliceId: 2, State: "SELECT", Cmdline: "postgres: con300 cmd12 slice2 SELECT", ProcStat: &pbc.ProcStat{Utime: 2000, Stime: 2000}},
	}, 2, 2)
	ps.RegisterProcfsStatWithDataQuality(base.Add(time.Second), []*pbc.GpPidProcInfo{
		{GpSegmentId: -1, SessId: 300, Pid: 1, Ccnt: 11, SliceId: 0, State: "SELECT", Cmdline: "postgres: con300 cmd11 SELECT", ProcStat: &pbc.ProcStat{Utime: 5, Stime: 1}},
		{GpSegmentId: 0, SessId: 300, Pid: 2, Ccnt: 11, SliceId: 1, State: "SELECT", Cmdline: "postgres: con300 cmd11 slice1 SELECT", ProcStat: &pbc.ProcStat{Utime: 30, Stime: 4}},
		{GpSegmentId: 0, SessId: 300, Pid: 3, Ccnt: 11, SliceId: 2, State: "SELECT", Cmdline: "postgres: con300 cmd11 slice2 SELECT", ProcStat: &pbc.ProcStat{Utime: 60, Stime: 8}},
		{GpSegmentId: 1, SessId: 300, Pid: 4, Ccnt: 11, SliceId: UnsetSliceId, State: "idle", Cmdline: "postgres: con300 cmd11 idle", ProcStat: &pbc.ProcStat{Utime: 3000, Stime: 3000}},
		{GpSegmentId: 1, SessId: 300, Pid: 5, Ccnt: 12, SliceId: 2, State: "SELECT", Cmdline: "postgres: con300 cmd12 slice2 SELECT", ProcStat: &pbc.ProcStat{Utime: 4000, Stime: 4000}},
	}, 2, 2)
	first, last, err := ps.get5Min()
	require.NoError(t, err)

	metrics, err := ps.getProcfsQueryRuntimeMetrics(first, last, &pbc.QueryKey{Ssid: 300, Ccnt: 11})

	require.NoError(t, err)
	require.Len(t, metrics, 3)
	assert.Equal(t, QueryCellKey{SliceID: 0, Segindex: -1}, metrics[0].QueryCellKey)
	assert.Equal(t, QueryCellKey{SliceID: 1, Segindex: 0}, metrics[1].QueryCellKey)
	assert.Equal(t, QueryCellKey{SliceID: 2, Segindex: 0}, metrics[2].QueryCellKey)
	assert.Equal(t, int64(5), metrics[0].RuntimeMetrics.Utime)
	assert.Equal(t, int64(20), metrics[1].RuntimeMetrics.Utime)
	assert.Equal(t, int64(40), metrics[2].RuntimeMetrics.Utime)
}

func TestGetProcfsQueryRuntimeMetricsNilAndMissingQuery(t *testing.T) {
	ps := NewProcfsStorage()
	base := time.Now().Add(-time.Second)
	ps.RegisterProcfsStat(base, nil)
	ps.RegisterProcfsStat(base.Add(time.Second), nil)
	first, last, err := ps.get5Min()
	require.NoError(t, err)

	metrics, err := ps.getProcfsQueryRuntimeMetrics(first, last, nil)
	require.Error(t, err)
	assert.Nil(t, metrics)

	metrics, err = ps.getProcfsQueryRuntimeMetrics(first, last, &pbc.QueryKey{Ssid: 999, Ccnt: 1})
	require.NoError(t, err)
	assert.Nil(t, metrics)
}
