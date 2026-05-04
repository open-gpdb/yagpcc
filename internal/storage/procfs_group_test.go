package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/utils"
)

// ---------------------------------------------------------------------------
// nonNegativeDiff
// ---------------------------------------------------------------------------

func TestNonNegativeDiff_Int(t *testing.T) {
	t.Run("positive diff", func(t *testing.T) { assert.Equal(t, 7, nonNegativeDiff(3, 10)) })
	t.Run("zero diff", func(t *testing.T) { assert.Equal(t, 0, nonNegativeDiff(5, 5)) })
	t.Run("negative diff clamped", func(t *testing.T) { assert.Equal(t, 0, nonNegativeDiff(10, 3)) })
	t.Run("zero values", func(t *testing.T) { assert.Equal(t, 0, nonNegativeDiff(0, 0)) })
	t.Run("first zero", func(t *testing.T) { assert.Equal(t, 42, nonNegativeDiff(0, 42)) })
	t.Run("last zero", func(t *testing.T) { assert.Equal(t, 0, nonNegativeDiff(42, 0)) })
}

func TestNonNegativeDiff_Int32(t *testing.T) {
	assert.Equal(t, int32(100), nonNegativeDiff(int32(50), int32(150)))
	assert.Equal(t, int32(0), nonNegativeDiff(int32(150), int32(50)))
}

func TestNonNegativeDiff_Int64(t *testing.T) {
	assert.Equal(t, int64(1000), nonNegativeDiff(int64(500), int64(1500)))
	assert.Equal(t, int64(0), nonNegativeDiff(int64(1500), int64(500)))
	assert.Equal(t, int64(1000), nonNegativeDiff(int64(1_000_000_000_000), int64(1_000_000_001_000)))
}

// ---------------------------------------------------------------------------
// helper
// ---------------------------------------------------------------------------

func makeProcInfo(
	segID, sessID, pid int64, cmdline, state string,
	utime, stime, cutime, cstime, guestTime, cguestTime int64,
	rchar, wchar, syscr, syscw, readBytes, writeBytes, cancelledWriteBytes int64,
) *pbc.GpPidProcInfo {
	return &pbc.GpPidProcInfo{
		GpSegmentId: segID, SessId: sessID, Pid: pid, Cmdline: cmdline, State: state,
		ProcStatus: &pbc.ProcStatus{VmSize: 1024, VmRss: 512, VmPeak: 2048},
		ProcStat: &pbc.ProcStat{
			Pid: pid, Comm: "postgres", State: state, Ppid: 1,
			Pgrp: int32(pid), Session: int32(pid), Tpgid: -1, Flags: 0x0040,
			MinFlt: 100, CminFlt: 10, MajFlt: 5, CmajFlt: 1,
			Utime: utime, Stime: stime, Cutime: cutime, Cstime: cstime,
			Priority: 20, NumThreads: 4, Starttime: 12345,
			Vsize: 1048576, Rss: 256, RssLimit: 999999,
			StartCode: 0x400000, EndCode: 0x500000, StartStack: 0x7fff0000,
			Processor: 3, DelayAcctBlkIoTicks: 42,
			GuestTime: guestTime, CguestTime: cguestTime,
		},
		ProcIo: &pbc.ProcIO{
			Rchar: rchar, Wchar: wchar, Syscr: syscr, Syscw: syscw,
			ReadBytes: readBytes, WriteBytes: writeBytes, CancelledWriteBytes: cancelledWriteBytes,
		},
	}
}

// ---------------------------------------------------------------------------
// ProcfsDiff
// ---------------------------------------------------------------------------

func TestProcfsDiff_BothNil(t *testing.T) {
	result, err := ProcfsDiff(nil, nil)
	assert.Nil(t, result)
	assert.EqualError(t, err, "first and last both nil")
}

func TestProcfsDiff_FirstNil_ReturnsLast(t *testing.T) {
	last := &pbc.GpPidProcInfo{Pid: 1234, ProcStat: &pbc.ProcStat{Pid: 1234, Utime: 100}, ProcIo: &pbc.ProcIO{Rchar: 500}}
	result, err := ProcfsDiff(nil, last)
	require.NoError(t, err)
	assert.Same(t, last, result)
}

func TestProcfsDiff_LastNil_ReturnsFirst(t *testing.T) {
	first := &pbc.GpPidProcInfo{Pid: 5678, ProcStat: &pbc.ProcStat{Pid: 5678}, ProcIo: &pbc.ProcIO{Wchar: 300}}
	result, err := ProcfsDiff(first, nil)
	require.NoError(t, err)
	assert.Same(t, first, result)
}

func TestProcfsDiff_NormalDiff(t *testing.T) {
	first := makeProcInfo(1, 10, 100, "old", "S", 100, 200, 10, 20, 5, 3, 1000, 2000, 300, 400, 500, 600, 50)
	last := makeProcInfo(1, 10, 100, "new", "R", 350, 550, 40, 60, 15, 8, 3000, 5000, 800, 900, 1200, 1500, 120)
	result, err := ProcfsDiff(first, last)
	require.NoError(t, err)

	assert.Equal(t, int64(1), result.GpSegmentId)
	assert.Equal(t, "new", result.Cmdline)
	assert.Equal(t, "R", result.State)
	utils.AssertProtoMessagesEqual(t, last.ProcStatus, result.ProcStatus)
	assert.Equal(t, int64(100), result.ProcStat.Pid)
	assert.Equal(t, int64(1), result.ProcStat.Ppid)
	assert.Equal(t, int32(20), result.ProcStat.Priority)
	assert.Equal(t, int64(250), result.ProcStat.Utime)
	assert.Equal(t, int64(350), result.ProcStat.Stime)
	assert.Equal(t, int64(30), result.ProcStat.Cutime)
	assert.Equal(t, int64(40), result.ProcStat.Cstime)
	assert.Equal(t, int64(10), result.ProcStat.GuestTime)
	assert.Equal(t, int64(5), result.ProcStat.CguestTime)
	assert.Equal(t, int64(2000), result.ProcIo.Rchar)
	assert.Equal(t, int64(3000), result.ProcIo.Wchar)
	assert.Equal(t, int64(500), result.ProcIo.Syscr)
	assert.Equal(t, int64(500), result.ProcIo.Syscw)
	assert.Equal(t, int64(700), result.ProcIo.ReadBytes)
	assert.Equal(t, int64(900), result.ProcIo.WriteBytes)
	assert.Equal(t, int64(70), result.ProcIo.CancelledWriteBytes)
}

func TestProcfsDiff_CounterReset_ClampedToZero(t *testing.T) {
	first := makeProcInfo(1, 10, 100, "pg", "S", 500, 600, 70, 80, 30, 20, 9000, 8000, 700, 600, 5000, 4000, 300)
	last := makeProcInfo(1, 10, 100, "pg", "S", 100, 200, 10, 20, 5, 3, 1000, 2000, 100, 200, 500, 600, 50)
	result, err := ProcfsDiff(first, last)
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.ProcStat.Utime)
	assert.Equal(t, int64(0), result.ProcStat.Stime)
	assert.Equal(t, int64(0), result.ProcStat.GuestTime)
	assert.Equal(t, int64(0), result.ProcIo.Rchar)
	assert.Equal(t, int64(0), result.ProcIo.Wchar)
	assert.Equal(t, int64(0), result.ProcIo.CancelledWriteBytes)
}

func TestProcfsDiff_MixedCounters(t *testing.T) {
	first := makeProcInfo(2, 20, 200, "pg", "S", 100, 500, 10, 80, 5, 20, 1000, 9000, 300, 600, 500, 4000, 50)
	last := makeProcInfo(2, 20, 200, "pg", "R", 300, 200, 40, 20, 15, 8, 3000, 2000, 800, 200, 1200, 1500, 120)
	result, err := ProcfsDiff(first, last)
	require.NoError(t, err)
	assert.Equal(t, int64(200), result.ProcStat.Utime)
	assert.Equal(t, int64(0), result.ProcStat.Stime)
	assert.Equal(t, int64(30), result.ProcStat.Cutime)
	assert.Equal(t, int64(0), result.ProcStat.Cstime)
	assert.Equal(t, int64(10), result.ProcStat.GuestTime)
	assert.Equal(t, int64(0), result.ProcStat.CguestTime)
	assert.Equal(t, int64(2000), result.ProcIo.Rchar)
	assert.Equal(t, int64(0), result.ProcIo.Wchar)
	assert.Equal(t, int64(700), result.ProcIo.ReadBytes)
	assert.Equal(t, int64(0), result.ProcIo.WriteBytes)
	assert.Equal(t, int64(70), result.ProcIo.CancelledWriteBytes)
}

func TestProcfsDiff_IdenticalMeasurements(t *testing.T) {
	a := makeProcInfo(1, 10, 100, "pg", "S", 100, 200, 10, 20, 5, 3, 1000, 2000, 300, 400, 500, 600, 50)
	b := makeProcInfo(1, 10, 100, "pg", "S", 100, 200, 10, 20, 5, 3, 1000, 2000, 300, 400, 500, 600, 50)
	result, err := ProcfsDiff(a, b)
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.ProcStat.Utime)
	assert.Equal(t, int64(0), result.ProcStat.Stime)
	assert.Equal(t, int64(0), result.ProcIo.Rchar)
	assert.Equal(t, int64(0), result.ProcIo.WriteBytes)
}

func TestProcfsDiff_SnapshotFieldsFromLast(t *testing.T) {
	first := makeProcInfo(1, 10, 100, "first", "S", 100, 200, 10, 20, 5, 3, 1000, 2000, 300, 400, 500, 600, 50)
	last := &pbc.GpPidProcInfo{
		GpSegmentId: 99, SessId: 77, Pid: 888, Cmdline: "last", State: "D",
		ProcStatus: &pbc.ProcStatus{VmSize: 9999, VmRss: 8888, VmPeak: 7777},
		ProcStat: &pbc.ProcStat{
			Pid: 999, Comm: "pg_worker", State: "D", Ppid: 42, Pgrp: 43,
			MinFlt: 555, CminFlt: 66, MajFlt: 77, CmajFlt: 88,
			Utime: 200, Stime: 300, Cutime: 20, Cstime: 30,
			Priority: 10, Nice: -5, NumThreads: 16, Processor: 7,
			Vsize: 2097152, Rss: 512, DelayAcctBlkIoTicks: 99,
			GuestTime: 10, CguestTime: 6,
		},
		ProcIo: &pbc.ProcIO{Rchar: 2000, Wchar: 3000, Syscr: 400, Syscw: 500, ReadBytes: 600, WriteBytes: 700, CancelledWriteBytes: 80},
	}
	result, err := ProcfsDiff(first, last)
	require.NoError(t, err)
	assert.Equal(t, int64(99), result.GpSegmentId)
	assert.Equal(t, int64(999), result.ProcStat.Pid)
	assert.Equal(t, int64(42), result.ProcStat.Ppid)
	assert.Equal(t, int64(455), result.ProcStat.MinFlt)
	assert.Equal(t, int64(100), result.ProcStat.Utime)
	assert.Equal(t, int64(100), result.ProcStat.Stime)
	assert.Equal(t, int64(5), result.ProcStat.GuestTime)
	assert.Equal(t, int64(3), result.ProcStat.CguestTime)
	assert.Equal(t, int64(1000), result.ProcIo.Rchar)
	assert.Equal(t, int64(30), result.ProcIo.CancelledWriteBytes)
}

func TestProcfsDiff_ResultIsNewObject(t *testing.T) {
	first := makeProcInfo(1, 10, 100, "cmd", "S", 10, 20, 1, 2, 1, 1, 100, 200, 30, 40, 50, 60, 5)
	last := makeProcInfo(1, 10, 100, "cmd", "S", 20, 40, 2, 4, 2, 2, 200, 400, 60, 80, 100, 120, 10)
	result, err := ProcfsDiff(first, last)
	require.NoError(t, err)
	assert.NotSame(t, first, result)
	assert.NotSame(t, last, result)
}

func TestProcfsDiff_NilSubStructs(t *testing.T) {
	t.Run("first nil ProcStat", func(t *testing.T) {
		result, err := ProcfsDiff(&pbc.GpPidProcInfo{ProcIo: &pbc.ProcIO{Rchar: 100}}, &pbc.GpPidProcInfo{ProcStat: &pbc.ProcStat{Utime: 200, Pid: 42}, ProcIo: &pbc.ProcIO{Rchar: 300}})
		require.NoError(t, err)
		assert.Equal(t, int64(200), result.ProcStat.Utime)
		assert.Equal(t, int64(200), result.ProcIo.Rchar)
	})
	t.Run("last nil ProcStat", func(t *testing.T) {
		result, err := ProcfsDiff(&pbc.GpPidProcInfo{ProcStat: &pbc.ProcStat{Utime: 100}, ProcIo: &pbc.ProcIO{}}, &pbc.GpPidProcInfo{ProcIo: &pbc.ProcIO{}})
		require.NoError(t, err)
		assert.Equal(t, int64(0), result.ProcStat.Utime)
	})
	t.Run("both nil ProcStat", func(t *testing.T) {
		result, err := ProcfsDiff(&pbc.GpPidProcInfo{ProcIo: &pbc.ProcIO{}}, &pbc.GpPidProcInfo{ProcIo: &pbc.ProcIO{}})
		require.NoError(t, err)
		assert.Nil(t, result.ProcStat)
	})
	t.Run("first nil ProcIo", func(t *testing.T) {
		result, err := ProcfsDiff(&pbc.GpPidProcInfo{ProcStat: &pbc.ProcStat{}}, &pbc.GpPidProcInfo{ProcStat: &pbc.ProcStat{}, ProcIo: &pbc.ProcIO{Rchar: 500}})
		require.NoError(t, err)
		assert.Equal(t, int64(500), result.ProcIo.Rchar)
	})
	t.Run("last nil ProcIo", func(t *testing.T) {
		result, err := ProcfsDiff(&pbc.GpPidProcInfo{ProcStat: &pbc.ProcStat{}, ProcIo: &pbc.ProcIO{Rchar: 500}}, &pbc.GpPidProcInfo{ProcStat: &pbc.ProcStat{}})
		require.NoError(t, err)
		assert.Equal(t, int64(0), result.ProcIo.Rchar)
	})
	t.Run("both nil ProcIo", func(t *testing.T) {
		result, err := ProcfsDiff(&pbc.GpPidProcInfo{ProcStat: &pbc.ProcStat{}}, &pbc.GpPidProcInfo{ProcStat: &pbc.ProcStat{}})
		require.NoError(t, err)
		assert.Nil(t, result.ProcIo)
	})
	t.Run("all sub-structs nil", func(t *testing.T) {
		result, err := ProcfsDiff(&pbc.GpPidProcInfo{Pid: 1}, &pbc.GpPidProcInfo{Pid: 2})
		require.NoError(t, err)
		assert.Equal(t, int64(2), result.Pid)
		assert.Nil(t, result.ProcStat)
		assert.Nil(t, result.ProcIo)
	})
}
