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

package utils

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"testing"

	"github.com/prometheus/procfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsPermissionDenied(t *testing.T) {
	t.Run("nil error", func(t *testing.T) {
		assert.False(t, isPermissionDenied(nil))
	})

	t.Run("permission denied", func(t *testing.T) {
		assert.True(t, isPermissionDenied(fmt.Errorf("open /proc/1234/io: %w", syscall.EACCES)))
	})

	t.Run("operation not permitted", func(t *testing.T) {
		assert.True(t, isPermissionDenied(fmt.Errorf("open /proc/1234/io: %w", os.ErrPermission)))
	})

	t.Run("permission denied in string", func(t *testing.T) {
		assert.True(t, isPermissionDenied(errors.New("open /proc/1234/io: permission denied")))
	})

	t.Run("other error", func(t *testing.T) {
		assert.False(t, isPermissionDenied(errors.New("file not found")))
	})
}

func TestIsProcessGone(t *testing.T) {
	t.Run("nil error", func(t *testing.T) {
		assert.False(t, isProcessGone(nil))
	})

	t.Run("no such process", func(t *testing.T) {
		assert.True(t, isProcessGone(fmt.Errorf("open /proc/999999/stat: %w", syscall.ESRCH)))
	})

	t.Run("no such file or directory", func(t *testing.T) {
		assert.True(t, isProcessGone(fmt.Errorf("open /proc/999999/stat: %w", os.ErrNotExist)))
	})

	t.Run("no such file in string", func(t *testing.T) {
		assert.True(t, isProcessGone(errors.New("open /proc/999999/stat: no such file or directory")))
	})

	t.Run("no such process in string", func(t *testing.T) {
		assert.True(t, isProcessGone(errors.New("open /proc/999999/stat: no such process")))
	})

	t.Run("other error", func(t *testing.T) {
		assert.False(t, isProcessGone(errors.New("permission denied")))
	})
}

func TestConvertProcStat(t *testing.T) {
	src := &procfs.ProcStat{
		PID:                 42,
		Comm:                "postgres",
		State:               "S",
		PPID:                1,
		PGRP:                42,
		Session:             42,
		TTY:                 0,
		TPGID:               -1,
		Flags:               0x00400000,
		MinFlt:              100,
		CMinFlt:             10,
		MajFlt:              5,
		CMajFlt:             1,
		UTime:               500,
		STime:               200,
		CUTime:              50,
		CSTime:              20,
		Priority:            20,
		Nice:                0,
		NumThreads:          4,
		Starttime:           123456789,
		VSize:               1048576,
		RSS:                 256,
		RSSLimit:            18446744073709551615,
		Processor:           3,
		RTPriority:          0,
		Policy:              0,
		DelayAcctBlkIOTicks: 100,
		GuestTime:           0,
		CGuestTime:          0,
	}

	result := convertProcStat(src)

	require.NotNil(t, result)
	assert.Equal(t, int64(42), result.Pid)
	assert.Equal(t, "postgres", result.Comm)
	assert.Equal(t, "S", result.State)
	assert.Equal(t, int64(1), result.Ppid)
	assert.Equal(t, int32(42), result.Pgrp)
	assert.Equal(t, int32(42), result.Session)
	assert.Equal(t, int32(0), result.Tty)
	assert.Equal(t, int32(-1), result.Tpgid)
	assert.Equal(t, int32(0x00400000), result.Flags)
	assert.Equal(t, int64(100), result.MinFlt)
	assert.Equal(t, int64(10), result.CminFlt)
	assert.Equal(t, int64(5), result.MajFlt)
	assert.Equal(t, int64(1), result.CmajFlt)
	assert.Equal(t, int64(500), result.Utime)
	assert.Equal(t, int64(200), result.Stime)
	assert.Equal(t, int64(50), result.Cutime)
	assert.Equal(t, int64(20), result.Cstime)
	assert.Equal(t, int32(20), result.Priority)
	assert.Equal(t, int32(0), result.Nice)
	assert.Equal(t, int32(4), result.NumThreads)
	assert.Equal(t, int64(123456789), result.Starttime)
	assert.Equal(t, int64(1048576), result.Vsize)
	assert.Equal(t, int64(256), result.Rss)
	assert.Equal(t, int64(-1), result.RssLimit)
	assert.Equal(t, int32(3), result.Processor)
	assert.Equal(t, int32(0), result.RtPriority)
	assert.Equal(t, int32(0), result.Policy)
	assert.Equal(t, int64(100), result.DelayAcctBlkIoTicks)
	assert.Equal(t, int64(0), result.GuestTime)
	assert.Equal(t, int64(0), result.CguestTime)
}

func TestConvertProcStatus(t *testing.T) {
	src := &procfs.ProcStatus{
		PID:                      42,
		Name:                     "postgres",
		TGID:                     42,
		NSpids:                   []uint64{42, 1},
		VmPeak:                   2048000,
		VmSize:                   1024000,
		VmLck:                    0,
		VmPin:                    0,
		VmHWM:                    512000,
		VmRSS:                    256000,
		RssAnon:                  128000,
		RssFile:                  64000,
		RssShmem:                 64000,
		VmData:                   100000,
		VmStk:                    8192,
		VmExe:                    4096,
		VmLib:                    50000,
		VmPTE:                    1024,
		VmPMD:                    512,
		VmSwap:                   0,
		HugetlbPages:             0,
		VoluntaryCtxtSwitches:    1000,
		NonVoluntaryCtxtSwitches: 50,
		UIDs:                     [4]uint64{1000, 1000, 1000, 1000},
		GIDs:                     [4]uint64{100, 100, 100, 100},
		CpusAllowedList:          []uint64{0, 1, 2, 3},
	}

	result := convertProcStatus(src)

	require.NotNil(t, result)
	assert.Equal(t, int64(42), result.Pid)
	assert.Equal(t, "postgres", result.Name)
	assert.Equal(t, int32(42), result.Tgid)
	assert.Equal(t, []int64{42, 1}, result.NsPids)
	assert.Equal(t, int64(2048000), result.VmPeak)
	assert.Equal(t, int64(1024000), result.VmSize)
	assert.Equal(t, int64(0), result.VmLck)
	assert.Equal(t, int64(0), result.VmPin)
	assert.Equal(t, int64(512000), result.VmHwm)
	assert.Equal(t, int64(256000), result.VmRss)
	assert.Equal(t, int64(128000), result.RssAnon)
	assert.Equal(t, int64(64000), result.RssFile)
	assert.Equal(t, int64(64000), result.RssShmem)
	assert.Equal(t, int64(100000), result.VmData)
	assert.Equal(t, int64(8192), result.VmStk)
	assert.Equal(t, int64(4096), result.VmExe)
	assert.Equal(t, int64(50000), result.VmLib)
	assert.Equal(t, int64(1024), result.VmPte)
	assert.Equal(t, int64(512), result.VmPmd)
	assert.Equal(t, int64(0), result.VmSwap)
	assert.Equal(t, int64(0), result.HugetlbPages)
	assert.Equal(t, int64(1000), result.VoluntaryCtxtSwitches)
	assert.Equal(t, int64(50), result.NonVoluntaryCtxtSwitches)
	assert.Equal(t, []int64{1000, 1000, 1000, 1000}, result.Uids)
	assert.Equal(t, []int64{100, 100, 100, 100}, result.Gids)
	assert.Equal(t, []int64{0, 1, 2, 3}, result.CpusAllowedList)
}

func TestConvertProcIO(t *testing.T) {
	src := &procfs.ProcIO{
		RChar:               1000000,
		WChar:               500000,
		SyscR:               200,
		SyscW:               100,
		ReadBytes:           4096000,
		WriteBytes:          2048000,
		CancelledWriteBytes: 512,
	}

	result := convertProcIO(src)

	require.NotNil(t, result)
	assert.Equal(t, int64(1000000), result.Rchar)
	assert.Equal(t, int64(500000), result.Wchar)
	assert.Equal(t, int64(200), result.Syscr)
	assert.Equal(t, int64(100), result.Syscw)
	assert.Equal(t, int64(4096000), result.ReadBytes)
	assert.Equal(t, int64(2048000), result.WriteBytes)
	assert.Equal(t, int64(512), result.CancelledWriteBytes)
}

func TestParseCmdLineSessionStatus(t *testing.T) {
	tests := []struct {
		name     string
		cmdline  string
		expected string
	}{
		{
			name:     "idle session",
			cmdline:  "postgres:  5432, gpadmin postgres localhost(33326) con21 cmd237786 idle",
			expected: "idle",
		},
		{
			name:     "SELECT query",
			cmdline:  "postgres:  5432, monitor postgres localhost(22122) con38 cmd1006767 SELECT",
			expected: "SELECT",
		},
		{
			name:     "SELECT query with slice",
			cmdline:  "postgres:  5432, monitor postgres localhost(22122) con38 cmd1006767 slice2 SELECT",
			expected: "SELECT",
		},
		{
			name:     "idle session with different user",
			cmdline:  "postgres:  5432, monitor postgres localhost(31260) con3185445 cmd2631 idle",
			expected: "idle",
		},
		{
			name:     "idle session via unix socket",
			cmdline:  "postgres:  5432, gpadmin postgres [local] con3297072 cmd11 idle",
			expected: "idle",
		},
		{
			name:     "master logger process",
			cmdline:  "postgres:  5432, master logger process",
			expected: "",
		},
		{
			name:     "bgworker logerrors",
			cmdline:  "postgres:  5432, bgworker: logerrors",
			expected: "",
		},
		{
			name:     "checkpointer process",
			cmdline:  "postgres:  5432, checkpointer process",
			expected: "",
		},
		{
			name:     "writer process",
			cmdline:  "postgres:  5432, writer process",
			expected: "",
		},
		{
			name:     "wal writer process",
			cmdline:  "postgres:  5432, wal writer process",
			expected: "",
		},
		{
			name:     "archiver process",
			cmdline:  "postgres:  5432, archiver process   last was 000000010000000F00000015",
			expected: "",
		},
		{
			name:     "stats collector process",
			cmdline:  "postgres:  5432, stats collector process",
			expected: "",
		},
		{
			name:     "bgworker dtx recovery",
			cmdline:  "postgres:  5432, bgworker: dtx recovery process",
			expected: "",
		},
		{
			name:     "bgworker ftsprobe",
			cmdline:  "postgres:  5432, bgworker: ftsprobe process",
			expected: "",
		},
		{
			name:     "bgworker gp_relsizes_stats_worker",
			cmdline:  "postgres:  5432, bgworker: gp_relsizes_stats_worker",
			expected: "",
		},
		{
			name:     "bgworker diskquota launcher",
			cmdline:  "postgres:  5432, bgworker: [diskquota] - launcher",
			expected: "",
		},
		{
			name:     "bgworker ic proxy",
			cmdline:  "postgres:  5432, bgworker: ic proxy process",
			expected: "",
		},
		{
			name:     "bgworker global deadlock detector",
			cmdline:  "postgres:  5432, bgworker: global deadlock detector process",
			expected: "",
		},
		{
			name:     "wal sender process",
			cmdline:  "postgres:  5432, wal sender process gpadmin rc1b-srh6mco58rmiivfc.mdb.yandexcloud.net(24578) streaming F/58000F80",
			expected: "",
		},
		{
			name:     "empty string",
			cmdline:  "",
			expected: "",
		},
		{
			name:     "unrelated process",
			cmdline:  "/usr/bin/python3 /opt/script.py",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseCmdLineSessionStatus(tt.cmdline)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseCmdLineSliceID(t *testing.T) {
	tests := []struct {
		name    string
		cmdline string
		want    int64
		wantOK  bool
	}{
		{name: "active query with slice", cmdline: "postgres: con100 cmd7 slice2 SELECT", want: 2, wantOK: true},
		{name: "QE writer with two digits slice", cmdline: "postgres:  5432, gpadmin postgres localhost(33326) con21 cmd237786 slice12 INSERT", want: 12, wantOK: true},
		{name: "last slice token wins", cmdline: "prefix slice1 middle slice42 SELECT", want: 42, wantOK: true},
		{name: "idle session has no slice", cmdline: "postgres: con100 cmd7 idle", wantOK: false},
		{name: "slice without digits", cmdline: "postgres: con100 cmd7 slice SELECT", wantOK: false},
		{name: "slice overflow", cmdline: "postgres: con100 cmd7 slice999999999999999999999999 SELECT", wantOK: false},
		{name: "background process", cmdline: "postgres: checkpointer process", wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := ParseCmdLineSliceID(tt.cmdline)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGetPidProcInfo_NonExistentPid(t *testing.T) {
	// A very large PID is guaranteed not to exist.
	info, err := GetPidProcInfo(nil, 4194305000000, 1, 100)
	assert.ErrorIs(t, err, ErrProcessNotFound)
	assert.Nil(t, info)
}

func TestGetPidProcInfo_OutOfRangePid(t *testing.T) {
	// Negative and zero PIDs cannot correspond to real processes;
	// expect ErrProcessNotFound.
	tests := []struct {
		name string
		pid  int64
	}{
		{"negative", -1},
		{"zero", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info, err := GetPidProcInfo(nil, tt.pid, 1, 100)
			assert.ErrorIs(t, err, ErrProcessNotFound)
			assert.Nil(t, info)
		})
	}
}
