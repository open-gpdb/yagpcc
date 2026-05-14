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
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/prometheus/procfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// fakeProcDir is a helper that creates a temporary directory mimicking
// /proc/<pid>/ with the given file contents. Files with nil content are
// skipped (simulating a missing proc file).
type fakeProcDir struct {
	root string
	pid  int
}

func newFakeProcDir(t *testing.T, pid int) *fakeProcDir {
	t.Helper()
	root := t.TempDir()
	pidDir := filepath.Join(root, strconv.Itoa(pid))
	require.NoError(t, os.MkdirAll(pidDir, 0o755))
	return &fakeProcDir{root: root, pid: pid}
}

func (f *fakeProcDir) writeFile(t *testing.T, name, content string) {
	t.Helper()
	path := filepath.Join(f.root, strconv.Itoa(f.pid), name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
}

func (f *fakeProcDir) proc(t *testing.T) procfs.Proc {
	t.Helper()
	fs, err := procfs.NewFS(f.root)
	require.NoError(t, err)
	p, err := fs.Proc(f.pid)
	require.NoError(t, err)
	return p
}

// sampleStat returns a realistic /proc/<pid>/stat line for the given PID.
// Format matches what the Linux kernel writes to /proc/<pid>/stat.
func sampleStat(pid int) string {
	// Fields: pid (comm) state ppid pgrp session tty tpgid flags minflt cminflt
	// majflt cmajflt utime stime cutime cstime priority nice num_threads
	// itrealvalue starttime vsize rss rsslimit startcode endcode startstack
	// kstkesp kstkeip signal blocked sigignore sigcatch wchan nswap cnswap
	// exit_signal processor rt_priority policy delayacct_blkio_ticks guest_time
	// cguest_time
	return fmt.Sprintf("%d (postgres) S 1 %d %d 0 -1 4194304 100 10 5 1 500 200 50 20 20 0 4 0 123456789 1048576 256 18446744073709551615 0 0 0 0 0 0 0 0 0 0 0 0 0 3 0 0 100 0 0 0 0 0 0 0 0 0 0",
		pid, pid, pid)
}

// sampleStatus returns a realistic /proc/<pid>/status content.
func sampleStatus(pid int) string {
	return fmt.Sprintf(`Name:	postgres
Umask:	0022
State:	S (sleeping)
Tgid:	%d
Ngid:	0
Pid:	%d
PPid:	1
TracerPid:	0
Uid:	1000	1000	1000	1000
Gid:	100	100	100	100
FDSize:	256
NStgid:	%d
NSpid:	%d
VmPeak:	2048 kB
VmSize:	1024 kB
VmLck:	0 kB
VmPin:	0 kB
VmHWM:	512 kB
VmRSS:	256 kB
RssAnon:	128 kB
RssFile:	64 kB
RssShmem:	64 kB
VmData:	100 kB
VmStk:	8 kB
VmExe:	4 kB
VmLib:	50 kB
VmPTE:	1 kB
VmPMD:	0 kB
VmSwap:	0 kB
HugetlbPages:	0 kB
voluntary_ctxt_switches:	1000
nonvoluntary_ctxt_switches:	50
Cpus_allowed_list:	0-3`, pid, pid, pid, pid)
}

// sampleIO returns a realistic /proc/<pid>/io content.
func sampleIO() string {
	return `rchar: 1000000
wchar: 500000
syscr: 200
syscw: 100
read_bytes: 4096000
write_bytes: 2048000
cancelled_write_bytes: 512
`
}

func TestGetProcInfo_AllFiles(t *testing.T) {
	const pid = 42
	fake := newFakeProcDir(t, pid)
	logger := zap.NewNop().Sugar()

	// Session backend cmdline with "idle" status.
	cmdline := "postgres:  5432, gpadmin postgres localhost(33326) con21 cmd237786 idle"
	fake.writeFile(t, "cmdline", cmdline)
	fake.writeFile(t, "stat", sampleStat(pid))
	fake.writeFile(t, "status", sampleStatus(pid))
	fake.writeFile(t, "io", sampleIO())

	proc := fake.proc(t)
	info, err := getProcInfo(logger, proc, int64(pid), 7, 100)

	require.NoError(t, err)
	require.NotNil(t, info)

	// Pass-through fields.
	assert.Equal(t, int64(7), info.GpSegmentId)
	assert.Equal(t, int64(100), info.SessId)
	assert.Equal(t, int64(pid), info.Pid)

	// Cmdline and state extraction.
	assert.Equal(t, cmdline, info.Cmdline)
	assert.Equal(t, "idle", info.State)

	// ProcStat populated.
	require.NotNil(t, info.ProcStat)
	assert.Equal(t, int64(pid), info.ProcStat.Pid)
	assert.Equal(t, "postgres", info.ProcStat.Comm)
	assert.Equal(t, "S", info.ProcStat.State)
	assert.Equal(t, int64(1), info.ProcStat.Ppid)
	assert.Equal(t, int32(4), info.ProcStat.NumThreads)
	assert.Equal(t, int64(123456789), info.ProcStat.Starttime)
	assert.Equal(t, int64(1048576), info.ProcStat.Vsize)
	assert.Equal(t, int64(256), info.ProcStat.Rss)
	assert.Equal(t, int64(500), info.ProcStat.Utime)
	assert.Equal(t, int64(200), info.ProcStat.Stime)
	assert.Equal(t, int32(3), info.ProcStat.Processor)
	assert.Equal(t, int64(100), info.ProcStat.DelayAcctBlkIoTicks)

	// ProcStatus populated.
	require.NotNil(t, info.ProcStatus)
	assert.Equal(t, int64(pid), info.ProcStatus.Pid)
	assert.Equal(t, "postgres", info.ProcStatus.Name)
	assert.Equal(t, int32(pid), info.ProcStatus.Tgid)
	assert.Equal(t, int64(2048*1024), info.ProcStatus.VmPeak)
	assert.Equal(t, int64(1024*1024), info.ProcStatus.VmSize)
	assert.Equal(t, int64(512*1024), info.ProcStatus.VmHwm)
	assert.Equal(t, int64(256*1024), info.ProcStatus.VmRss)
	assert.Equal(t, int64(128*1024), info.ProcStatus.RssAnon)
	assert.Equal(t, int64(64*1024), info.ProcStatus.RssFile)
	assert.Equal(t, int64(64*1024), info.ProcStatus.RssShmem)
	assert.Equal(t, int64(1000), info.ProcStatus.VoluntaryCtxtSwitches)
	assert.Equal(t, int64(50), info.ProcStatus.NonVoluntaryCtxtSwitches)
	assert.Equal(t, []int64{1000, 1000, 1000, 1000}, info.ProcStatus.Uids)
	assert.Equal(t, []int64{100, 100, 100, 100}, info.ProcStatus.Gids)
	assert.Equal(t, []int64{0, 1, 2, 3}, info.ProcStatus.CpusAllowedList)

	// ProcIO populated.
	require.NotNil(t, info.ProcIo)
	assert.Equal(t, int64(1000000), info.ProcIo.Rchar)
	assert.Equal(t, int64(500000), info.ProcIo.Wchar)
	assert.Equal(t, int64(200), info.ProcIo.Syscr)
	assert.Equal(t, int64(100), info.ProcIo.Syscw)
	assert.Equal(t, int64(4096000), info.ProcIo.ReadBytes)
	assert.Equal(t, int64(2048000), info.ProcIo.WriteBytes)
	assert.Equal(t, int64(512), info.ProcIo.CancelledWriteBytes)
}

func TestGetProcInfo_SessionStateExtraction(t *testing.T) {
	logger := zap.NewNop().Sugar()
	tests := []struct {
		name          string
		cmdline       string
		expectedState string
	}{
		{
			name:          "idle session",
			cmdline:       "postgres:  5432, gpadmin postgres localhost(33326) con21 cmd237786 idle",
			expectedState: "idle",
		},
		{
			name:          "SELECT query",
			cmdline:       "postgres:  5432, monitor postgres localhost(22122) con38 cmd1006767 SELECT",
			expectedState: "SELECT",
		},
		{
			name:          "background process has no state",
			cmdline:       "postgres:  5432, checkpointer process",
			expectedState: "",
		},
		{
			name:          "empty cmdline",
			cmdline:       "",
			expectedState: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const pid = 99
			fake := newFakeProcDir(t, pid)
			fake.writeFile(t, "cmdline", tt.cmdline)
			fake.writeFile(t, "stat", sampleStat(pid))
			fake.writeFile(t, "status", sampleStatus(pid))
			fake.writeFile(t, "io", sampleIO())

			proc := fake.proc(t)
			info, err := getProcInfo(logger, proc, int64(pid), 1, 1)

			require.NoError(t, err)
			require.NotNil(t, info)
			assert.Equal(t, tt.expectedState, info.State)
		})
	}
}

func TestGetProcInfo_MissingCmdline(t *testing.T) {
	const pid = 10
	fake := newFakeProcDir(t, pid)
	logger := zap.NewNop().Sugar()
	// No cmdline file written.
	fake.writeFile(t, "stat", sampleStat(pid))
	fake.writeFile(t, "status", sampleStatus(pid))
	fake.writeFile(t, "io", sampleIO())

	proc := fake.proc(t)
	info, err := getProcInfo(logger, proc, int64(pid), 2, 200)

	require.NoError(t, err)
	require.NotNil(t, info)

	// Cmdline and state should be empty when file is missing.
	assert.Empty(t, info.Cmdline)
	assert.Empty(t, info.State)

	// Other fields should still be populated.
	require.NotNil(t, info.ProcStat)
	assert.Equal(t, int64(pid), info.ProcStat.Pid)
	require.NotNil(t, info.ProcStatus)
	assert.Equal(t, int64(pid), info.ProcStatus.Pid)
	require.NotNil(t, info.ProcIo)
}

func TestGetProcInfo_MissingStat(t *testing.T) {
	const pid = 11
	fake := newFakeProcDir(t, pid)
	logger := zap.NewNop().Sugar()
	fake.writeFile(t, "cmdline", "postgres:  5432, gpadmin postgres localhost(33326) con21 cmd1 idle")
	// No stat file written.
	fake.writeFile(t, "status", sampleStatus(pid))
	fake.writeFile(t, "io", sampleIO())

	proc := fake.proc(t)
	info, err := getProcInfo(logger, proc, int64(pid), 3, 300)

	require.NoError(t, err)
	require.NotNil(t, info)

	// Cmdline should be populated.
	assert.NotEmpty(t, info.Cmdline)
	assert.Equal(t, "idle", info.State)

	// ProcStat should be nil when stat file is missing.
	assert.Nil(t, info.ProcStat)

	// Other fields should still be populated.
	require.NotNil(t, info.ProcStatus)
	require.NotNil(t, info.ProcIo)
}

func TestGetProcInfo_MissingStatus(t *testing.T) {
	const pid = 12
	fake := newFakeProcDir(t, pid)
	logger := zap.NewNop().Sugar()
	fake.writeFile(t, "cmdline", "postgres:  5432, gpadmin postgres localhost(33326) con21 cmd1 idle")
	fake.writeFile(t, "stat", sampleStat(pid))
	// No status file written.
	fake.writeFile(t, "io", sampleIO())

	proc := fake.proc(t)
	info, err := getProcInfo(logger, proc, int64(pid), 4, 400)

	require.NoError(t, err)
	require.NotNil(t, info)

	assert.NotEmpty(t, info.Cmdline)
	require.NotNil(t, info.ProcStat)

	// ProcStatus should be nil when status file is missing.
	assert.Nil(t, info.ProcStatus)

	require.NotNil(t, info.ProcIo)
}

func TestGetProcInfo_MissingIO(t *testing.T) {
	const pid = 13
	fake := newFakeProcDir(t, pid)
	logger := zap.NewNop().Sugar()
	fake.writeFile(t, "cmdline", "postgres:  5432, gpadmin postgres localhost(33326) con21 cmd1 idle")
	fake.writeFile(t, "stat", sampleStat(pid))
	fake.writeFile(t, "status", sampleStatus(pid))
	// No io file written.

	proc := fake.proc(t)
	info, err := getProcInfo(logger, proc, int64(pid), 5, 500)

	require.NoError(t, err)
	require.NotNil(t, info)

	assert.NotEmpty(t, info.Cmdline)
	require.NotNil(t, info.ProcStat)
	require.NotNil(t, info.ProcStatus)

	// ProcIO should be nil when io file is missing.
	assert.Nil(t, info.ProcIo)
}

func TestGetProcInfo_AllFilesMissing(t *testing.T) {
	const pid = 14
	fake := newFakeProcDir(t, pid)
	logger := zap.NewNop().Sugar()
	// No files written at all — simulates a process that exists in the
	// directory listing but whose proc files have all vanished.

	proc := fake.proc(t)
	info, err := getProcInfo(logger, proc, int64(pid), 6, 600)

	require.NoError(t, err)
	require.NotNil(t, info)

	// Pass-through fields should still be set.
	assert.Equal(t, int64(6), info.GpSegmentId)
	assert.Equal(t, int64(600), info.SessId)
	assert.Equal(t, int64(pid), info.Pid)

	// All proc data should be nil/empty.
	assert.Empty(t, info.Cmdline)
	assert.Empty(t, info.State)
	assert.Nil(t, info.ProcStat)
	assert.Nil(t, info.ProcStatus)
	assert.Nil(t, info.ProcIo)
}

func TestGetProcInfo_PassThroughFields(t *testing.T) {
	// Verify that gpSegmentID and sessID are correctly passed through
	// for various values including zero and negative.
	logger := zap.NewNop().Sugar()
	tests := []struct {
		name        string
		gpSegmentID int64
		sessID      int64
	}{
		{"positive values", 7, 42},
		{"zero values", 0, 0},
		{"negative segment", -1, 100},
		{"large values", 999999, 888888},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const pid = 15
			fake := newFakeProcDir(t, pid)
			fake.writeFile(t, "cmdline", "test")
			fake.writeFile(t, "stat", sampleStat(pid))
			fake.writeFile(t, "status", sampleStatus(pid))
			fake.writeFile(t, "io", sampleIO())

			proc := fake.proc(t)
			info, err := getProcInfo(logger, proc, int64(pid), tt.gpSegmentID, tt.sessID)

			require.NoError(t, err)
			require.NotNil(t, info)
			assert.Equal(t, tt.gpSegmentID, info.GpSegmentId)
			assert.Equal(t, tt.sessID, info.SessId)
			assert.Equal(t, int64(pid), info.Pid)
		})
	}
}

func TestGetProcInfo_IOPermissionDenied(t *testing.T) {
	const pid = 17
	fake := newFakeProcDir(t, pid)
	logger := zap.NewNop().Sugar()
	fake.writeFile(t, "cmdline", "postgres:  5432, gpadmin postgres localhost(33326) con21 cmd1 idle")
	fake.writeFile(t, "stat", sampleStat(pid))
	fake.writeFile(t, "status", sampleStatus(pid))
	// Create the io file but make it unreadable to simulate permission denied
	ioPath := filepath.Join(fake.root, strconv.Itoa(pid), "io")
	err := os.WriteFile(ioPath, []byte(sampleIO()), 0o200) // write-only permissions
	require.NoError(t, err)

	proc := fake.proc(t)
	info, err := getProcInfo(logger, proc, int64(pid), 8, 800)

	require.NoError(t, err)
	require.NotNil(t, info)

	// Cmdline and state should be populated
	assert.NotEmpty(t, info.Cmdline)
	assert.Equal(t, "idle", info.State)

	// ProcStat and ProcStatus should be populated
	require.NotNil(t, info.ProcStat)
	require.NotNil(t, info.ProcStatus)

	// ProcIO should be nil when io file access is denied (permission denied)
	assert.Nil(t, info.ProcIo)
}

func TestGetProcInfo_MultiWordCmdline(t *testing.T) {
	// procfs cmdline uses null bytes as separators; the function joins them
	// with spaces.
	const pid = 16
	fake := newFakeProcDir(t, pid)
	logger := zap.NewNop().Sugar()
	// Simulate a cmdline with null-separated arguments.
	fake.writeFile(t, "cmdline", "/usr/bin/postgres\x00-D\x00/data")
	fake.writeFile(t, "stat", sampleStat(pid))
	fake.writeFile(t, "status", sampleStatus(pid))
	fake.writeFile(t, "io", sampleIO())

	proc := fake.proc(t)
	info, err := getProcInfo(logger, proc, int64(pid), 1, 1)

	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, "/usr/bin/postgres -D /data", info.Cmdline)
	// Not a session cmdline, so state should be empty.
	assert.Empty(t, info.State)
}
