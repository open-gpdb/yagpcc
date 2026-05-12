package utils

import (
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"syscall"

	"github.com/prometheus/procfs"
	"go.uber.org/zap"

	pb "github.com/open-gpdb/yagpcc/api/proto/common"
)

// ErrProcessNotFound is returned by GetPidProcInfo when the target PID does
// not exist, has already exited, or falls outside the valid PID range.
// Callers should check for this error with errors.Is and handle it
// accordingly (e.g. skip the PID, log at debug level, increment a counter).
var ErrProcessNotFound = errors.New("process not found")

// ParseCmdLineSessionStatus extracts the session status from a
// PostgreSQL/Greenplum backend process cmdline string (as seen in `ps` output
// or read from /proc/<pid>/cmdline).
//
// Session backend cmdlines contain " con<N> cmd<N> <status>" at the end,
// for example:
//
//	postgres:  5432, gpadmin postgres localhost(33326) con21 cmd237786 idle
//	postgres:  5432, monitor postgres localhost(22122) con38 cmd1006767 SELECT
//
// The function returns the status portion (e.g. "idle", "SELECT") or an
// empty string when the cmdline does not belong to a session backend
// (background processes like "checkpointer process", "bgworker: …", etc.).
func ParseCmdLineSessionStatus(cmdline string) string {
	// Session cmdlines always contain " cmd" followed by a number and the status.
	cmdIdx := strings.LastIndex(cmdline, " cmd")
	if cmdIdx == -1 {
		return ""
	}

	// Verify this is a session line by also checking for " con".
	if !strings.Contains(cmdline, " con") {
		return ""
	}

	// After " cmd" we expect "<digits> <status>".
	// Find the first space after "cmd<digits>" to locate the status.
	afterCmd := cmdline[cmdIdx+4:] // skip " cmd"
	spaceIdx := strings.IndexByte(afterCmd, ' ')
	if spaceIdx == -1 {
		return ""
	}

	status := strings.TrimSpace(afterCmd[spaceIdx+1:])
	return status
}

// GetPidProcInfo reads /proc/<pid>/{stat,status,cmdline,io} via procfs and
// returns a populated GpPidProcInfo protobuf message.
// gpSegmentID and sessID are pass-through identifiers written into the
// returned message so the caller can correlate the result with a
// Greenplum backend.
//
// If the process does not exist (or disappears mid-read) the function
// returns an error wrapping ErrProcessNotFound. Callers should use
// errors.Is(err, ErrProcessNotFound) to distinguish vanished PIDs from
// unexpected failures.
func GetPidProcInfo(logger *zap.SugaredLogger, pid int64, gpSegmentID, sessID int64) (*pb.GpPidProcInfo, error) {
	// Guard against int64 values that cannot be represented as a platform-native
	// int (required by procfs.NewProc). On 32-bit systems int is 32 bits wide, so
	// an int64 PID larger than math.MaxInt would overflow on conversion. PIDs
	// outside the valid range (1 … math.MaxInt) cannot correspond to a real OS
	// process.
	if pid <= 0 || pid > math.MaxInt {
		return nil, fmt.Errorf("pid %d out of valid range: %w", pid, ErrProcessNotFound)
	}
	proc, err := procfs.NewProc(int(pid))
	if err != nil {
		if isProcessGone(err) {
			return nil, fmt.Errorf("pid %d: %w", pid, ErrProcessNotFound)
		}
		return nil, err
	}

	return getProcInfo(logger, proc, pid, gpSegmentID, sessID)
}

// getProcInfo reads /proc/<pid>/{stat,status,cmdline,io} from the given
// procfs.Proc handle and returns a populated GpPidProcInfo protobuf message.
// It is separated from GetPidProcInfo to allow testing with a fake procfs
// directory.
func getProcInfo(logger *zap.SugaredLogger, proc procfs.Proc, pid, gpSegmentID, sessID int64) (*pb.GpPidProcInfo, error) {
	info := &pb.GpPidProcInfo{
		GpSegmentId: gpSegmentID,
		SessId:      sessID,
		Pid:         pid,
	}

	// /proc/<pid>/cmdline
	if cmdline, err := proc.CmdLine(); err == nil {
		info.Cmdline = strings.Join(cmdline, " ")
		info.State = ParseCmdLineSessionStatus(info.Cmdline)
	} else if !isProcessGone(err) {
		return nil, err
	}

	// /proc/<pid>/stat
	if stat, err := proc.Stat(); err == nil {
		info.ProcStat = convertProcStat(&stat)
	} else if !isProcessGone(err) {
		return nil, err
	}

	// /proc/<pid>/status
	if status, err := proc.NewStatus(); err == nil {
		info.ProcStatus = convertProcStatus(&status)
	} else if !isProcessGone(err) {
		return nil, err
	}

	// /proc/<pid>/io
	if pio, err := proc.IO(); err == nil {
		info.ProcIo = convertProcIO(&pio)
	} else if !isProcessGone(err) && !isPermissionDenied(err) {
		return nil, err
	} else if isPermissionDenied(err) {
		// log permission denied for convenience
		if logger != nil {
			logger.Debugf("Permission denied reading io stat for %v error %v ", pid, err)
		}
	}

	return info, nil
}

// isProcessGone returns true when the error indicates the target PID
// no longer exists (ENOENT / ESRCH or errors wrapping them).
func isProcessGone(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
		return true
	}
	// Also check for error strings containing "no such file" or "no such process"
	errStr := err.Error()
	return strings.Contains(errStr, "no such file") || strings.Contains(errStr, "no such process")
}

// isPermissionDenied returns true when the error indicates a permission denied error (EACCES)
func isPermissionDenied(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, os.ErrPermission) || errors.Is(err, syscall.EACCES) {
		return true
	}
	// Also check for error strings containing "permission denied"
	return strings.Contains(err.Error(), "permission denied")
}

// convertProcStat maps prometheus/procfs.ProcStat → protobuf ProcStat.
func convertProcStat(s *procfs.ProcStat) *pb.ProcStat {
	return &pb.ProcStat{
		Pid:                 int64(s.PID),
		Comm:                s.Comm,
		State:               s.State,
		Ppid:                int64(s.PPID),
		Pgrp:                int32(s.PGRP),
		Session:             int32(s.Session),
		Tty:                 int32(s.TTY),
		Tpgid:               int32(s.TPGID),
		Flags:               int32(s.Flags),
		MinFlt:              int64(s.MinFlt),
		CminFlt:             int64(s.CMinFlt),
		MajFlt:              int64(s.MajFlt),
		CmajFlt:             int64(s.CMajFlt),
		Utime:               int64(s.UTime),
		Stime:               int64(s.STime),
		Cutime:              int64(s.CUTime),
		Cstime:              int64(s.CSTime),
		Priority:            int32(s.Priority),
		Nice:                int32(s.Nice),
		NumThreads:          int32(s.NumThreads),
		Starttime:           int64(s.Starttime),
		Vsize:               int64(s.VSize),
		Rss:                 int64(s.RSS),
		RssLimit:            int64(s.RSSLimit),
		Processor:           int32(s.Processor),
		RtPriority:          int32(s.RTPriority),
		Policy:              int32(s.Policy),
		DelayAcctBlkIoTicks: int64(s.DelayAcctBlkIOTicks),
		GuestTime:           int64(s.GuestTime),
		CguestTime:          int64(s.CGuestTime),
	}
}

// convertProcStatus maps prometheus/procfs.ProcStatus → protobuf ProcStatus.
func convertProcStatus(s *procfs.ProcStatus) *pb.ProcStatus {
	return &pb.ProcStatus{
		Pid:                      int64(s.PID),
		Name:                     s.Name,
		Tgid:                     int32(s.TGID),
		NsPids:                   uint64SliceToInt64(s.NSpids),
		VmPeak:                   int64(s.VmPeak),
		VmSize:                   int64(s.VmSize),
		VmLck:                    int64(s.VmLck),
		VmPin:                    int64(s.VmPin),
		VmHwm:                    int64(s.VmHWM),
		VmRss:                    int64(s.VmRSS),
		RssAnon:                  int64(s.RssAnon),
		RssFile:                  int64(s.RssFile),
		RssShmem:                 int64(s.RssShmem),
		VmData:                   int64(s.VmData),
		VmStk:                    int64(s.VmStk),
		VmExe:                    int64(s.VmExe),
		VmLib:                    int64(s.VmLib),
		VmPte:                    int64(s.VmPTE),
		VmPmd:                    int64(s.VmPMD),
		VmSwap:                   int64(s.VmSwap),
		HugetlbPages:             int64(s.HugetlbPages),
		VoluntaryCtxtSwitches:    int64(s.VoluntaryCtxtSwitches),
		NonVoluntaryCtxtSwitches: int64(s.NonVoluntaryCtxtSwitches),
		Uids:                     uint64SliceToInt64(s.UIDs[:]),
		Gids:                     uint64SliceToInt64(s.GIDs[:]),
		CpusAllowedList:          uint64SliceToInt64(s.CpusAllowedList),
	}
}

// convertProcIO maps prometheus/procfs.ProcIO → protobuf ProcIO.
func convertProcIO(io *procfs.ProcIO) *pb.ProcIO {
	return &pb.ProcIO{
		Rchar:               int64(io.RChar),
		Wchar:               int64(io.WChar),
		Syscr:               int64(io.SyscR),
		Syscw:               int64(io.SyscW),
		ReadBytes:           int64(io.ReadBytes),
		WriteBytes:          int64(io.WriteBytes),
		CancelledWriteBytes: io.CancelledWriteBytes,
	}
}

// uint64SliceToInt64 converts a []uint64 to []int64.
func uint64SliceToInt64(src []uint64) []int64 {
	dst := make([]int64, len(src))
	for i, v := range src {
		dst[i] = int64(v)
	}
	return dst
}
