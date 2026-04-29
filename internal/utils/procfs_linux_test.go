package utils

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetPidProcInfo_CurrentProcess(t *testing.T) {
	pid := os.Getpid()
	info, err := GetPidProcInfo(pid, 7, 42)

	require.NoError(t, err)
	require.NotNil(t, info)

	assert.Equal(t, int64(7), info.GpSegmentId)
	assert.Equal(t, int64(42), info.SessId)
	assert.Equal(t, int64(pid), info.Pid)
	assert.NotEmpty(t, info.Cmdline)

	require.NotNil(t, info.ProcStat)
	assert.Equal(t, int32(pid), info.ProcStat.Pid)
	assert.NotEmpty(t, info.ProcStat.Comm)
	assert.NotEmpty(t, info.ProcStat.State)
	assert.Greater(t, info.ProcStat.NumThreads, int32(0))

	require.NotNil(t, info.ProcStatus)
	assert.Equal(t, int32(pid), info.ProcStatus.Pid)
	assert.NotEmpty(t, info.ProcStatus.Name)
	assert.Greater(t, info.ProcStatus.VmSize, uint64(0))
	assert.Greater(t, info.ProcStatus.VmRss, uint64(0))
}
