package grpc_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/open-gpdb/yagpcc/api/proto/agent_segment"
)

func TestGpPidProcInfo_CurrentProcess(t *testing.T) {
	server := newTestGetQueryInfoServer(t)
	pid := int64(os.Getpid())

	resp, err := server.GpPidProcInfo(context.Background(), &pb.GetPidProcInfoReq{
		SegmentProcess: []*pb.SegmentProcess{
			{GpSegmentId: 7, SessId: 42, Pid: pid},
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, resp.PidProcData, 1)

	info := resp.PidProcData[0]
	require.NotNil(t, info)
	assert.Equal(t, int64(7), info.GpSegmentId)
	assert.Equal(t, int64(42), info.SessId)
	assert.Equal(t, pid, info.Pid)
	assert.NotEmpty(t, info.Cmdline)

	require.NotNil(t, info.ProcStat)
	assert.Equal(t, int32(pid), info.ProcStat.Pid)
	assert.NotEmpty(t, info.ProcStat.Comm)
	assert.NotEmpty(t, info.ProcStat.State)

	require.NotNil(t, info.ProcStatus)
	assert.Equal(t, int32(pid), info.ProcStatus.Pid)
	assert.NotEmpty(t, info.ProcStatus.Name)
}

func TestGpPidProcInfo_MixExistingAndNonExistentPids(t *testing.T) {
	server := newTestGetQueryInfoServer(t)
	pid := int64(os.Getpid())

	resp, err := server.GpPidProcInfo(context.Background(), &pb.GetPidProcInfoReq{
		SegmentProcess: []*pb.SegmentProcess{
			{GpSegmentId: 0, SessId: 10, Pid: 4194305000000}, // non-existent
			{GpSegmentId: 1, SessId: 20, Pid: pid},           // current process
			{GpSegmentId: 2, SessId: 30, Pid: 4194305000001}, // non-existent
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	// Only the existing PID should produce a result; non-existent ones are skipped.
	require.Len(t, resp.PidProcData, 1)

	info := resp.PidProcData[0]
	require.NotNil(t, info)
	assert.Equal(t, int64(1), info.GpSegmentId)
	assert.Equal(t, int64(20), info.SessId)
	assert.Equal(t, pid, info.Pid)
	assert.NotEmpty(t, info.Cmdline)
}
