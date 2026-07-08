package grpc

import (
	"testing"

	"github.com/prometheus/procfs"
	"github.com/stretchr/testify/assert"
)

func TestCPUUsage(t *testing.T) {
	assert.Equal(t, 0.0, cpuUsage(procfs.CPUStat{}))
	assert.Equal(t, 0.75, cpuUsage(procfs.CPUStat{User: 30, System: 45, Idle: 25}))
	assert.Equal(t, 0.5, cpuUsage(procfs.CPUStat{User: 25, System: 25, Idle: 40, Iowait: 10}))
}
