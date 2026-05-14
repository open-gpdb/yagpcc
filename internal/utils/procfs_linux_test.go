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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetPidProcInfo_CurrentProcess(t *testing.T) {
	pid := int64(os.Getpid())
	info, err := GetPidProcInfo(nil, pid, 7, 42)

	require.NoError(t, err)
	require.NotNil(t, info)

	assert.Equal(t, int64(7), info.GpSegmentId)
	assert.Equal(t, int64(42), info.SessId)
	assert.Equal(t, pid, info.Pid)
	assert.NotEmpty(t, info.Cmdline)

	require.NotNil(t, info.ProcStat)
	assert.Equal(t, int64(pid), info.ProcStat.Pid)
	assert.NotEmpty(t, info.ProcStat.Comm)
	assert.NotEmpty(t, info.ProcStat.State)
	assert.Greater(t, info.ProcStat.NumThreads, int32(0))

	require.NotNil(t, info.ProcStatus)
	assert.Equal(t, int64(pid), info.ProcStatus.Pid)
	assert.NotEmpty(t, info.ProcStatus.Name)
	assert.Greater(t, info.ProcStatus.VmSize, int64(0))
	assert.Greater(t, info.ProcStatus.VmRss, int64(0))

	require.NotNil(t, info.ProcIo)
	// The current process must have read at least something (the test binary itself).
	assert.GreaterOrEqual(t, info.ProcIo.Rchar, int64(0))
	assert.GreaterOrEqual(t, info.ProcIo.Wchar, int64(0))
	assert.GreaterOrEqual(t, info.ProcIo.Syscr, int64(0))
	assert.GreaterOrEqual(t, info.ProcIo.Syscw, int64(0))
}
