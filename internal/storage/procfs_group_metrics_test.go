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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/utils"
)

// ===========================================================================
// GroupProcfsMetrics - error / nil handling
// ===========================================================================

func TestGroupProcfsMetrics_NilSource_NoOp(t *testing.T) {
	dest := &pbc.GpPidProcInfo{Pid: 1}
	ir := make(map[MapAggregateKey]int64)
	err := GroupProcfsMetrics(dest, nil, AggMax, "host1", ir)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), dest.Pid)
	assert.Nil(t, dest.ProcStat)
	assert.Nil(t, dest.ProcIo)
}

func TestGroupProcfsMetrics_NilDest_Error(t *testing.T) {
	ir := make(map[MapAggregateKey]int64)
	err := GroupProcfsMetrics(nil, &pbc.GpPidProcInfo{Pid: 1}, AggMax, "host1", ir)
	assert.EqualError(t, err, "cannot merge with nil procfs dst")
}

func TestGroupProcfsMetrics_NilIntermediateResults_Error(t *testing.T) {
	err := GroupProcfsMetrics(&pbc.GpPidProcInfo{}, &pbc.GpPidProcInfo{}, AggMax, "host1", nil)
	assert.EqualError(t, err, "need map for store intermediate procfs results")
}

// ===========================================================================
// GroupProcfsMetrics - ProcStat
// ===========================================================================

func TestGroupProcfsMetrics_ProcStat_CloneOnEmptyDest(t *testing.T) {
	dest := &pbc.GpPidProcInfo{}
	source := &pbc.GpPidProcInfo{
		ProcStat: &pbc.ProcStat{
			Pid: 42, Comm: "postgres",
			Utime: 100, Stime: 200, Cutime: 10, Cstime: 20,
			GuestTime: 5, CguestTime: 3, Vsize: 1024, Rss: 512,
		},
	}
	ir := make(map[MapAggregateKey]int64)
	err := GroupProcfsMetrics(dest, source, AggMax, "host1", ir)
	require.NoError(t, err)
	require.NotNil(t, dest.ProcStat)
	utils.AssertProtoMessagesEqual(t, source.ProcStat, dest.ProcStat)
	assert.NotSame(t, source.ProcStat, dest.ProcStat)
}

func TestGroupProcfsMetrics_ProcStat_SumCounters(t *testing.T) {
	dest := &pbc.GpPidProcInfo{
		ProcStat: &pbc.ProcStat{
			Utime: 100, Stime: 200, Cutime: 10, Cstime: 20,
			GuestTime: 5, CguestTime: 3,
		},
	}
	source := &pbc.GpPidProcInfo{
		ProcStat: &pbc.ProcStat{
			Utime: 50, Stime: 75, Cutime: 5, Cstime: 8,
			GuestTime: 2, CguestTime: 1,
		},
	}
	ir := make(map[MapAggregateKey]int64)
	err := GroupProcfsMetrics(dest, source, AggMax, "host1", ir)
	require.NoError(t, err)
	assert.Equal(t, int64(150), dest.ProcStat.Utime)
	assert.Equal(t, int64(275), dest.ProcStat.Stime)
	assert.Equal(t, int64(15), dest.ProcStat.Cutime)
	assert.Equal(t, int64(28), dest.ProcStat.Cstime)
	assert.Equal(t, int64(7), dest.ProcStat.GuestTime)
	assert.Equal(t, int64(4), dest.ProcStat.CguestTime)
}

func TestGroupProcfsMetrics_ProcStat_MultipleMerges(t *testing.T) {
	dest := &pbc.GpPidProcInfo{}
	ir := make(map[MapAggregateKey]int64)
	sources := []*pbc.GpPidProcInfo{
		{ProcStat: &pbc.ProcStat{Utime: 10, Stime: 20, Vsize: 100, Rss: 50}},
		{ProcStat: &pbc.ProcStat{Utime: 30, Stime: 40, Vsize: 200, Rss: 100}},
		{ProcStat: &pbc.ProcStat{Utime: 50, Stime: 60, Vsize: 150, Rss: 75}},
	}
	for _, src := range sources {
		require.NoError(t, GroupProcfsMetrics(dest, src, AggMax, "host1", ir))
	}
	assert.Equal(t, int64(90), dest.ProcStat.Utime)
	assert.Equal(t, int64(120), dest.ProcStat.Stime)
	// Vsize/Rss via intermediate: sum per host = 100+200+150=450 / 50+100+75=225
	assert.Equal(t, int64(450), dest.ProcStat.Vsize)
	assert.Equal(t, int64(225), dest.ProcStat.Rss)
}

// ===========================================================================
// GroupProcfsMetrics - ProcIo
// ===========================================================================

func TestGroupProcfsMetrics_ProcIo_CloneOnEmptyDest(t *testing.T) {
	dest := &pbc.GpPidProcInfo{}
	source := &pbc.GpPidProcInfo{
		ProcIo: &pbc.ProcIO{
			Rchar: 1000, Wchar: 2000, Syscr: 300, Syscw: 400,
			ReadBytes: 500, WriteBytes: 600, CancelledWriteBytes: 50,
		},
	}
	ir := make(map[MapAggregateKey]int64)
	err := GroupProcfsMetrics(dest, source, AggMax, "host1", ir)
	require.NoError(t, err)
	require.NotNil(t, dest.ProcIo)
	utils.AssertProtoMessagesEqual(t, source.ProcIo, dest.ProcIo)
	assert.NotSame(t, source.ProcIo, dest.ProcIo)
}

func TestGroupProcfsMetrics_ProcIo_SumCounters(t *testing.T) {
	dest := &pbc.GpPidProcInfo{
		ProcIo: &pbc.ProcIO{
			Rchar: 1000, Wchar: 2000, Syscr: 300, Syscw: 400,
			ReadBytes: 500, WriteBytes: 600, CancelledWriteBytes: 50,
		},
	}
	source := &pbc.GpPidProcInfo{
		ProcIo: &pbc.ProcIO{
			Rchar: 500, Wchar: 800, Syscr: 100, Syscw: 200,
			ReadBytes: 250, WriteBytes: 300, CancelledWriteBytes: 25,
		},
	}
	ir := make(map[MapAggregateKey]int64)
	err := GroupProcfsMetrics(dest, source, AggMax, "host1", ir)
	require.NoError(t, err)
	assert.Equal(t, int64(1500), dest.ProcIo.Rchar)
	assert.Equal(t, int64(2800), dest.ProcIo.Wchar)
	assert.Equal(t, int64(400), dest.ProcIo.Syscr)
	assert.Equal(t, int64(600), dest.ProcIo.Syscw)
	assert.Equal(t, int64(750), dest.ProcIo.ReadBytes)
	assert.Equal(t, int64(900), dest.ProcIo.WriteBytes)
	assert.Equal(t, int64(75), dest.ProcIo.CancelledWriteBytes)
}

// ===========================================================================
// GroupProcfsMetrics - mixed scenarios
// ===========================================================================

func TestGroupProcfsMetrics_BothProcStatAndProcIo(t *testing.T) {
	dest := &pbc.GpPidProcInfo{
		ProcStat: &pbc.ProcStat{Utime: 100, Stime: 200},
		ProcIo:   &pbc.ProcIO{Rchar: 1000, Wchar: 2000},
	}
	source := &pbc.GpPidProcInfo{
		ProcStat: &pbc.ProcStat{Utime: 50, Stime: 75},
		ProcIo:   &pbc.ProcIO{Rchar: 500, Wchar: 800},
	}
	ir := make(map[MapAggregateKey]int64)
	err := GroupProcfsMetrics(dest, source, AggMax, "host1", ir)
	require.NoError(t, err)
	assert.Equal(t, int64(150), dest.ProcStat.Utime)
	assert.Equal(t, int64(275), dest.ProcStat.Stime)
	assert.Equal(t, int64(1500), dest.ProcIo.Rchar)
	assert.Equal(t, int64(2800), dest.ProcIo.Wchar)
}

func TestGroupProcfsMetrics_SourceOnlyProcStat(t *testing.T) {
	dest := &pbc.GpPidProcInfo{ProcIo: &pbc.ProcIO{Rchar: 1000}}
	source := &pbc.GpPidProcInfo{ProcStat: &pbc.ProcStat{Utime: 100, Vsize: 512, Rss: 256}}
	ir := make(map[MapAggregateKey]int64)
	err := GroupProcfsMetrics(dest, source, AggMax, "host1", ir)
	require.NoError(t, err)
	require.NotNil(t, dest.ProcStat)
	assert.Equal(t, int64(100), dest.ProcStat.Utime)
	assert.Equal(t, int64(1000), dest.ProcIo.Rchar) // unchanged
}

func TestGroupProcfsMetrics_SourceOnlyProcIo(t *testing.T) {
	dest := &pbc.GpPidProcInfo{ProcStat: &pbc.ProcStat{Utime: 100}}
	source := &pbc.GpPidProcInfo{ProcIo: &pbc.ProcIO{Rchar: 500}}
	ir := make(map[MapAggregateKey]int64)
	err := GroupProcfsMetrics(dest, source, AggMax, "host1", ir)
	require.NoError(t, err)
	assert.Equal(t, int64(100), dest.ProcStat.Utime) // unchanged
	require.NotNil(t, dest.ProcIo)
	assert.Equal(t, int64(500), dest.ProcIo.Rchar)
}

func TestGroupProcfsMetrics_SourceEmptySubStructs(t *testing.T) {
	dest := &pbc.GpPidProcInfo{
		ProcStat: &pbc.ProcStat{Utime: 100},
		ProcIo:   &pbc.ProcIO{Rchar: 500},
	}
	source := &pbc.GpPidProcInfo{Pid: 42} // no ProcStat, no ProcIo
	ir := make(map[MapAggregateKey]int64)
	err := GroupProcfsMetrics(dest, source, AggMax, "host1", ir)
	require.NoError(t, err)
	assert.Equal(t, int64(100), dest.ProcStat.Utime)
	assert.Equal(t, int64(500), dest.ProcIo.Rchar)
}

// ===========================================================================
// GroupProcfsMetrics - Vsize/Rss with AggSegmentHost
// ===========================================================================

func TestGroupProcfsMetrics_VsizeRss_AggSegmentHost(t *testing.T) {
	for _, tt := range []struct {
		name          string
		hostname      string
		ir            map[MapAggregateKey]int64
		source        *pbc.GpPidProcInfo
		expectedVsize int64
		expectedRss   int64
	}{
		{
			name: "initial set", hostname: "seg1",
			ir:            map[MapAggregateKey]int64{},
			source:        &pbc.GpPidProcInfo{ProcStat: &pbc.ProcStat{Vsize: 100, Rss: 50}},
			expectedVsize: 100, expectedRss: 50,
		},
		{
			name: "same host accumulates", hostname: "seg1",
			ir: map[MapAggregateKey]int64{
				{MetricName: "Vsize", Hostname: "seg1"}: 100,
				{MetricName: "Rss", Hostname: "seg1"}:   50,
			},
			source:        &pbc.GpPidProcInfo{ProcStat: &pbc.ProcStat{Vsize: 200, Rss: 100}},
			expectedVsize: 300, expectedRss: 150,
		},
		{
			name: "other host values ignored", hostname: "seg1",
			ir: map[MapAggregateKey]int64{
				{MetricName: "Vsize", Hostname: "seg1"}: 100,
				{MetricName: "Rss", Hostname: "seg1"}:   50,
				{MetricName: "Vsize", Hostname: "seg2"}: 9999,
				{MetricName: "Rss", Hostname: "seg2"}:   8888,
			},
			source:        &pbc.GpPidProcInfo{ProcStat: &pbc.ProcStat{Vsize: 50, Rss: 25}},
			expectedVsize: 150, expectedRss: 75,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dest := &pbc.GpPidProcInfo{}
			err := GroupProcfsMetrics(dest, tt.source, AggSegmentHost, tt.hostname, tt.ir)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedVsize, dest.ProcStat.Vsize)
			assert.Equal(t, tt.expectedRss, dest.ProcStat.Rss)
		})
	}
}

// ===========================================================================
// GroupProcfsMetrics - Vsize/Rss with AggMax
// ===========================================================================

func TestGroupProcfsMetrics_VsizeRss_AggMax(t *testing.T) {
	for _, tt := range []struct {
		name          string
		hostname      string
		ir            map[MapAggregateKey]int64
		dest          *pbc.GpPidProcInfo
		source        *pbc.GpPidProcInfo
		expectedVsize int64
		expectedRss   int64
	}{
		{
			name: "initial set", hostname: "seg1",
			ir:            map[MapAggregateKey]int64{},
			dest:          &pbc.GpPidProcInfo{},
			source:        &pbc.GpPidProcInfo{ProcStat: &pbc.ProcStat{Vsize: 100, Rss: 50}},
			expectedVsize: 100, expectedRss: 50,
		},
		{
			name: "picks max across hosts", hostname: "seg1",
			ir: map[MapAggregateKey]int64{
				{MetricName: "Vsize", Hostname: "seg1"}: 100,
				{MetricName: "Rss", Hostname: "seg1"}:   50,
				{MetricName: "Vsize", Hostname: "seg2"}: 500,
				{MetricName: "Rss", Hostname: "seg2"}:   250,
			},
			dest:   &pbc.GpPidProcInfo{ProcStat: &pbc.ProcStat{Vsize: 10, Rss: 5}},
			source: &pbc.GpPidProcInfo{ProcStat: &pbc.ProcStat{Vsize: 50, Rss: 25}},
			// After UpdateIntermediateKey: seg1 Vsize=150, Rss=75; seg2 unchanged at 500/250
			// AggMax picks max across all hosts: max(150, 500) = 500, max(75, 250) = 250
			expectedVsize: 500, expectedRss: 250,
		},
		{
			name: "dest already larger", hostname: "seg1",
			ir: map[MapAggregateKey]int64{
				{MetricName: "Vsize", Hostname: "seg1"}: 100,
				{MetricName: "Rss", Hostname: "seg1"}:   50,
			},
			dest:          &pbc.GpPidProcInfo{ProcStat: &pbc.ProcStat{Vsize: 9999, Rss: 8888}},
			source:        &pbc.GpPidProcInfo{ProcStat: &pbc.ProcStat{Vsize: 10, Rss: 5}},
			expectedVsize: 9999, expectedRss: 8888,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := GroupProcfsMetrics(tt.dest, tt.source, AggMax, tt.hostname, tt.ir)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedVsize, tt.dest.ProcStat.Vsize)
			assert.Equal(t, tt.expectedRss, tt.dest.ProcStat.Rss)
		})
	}
}

// ===========================================================================
// GroupProcfsMetrics - table-driven positive (full proto comparison)
// ===========================================================================

func TestGroupProcfsMetrics_Positive(t *testing.T) {
	for _, tt := range []struct {
		name     string
		dest     *pbc.GpPidProcInfo
		source   *pbc.GpPidProcInfo
		expected *pbc.GpPidProcInfo
	}{
		{
			name: "nil source is no-op",
			dest: &pbc.GpPidProcInfo{Pid: 1}, source: nil,
			expected: &pbc.GpPidProcInfo{Pid: 1},
		},
		{
			name: "ProcStat on empty dest",
			dest: &pbc.GpPidProcInfo{},
			source: &pbc.GpPidProcInfo{ProcStat: &pbc.ProcStat{
				Utime: 100, Stime: 200, Cutime: 10, Cstime: 20,
				GuestTime: 5, CguestTime: 3, Vsize: 1024, Rss: 512, Pid: 42, Comm: "pg",
			}},
			expected: &pbc.GpPidProcInfo{ProcStat: &pbc.ProcStat{
				Utime: 100, Stime: 200, Cutime: 10, Cstime: 20,
				GuestTime: 5, CguestTime: 3, Vsize: 1024, Rss: 512, Pid: 42, Comm: "pg",
			}},
		},
		{
			name: "ProcIo on empty dest",
			dest: &pbc.GpPidProcInfo{},
			source: &pbc.GpPidProcInfo{ProcIo: &pbc.ProcIO{
				Rchar: 1000, Wchar: 2000, Syscr: 300, Syscw: 400,
				ReadBytes: 500, WriteBytes: 600, CancelledWriteBytes: 50,
			}},
			expected: &pbc.GpPidProcInfo{ProcIo: &pbc.ProcIO{
				Rchar: 1000, Wchar: 2000, Syscr: 300, Syscw: 400,
				ReadBytes: 500, WriteBytes: 600, CancelledWriteBytes: 50,
			}},
		},
		{
			name: "ProcIo on non-empty dest sums",
			dest: &pbc.GpPidProcInfo{ProcIo: &pbc.ProcIO{
				Rchar: 1000, Wchar: 2000, Syscr: 300, Syscw: 400,
				ReadBytes: 500, WriteBytes: 600, CancelledWriteBytes: 50,
			}},
			source: &pbc.GpPidProcInfo{ProcIo: &pbc.ProcIO{
				Rchar: 500, Wchar: 800, Syscr: 100, Syscw: 200,
				ReadBytes: 250, WriteBytes: 300, CancelledWriteBytes: 25,
			}},
			expected: &pbc.GpPidProcInfo{ProcIo: &pbc.ProcIO{
				Rchar: 1500, Wchar: 2800, Syscr: 400, Syscw: 600,
				ReadBytes: 750, WriteBytes: 900, CancelledWriteBytes: 75,
			}},
		},
		{
			name: "nil ProcStat in source does not affect dest",
			dest: &pbc.GpPidProcInfo{
				ProcStat: &pbc.ProcStat{Utime: 100},
				ProcIo:   &pbc.ProcIO{Rchar: 500},
			},
			source: &pbc.GpPidProcInfo{ProcIo: &pbc.ProcIO{Rchar: 200}},
			expected: &pbc.GpPidProcInfo{
				ProcStat: &pbc.ProcStat{Utime: 100},
				ProcIo:   &pbc.ProcIO{Rchar: 700},
			},
		},
		{
			name: "nil ProcIo in source does not affect dest",
			dest: &pbc.GpPidProcInfo{
				ProcStat: &pbc.ProcStat{Utime: 100},
				ProcIo:   &pbc.ProcIO{Rchar: 500},
			},
			source: &pbc.GpPidProcInfo{ProcStat: &pbc.ProcStat{Utime: 50}},
			expected: &pbc.GpPidProcInfo{
				ProcStat: &pbc.ProcStat{Utime: 150},
				ProcIo:   &pbc.ProcIO{Rchar: 500},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ir := make(map[MapAggregateKey]int64)
			err := GroupProcfsMetrics(tt.dest, tt.source, AggMax, "hostname", ir)
			assert.NoError(t, err)
			utils.AssertProtoMessagesEqual(t, tt.expected, tt.dest)
		})
	}
}

// ===========================================================================
// GroupProcfsMetrics - intermediate results tracking
// ===========================================================================

func TestGroupProcfsMetrics_IntermediateResultsUpdated(t *testing.T) {
	dest := &pbc.GpPidProcInfo{}
	ir := make(map[MapAggregateKey]int64)

	require.NoError(t, GroupProcfsMetrics(dest, &pbc.GpPidProcInfo{ProcStat: &pbc.ProcStat{Vsize: 100, Rss: 50}}, AggMax, "host1", ir))
	assert.Equal(t, int64(100), ir[MapAggregateKey{MetricName: "Vsize", Hostname: "host1"}])
	assert.Equal(t, int64(50), ir[MapAggregateKey{MetricName: "Rss", Hostname: "host1"}])

	require.NoError(t, GroupProcfsMetrics(dest, &pbc.GpPidProcInfo{ProcStat: &pbc.ProcStat{Vsize: 200, Rss: 75}}, AggMax, "host1", ir))
	assert.Equal(t, int64(300), ir[MapAggregateKey{MetricName: "Vsize", Hostname: "host1"}])
	assert.Equal(t, int64(125), ir[MapAggregateKey{MetricName: "Rss", Hostname: "host1"}])
}

func TestGroupProcfsMetrics_IntermediateResults_MultipleHosts(t *testing.T) {
	dest := &pbc.GpPidProcInfo{}
	ir := make(map[MapAggregateKey]int64)

	require.NoError(t, GroupProcfsMetrics(dest, &pbc.GpPidProcInfo{ProcStat: &pbc.ProcStat{Vsize: 100, Rss: 50}}, AggMax, "host1", ir))
	require.NoError(t, GroupProcfsMetrics(dest, &pbc.GpPidProcInfo{ProcStat: &pbc.ProcStat{Vsize: 200, Rss: 75}}, AggMax, "host2", ir))

	assert.Equal(t, int64(100), ir[MapAggregateKey{MetricName: "Vsize", Hostname: "host1"}])
	assert.Equal(t, int64(200), ir[MapAggregateKey{MetricName: "Vsize", Hostname: "host2"}])
	assert.Equal(t, int64(50), ir[MapAggregateKey{MetricName: "Rss", Hostname: "host1"}])
	assert.Equal(t, int64(75), ir[MapAggregateKey{MetricName: "Rss", Hostname: "host2"}])

	// AggMax picks max across all hosts: max(100, 200) = 200
	assert.Equal(t, int64(200), dest.ProcStat.Vsize)
	assert.Equal(t, int64(75), dest.ProcStat.Rss)
}
