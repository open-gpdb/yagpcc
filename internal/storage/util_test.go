package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"

	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/utils"
)

func TestGroupGPMetrics_Positive(t *testing.T) {
	for _, tt := range []struct {
		name     string
		dest     *pbc.GPMetrics
		source   *pbc.GPMetrics
		expected *pbc.GPMetrics
	}{
		{
			name:     "nil input",
			dest:     nil,
			source:   nil,
			expected: nil,
		},
		{
			name: "SystemStat on empty dst",
			dest: &pbc.GPMetrics{
				SystemStat: nil,
			},
			source: &pbc.GPMetrics{
				SystemStat: &pbc.SystemStat{
					UserTimeSeconds:     2.0,
					KernelTimeSeconds:   3.0,
					Vsize:               5.0,
					Rss:                 7.0,
					VmSizeKb:            11.0,
					VmPeakKb:            13.0,
					Rchar:               17.0,
					Wchar:               19.0,
					Syscr:               23.0,
					Syscw:               29.0,
					ReadBytes:           31.0,
					WriteBytes:          37.0,
					CancelledWriteBytes: 41.0,
				},
			},
			expected: &pbc.GPMetrics{
				SystemStat: &pbc.SystemStat{
					RunningTimeSeconds:  5.0,
					UserTimeSeconds:     2.0,
					KernelTimeSeconds:   3.0,
					Vsize:               5.0,
					Rss:                 7.0,
					VmSizeKb:            11.0,
					VmPeakKb:            13.0,
					Rchar:               17.0,
					Wchar:               19.0,
					Syscr:               23.0,
					Syscw:               29.0,
					ReadBytes:           31.0,
					WriteBytes:          37.0,
					CancelledWriteBytes: 41.0,
				},
			},
		},
		{
			name: "SystemStat on non empty dst",
			dest: &pbc.GPMetrics{
				SystemStat: &pbc.SystemStat{
					UserTimeSeconds:     3.0,
					KernelTimeSeconds:   5.0,
					Vsize:               7.0,
					Rss:                 11.0,
					VmSizeKb:            13.0,
					VmPeakKb:            17.0,
					Rchar:               19.0,
					Wchar:               23.0,
					Syscr:               29.0,
					Syscw:               31.0,
					ReadBytes:           37.0,
					WriteBytes:          41.0,
					CancelledWriteBytes: 43.0,
				},
			},
			source: &pbc.GPMetrics{
				SystemStat: &pbc.SystemStat{
					RunningTimeSeconds:  1.0,
					UserTimeSeconds:     2.0,
					KernelTimeSeconds:   3.0,
					Vsize:               5.0,
					Rss:                 7.0,
					VmSizeKb:            11.0,
					VmPeakKb:            13.0,
					Rchar:               17.0,
					Wchar:               19.0,
					Syscr:               23.0,
					Syscw:               29.0,
					ReadBytes:           31.0,
					WriteBytes:          37.0,
					CancelledWriteBytes: 41.0,
				},
			},
			expected: &pbc.GPMetrics{
				SystemStat: &pbc.SystemStat{
					RunningTimeSeconds:  13.0,
					UserTimeSeconds:     5.0,
					KernelTimeSeconds:   8.0,
					Vsize:               7.0,
					Rss:                 11.0,
					VmSizeKb:            13.0,
					VmPeakKb:            17.0,
					Rchar:               36.0,
					Wchar:               42.0,
					Syscr:               52.0,
					Syscw:               60.0,
					ReadBytes:           68.0,
					WriteBytes:          78.0,
					CancelledWriteBytes: 84.0,
				},
			},
		},
		{
			name: "SystemStat empty on non empty dst",
			dest: &pbc.GPMetrics{
				SystemStat: &pbc.SystemStat{
					RunningTimeSeconds:  8.0,
					UserTimeSeconds:     3.0,
					KernelTimeSeconds:   5.0,
					Vsize:               7.0,
					Rss:                 11.0,
					VmSizeKb:            13.0,
					VmPeakKb:            17.0,
					Rchar:               19.0,
					Wchar:               23.0,
					Syscr:               29.0,
					Syscw:               31.0,
					ReadBytes:           37.0,
					WriteBytes:          41.0,
					CancelledWriteBytes: 43.0,
				},
			},
			source: &pbc.GPMetrics{
				SystemStat: nil,
			},
			expected: &pbc.GPMetrics{
				SystemStat: &pbc.SystemStat{
					RunningTimeSeconds:  8.0,
					UserTimeSeconds:     3.0,
					KernelTimeSeconds:   5.0,
					Vsize:               7.0,
					Rss:                 11.0,
					VmSizeKb:            13.0,
					VmPeakKb:            17.0,
					Rchar:               19.0,
					Wchar:               23.0,
					Syscr:               29.0,
					Syscw:               31.0,
					ReadBytes:           37.0,
					WriteBytes:          41.0,
					CancelledWriteBytes: 43.0,
				},
			},
		},
		{
			name: "Spill on empty dst",
			dest: &pbc.GPMetrics{
				Spill: nil,
			},
			source: &pbc.GPMetrics{
				Spill: &pbc.SpillInfo{
					FileCount:  1.0,
					TotalBytes: 2.0,
				},
			},
			expected: &pbc.GPMetrics{
				Spill: &pbc.SpillInfo{
					FileCount:  1.0,
					TotalBytes: 2.0,
				},
			},
		},
		{
			name: "Spill on non empty dst",
			dest: &pbc.GPMetrics{
				Spill: &pbc.SpillInfo{
					FileCount:  1.0,
					TotalBytes: 2.0,
				},
			},
			source: &pbc.GPMetrics{
				Spill: &pbc.SpillInfo{
					FileCount:  2.0,
					TotalBytes: 3.0,
				},
			},
			expected: &pbc.GPMetrics{
				Spill: &pbc.SpillInfo{
					FileCount:  3.0,
					TotalBytes: 5.0,
				},
			},
		},
		{
			name: "Spill empty on non empty dst",
			dest: &pbc.GPMetrics{
				Spill: &pbc.SpillInfo{
					FileCount:  1.0,
					TotalBytes: 2.0,
				},
			},
			source: &pbc.GPMetrics{
				Spill: nil,
			},
			expected: &pbc.GPMetrics{
				Spill: &pbc.SpillInfo{
					FileCount:  1.0,
					TotalBytes: 2.0,
				},
			},
		},
		{
			name: "Instrumentation on empty dst",
			dest: &pbc.GPMetrics{
				Instrumentation: nil,
			},
			source: &pbc.GPMetrics{
				Instrumentation: &pbc.MetricInstrumentation{
					Ntuples:           1.0,
					Nloops:            2.0,
					Tuplecount:        3.0,
					Firsttuple:        5.0,
					Startup:           7.0,
					Total:             11.0,
					SharedBlksHit:     13.0,
					SharedBlksRead:    17.0,
					SharedBlksDirtied: 19.0,
					SharedBlksWritten: 23.0,
					LocalBlksHit:      29.0,
					LocalBlksRead:     31.0,
					LocalBlksDirtied:  37.0,
					LocalBlksWritten:  41.0,
					TempBlksRead:      43.0,
					TempBlksWritten:   47.0,
					BlkReadTime:       53.0,
					BlkWriteTime:      59.0,
					StartupTime:       61.0,
					InheritedCalls:    67.0,
					InheritedTime:     71.0,
				},
			},
			expected: &pbc.GPMetrics{
				Instrumentation: &pbc.MetricInstrumentation{
					Ntuples:           1.0,
					Nloops:            2.0,
					Tuplecount:        3.0,
					Firsttuple:        5.0,
					Startup:           7.0,
					Total:             11.0,
					SharedBlksHit:     13.0,
					SharedBlksRead:    17.0,
					SharedBlksDirtied: 19.0,
					SharedBlksWritten: 23.0,
					LocalBlksHit:      29.0,
					LocalBlksRead:     31.0,
					LocalBlksDirtied:  37.0,
					LocalBlksWritten:  41.0,
					TempBlksRead:      43.0,
					TempBlksWritten:   47.0,
					BlkReadTime:       53.0,
					BlkWriteTime:      59.0,
					StartupTime:       61.0,
					InheritedCalls:    67.0,
					InheritedTime:     71.0,
				},
			},
		},
		{
			name: "Instrumentation on non empty dst",
			dest: &pbc.GPMetrics{
				Instrumentation: &pbc.MetricInstrumentation{
					Ntuples:           1.0,
					Nloops:            2.0,
					Tuplecount:        3.0,
					Firsttuple:        5.0,
					Startup:           7.0,
					Total:             11.0,
					SharedBlksHit:     13.0,
					SharedBlksRead:    17.0,
					SharedBlksDirtied: 19.0,
					SharedBlksWritten: 23.0,
					LocalBlksHit:      29.0,
					LocalBlksRead:     31.0,
					LocalBlksDirtied:  37.0,
					LocalBlksWritten:  41.0,
					TempBlksRead:      43.0,
					TempBlksWritten:   47.0,
					BlkReadTime:       53.0,
					BlkWriteTime:      59.0,
					StartupTime:       61.0,
					InheritedCalls:    67.0,
					InheritedTime:     71.0,
				},
			},
			source: &pbc.GPMetrics{
				Instrumentation: &pbc.MetricInstrumentation{
					Ntuples:           2.0,
					Nloops:            3.0,
					Tuplecount:        5.0,
					Firsttuple:        7.0,
					Startup:           11.0,
					Total:             13.0,
					SharedBlksHit:     17.0,
					SharedBlksRead:    19.0,
					SharedBlksDirtied: 23.0,
					SharedBlksWritten: 29.0,
					LocalBlksHit:      31.0,
					LocalBlksRead:     37.0,
					LocalBlksDirtied:  41.0,
					LocalBlksWritten:  43.0,
					TempBlksRead:      47.0,
					TempBlksWritten:   53.0,
					BlkReadTime:       59.0,
					BlkWriteTime:      61.0,
					StartupTime:       67.0,
					InheritedCalls:    71.0,
					InheritedTime:     73.0,
				},
			},
			expected: &pbc.GPMetrics{
				Instrumentation: &pbc.MetricInstrumentation{
					Ntuples:           3.0,
					Nloops:            5.0,
					Tuplecount:        8.0,
					Firsttuple:        12.0,
					Startup:           18.0,
					Total:             24.0,
					SharedBlksHit:     30.0,
					SharedBlksRead:    36.0,
					SharedBlksDirtied: 42.0,
					SharedBlksWritten: 52.0,
					LocalBlksHit:      60.0,
					LocalBlksRead:     68.0,
					LocalBlksDirtied:  78.0,
					LocalBlksWritten:  84.0,
					TempBlksRead:      90.0,
					TempBlksWritten:   100.0,
					BlkReadTime:       112.0,
					BlkWriteTime:      120.0,
					StartupTime:       128.0,
					InheritedCalls:    138.0,
					InheritedTime:     144.0,
				},
			},
		},
		{
			name: "Instrumentation empty on non empty dst",
			dest: &pbc.GPMetrics{
				Instrumentation: &pbc.MetricInstrumentation{
					Ntuples:           1.0,
					Nloops:            2.0,
					Tuplecount:        3.0,
					Firsttuple:        5.0,
					Startup:           7.0,
					Total:             11.0,
					SharedBlksHit:     13.0,
					SharedBlksRead:    17.0,
					SharedBlksDirtied: 19.0,
					SharedBlksWritten: 23.0,
					LocalBlksHit:      29.0,
					LocalBlksRead:     31.0,
					LocalBlksDirtied:  37.0,
					LocalBlksWritten:  41.0,
					TempBlksRead:      43.0,
					TempBlksWritten:   47.0,
					BlkReadTime:       53.0,
					BlkWriteTime:      59.0,
					StartupTime:       61.0,
					InheritedCalls:    67.0,
					InheritedTime:     71.0,
				},
			},
			source: &pbc.GPMetrics{
				Instrumentation: nil,
			},
			expected: &pbc.GPMetrics{
				Instrumentation: &pbc.MetricInstrumentation{
					Ntuples:           1.0,
					Nloops:            2.0,
					Tuplecount:        3.0,
					Firsttuple:        5.0,
					Startup:           7.0,
					Total:             11.0,
					SharedBlksHit:     13.0,
					SharedBlksRead:    17.0,
					SharedBlksDirtied: 19.0,
					SharedBlksWritten: 23.0,
					LocalBlksHit:      29.0,
					LocalBlksRead:     31.0,
					LocalBlksDirtied:  37.0,
					LocalBlksWritten:  41.0,
					TempBlksRead:      43.0,
					TempBlksWritten:   47.0,
					BlkReadTime:       53.0,
					BlkWriteTime:      59.0,
					StartupTime:       61.0,
					InheritedCalls:    67.0,
					InheritedTime:     71.0,
				},
			},
		},
		{
			name: "Instrumentation.Sent on empty dst",
			dest: &pbc.GPMetrics{
				Instrumentation: &pbc.MetricInstrumentation{
					Sent: nil,
				},
			},
			source: &pbc.GPMetrics{
				Instrumentation: &pbc.MetricInstrumentation{
					Sent: &pbc.NetworkStat{
						TotalBytes: 1.0,
						TupleBytes: 2.0,
						Chunks:     3.0,
					},
				},
			},
			expected: &pbc.GPMetrics{
				Instrumentation: &pbc.MetricInstrumentation{
					Sent: &pbc.NetworkStat{
						TotalBytes: 1.0,
						TupleBytes: 2.0,
						Chunks:     3.0,
					},
				},
			},
		},
		{
			name: "Instrumentation.Sent on non empty dst",
			dest: &pbc.GPMetrics{
				Instrumentation: &pbc.MetricInstrumentation{
					Sent: &pbc.NetworkStat{
						TotalBytes: 1.0,
						TupleBytes: 2.0,
						Chunks:     3.0,
					},
				},
			},
			source: &pbc.GPMetrics{
				Instrumentation: &pbc.MetricInstrumentation{
					Sent: &pbc.NetworkStat{
						TotalBytes: 2.0,
						TupleBytes: 3.0,
						Chunks:     5.0,
					},
				},
			},
			expected: &pbc.GPMetrics{
				Instrumentation: &pbc.MetricInstrumentation{
					Sent: &pbc.NetworkStat{
						TotalBytes: 3.0,
						TupleBytes: 5.0,
						Chunks:     8.0,
					},
				},
			},
		},
		{
			name: "Instrumentation.Sent empty on non empty dst",
			dest: &pbc.GPMetrics{
				Instrumentation: &pbc.MetricInstrumentation{
					Sent: &pbc.NetworkStat{
						TotalBytes: 1.0,
						TupleBytes: 2.0,
						Chunks:     3.0,
					},
				},
			},
			source: &pbc.GPMetrics{
				Instrumentation: nil,
			},
			expected: &pbc.GPMetrics{
				Instrumentation: &pbc.MetricInstrumentation{
					Sent: &pbc.NetworkStat{
						TotalBytes: 1.0,
						TupleBytes: 2.0,
						Chunks:     3.0,
					},
				},
			},
		},
		{
			name: "Instrumentation.Received on empty dst",
			dest: &pbc.GPMetrics{
				Instrumentation: &pbc.MetricInstrumentation{
					Received: nil,
				},
			},
			source: &pbc.GPMetrics{
				Instrumentation: &pbc.MetricInstrumentation{
					Received: &pbc.NetworkStat{
						TotalBytes: 1.0,
						TupleBytes: 2.0,
						Chunks:     3.0,
					},
				},
			},
			expected: &pbc.GPMetrics{
				Instrumentation: &pbc.MetricInstrumentation{
					Received: &pbc.NetworkStat{
						TotalBytes: 1.0,
						TupleBytes: 2.0,
						Chunks:     3.0,
					},
				},
			},
		},
		{
			name: "Instrumentation.Received on non empty dst",
			dest: &pbc.GPMetrics{
				Instrumentation: &pbc.MetricInstrumentation{
					Received: &pbc.NetworkStat{
						TotalBytes: 1.0,
						TupleBytes: 2.0,
						Chunks:     3.0,
					},
				},
			},
			source: &pbc.GPMetrics{
				Instrumentation: &pbc.MetricInstrumentation{
					Received: &pbc.NetworkStat{
						TotalBytes: 2.0,
						TupleBytes: 3.0,
						Chunks:     5.0,
					},
				},
			},
			expected: &pbc.GPMetrics{
				Instrumentation: &pbc.MetricInstrumentation{
					Received: &pbc.NetworkStat{
						TotalBytes: 3.0,
						TupleBytes: 5.0,
						Chunks:     8.0,
					},
				},
			},
		},
		{
			name: "Instrumentation.Received empty on non empty dst",
			dest: &pbc.GPMetrics{
				Instrumentation: &pbc.MetricInstrumentation{
					Received: &pbc.NetworkStat{
						TotalBytes: 1.0,
						TupleBytes: 2.0,
						Chunks:     3.0,
					},
				},
			},
			source: &pbc.GPMetrics{
				Instrumentation: &pbc.MetricInstrumentation{
					Received: nil,
				},
			},
			expected: &pbc.GPMetrics{
				Instrumentation: &pbc.MetricInstrumentation{
					Received: &pbc.NetworkStat{
						TotalBytes: 1.0,
						TupleBytes: 2.0,
						Chunks:     3.0,
					},
				},
			},
		},
		{
			name: "Instrumentation.Interconnect on empty dst",
			dest: &pbc.GPMetrics{
				Instrumentation: &pbc.MetricInstrumentation{
					Interconnect: nil,
				},
			},
			source: &pbc.GPMetrics{
				Instrumentation: &pbc.MetricInstrumentation{
					Interconnect: &pbc.InterconnectStat{
						TotalRecvQueueSize:        1.0,
						RecvQueueSizeCountingTime: 2.0,
						TotalCapacity:             3.0,
						CapacityCountingTime:      5.0,
						TotalBuffers:              7.0,
						BufferCountingTime:        11.0,
						ActiveConnectionsNum:      13.0,
						Retransmits:               17.0,
						StartupCachedPktNum:       19.0,
						MismatchNum:               23.0,
						CrcErrors:                 29.0,
						SndPktNum:                 31.0,
						RecvPktNum:                37.0,
						DisorderedPktNum:          41.0,
						DuplicatedPktNum:          43.0,
						RecvAckNum:                47.0,
						StatusQueryMsgNum:         53.0,
					},
				},
			},
			expected: &pbc.GPMetrics{
				Instrumentation: &pbc.MetricInstrumentation{
					Interconnect: &pbc.InterconnectStat{
						TotalRecvQueueSize:        1.0,
						RecvQueueSizeCountingTime: 2.0,
						TotalCapacity:             3.0,
						CapacityCountingTime:      5.0,
						TotalBuffers:              7.0,
						BufferCountingTime:        11.0,
						ActiveConnectionsNum:      13.0,
						Retransmits:               17.0,
						StartupCachedPktNum:       19.0,
						MismatchNum:               23.0,
						CrcErrors:                 29.0,
						SndPktNum:                 31.0,
						RecvPktNum:                37.0,
						DisorderedPktNum:          41.0,
						DuplicatedPktNum:          43.0,
						RecvAckNum:                47.0,
						StatusQueryMsgNum:         53.0,
					},
				},
			},
		},
		{
			name: "Instrumentation.Interconnect on non empty dst",
			dest: &pbc.GPMetrics{
				Instrumentation: &pbc.MetricInstrumentation{
					Interconnect: &pbc.InterconnectStat{
						TotalRecvQueueSize:        1.0,
						RecvQueueSizeCountingTime: 2.0,
						TotalCapacity:             3.0,
						CapacityCountingTime:      5.0,
						TotalBuffers:              7.0,
						BufferCountingTime:        11.0,
						ActiveConnectionsNum:      13.0,
						Retransmits:               17.0,
						StartupCachedPktNum:       19.0,
						MismatchNum:               23.0,
						CrcErrors:                 29.0,
						SndPktNum:                 31.0,
						RecvPktNum:                37.0,
						DisorderedPktNum:          41.0,
						DuplicatedPktNum:          43.0,
						RecvAckNum:                47.0,
						StatusQueryMsgNum:         53.0,
					},
				},
			},
			source: &pbc.GPMetrics{
				Instrumentation: &pbc.MetricInstrumentation{
					Interconnect: &pbc.InterconnectStat{
						TotalRecvQueueSize:        2.0,
						RecvQueueSizeCountingTime: 3.0,
						TotalCapacity:             5.0,
						CapacityCountingTime:      7.0,
						TotalBuffers:              11.0,
						BufferCountingTime:        13.0,
						ActiveConnectionsNum:      17.0,
						Retransmits:               19.0,
						StartupCachedPktNum:       23.0,
						MismatchNum:               29.0,
						CrcErrors:                 31.0,
						SndPktNum:                 37.0,
						RecvPktNum:                41.0,
						DisorderedPktNum:          43.0,
						DuplicatedPktNum:          47.0,
						RecvAckNum:                53.0,
						StatusQueryMsgNum:         59.0,
					},
				},
			},
			expected: &pbc.GPMetrics{
				Instrumentation: &pbc.MetricInstrumentation{
					Interconnect: &pbc.InterconnectStat{
						TotalRecvQueueSize:        2.0,
						RecvQueueSizeCountingTime: 5.0,
						TotalCapacity:             5.0,
						CapacityCountingTime:      12.0,
						TotalBuffers:              7.0,
						BufferCountingTime:        24.0,
						ActiveConnectionsNum:      17.0,
						Retransmits:               36.0,
						StartupCachedPktNum:       42.0,
						MismatchNum:               52.0,
						CrcErrors:                 60.0,
						SndPktNum:                 68.0,
						RecvPktNum:                78.0,
						DisorderedPktNum:          84.0,
						DuplicatedPktNum:          90.0,
						RecvAckNum:                100.0,
						StatusQueryMsgNum:         112.0,
					},
				},
			},
		},
		{
			name: "Instrumentation.Interconnect empty on non empty dst",
			dest: &pbc.GPMetrics{
				Instrumentation: &pbc.MetricInstrumentation{
					Interconnect: &pbc.InterconnectStat{
						TotalRecvQueueSize:        1.0,
						RecvQueueSizeCountingTime: 2.0,
						TotalCapacity:             3.0,
						CapacityCountingTime:      5.0,
						TotalBuffers:              7.0,
						BufferCountingTime:        11.0,
						ActiveConnectionsNum:      13.0,
						Retransmits:               17.0,
						StartupCachedPktNum:       19.0,
						MismatchNum:               23.0,
						CrcErrors:                 29.0,
						SndPktNum:                 31.0,
						RecvPktNum:                37.0,
						DisorderedPktNum:          41.0,
						DuplicatedPktNum:          43.0,
						RecvAckNum:                47.0,
						StatusQueryMsgNum:         53.0,
					},
				},
			},
			source: &pbc.GPMetrics{
				Instrumentation: &pbc.MetricInstrumentation{
					Interconnect: nil,
				},
			},
			expected: &pbc.GPMetrics{
				Instrumentation: &pbc.MetricInstrumentation{
					Interconnect: &pbc.InterconnectStat{
						TotalRecvQueueSize:        1.0,
						RecvQueueSizeCountingTime: 2.0,
						TotalCapacity:             3.0,
						CapacityCountingTime:      5.0,
						TotalBuffers:              7.0,
						BufferCountingTime:        11.0,
						ActiveConnectionsNum:      13.0,
						Retransmits:               17.0,
						StartupCachedPktNum:       19.0,
						MismatchNum:               23.0,
						CrcErrors:                 29.0,
						SndPktNum:                 31.0,
						RecvPktNum:                37.0,
						DisorderedPktNum:          41.0,
						DuplicatedPktNum:          43.0,
						RecvAckNum:                47.0,
						StatusQueryMsgNum:         53.0,
					},
				},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			intermediateResults := make(map[MapAggregateKey]uint64, 0)
			err := GroupGPMetrics(tt.dest, tt.source, AggMax, "hostname", intermediateResults)

			assert.NoError(t, err)
			// dest mutated during the call
			utils.AssertProtoMessagesEqual(t, tt.expected, tt.dest)
		})
	}
}

func TestGroupGPMetrics_Negative(t *testing.T) {
	for _, tt := range []struct {
		name                 string
		dest                 *pbc.GPMetrics
		source               *pbc.GPMetrics
		expectedErrorMessage string
	}{
		{
			name:                 "nil destination",
			dest:                 nil,
			source:               &pbc.GPMetrics{},
			expectedErrorMessage: "cannot merge with nil dst",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			intermediateResults := make(map[MapAggregateKey]uint64, 0)
			err := GroupGPMetrics(tt.dest, tt.source, AggSegmentHost, "hostname", intermediateResults)

			assert.EqualError(t, err, tt.expectedErrorMessage)
		})
	}
}

func TestIntermediateResults(t *testing.T) {
	intermediateResults := make(map[MapAggregateKey]uint64, 0)
	UpdateIntermediateKey(intermediateResults, MapAggregateKey{MetricName: "Vsize", Hostname: "seg1"}, 5)
	assert.Equal(t, uint64(5), intermediateResults[MapAggregateKey{MetricName: "Vsize", Hostname: "seg1"}])
	UpdateIntermediateKey(intermediateResults, MapAggregateKey{MetricName: "Vsize", Hostname: "seg2"}, 4)
	assert.Equal(t, uint64(4), intermediateResults[MapAggregateKey{MetricName: "Vsize", Hostname: "seg2"}])
	UpdateIntermediateKey(intermediateResults, MapAggregateKey{MetricName: "Vsize", Hostname: "seg1"}, 5)
	assert.Equal(t, uint64(10), intermediateResults[MapAggregateKey{MetricName: "Vsize", Hostname: "seg1"}])
}

func TestGroupGPMetrics_MemorySum(t *testing.T) {
	for _, tt := range []struct {
		name                string
		hostname            string
		intermediateResults map[MapAggregateKey]uint64
		source              *pbc.GPMetrics
		expected            *pbc.GPMetrics
	}{
		{
			name:                "initial set",
			hostname:            "seg1",
			intermediateResults: map[MapAggregateKey]uint64{},
			source: &pbc.GPMetrics{
				SystemStat: &pbc.SystemStat{
					Vsize:    5,
					Rss:      7,
					VmSizeKb: 11,
					VmPeakKb: 13,
				},
			},
			expected: &pbc.GPMetrics{
				SystemStat: &pbc.SystemStat{
					Vsize:    5,
					Rss:      7,
					VmSizeKb: 11,
					VmPeakKb: 13,
				},
			},
		},
		{
			name:     "add stat for other slice",
			hostname: "seg1",
			intermediateResults: map[MapAggregateKey]uint64{
				{MetricName: "Vsize", Hostname: "seg1"}:    5,
				{MetricName: "Rss", Hostname: "seg1"}:      7,
				{MetricName: "VmSizeKb", Hostname: "seg1"}: 11,
				{MetricName: "VmPeakKb", Hostname: "seg1"}: 13,
			},
			source: &pbc.GPMetrics{
				SystemStat: &pbc.SystemStat{
					Vsize:    1,
					Rss:      1,
					VmSizeKb: 1,
					VmPeakKb: 1,
				},
			},
			expected: &pbc.GPMetrics{
				SystemStat: &pbc.SystemStat{
					Vsize:    6,
					Rss:      8,
					VmSizeKb: 12,
					VmPeakKb: 14,
				},
			},
		},
		{
			name:     "mixed hosts",
			hostname: "seg1",
			intermediateResults: map[MapAggregateKey]uint64{
				{MetricName: "Vsize", Hostname: "seg1"}: 5,
				{MetricName: "Rss", Hostname: "seg2"}:   7,
				{MetricName: "Vsize", Hostname: "seg3"}: 11,
				{MetricName: "Vsize", Hostname: "seg4"}: 13,
			},
			source: &pbc.GPMetrics{
				SystemStat: &pbc.SystemStat{
					Vsize:    1,
					Rss:      1,
					VmSizeKb: 1,
					VmPeakKb: 1,
				},
			},
			expected: &pbc.GPMetrics{
				SystemStat: &pbc.SystemStat{
					Vsize:    6,
					Rss:      1,
					VmSizeKb: 1,
					VmPeakKb: 1,
				},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {

			dest := &pbc.GPMetrics{SystemStat: nil}
			err := GroupGPMetrics(dest, tt.source, AggSegmentHost, tt.hostname, tt.intermediateResults)

			assert.NoError(t, err)
			// dest mutated during the call
			utils.AssertProtoMessagesEqual(t, tt.expected, dest)

		})
	}
}

func TestGroupGPMetrics_MemoryMax(t *testing.T) {
	for _, tt := range []struct {
		name                string
		hostname            string
		intermediateResults map[MapAggregateKey]uint64
		dest                *pbc.GPMetrics
		source              *pbc.GPMetrics
		expected            *pbc.GPMetrics
	}{
		{
			name:     "initial set",
			hostname: "seg1",
			dest: &pbc.GPMetrics{
				SystemStat: nil,
			},
			intermediateResults: map[MapAggregateKey]uint64{},
			source: &pbc.GPMetrics{
				SystemStat: &pbc.SystemStat{
					Vsize:    5,
					Rss:      7,
					VmSizeKb: 11,
					VmPeakKb: 13,
				},
			},
			expected: &pbc.GPMetrics{
				SystemStat: &pbc.SystemStat{
					Vsize:    5,
					Rss:      7,
					VmSizeKb: 11,
					VmPeakKb: 13,
				},
			},
		},
		{
			name:     "add stat for other slice",
			hostname: "seg1",
			intermediateResults: map[MapAggregateKey]uint64{
				{MetricName: "Vsize", Hostname: "seg1"}:    5,
				{MetricName: "Rss", Hostname: "seg1"}:      7,
				{MetricName: "VmSizeKb", Hostname: "seg1"}: 11,
				{MetricName: "VmPeakKb", Hostname: "seg1"}: 13,
			},
			dest: &pbc.GPMetrics{
				SystemStat: &pbc.SystemStat{
					Vsize:    1,
					Rss:      1,
					VmSizeKb: 1,
					VmPeakKb: 15,
				},
			},
			source: &pbc.GPMetrics{
				SystemStat: &pbc.SystemStat{
					Vsize:    1,
					Rss:      1,
					VmSizeKb: 1,
					VmPeakKb: 1,
				},
			},
			expected: &pbc.GPMetrics{
				SystemStat: &pbc.SystemStat{
					Vsize:    6,
					Rss:      8,
					VmSizeKb: 12,
					VmPeakKb: 15,
				},
			},
		},
		{
			name:     "mixed hosts",
			hostname: "seg1",
			intermediateResults: map[MapAggregateKey]uint64{
				{MetricName: "Vsize", Hostname: "seg1"}:    5,
				{MetricName: "Rss", Hostname: "seg2"}:      7,
				{MetricName: "Vsize", Hostname: "seg3"}:    11,
				{MetricName: "Vsize", Hostname: "seg4"}:    13,
				{MetricName: "VmSizeKb", Hostname: "seg3"}: 11,
				{MetricName: "VmPeakKb", Hostname: "seg5"}: 13,
			},
			dest: &pbc.GPMetrics{
				SystemStat: &pbc.SystemStat{
					Vsize:    1,
					Rss:      1,
					VmSizeKb: 1,
					VmPeakKb: 15,
				},
			},
			source: &pbc.GPMetrics{
				SystemStat: &pbc.SystemStat{
					Vsize:    1,
					Rss:      1,
					VmSizeKb: 1,
					VmPeakKb: 1,
				},
			},
			expected: &pbc.GPMetrics{
				SystemStat: &pbc.SystemStat{
					Vsize:    13,
					Rss:      7,
					VmSizeKb: 11,
					VmPeakKb: 15,
				},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {

			err := GroupGPMetrics(tt.dest, tt.source, AggMax, tt.hostname, tt.intermediateResults)

			assert.NoError(t, err)
			// dest mutated during the call
			utils.AssertProtoMessagesEqual(t, tt.expected, tt.dest)

		})
	}
}
