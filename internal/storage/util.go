package storage

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
)

type (
	MapAggregateKey = struct {
		MetricName string
		Hostname   string
	}
)

func GroupAggMetrics(dest *pbm.AggregatedMetrics, queryDuration time.Duration) error {
	if dest == nil {
		return fmt.Errorf("cannot merge with nil dst")
	}
	if dest.Calls == 0 {
		dest.MaxTime = float64(queryDuration)
		dest.MinTime = float64(queryDuration)
	}
	dest.Calls += 1
	dest.TotalTime += float64(queryDuration)
	dest.MeanTime = dest.TotalTime / float64(dest.Calls)
	if dest.MaxTime < float64(queryDuration) {
		dest.MaxTime = float64(queryDuration)
	}
	if dest.MinTime > float64(queryDuration) {
		dest.MinTime = float64(queryDuration)
	}
	return nil
}

func UpdateIntermediateKey(intermediateResults map[MapAggregateKey]uint64, mapKey MapAggregateKey, value uint64) {
	valMap, ok := intermediateResults[mapKey]
	if !ok {
		intermediateResults[mapKey] = value
		return
	}
	intermediateResults[mapKey] = valMap + value
}

func GroupGPMetrics(dest *pbc.GPMetrics, source *pbc.GPMetrics, aggKind AggregateKind, segHostname string, intermediateResults map[MapAggregateKey]uint64) error {
	if source == nil {
		return nil
	}
	if dest == nil {
		return fmt.Errorf("cannot merge with nil dst")
	}
	if intermediateResults == nil {
		return fmt.Errorf("need map for store intermediate results")
	}
	if source.SystemStat != nil {
		if dest.SystemStat == nil {
			dest.SystemStat = proto.Clone(source.SystemStat).(*pbc.SystemStat)
		} else {
			dest.SystemStat.UserTimeSeconds += source.SystemStat.UserTimeSeconds
			dest.SystemStat.KernelTimeSeconds += source.SystemStat.KernelTimeSeconds
			dest.SystemStat.Rchar += source.SystemStat.Rchar
			dest.SystemStat.Wchar += source.SystemStat.Wchar
			dest.SystemStat.Syscr += source.SystemStat.Syscr
			dest.SystemStat.Syscw += source.SystemStat.Syscw
			dest.SystemStat.ReadBytes += source.SystemStat.ReadBytes
			dest.SystemStat.WriteBytes += source.SystemStat.WriteBytes
			dest.SystemStat.CancelledWriteBytes += source.SystemStat.CancelledWriteBytes

		}
		dest.SystemStat.RunningTimeSeconds = dest.SystemStat.UserTimeSeconds + dest.SystemStat.KernelTimeSeconds
		// update intermediate results
		UpdateIntermediateKey(intermediateResults,
			MapAggregateKey{MetricName: "Vsize", Hostname: segHostname},
			source.SystemStat.Vsize)
		UpdateIntermediateKey(intermediateResults,
			MapAggregateKey{MetricName: "Rss", Hostname: segHostname},
			source.SystemStat.Rss)
		UpdateIntermediateKey(intermediateResults,
			MapAggregateKey{MetricName: "VmSizeKb", Hostname: segHostname},
			source.SystemStat.VmSizeKb)
		UpdateIntermediateKey(intermediateResults,
			MapAggregateKey{MetricName: "VmPeakKb", Hostname: segHostname},
			source.SystemStat.VmPeakKb)

		// choose maximum value
		for key, val := range intermediateResults {
			if aggKind == AggSegmentHost && key.Hostname != segHostname {
				// skip other hosts
				continue
			}
			if key.MetricName == "Vsize" {
				if aggKind == AggMax {
					dest.SystemStat.Vsize = max(dest.SystemStat.Vsize, val)
				} else {
					dest.SystemStat.Vsize = val
				}
			}
			if key.MetricName == "Rss" {
				if aggKind == AggMax {
					dest.SystemStat.Rss = max(dest.SystemStat.Rss, val)
				} else {
					dest.SystemStat.Rss = val
				}
			}
			if key.MetricName == "VmSizeKb" {
				if aggKind == AggMax {
					dest.SystemStat.VmSizeKb = max(dest.SystemStat.VmSizeKb, val)
				} else {
					dest.SystemStat.VmSizeKb = val
				}
			}
			if key.MetricName == "VmPeakKb" {
				if aggKind == AggMax {
					dest.SystemStat.VmPeakKb = max(dest.SystemStat.VmPeakKb, val)
				} else {
					dest.SystemStat.VmPeakKb = val
				}
			}
		}
	}

	if source.Spill != nil {
		if dest.Spill == nil {
			dest.Spill = proto.Clone(source.Spill).(*pbc.SpillInfo)
		} else {
			dest.Spill.FileCount += source.Spill.FileCount
			dest.Spill.TotalBytes += source.Spill.TotalBytes
		}
	}

	if source.Instrumentation != nil {
		if dest.Instrumentation == nil {
			dest.Instrumentation = proto.Clone(source.Instrumentation).(*pbc.MetricInstrumentation)
		} else {
			dest.Instrumentation.Ntuples += source.Instrumentation.Ntuples
			dest.Instrumentation.Nloops += source.Instrumentation.Nloops
			dest.Instrumentation.Tuplecount += source.Instrumentation.Tuplecount
			dest.Instrumentation.Firsttuple += source.Instrumentation.Firsttuple
			dest.Instrumentation.Startup += source.Instrumentation.Startup
			dest.Instrumentation.Total += source.Instrumentation.Total
			dest.Instrumentation.SharedBlksHit += source.Instrumentation.SharedBlksHit
			dest.Instrumentation.SharedBlksRead += source.Instrumentation.SharedBlksRead
			dest.Instrumentation.SharedBlksWritten += source.Instrumentation.SharedBlksWritten
			dest.Instrumentation.SharedBlksDirtied += source.Instrumentation.SharedBlksDirtied
			dest.Instrumentation.LocalBlksHit += source.Instrumentation.LocalBlksHit
			dest.Instrumentation.LocalBlksRead += source.Instrumentation.LocalBlksRead
			dest.Instrumentation.LocalBlksWritten += source.Instrumentation.LocalBlksWritten
			dest.Instrumentation.LocalBlksDirtied += source.Instrumentation.LocalBlksDirtied
			dest.Instrumentation.TempBlksRead += source.Instrumentation.TempBlksRead
			dest.Instrumentation.TempBlksWritten += source.Instrumentation.TempBlksWritten
			dest.Instrumentation.BlkReadTime += source.Instrumentation.BlkReadTime
			dest.Instrumentation.BlkWriteTime += source.Instrumentation.BlkWriteTime
			dest.Instrumentation.StartupTime += source.Instrumentation.StartupTime
			dest.Instrumentation.InheritedCalls += source.Instrumentation.InheritedCalls
			dest.Instrumentation.InheritedTime += source.Instrumentation.InheritedTime

			if source.Instrumentation.Sent != nil {
				if dest.Instrumentation.Sent == nil {
					dest.Instrumentation.Sent = proto.Clone(source.Instrumentation.Sent).(*pbc.NetworkStat)
				} else {
					dest.Instrumentation.Sent.TotalBytes += source.Instrumentation.Sent.TotalBytes
					dest.Instrumentation.Sent.TupleBytes += source.Instrumentation.Sent.TupleBytes
					dest.Instrumentation.Sent.Chunks += source.Instrumentation.Sent.Chunks
					dest.Instrumentation.Sent.TotalBytesLong += source.Instrumentation.Sent.TotalBytesLong
					dest.Instrumentation.Sent.TupleBytesLong += source.Instrumentation.Sent.TupleBytesLong
					dest.Instrumentation.Sent.ChunksLong += source.Instrumentation.Sent.ChunksLong
				}
			}

			if source.Instrumentation.Received != nil {
				if dest.Instrumentation.Received == nil {
					dest.Instrumentation.Received = proto.Clone(source.Instrumentation.Received).(*pbc.NetworkStat)
				} else {
					dest.Instrumentation.Received.TotalBytes += source.Instrumentation.Received.TotalBytes
					dest.Instrumentation.Received.TupleBytes += source.Instrumentation.Received.TupleBytes
					dest.Instrumentation.Received.Chunks += source.Instrumentation.Received.Chunks
					dest.Instrumentation.Received.TotalBytesLong += source.Instrumentation.Received.TotalBytesLong
					dest.Instrumentation.Received.TupleBytesLong += source.Instrumentation.Received.TupleBytesLong
					dest.Instrumentation.Received.ChunksLong += source.Instrumentation.Received.ChunksLong
				}
			}

			if source.Instrumentation.Interconnect != nil {
				if dest.Instrumentation.Interconnect == nil {
					dest.Instrumentation.Interconnect = proto.Clone(source.Instrumentation.Interconnect).(*pbc.InterconnectStat)
				} else {
					dstIc := dest.Instrumentation.Interconnect
					srcIc := source.Instrumentation.Interconnect

					dstIc.TotalRecvQueueSize = max(dstIc.TotalRecvQueueSize, srcIc.TotalRecvQueueSize)
					dstIc.RecvQueueSizeCountingTime += srcIc.RecvQueueSizeCountingTime
					dstIc.TotalCapacity = max(dstIc.TotalCapacity, srcIc.TotalCapacity)
					dstIc.CapacityCountingTime += srcIc.CapacityCountingTime
					dstIc.TotalBuffers = min(dstIc.TotalBuffers, srcIc.TotalBuffers)
					dstIc.BufferCountingTime += srcIc.BufferCountingTime
					dstIc.ActiveConnectionsNum = max(dstIc.ActiveConnectionsNum, srcIc.ActiveConnectionsNum)
					dstIc.Retransmits += srcIc.Retransmits
					dstIc.StartupCachedPktNum += srcIc.StartupCachedPktNum
					dstIc.MismatchNum += srcIc.MismatchNum
					dstIc.CrcErrors += srcIc.CrcErrors
					dstIc.SndPktNum += srcIc.SndPktNum
					dstIc.RecvPktNum += srcIc.RecvPktNum
					dstIc.DisorderedPktNum += srcIc.DisorderedPktNum
					dstIc.DuplicatedPktNum += srcIc.DuplicatedPktNum
					dstIc.RecvAckNum += srcIc.RecvAckNum
					dstIc.StatusQueryMsgNum += srcIc.StatusQueryMsgNum
				}
			}
		}
	}

	return nil
}

func chooseTimestamp(dest *timestamppb.Timestamp, source *timestamppb.Timestamp) *timestamppb.Timestamp {
	if source == nil {
		return dest
	}
	if dest == nil {
		return source
	}
	if source.Seconds > dest.Seconds {
		return source
	}
	return dest
}

func MergeQueryInfo(dest *pbc.QueryInfo, source *pbc.QueryInfo) error {
	if source == nil {
		return nil
	}
	if dest == nil {
		return fmt.Errorf("cannot merge with nil dst")
	}
	dest.Generator = max(dest.Generator, source.Generator)
	dest.QueryId = max(dest.QueryId, source.QueryId)
	dest.PlanId = max(dest.PlanId, source.PlanId)
	dest.QueryText = max(dest.QueryText, source.QueryText)
	dest.PlanText = max(dest.PlanText, source.PlanText)
	dest.UserName = max(dest.UserName, source.UserName)
	dest.DatabaseName = max(dest.DatabaseName, source.DatabaseName)
	dest.Rsgname = max(dest.Rsgname, source.Rsgname)
	dest.AnalyzeText = max(dest.AnalyzeText, source.AnalyzeText)
	dest.SubmitTime = chooseTimestamp(dest.SubmitTime, source.SubmitTime)
	dest.StartTime = chooseTimestamp(dest.StartTime, source.StartTime)
	dest.EndTime = chooseTimestamp(dest.EndTime, source.EndTime)
	return nil
}

func MergeGPMetrics(dest *pbc.GPMetrics, source *pbc.GPMetrics) error {
	if source == nil {
		return nil
	}
	if dest == nil {
		return fmt.Errorf("cannot merge with nil dst")
	}

	if source.SystemStat != nil {
		if dest.SystemStat == nil {
			dest.SystemStat = proto.Clone(source.SystemStat).(*pbc.SystemStat)
		} else {
			dest.SystemStat.UserTimeSeconds = max(source.SystemStat.UserTimeSeconds, dest.SystemStat.UserTimeSeconds)
			dest.SystemStat.KernelTimeSeconds = max(source.SystemStat.KernelTimeSeconds, dest.SystemStat.KernelTimeSeconds)
			dest.SystemStat.Vsize = max(dest.SystemStat.Vsize, source.SystemStat.Vsize)
			dest.SystemStat.Rss = max(dest.SystemStat.Rss, source.SystemStat.Rss)
			dest.SystemStat.VmSizeKb = max(dest.SystemStat.VmSizeKb, source.SystemStat.VmSizeKb)
			dest.SystemStat.VmPeakKb = max(dest.SystemStat.VmPeakKb, source.SystemStat.VmPeakKb)
			dest.SystemStat.Rchar = max(source.SystemStat.Rchar, dest.SystemStat.Rchar)
			dest.SystemStat.Wchar = max(source.SystemStat.Wchar, dest.SystemStat.Wchar)
			dest.SystemStat.Syscr = max(source.SystemStat.Syscr, dest.SystemStat.Syscr)
			dest.SystemStat.Syscw = max(source.SystemStat.Syscw, dest.SystemStat.Syscw)
			dest.SystemStat.ReadBytes = max(source.SystemStat.ReadBytes, dest.SystemStat.ReadBytes)
			dest.SystemStat.WriteBytes = max(source.SystemStat.WriteBytes, dest.SystemStat.WriteBytes)
			dest.SystemStat.CancelledWriteBytes = max(source.SystemStat.CancelledWriteBytes, dest.SystemStat.CancelledWriteBytes)
		}
		dest.SystemStat.RunningTimeSeconds = dest.SystemStat.UserTimeSeconds + dest.SystemStat.KernelTimeSeconds
	}

	if source.Spill != nil {
		if dest.Spill == nil {
			dest.Spill = proto.Clone(source.Spill).(*pbc.SpillInfo)
		} else {
			dest.Spill.FileCount = max(source.Spill.FileCount, dest.Spill.FileCount)
			dest.Spill.TotalBytes = max(source.Spill.TotalBytes, dest.Spill.TotalBytes)
		}
	}

	if source.Instrumentation != nil {
		if dest.Instrumentation == nil {
			dest.Instrumentation = proto.Clone(source.Instrumentation).(*pbc.MetricInstrumentation)
		} else {
			dest.Instrumentation.Ntuples = max(source.Instrumentation.Ntuples, dest.Instrumentation.Ntuples)
			dest.Instrumentation.Nloops = max(source.Instrumentation.Nloops, dest.Instrumentation.Nloops)
			dest.Instrumentation.Tuplecount = max(source.Instrumentation.Tuplecount, dest.Instrumentation.Tuplecount)
			dest.Instrumentation.Firsttuple = max(source.Instrumentation.Firsttuple, dest.Instrumentation.Firsttuple)
			dest.Instrumentation.Startup = max(source.Instrumentation.Startup, dest.Instrumentation.Startup)
			dest.Instrumentation.Total = max(source.Instrumentation.Total, dest.Instrumentation.Total)
			dest.Instrumentation.SharedBlksHit = max(source.Instrumentation.SharedBlksHit, dest.Instrumentation.SharedBlksHit)
			dest.Instrumentation.SharedBlksRead = max(source.Instrumentation.SharedBlksRead, dest.Instrumentation.SharedBlksRead)
			dest.Instrumentation.SharedBlksWritten = max(source.Instrumentation.SharedBlksWritten, dest.Instrumentation.SharedBlksWritten)
			dest.Instrumentation.SharedBlksDirtied = max(source.Instrumentation.SharedBlksDirtied, dest.Instrumentation.SharedBlksDirtied)
			dest.Instrumentation.LocalBlksHit = max(source.Instrumentation.LocalBlksHit, dest.Instrumentation.LocalBlksHit)
			dest.Instrumentation.LocalBlksRead = max(source.Instrumentation.LocalBlksRead, dest.Instrumentation.LocalBlksRead)
			dest.Instrumentation.LocalBlksWritten = max(source.Instrumentation.LocalBlksWritten, dest.Instrumentation.LocalBlksWritten)
			dest.Instrumentation.LocalBlksDirtied = max(source.Instrumentation.LocalBlksDirtied, dest.Instrumentation.LocalBlksDirtied)
			dest.Instrumentation.TempBlksRead = max(source.Instrumentation.TempBlksRead, dest.Instrumentation.TempBlksRead)
			dest.Instrumentation.TempBlksWritten = max(source.Instrumentation.TempBlksWritten, dest.Instrumentation.TempBlksWritten)
			dest.Instrumentation.BlkReadTime = max(source.Instrumentation.BlkReadTime, dest.Instrumentation.BlkReadTime)
			dest.Instrumentation.BlkWriteTime = max(source.Instrumentation.BlkWriteTime, dest.Instrumentation.BlkWriteTime)
			dest.Instrumentation.StartupTime = max(source.Instrumentation.StartupTime, dest.Instrumentation.StartupTime)
			dest.Instrumentation.InheritedCalls = max(source.Instrumentation.InheritedCalls, dest.Instrumentation.InheritedCalls)
			dest.Instrumentation.InheritedTime = max(source.Instrumentation.InheritedTime, dest.Instrumentation.InheritedTime)

			if source.Instrumentation.Sent != nil {
				if dest.Instrumentation.Sent == nil {
					dest.Instrumentation.Sent = proto.Clone(source.Instrumentation.Sent).(*pbc.NetworkStat)
					dest.Instrumentation.Sent.TotalBytesLong = max(uint64(source.Instrumentation.Sent.TotalBytes), source.Instrumentation.Sent.TotalBytesLong)
					dest.Instrumentation.Sent.TupleBytesLong = max(uint64(source.Instrumentation.Sent.TupleBytes), source.Instrumentation.Sent.TupleBytesLong)
					dest.Instrumentation.Sent.ChunksLong = max(uint64(source.Instrumentation.Sent.Chunks), source.Instrumentation.Sent.ChunksLong)
				} else {
					dest.Instrumentation.Sent.TotalBytesLong = max(max(uint64(source.Instrumentation.Sent.TotalBytes), source.Instrumentation.Sent.TotalBytesLong), dest.Instrumentation.Sent.TotalBytesLong)
					dest.Instrumentation.Sent.TupleBytesLong = max(max(uint64(source.Instrumentation.Sent.TupleBytes), source.Instrumentation.Sent.TupleBytesLong), dest.Instrumentation.Sent.TupleBytesLong)
					dest.Instrumentation.Sent.ChunksLong = max(max(uint64(source.Instrumentation.Sent.Chunks), source.Instrumentation.Sent.ChunksLong), dest.Instrumentation.Sent.ChunksLong)
				}
			}

			if source.Instrumentation.Received != nil {
				if dest.Instrumentation.Received == nil {
					dest.Instrumentation.Received = proto.Clone(source.Instrumentation.Received).(*pbc.NetworkStat)
					dest.Instrumentation.Received.TotalBytesLong = max(uint64(source.Instrumentation.Received.TotalBytes), source.Instrumentation.Received.TotalBytesLong)
					dest.Instrumentation.Received.TupleBytesLong = max(uint64(source.Instrumentation.Received.TupleBytes), source.Instrumentation.Received.TupleBytesLong)
					dest.Instrumentation.Received.ChunksLong = max(uint64(source.Instrumentation.Received.Chunks), source.Instrumentation.Received.ChunksLong)
				} else {
					dest.Instrumentation.Received.TotalBytesLong = max(max(uint64(source.Instrumentation.Received.TotalBytes), source.Instrumentation.Received.TotalBytesLong), dest.Instrumentation.Received.TotalBytesLong)
					dest.Instrumentation.Received.TupleBytesLong = max(max(uint64(source.Instrumentation.Received.TupleBytes), source.Instrumentation.Received.TupleBytesLong), dest.Instrumentation.Received.TupleBytesLong)
					dest.Instrumentation.Received.ChunksLong = max(max(uint64(source.Instrumentation.Received.Chunks), source.Instrumentation.Received.ChunksLong), dest.Instrumentation.Received.ChunksLong)
				}
			}

			if source.Instrumentation.Interconnect != nil {
				if dest.Instrumentation.Interconnect == nil {
					dest.Instrumentation.Interconnect = proto.Clone(source.Instrumentation.Interconnect).(*pbc.InterconnectStat)
				} else {
					dstIc := dest.Instrumentation.Interconnect
					srcIc := source.Instrumentation.Interconnect

					dstIc.TotalRecvQueueSize = max(dstIc.TotalRecvQueueSize, srcIc.TotalRecvQueueSize)
					dstIc.RecvQueueSizeCountingTime = max(srcIc.RecvQueueSizeCountingTime, dstIc.RecvQueueSizeCountingTime)
					dstIc.TotalCapacity = max(dstIc.TotalCapacity, srcIc.TotalCapacity)
					dstIc.CapacityCountingTime = max(srcIc.CapacityCountingTime, dstIc.CapacityCountingTime)
					dstIc.TotalBuffers = min(dstIc.TotalBuffers, srcIc.TotalBuffers)
					dstIc.BufferCountingTime = max(srcIc.BufferCountingTime, dstIc.BufferCountingTime)
					dstIc.ActiveConnectionsNum = max(dstIc.ActiveConnectionsNum, srcIc.ActiveConnectionsNum)
					dstIc.Retransmits = max(srcIc.Retransmits, dstIc.Retransmits)
					dstIc.StartupCachedPktNum = max(srcIc.StartupCachedPktNum, dstIc.StartupCachedPktNum)
					dstIc.MismatchNum = max(srcIc.MismatchNum, dstIc.MismatchNum)
					dstIc.CrcErrors = max(srcIc.CrcErrors, dstIc.CrcErrors)
					dstIc.SndPktNum = max(srcIc.SndPktNum, dstIc.SndPktNum)
					dstIc.RecvPktNum = max(srcIc.RecvPktNum, dstIc.RecvPktNum)
					dstIc.DisorderedPktNum = max(srcIc.DisorderedPktNum, dstIc.DisorderedPktNum)
					dstIc.DuplicatedPktNum = max(srcIc.DuplicatedPktNum, dstIc.DuplicatedPktNum)
					dstIc.RecvAckNum = max(srcIc.RecvAckNum, dstIc.RecvAckNum)
					dstIc.StatusQueryMsgNum = max(srcIc.StatusQueryMsgNum, dstIc.StatusQueryMsgNum)
				}
			}
		}
	}

	return nil
}
