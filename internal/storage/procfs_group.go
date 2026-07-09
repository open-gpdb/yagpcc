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
	"fmt"

	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"google.golang.org/protobuf/proto"
)

func nonNegativeDiff[T int | int32 | int64](first, last T) T {
	if last < first {
		return 0
	}
	return last - first
}

// diffProcStat computes the diff between two ProcStat snapshots.
// Snapshot fields are taken from last; cumulative counters are diffed.
// If either argument is nil it is treated as a zero-valued ProcStat.
func diffProcStat(first, last *pbc.ProcStat) *pbc.ProcStat {
	if first == nil && last == nil {
		return nil
	}
	if first == nil {
		first = &pbc.ProcStat{}
	}
	if last == nil {
		last = &pbc.ProcStat{}
	}
	return &pbc.ProcStat{
		Pid:                 last.Pid,
		Comm:                last.Comm,
		State:               last.State,
		Ppid:                last.Ppid,
		Pgrp:                last.Pgrp,
		Session:             last.Session,
		Tty:                 last.Tty,
		Tpgid:               last.Tpgid,
		Flags:               last.Flags,
		MinFlt:              nonNegativeDiff(first.MinFlt, last.MinFlt),
		CminFlt:             nonNegativeDiff(first.CminFlt, last.CminFlt),
		MajFlt:              nonNegativeDiff(first.MajFlt, last.MajFlt),
		CmajFlt:             nonNegativeDiff(first.CmajFlt, last.CmajFlt),
		Utime:               nonNegativeDiff(first.Utime, last.Utime),
		Stime:               nonNegativeDiff(first.Stime, last.Stime),
		Cutime:              nonNegativeDiff(first.Cutime, last.Cutime),
		Cstime:              nonNegativeDiff(first.Cstime, last.Cstime),
		Priority:            last.Priority,
		Nice:                last.Nice,
		NumThreads:          last.NumThreads,
		Starttime:           last.Starttime,
		Vsize:               last.Vsize,
		Rss:                 last.Rss,
		RssLimit:            last.RssLimit,
		StartCode:           last.StartCode,
		EndCode:             last.EndCode,
		StartStack:          last.StartStack,
		Processor:           last.Processor,
		RtPriority:          last.RtPriority,
		Policy:              last.Policy,
		DelayAcctBlkIoTicks: last.DelayAcctBlkIoTicks,
		GuestTime:           nonNegativeDiff(first.GuestTime, last.GuestTime),
		CguestTime:          nonNegativeDiff(first.CguestTime, last.CguestTime),
	}
}

// diffProcIO computes the diff between two ProcIO snapshots.
// All fields are cumulative counters, so every field is diffed.
// If either argument is nil it is treated as a zero-valued ProcIO.
func diffProcIO(first, last *pbc.ProcIO) *pbc.ProcIO {
	if first == nil && last == nil {
		return nil
	}
	if first == nil {
		first = &pbc.ProcIO{}
	}
	if last == nil {
		last = &pbc.ProcIO{}
	}
	return &pbc.ProcIO{
		Rchar:               nonNegativeDiff(first.Rchar, last.Rchar),
		Wchar:               nonNegativeDiff(first.Wchar, last.Wchar),
		Syscr:               nonNegativeDiff(first.Syscr, last.Syscr),
		Syscw:               nonNegativeDiff(first.Syscw, last.Syscw),
		ReadBytes:           nonNegativeDiff(first.ReadBytes, last.ReadBytes),
		WriteBytes:          nonNegativeDiff(first.WriteBytes, last.WriteBytes),
		CancelledWriteBytes: nonNegativeDiff(first.CancelledWriteBytes, last.CancelledWriteBytes),
	}
}

// ProcfsDiff returns a top-like diff between two procfs measurements.
// Snapshot (point-in-time) fields are taken from last; cumulative counters
// are diffed via nonNegativeDiff. Nil ProcStat / ProcIo / ProcStatus
// sub-structs are handled gracefully (treated as zero-valued).
func ProcfsDiff(first, last *pbc.GpPidProcInfo) (*pbc.GpPidProcInfo, error) {
	if first == nil && last == nil {
		return nil, fmt.Errorf("first and last both nil")
	}
	if first == nil {
		return last, nil
	}
	if last == nil {
		return first, nil
	}
	return &pbc.GpPidProcInfo{
		GpSegmentId: last.GpSegmentId,
		SessId:      last.SessId,
		Pid:         last.Pid,
		Cmdline:     last.Cmdline,
		State:       last.State,
		Ccnt:        last.Ccnt,
		SliceId:     last.SliceId,
		ProcStatus:  last.ProcStatus,
		ProcStat:    diffProcStat(first.ProcStat, last.ProcStat),
		ProcIo:      diffProcIO(first.ProcIo, last.ProcIo),
		ProcSpill:   last.ProcSpill,
	}, nil
}

func GroupProcfsMetrics(dest *pbc.GpPidProcInfo, source *pbc.GpPidProcInfo, aggKind AggregateKind, segHostname string, intermediateResults map[MapAggregateKey]int64) error {
	if source == nil {
		return nil
	}
	if dest == nil {
		return fmt.Errorf("cannot merge with nil procfs dst")
	}
	if intermediateResults == nil {
		return fmt.Errorf("need map for store intermediate procfs results")
	}
	// group only procstat and procio
	if source.ProcStat != nil {
		if dest.ProcStat == nil {
			dest.ProcStat = proto.Clone(source.ProcStat).(*pbc.ProcStat)
		} else {
			dest.ProcStat.Utime += source.ProcStat.Utime
			dest.ProcStat.Stime += source.ProcStat.Stime
			dest.ProcStat.Cutime += source.ProcStat.Cutime
			dest.ProcStat.Cstime += source.ProcStat.Cstime
			dest.ProcStat.GuestTime += source.ProcStat.GuestTime
			dest.ProcStat.CguestTime += source.ProcStat.CguestTime
		}

		// update intermediate results
		UpdateIntermediateKey(intermediateResults,
			MapAggregateKey{MetricName: "Vsize", Hostname: segHostname},
			source.ProcStat.Vsize)
		UpdateIntermediateKey(intermediateResults,
			MapAggregateKey{MetricName: "Rss", Hostname: segHostname},
			source.ProcStat.Rss)

		// choose maximum value
		for key, val := range intermediateResults {
			if aggKind == AggSegmentHost && key.Hostname != segHostname {
				// skip other hosts
				continue
			}
			if key.MetricName == "Vsize" {
				if aggKind == AggMax {
					dest.ProcStat.Vsize = max(dest.ProcStat.Vsize, val)
				} else {
					dest.ProcStat.Vsize = val
				}
			}
			if key.MetricName == "Rss" {
				if aggKind == AggMax {
					dest.ProcStat.Rss = max(dest.ProcStat.Rss, val)
				} else {
					dest.ProcStat.Rss = val
				}
			}
		}
	}

	if source.ProcIo != nil {
		if dest.ProcIo == nil {
			dest.ProcIo = proto.Clone(source.ProcIo).(*pbc.ProcIO)
		} else {
			dest.ProcIo.Rchar += source.ProcIo.Rchar
			dest.ProcIo.Wchar += source.ProcIo.Wchar
			dest.ProcIo.Syscr += source.ProcIo.Syscr
			dest.ProcIo.Syscw += source.ProcIo.Syscw
			dest.ProcIo.ReadBytes += source.ProcIo.ReadBytes
			dest.ProcIo.WriteBytes += source.ProcIo.WriteBytes
			dest.ProcIo.CancelledWriteBytes += source.ProcIo.CancelledWriteBytes
		}
	}

	if source.ProcSpill != nil {
		if dest.ProcSpill == nil {
			dest.ProcSpill = proto.Clone(source.ProcSpill).(*pbc.ProcSpill)
		} else {
			dest.ProcSpill.Size += source.ProcSpill.Size
			dest.ProcSpill.Files += source.ProcSpill.Files
		}
	}

	return nil
}
