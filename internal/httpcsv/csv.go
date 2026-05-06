package httpcsv

import (
	"encoding/csv"
	"fmt"
	"io"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
)

// sessionStateHeaders returns the CSV header row for SessionState records.
func sessionStateHeaders() []string {
	return []string{
		// Timestamps & identity
		"time",
		"cluster_id",
		"hostname",

		// SessionKey
		"session_key.sess_id",
		"session_key.tm_id",

		// SessionInfo
		"session_info.pid",
		"session_info.database",
		"session_info.user",
		"session_info.application_name",
		"session_info.client_addr",
		"session_info.client_hostname",
		"session_info.client_port",
		"session_info.backend_start",
		"session_info.xact_start",
		"session_info.query_start",
		"session_info.state_change",
		"session_info.waiting_reason",
		"session_info.waiting",
		"session_info.state",
		"session_info.backend_xid",
		"session_info.backend_xmin",
		"session_info.rsgid",
		"session_info.rsgname",
		"session_info.rsgqueueduration",
		"session_info.blocked_by_sess_id",
		"session_info.wait_mode",
		"session_info.locked_item",
		"session_info.locked_mode",
		"session_info.wait_event",
		"session_info.wait_event_type",

		// Running query
		"running_query.tmid",
		"running_query.ssid",
		"running_query.ccnt",

		// Running query info
		"running_query_info.generator",
		"running_query_info.query_id",
		"running_query_info.plan_id",
		"running_query_info.query_text",
		"running_query_info.user_name",
		"running_query_info.database_name",
		"running_query_info.rsgname",

		// Running query status
		"running_query_status",

		// Total metrics — system stat
		"total_metrics.system_stat.running_time_seconds",
		"total_metrics.system_stat.user_time_seconds",
		"total_metrics.system_stat.kernel_time_seconds",
		"total_metrics.system_stat.vsize",
		"total_metrics.system_stat.rss",
		"total_metrics.system_stat.vm_peak_kb",
		"total_metrics.system_stat.rchar",
		"total_metrics.system_stat.wchar",
		"total_metrics.system_stat.syscr",
		"total_metrics.system_stat.syscw",
		"total_metrics.system_stat.read_bytes",
		"total_metrics.system_stat.write_bytes",
		"total_metrics.system_stat.cancelled_write_bytes",

		// Total metrics — instrumentation
		"total_metrics.instrumentation.ntuples",
		"total_metrics.instrumentation.nloops",
		"total_metrics.instrumentation.tuplecount",
		"total_metrics.instrumentation.firsttuple",
		"total_metrics.instrumentation.startup",
		"total_metrics.instrumentation.total",
		"total_metrics.instrumentation.shared_blks_hit",
		"total_metrics.instrumentation.shared_blks_read",
		"total_metrics.instrumentation.shared_blks_dirtied",
		"total_metrics.instrumentation.shared_blks_written",
		"total_metrics.instrumentation.local_blks_hit",
		"total_metrics.instrumentation.local_blks_read",
		"total_metrics.instrumentation.local_blks_dirtied",
		"total_metrics.instrumentation.local_blks_written",
		"total_metrics.instrumentation.temp_blks_read",
		"total_metrics.instrumentation.temp_blks_written",
		"total_metrics.instrumentation.blk_read_time",
		"total_metrics.instrumentation.blk_write_time",
		"total_metrics.instrumentation.startup_time",
		"total_metrics.instrumentation.inherited_calls",
		"total_metrics.instrumentation.inherited_time",

		// Total metrics — instrumentation — network sent
		"total_metrics.instrumentation.sent.total_bytes",
		"total_metrics.instrumentation.sent.tuple_bytes",
		"total_metrics.instrumentation.sent.chunks",

		// Total metrics — instrumentation — network received
		"total_metrics.instrumentation.received.total_bytes",
		"total_metrics.instrumentation.received.tuple_bytes",
		"total_metrics.instrumentation.received.chunks",

		// Total metrics — spill
		"total_metrics.spill.file_count",
		"total_metrics.spill.total_bytes",

		// Last metrics — system stat
		"last_metrics.system_stat.running_time_seconds",
		"last_metrics.system_stat.user_time_seconds",
		"last_metrics.system_stat.kernel_time_seconds",
		"last_metrics.system_stat.vsize",
		"last_metrics.system_stat.rss",
		"last_metrics.system_stat.vm_peak_kb",
		"last_metrics.system_stat.rchar",
		"last_metrics.system_stat.wchar",
		"last_metrics.system_stat.syscr",
		"last_metrics.system_stat.syscw",
		"last_metrics.system_stat.read_bytes",
		"last_metrics.system_stat.write_bytes",
		"last_metrics.system_stat.cancelled_write_bytes",

		// Last metrics — instrumentation
		"last_metrics.instrumentation.ntuples",
		"last_metrics.instrumentation.nloops",
		"last_metrics.instrumentation.tuplecount",
		"last_metrics.instrumentation.firsttuple",
		"last_metrics.instrumentation.startup",
		"last_metrics.instrumentation.total",
		"last_metrics.instrumentation.shared_blks_hit",
		"last_metrics.instrumentation.shared_blks_read",
		"last_metrics.instrumentation.shared_blks_dirtied",
		"last_metrics.instrumentation.shared_blks_written",
		"last_metrics.instrumentation.local_blks_hit",
		"last_metrics.instrumentation.local_blks_read",
		"last_metrics.instrumentation.local_blks_dirtied",
		"last_metrics.instrumentation.local_blks_written",
		"last_metrics.instrumentation.temp_blks_read",
		"last_metrics.instrumentation.temp_blks_written",
		"last_metrics.instrumentation.blk_read_time",
		"last_metrics.instrumentation.blk_write_time",
		"last_metrics.instrumentation.startup_time",
		"last_metrics.instrumentation.inherited_calls",
		"last_metrics.instrumentation.inherited_time",

		// Last metrics — instrumentation — network sent
		"last_metrics.instrumentation.sent.total_bytes",
		"last_metrics.instrumentation.sent.tuple_bytes",
		"last_metrics.instrumentation.sent.chunks",

		// Last metrics — instrumentation — network received
		"last_metrics.instrumentation.received.total_bytes",
		"last_metrics.instrumentation.received.tuple_bytes",
		"last_metrics.instrumentation.received.chunks",

		// Last metrics — spill
		"last_metrics.spill.file_count",
		"last_metrics.spill.total_bytes",

		// Query metrics — system stat
		"query_metrics.system_stat.running_time_seconds",
		"query_metrics.system_stat.user_time_seconds",
		"query_metrics.system_stat.kernel_time_seconds",
		"query_metrics.system_stat.vsize",
		"query_metrics.system_stat.rss",
		"query_metrics.system_stat.vm_peak_kb",
		"query_metrics.system_stat.rchar",
		"query_metrics.system_stat.wchar",
		"query_metrics.system_stat.syscr",
		"query_metrics.system_stat.syscw",
		"query_metrics.system_stat.read_bytes",
		"query_metrics.system_stat.write_bytes",
		"query_metrics.system_stat.cancelled_write_bytes",

		// Query metrics — instrumentation
		"query_metrics.instrumentation.ntuples",
		"query_metrics.instrumentation.nloops",
		"query_metrics.instrumentation.tuplecount",
		"query_metrics.instrumentation.firsttuple",
		"query_metrics.instrumentation.startup",
		"query_metrics.instrumentation.total",
		"query_metrics.instrumentation.shared_blks_hit",
		"query_metrics.instrumentation.shared_blks_read",
		"query_metrics.instrumentation.shared_blks_dirtied",
		"query_metrics.instrumentation.shared_blks_written",
		"query_metrics.instrumentation.local_blks_hit",
		"query_metrics.instrumentation.local_blks_read",
		"query_metrics.instrumentation.local_blks_dirtied",
		"query_metrics.instrumentation.local_blks_written",
		"query_metrics.instrumentation.temp_blks_read",
		"query_metrics.instrumentation.temp_blks_written",
		"query_metrics.instrumentation.blk_read_time",
		"query_metrics.instrumentation.blk_write_time",
		"query_metrics.instrumentation.startup_time",
		"query_metrics.instrumentation.inherited_calls",
		"query_metrics.instrumentation.inherited_time",

		// Query metrics — instrumentation — network sent
		"query_metrics.instrumentation.sent.total_bytes",
		"query_metrics.instrumentation.sent.tuple_bytes",
		"query_metrics.instrumentation.sent.chunks",

		// Query metrics — instrumentation — network received
		"query_metrics.instrumentation.received.total_bytes",
		"query_metrics.instrumentation.received.tuple_bytes",
		"query_metrics.instrumentation.received.chunks",

		// Query metrics — spill
		"query_metrics.spill.file_count",
		"query_metrics.spill.total_bytes",

		// Running query level & slices
		"running_query_level",
		"running_query_slices",

		// Aggregated metrics
		"aggregated_metrics.calls",
		"aggregated_metrics.min_time",
		"aggregated_metrics.max_time",
		"aggregated_metrics.mean_time",
		"aggregated_metrics.stddev_time",
		"aggregated_metrics.total_time",
	}
}

// formatTimestamp formats a protobuf Timestamp to RFC3339Nano string, or empty if nil.
func formatTimestamp(ts interface {
	AsTime() interface{ Format(string) string }
}) string {
	if ts == nil {
		return ""
	}
	return ts.AsTime().Format("2006-01-02T15:04:05.000000000Z07:00")
}

// gpMetricsFields returns the flattened CSV fields for a GPMetrics message.
func gpMetricsFields(m *pbc.GPMetrics) []string {
	if m == nil {
		// 13 system stat + 21 instrumentation + 6 network + 2 spill = 42 fields
		return make([]string, 42)
	}
	fields := make([]string, 0, 42)

	// System stat
	if ss := m.SystemStat; ss != nil {
		fields = append(fields,
			fmt.Sprintf("%g", ss.RunningTimeSeconds),
			fmt.Sprintf("%g", ss.UserTimeSeconds),
			fmt.Sprintf("%g", ss.KernelTimeSeconds),
			fmt.Sprintf("%d", ss.Vsize),
			fmt.Sprintf("%d", ss.Rss),
			fmt.Sprintf("%d", ss.VmPeakKb),
			fmt.Sprintf("%d", ss.Rchar),
			fmt.Sprintf("%d", ss.Wchar),
			fmt.Sprintf("%d", ss.Syscr),
			fmt.Sprintf("%d", ss.Syscw),
			fmt.Sprintf("%d", ss.ReadBytes),
			fmt.Sprintf("%d", ss.WriteBytes),
			fmt.Sprintf("%d", ss.CancelledWriteBytes),
		)
	} else {
		fields = append(fields, make([]string, 13)...)
	}

	// Instrumentation
	if instr := m.Instrumentation; instr != nil {
		fields = append(fields,
			fmt.Sprintf("%d", instr.Ntuples),
			fmt.Sprintf("%d", instr.Nloops),
			fmt.Sprintf("%d", instr.Tuplecount),
			fmt.Sprintf("%g", instr.Firsttuple),
			fmt.Sprintf("%g", instr.Startup),
			fmt.Sprintf("%g", instr.Total),
			fmt.Sprintf("%d", instr.SharedBlksHit),
			fmt.Sprintf("%d", instr.SharedBlksRead),
			fmt.Sprintf("%d", instr.SharedBlksDirtied),
			fmt.Sprintf("%d", instr.SharedBlksWritten),
			fmt.Sprintf("%d", instr.LocalBlksHit),
			fmt.Sprintf("%d", instr.LocalBlksRead),
			fmt.Sprintf("%d", instr.LocalBlksDirtied),
			fmt.Sprintf("%d", instr.LocalBlksWritten),
			fmt.Sprintf("%d", instr.TempBlksRead),
			fmt.Sprintf("%d", instr.TempBlksWritten),
			fmt.Sprintf("%g", instr.BlkReadTime),
			fmt.Sprintf("%g", instr.BlkWriteTime),
			fmt.Sprintf("%g", instr.StartupTime),
			fmt.Sprintf("%d", instr.InheritedCalls),
			fmt.Sprintf("%g", instr.InheritedTime),
		)

		// Network sent
		if sent := instr.Sent; sent != nil {
			fields = append(fields,
				fmt.Sprintf("%d", sent.TotalBytes),
				fmt.Sprintf("%d", sent.TupleBytes),
				fmt.Sprintf("%d", sent.Chunks),
			)
		} else {
			fields = append(fields, "", "", "")
		}

		// Network received
		if recv := instr.Received; recv != nil {
			fields = append(fields,
				fmt.Sprintf("%d", recv.TotalBytes),
				fmt.Sprintf("%d", recv.TupleBytes),
				fmt.Sprintf("%d", recv.Chunks),
			)
		} else {
			fields = append(fields, "", "", "")
		}
	} else {
		// 21 instrumentation + 6 network = 27
		fields = append(fields, make([]string, 27)...)
	}

	// Spill
	if sp := m.Spill; sp != nil {
		fields = append(fields,
			fmt.Sprintf("%d", sp.FileCount),
			fmt.Sprintf("%d", sp.TotalBytes),
		)
	} else {
		fields = append(fields, "", "")
	}

	return fields
}

// sessionStateToRecord converts a SessionState proto message to a flat CSV record.
func sessionStateToRecord(s *pbc.SessionState) []string {
	record := make([]string, 0, 220)

	// Timestamp
	if s.Time != nil {
		record = append(record, s.Time.AsTime().Format("2006-01-02T15:04:05.000000000Z07:00"))
	} else {
		record = append(record, "")
	}

	record = append(record, s.ClusterId, s.Hostname)

	// SessionKey
	if sk := s.SessionKey; sk != nil {
		record = append(record,
			fmt.Sprintf("%d", sk.SessId),
			fmt.Sprintf("%d", sk.TmId),
		)
	} else {
		record = append(record, "", "")
	}

	// SessionInfo
	if si := s.SessionInfo; si != nil {
		record = append(record,
			fmt.Sprintf("%d", si.Pid),
			si.Database,
			si.User,
			si.ApplicationName,
			si.ClientAddr,
			si.ClientHostname,
			fmt.Sprintf("%d", si.ClientPort),
		)
		if si.BackendStart != nil {
			record = append(record, si.BackendStart.AsTime().Format("2006-01-02T15:04:05.000000000Z07:00"))
		} else {
			record = append(record, "")
		}
		if si.XactStart != nil {
			record = append(record, si.XactStart.AsTime().Format("2006-01-02T15:04:05.000000000Z07:00"))
		} else {
			record = append(record, "")
		}
		if si.QueryStart != nil {
			record = append(record, si.QueryStart.AsTime().Format("2006-01-02T15:04:05.000000000Z07:00"))
		} else {
			record = append(record, "")
		}
		if si.StateChange != nil {
			record = append(record, si.StateChange.AsTime().Format("2006-01-02T15:04:05.000000000Z07:00"))
		} else {
			record = append(record, "")
		}
		record = append(record,
			si.WaitingReason,
			fmt.Sprintf("%t", si.Waiting),
			si.State,
			si.BackendXid,
			si.BackendXmin,
			fmt.Sprintf("%d", si.Rsgid),
			si.Rsgname,
			si.Rsgqueueduration,
			fmt.Sprintf("%d", si.BlockedBySessId),
			si.WaitMode,
			si.LockedItem,
			si.LockedMode,
			si.WaitEvent,
			si.WaitEventType,
		)
	} else {
		// 25 SessionInfo fields
		record = append(record, make([]string, 25)...)
	}

	// Running query key
	if rq := s.RunningQuery; rq != nil {
		record = append(record,
			fmt.Sprintf("%d", rq.Tmid),
			fmt.Sprintf("%d", rq.Ssid),
			fmt.Sprintf("%d", rq.Ccnt),
		)
	} else {
		record = append(record, "", "", "")
	}

	// Running query info
	if rqi := s.RunningQueryInfo; rqi != nil {
		record = append(record,
			rqi.Generator.String(),
			fmt.Sprintf("%d", rqi.QueryId),
			fmt.Sprintf("%d", rqi.PlanId),
			rqi.QueryText,
			rqi.UserName,
			rqi.DatabaseName,
			rqi.Rsgname,
		)
	} else {
		record = append(record, make([]string, 7)...)
	}

	// Running query status
	record = append(record, s.RunningQueryStatus.String())

	// Total, Last, Query metrics (42 fields each)
	record = append(record, gpMetricsFields(s.TotalMetrics)...)
	record = append(record, gpMetricsFields(s.LastMetrics)...)
	record = append(record, gpMetricsFields(s.QueryMetrics)...)

	// Running query level & slices
	record = append(record,
		fmt.Sprintf("%d", s.RunningQueryLevel),
		fmt.Sprintf("%d", s.RunningQuerySlices),
	)

	// Aggregated metrics
	if am := s.AggregatedMetrics; am != nil {
		record = append(record,
			fmt.Sprintf("%d", am.Calls),
			fmt.Sprintf("%g", am.MinTime),
			fmt.Sprintf("%g", am.MaxTime),
			fmt.Sprintf("%g", am.MeanTime),
			fmt.Sprintf("%g", am.StddevTime),
			fmt.Sprintf("%g", am.TotalTime),
		)
	} else {
		record = append(record, make([]string, 6)...)
	}

	return record
}

// WriteSessionsCSV writes SessionState records as CSV to the given writer.
func WriteSessionsCSV(w io.Writer, sessions []*pbc.SessionState) error {
	cw := csv.NewWriter(w)
	defer cw.Flush()

	if err := cw.Write(sessionStateHeaders()); err != nil {
		return fmt.Errorf("write csv header: %w", err)
	}

	for _, s := range sessions {
		if err := cw.Write(sessionStateToRecord(s)); err != nil {
			return fmt.Errorf("write csv record: %w", err)
		}
	}

	return cw.Error()
}

// totalSessionsStatHeaders returns the CSV header row for GetTotalSessionsStat.
func totalSessionsStatHeaders() []string {
	return []string{"state", "count"}
}

// WriteTotalSessionsStatCSV writes SessionStat records as CSV.
func WriteTotalSessionsStatCSV(w io.Writer, stats []*pbm.SessionStat) error {
	cw := csv.NewWriter(w)
	defer cw.Flush()

	if err := cw.Write(totalSessionsStatHeaders()); err != nil {
		return fmt.Errorf("write csv header: %w", err)
	}

	for _, s := range stats {
		record := []string{
			s.State,
			fmt.Sprintf("%g", s.Count),
		}
		if err := cw.Write(record); err != nil {
			return fmt.Errorf("write csv record: %w", err)
		}
	}

	return cw.Error()
}

// queryDataHeaders returns the CSV header row for TotalQueryData (GetGPQuery).
func queryDataHeaders() []string {
	return []string{
		// QueryStat
		"query_stat.cluster_id",
		"query_stat.hostname",
		"query_stat.collect_time",
		"query_stat.query_key.tmid",
		"query_stat.query_key.ssid",
		"query_stat.query_key.ccnt",
		"query_stat.query_info.generator",
		"query_stat.query_info.query_id",
		"query_stat.query_info.plan_id",
		"query_stat.query_info.query_text",
		"query_stat.query_info.user_name",
		"query_stat.query_info.database_name",
		"query_stat.query_info.rsgname",
		"query_stat.stat_kind",
		"query_stat.query_status",
		"query_stat.start_time",
		"query_stat.end_time",
		"query_stat.completed",
		"query_stat.blocked_by_sess_id",
		"query_stat.wait_mode",
		"query_stat.locked_item",
		"query_stat.locked_mode",
		"query_stat.session_state",
		"query_stat.message",
		"query_stat.slices",

		// QueryStat total_query_metrics (42 fields)
		"query_stat.total_query_metrics.system_stat.running_time_seconds",
		"query_stat.total_query_metrics.system_stat.user_time_seconds",
		"query_stat.total_query_metrics.system_stat.kernel_time_seconds",
		"query_stat.total_query_metrics.system_stat.vsize",
		"query_stat.total_query_metrics.system_stat.rss",
		"query_stat.total_query_metrics.system_stat.vm_peak_kb",
		"query_stat.total_query_metrics.system_stat.rchar",
		"query_stat.total_query_metrics.system_stat.wchar",
		"query_stat.total_query_metrics.system_stat.syscr",
		"query_stat.total_query_metrics.system_stat.syscw",
		"query_stat.total_query_metrics.system_stat.read_bytes",
		"query_stat.total_query_metrics.system_stat.write_bytes",
		"query_stat.total_query_metrics.system_stat.cancelled_write_bytes",
		"query_stat.total_query_metrics.instrumentation.ntuples",
		"query_stat.total_query_metrics.instrumentation.nloops",
		"query_stat.total_query_metrics.instrumentation.tuplecount",
		"query_stat.total_query_metrics.instrumentation.firsttuple",
		"query_stat.total_query_metrics.instrumentation.startup",
		"query_stat.total_query_metrics.instrumentation.total",
		"query_stat.total_query_metrics.instrumentation.shared_blks_hit",
		"query_stat.total_query_metrics.instrumentation.shared_blks_read",
		"query_stat.total_query_metrics.instrumentation.shared_blks_dirtied",
		"query_stat.total_query_metrics.instrumentation.shared_blks_written",
		"query_stat.total_query_metrics.instrumentation.local_blks_hit",
		"query_stat.total_query_metrics.instrumentation.local_blks_read",
		"query_stat.total_query_metrics.instrumentation.local_blks_dirtied",
		"query_stat.total_query_metrics.instrumentation.local_blks_written",
		"query_stat.total_query_metrics.instrumentation.temp_blks_read",
		"query_stat.total_query_metrics.instrumentation.temp_blks_written",
		"query_stat.total_query_metrics.instrumentation.blk_read_time",
		"query_stat.total_query_metrics.instrumentation.blk_write_time",
		"query_stat.total_query_metrics.instrumentation.startup_time",
		"query_stat.total_query_metrics.instrumentation.inherited_calls",
		"query_stat.total_query_metrics.instrumentation.inherited_time",
		"query_stat.total_query_metrics.instrumentation.sent.total_bytes",
		"query_stat.total_query_metrics.instrumentation.sent.tuple_bytes",
		"query_stat.total_query_metrics.instrumentation.sent.chunks",
		"query_stat.total_query_metrics.instrumentation.received.total_bytes",
		"query_stat.total_query_metrics.instrumentation.received.tuple_bytes",
		"query_stat.total_query_metrics.instrumentation.received.chunks",
		"query_stat.total_query_metrics.spill.file_count",
		"query_stat.total_query_metrics.spill.total_bytes",

		// QueryStat aggregated_metrics
		"query_stat.aggregated_metrics.calls",
		"query_stat.aggregated_metrics.min_time",
		"query_stat.aggregated_metrics.max_time",
		"query_stat.aggregated_metrics.mean_time",
		"query_stat.aggregated_metrics.stddev_time",
		"query_stat.aggregated_metrics.total_time",
	}
}

// queryStatToRecord converts a QueryStat to a flat CSV record.
func queryStatToRecord(qs *pbm.QueryStat) []string {
	if qs == nil {
		return make([]string, 25+42+6)
	}

	record := make([]string, 0, 25+42+6)

	record = append(record, qs.ClusterId, qs.Hostname)

	if qs.CollectTime != nil {
		record = append(record, qs.CollectTime.AsTime().Format("2006-01-02T15:04:05.000000000Z07:00"))
	} else {
		record = append(record, "")
	}

	if qk := qs.QueryKey; qk != nil {
		record = append(record,
			fmt.Sprintf("%d", qk.Tmid),
			fmt.Sprintf("%d", qk.Ssid),
			fmt.Sprintf("%d", qk.Ccnt),
		)
	} else {
		record = append(record, "", "", "")
	}

	if qi := qs.QueryInfo; qi != nil {
		record = append(record,
			qi.Generator.String(),
			fmt.Sprintf("%d", qi.QueryId),
			fmt.Sprintf("%d", qi.PlanId),
			qi.QueryText,
			qi.UserName,
			qi.DatabaseName,
			qi.Rsgname,
		)
	} else {
		record = append(record, make([]string, 7)...)
	}

	record = append(record,
		qs.StatKind.String(),
		qs.QueryStatus.String(),
	)

	if qs.StartTime != nil {
		record = append(record, qs.StartTime.AsTime().Format("2006-01-02T15:04:05.000000000Z07:00"))
	} else {
		record = append(record, "")
	}
	if qs.EndTime != nil {
		record = append(record, qs.EndTime.AsTime().Format("2006-01-02T15:04:05.000000000Z07:00"))
	} else {
		record = append(record, "")
	}

	record = append(record,
		fmt.Sprintf("%t", qs.Completed),
		fmt.Sprintf("%d", qs.BlockedBySessId),
		qs.WaitMode,
		qs.LockedItem,
		qs.LockedMode,
		qs.SessionState,
		qs.Message,
		fmt.Sprintf("%d", qs.Slices),
	)

	record = append(record, gpMetricsFields(qs.TotalQueryMetrics)...)

	if am := qs.AggregatedMetrics; am != nil {
		record = append(record,
			fmt.Sprintf("%d", am.Calls),
			fmt.Sprintf("%g", am.MinTime),
			fmt.Sprintf("%g", am.MaxTime),
			fmt.Sprintf("%g", am.MeanTime),
			fmt.Sprintf("%g", am.StddevTime),
			fmt.Sprintf("%g", am.TotalTime),
		)
	} else {
		record = append(record, make([]string, 6)...)
	}

	return record
}

// WriteQueryDataCSV writes a TotalQueryData as CSV (query stat row + segment metrics rows).
func WriteQueryDataCSV(w io.Writer, data *pbm.TotalQueryData) error {
	cw := csv.NewWriter(w)
	defer cw.Flush()

	if err := cw.Write(queryDataHeaders()); err != nil {
		return fmt.Errorf("write csv header: %w", err)
	}

	if data != nil && data.QueryStat != nil {
		if err := cw.Write(queryStatToRecord(data.QueryStat)); err != nil {
			return fmt.Errorf("write csv record: %w", err)
		}
	}

	return cw.Error()
}
