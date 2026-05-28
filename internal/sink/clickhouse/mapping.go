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

// File: mapping.go converts in-memory yagpcc structures (TotalQueryData,
// AggregatedBucket, Session) into the column tuples expected by the
// ClickHouse INSERT statements. AggregatedBucket itself is defined here
// because it is the producer-facing type the orchestrator builds from the
// existing AggKey/AggVal pair in internal/storage.
package clickhouse

import (
	"encoding/json"
	"time"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
)

// Status / generator strings used by the Enum8 columns. The DDL declares the
// same names; using strings (not numeric IDs) lets ClickHouse decode them
// into the enum and gives us readable test fixtures.
const (
	statusUnknown    = "UNKNOWN"
	statusSubmit     = "SUBMIT"
	statusStart      = "START"
	statusDone       = "DONE"
	statusQueryDone  = "QUERY_DONE"
	statusError      = "ERROR"
	statusCancelling = "CANCELLING"
	statusCanceled   = "CANCELED"
	statusEnd        = "END"

	generatorUnknown   = "UNKNOWN"
	generatorPlanner   = "PLANNER"
	generatorOptimizer = "OPTIMIZER"
)

// queryStatusToCH maps the protobuf QueryStatus enum to the string variant
// used by the ClickHouse Enum8 column. Unknown values fall back to "UNKNOWN"
// so that drift in the proto does not crash inserts.
func queryStatusToCH(s pbc.QueryStatus) string {
	switch s {
	case pbc.QueryStatus_QUERY_STATUS_SUBMIT:
		return statusSubmit
	case pbc.QueryStatus_QUERY_STATUS_START:
		return statusStart
	case pbc.QueryStatus_QUERY_STATUS_DONE:
		return statusDone
	case pbc.QueryStatus_QUERY_STATUS_QUERY_DONE:
		return statusQueryDone
	case pbc.QueryStatus_QUERY_STATUS_ERROR:
		return statusError
	case pbc.QueryStatus_QUERY_STATUS_CANCELLING:
		return statusCancelling
	case pbc.QueryStatus_QUERY_STATUS_CANCELED:
		return statusCanceled
	case pbc.QueryStatus_QUERY_STATUS_END:
		return statusEnd
	default:
		return statusUnknown
	}
}

// generatorToCH maps PlanGenerator to the string variant used by the
// ClickHouse Enum8 column.
func generatorToCH(g pbc.PlanGenerator) string {
	switch g {
	case pbc.PlanGenerator_PLAN_GENERATOR_PLANNER:
		return generatorPlanner
	case pbc.PlanGenerator_PLAN_GENERATOR_OPTIMIZER:
		return generatorOptimizer
	default:
		return generatorUnknown
	}
}

// timestampToTime converts a non-nil/valid protobuf Timestamp to time.Time.
// Returns the zero value when the timestamp is missing or invalid; callers
// decide whether to treat the zero value as Nullable NULL.
func timestampToTime(ts interface {
	IsValid() bool
	AsTime() time.Time
}) time.Time {
	if ts == nil {
		return time.Time{}
	}
	if !ts.IsValid() {
		return time.Time{}
	}
	return ts.AsTime()
}

// nullableTime returns nil when t is the zero value, otherwise &t. Used for
// Nullable(DateTime64) columns.
func nullableTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// queryTotals accumulates the per-segment SystemStat / MetricInstrumentation
// numbers into a single set of total_* values for a query_events row.
type queryTotals struct {
	cpuUserSec      float64
	cpuKernelSec    float64
	runningSec      float64
	rssBytes        uint64
	readBytes       uint64
	writeBytes      uint64
	ntuples         uint64
	sharedBlksHit   uint64
	sharedBlksRead  uint64
	blkReadTimeSec  float64
	blkWriteTimeSec float64
	sentBytes       uint64
	recvBytes       uint64
	spillFiles      int32
	spillBytes      int64
}

// addSegment folds one segment's GPMetrics into the running totals. Most
// counters follow the proto's "Aggregation: sum" annotation; Rss is the
// instantaneous resident memory at sample time (proto annotation: max), so
// we keep the segment-wide maximum rather than summing across segments.
func (t *queryTotals) addSegment(m *pbc.GPMetrics) {
	if m == nil {
		return
	}
	if s := m.SystemStat; s != nil {
		t.cpuUserSec += s.UserTimeSeconds
		t.cpuKernelSec += s.KernelTimeSeconds
		t.runningSec += s.UserTimeSeconds + s.KernelTimeSeconds
		if s.Rss > t.rssBytes {
			t.rssBytes = s.Rss
		}
		t.readBytes += s.ReadBytes
		t.writeBytes += s.WriteBytes
	}
	if i := m.Instrumentation; i != nil {
		t.ntuples += i.Ntuples
		t.sharedBlksHit += i.SharedBlksHit
		t.sharedBlksRead += i.SharedBlksRead
		t.blkReadTimeSec += i.BlkReadTime
		t.blkWriteTimeSec += i.BlkWriteTime
		if i.Sent != nil {
			t.sentBytes += i.Sent.TotalBytes
		}
		if i.Received != nil {
			t.recvBytes += i.Received.TotalBytes
		}
	}
	if sp := m.Spill; sp != nil {
		t.spillFiles += sp.FileCount
		t.spillBytes += sp.TotalBytes
	}
}

// segmentTuple is the column layout for the Array(Tuple(...)) `segments`
// column. The slice order matches the DDL: dbid, segindex, cpu_user_sec,
// cpu_kernel_sec, rss_bytes, ntuples, read_bytes, write_bytes.
func segmentTuple(s *pbm.SegmentMetrics) []any {
	var (
		dbid, segindex int32
		cpuUser        float64
		cpuKernel      float64
		rss            uint64
		ntuples        uint64
		readBytes      uint64
		writeBytes     uint64
	)
	if s.SegmentKey != nil {
		dbid = s.SegmentKey.Dbid
		segindex = s.SegmentKey.Segindex
	}
	if m := s.SegmentMetrics; m != nil {
		if sys := m.SystemStat; sys != nil {
			cpuUser = sys.UserTimeSeconds
			cpuKernel = sys.KernelTimeSeconds
			rss = sys.Rss
			readBytes = sys.ReadBytes
			writeBytes = sys.WriteBytes
		}
		if i := m.Instrumentation; i != nil {
			ntuples = i.Ntuples
		}
	}
	return []any{
		dbid, segindex,
		cpuUser, cpuKernel,
		rss, ntuples,
		readBytes, writeBytes,
	}
}

// planTreeJSON returns a JSON document with the plan information available
// in QueryInfo. v1 stores plan_text / analyze_text / generator under stable
// keys; the column is reserved for the structured node-by-node tree v2 will
// switch to. An empty QueryInfo yields "{}" (never an empty string) so the
// column always parses as JSON downstream.
func planTreeJSON(qi *pbc.QueryInfo) string {
	payload := struct {
		Generator   string `json:"generator,omitempty"`
		PlanText    string `json:"plan_text,omitempty"`
		AnalyzeText string `json:"analyze_text,omitempty"`
	}{}
	if qi != nil {
		payload.Generator = generatorToCH(qi.Generator)
		payload.PlanText = qi.PlanText
		payload.AnalyzeText = qi.AnalyzeText
		if payload.Generator == generatorUnknown {
			payload.Generator = ""
		}
	}
	b, err := json.Marshal(payload)
	if err != nil {
		// json.Marshal on a fixed struct with string fields cannot fail; fall
		// back to a literal empty object so writers never block on this.
		return "{}"
	}
	return string(b)
}

// toQueryEventRow converts a TotalQueryData snapshot into the column slice
// expected by the yagpcc.query_events INSERT statement. The slice order
// matches the column order declared in 0001_init.up.sql; reordering either
// side requires updating the other.
//
// Missing sub-messages are tolerated: a TotalQueryData with no QueryStat
// returns nil (caller drops the row), but a TotalQueryData with no QueryInfo
// or no segments produces a row with zeros / empty strings rather than an
// error.
func toQueryEventRow(qT *pbm.TotalQueryData, schemaVersion uint32, yagpccVersion string) []any {
	if qT == nil || qT.QueryStat == nil {
		return nil
	}
	qs := qT.QueryStat
	qi := qs.QueryInfo
	qk := qs.QueryKey

	submit := time.Time{}
	if qi != nil {
		submit = timestampToTime(qi.SubmitTime)
	}
	start := timestampToTime(qs.StartTime)
	end := timestampToTime(qs.EndTime)
	collect := timestampToTime(qs.CollectTime)
	if collect.IsZero() {
		// CollectTime is normally set by master to EndTime — fall back to
		// EndTime / StartTime / now in that order so event_time is never zero.
		switch {
		case !end.IsZero():
			collect = end
		case !start.IsZero():
			collect = start
		default:
			collect = time.Now().UTC()
		}
	}

	var durationMs *uint64
	if !start.IsZero() && !end.IsZero() && !end.Before(start) {
		d := uint64(end.Sub(start) / time.Millisecond)
		durationMs = &d
	}

	var (
		queryID, planID uint64
		userName        string
		databaseName    string
		rsgname         string
		queryText       string
		planText        string
		generator       = generatorUnknown
	)
	if qi != nil {
		queryID = qi.QueryId
		planID = qi.PlanId
		userName = qi.UserName
		databaseName = qi.DatabaseName
		rsgname = qi.Rsgname
		queryText = qi.QueryText
		planText = qi.PlanText
		generator = generatorToCH(qi.Generator)
	}

	var tmid, ssid, ccnt int32
	if qk != nil {
		tmid = qk.Tmid
		ssid = qk.Ssid
		ccnt = qk.Ccnt
	}

	totals := queryTotals{}
	segs := make([][]any, 0, len(qT.SegmentQueryMetrics))
	for _, sm := range qT.SegmentQueryMetrics {
		if sm == nil {
			continue
		}
		totals.addSegment(sm.SegmentMetrics)
		segs = append(segs, segmentTuple(sm))
	}

	return []any{
		collect,
		queryStatusToCH(qs.QueryStatus),
		queryID,
		planID,
		tmid,
		ssid,
		ccnt,
		nullableTime(submit),
		nullableTime(start),
		nullableTime(end),
		durationMs,
		userName,
		databaseName,
		rsgname,
		generator,
		queryText,
		planText,
		segs,
		totals.cpuUserSec,
		totals.cpuKernelSec,
		totals.runningSec,
		totals.rssBytes,
		totals.readBytes,
		totals.writeBytes,
		totals.ntuples,
		totals.sharedBlksHit,
		totals.sharedBlksRead,
		totals.blkReadTimeSec,
		totals.blkWriteTimeSec,
		totals.sentBytes,
		totals.recvBytes,
		totals.spillFiles,
		totals.spillBytes,
		planTreeJSON(qi),
		yagpccVersion,
		schemaVersion,
	}
}

// AggregatedBucket is the sink-package shape of one row about to be inserted
// into yagpcc.aggregated_metrics. The orchestrator (Task 13/14) is responsible
// for translating the master-side AggKey/AggVal pair from internal/storage
// into this struct so the writer stays decoupled from that package.
//
// Field order is intentionally NOT load-bearing (mapping is by name through
// toAggregatedRow); the column order in the produced row matches the DDL.
type AggregatedBucket struct {
	// BucketTime is the start of the aggregation interval — typically
	// AggKey.StartTime truncated to flush_interval.
	BucketTime time.Time
	// Identity tuple used for both INSERT key and SummingMergeTree ORDER BY.
	QueryID       uint64
	PlanID        uint64
	User          string
	Database      string
	ResourceGroup string
	// Executions is the number of queries folded into this bucket.
	Executions uint64
	// Summed metrics. Field semantics match the DDL columns: TotalCPUSec is
	// user+kernel CPU across all segments, TotalRunningSec is wall time spent
	// running, TotalIOBytes folds read+write together since the aggregated
	// table doesn't track them separately.
	TotalCPUSec     float64
	TotalRunningSec float64
	TotalRSSBytes   uint64
	TotalIOBytes    uint64
	TotalNTuples    uint64
	// Duration roll-ups. AvgDurationMs is the running mean (not a sum) — the
	// master's AggregatedMetrics keeps it pre-computed; the writer just
	// passes it through.
	AvgDurationMs float64
	MaxDurationMs uint64
}

// toAggregatedRow converts an AggregatedBucket into the column slice expected
// by yagpcc.aggregated_metrics. The slice order matches the DDL column order
// declared in 0001_init.up.sql; reordering either side requires updating the
// other.
//
// Returns nil when bucket is nil so callers can drop the row with a
// mapping_error counter increment instead of panicking.
func toAggregatedRow(bucket *AggregatedBucket) []any {
	if bucket == nil {
		return nil
	}
	return []any{
		bucket.BucketTime,
		bucket.QueryID,
		bucket.PlanID,
		bucket.User,
		bucket.Database,
		bucket.ResourceGroup,
		bucket.Executions,
		bucket.TotalCPUSec,
		bucket.TotalRunningSec,
		bucket.TotalRSSBytes,
		bucket.TotalIOBytes,
		bucket.TotalNTuples,
		bucket.AvgDurationMs,
		bucket.MaxDurationMs,
	}
}

// Session is the sink-package shape of a single pg_stat_activity row about
// to be inserted into yagpcc.session_snapshots. The orchestrator
// (Task 13/14) is responsible for converting from internal/gp.GpStatActivity
// into this struct so the sink package stays decoupled from internal/gp.
//
// Field semantics match the DDL columns in 0001_init.up.sql: BackendStart is
// non-nullable (the snapshot would not exist without a backend), while
// XactStart/QueryStart/StateChange map to Nullable(DateTime64) and use
// pointer types. GP6-specific note: the wait state is represented as a
// boolean rather than wait_event_type, matching pg_stat_activity.waiting.
type Session struct {
	SnapshotTime time.Time
	SessionID    int32
	PID          int32
	User         string
	Database     string
	Application  string
	ClientAddr   string
	BackendStart time.Time
	XactStart    *time.Time
	QueryStart   *time.Time
	StateChange  *time.Time
	State        string
	Waiting      bool
	Query        string
}

// toSessionRow converts a Session into the column slice expected by
// yagpcc.session_snapshots. The slice order matches the DDL column order in
// 0001_init.up.sql (14 columns); reordering either side requires updating
// the other. The boolean Waiting is encoded as UInt8 to match the table's
// column type.
//
// Returns nil when s is nil so callers can drop the row with a
// mapping_error counter increment instead of panicking.
func toSessionRow(s *Session) []any {
	if s == nil {
		return nil
	}
	var waiting uint8
	if s.Waiting {
		waiting = 1
	}
	return []any{
		s.SnapshotTime,
		s.SessionID,
		s.PID,
		s.User,
		s.Database,
		s.Application,
		s.ClientAddr,
		s.BackendStart,
		s.XactStart,
		s.QueryStart,
		s.StateChange,
		s.State,
		waiting,
		s.Query,
	}
}

// queryEventDuration returns the StartTime → EndTime span of qT, or 0 when
// either bound is missing or the times are inverted. Used by the writer's
// min_duration filter.
func queryEventDuration(qT *pbm.TotalQueryData) time.Duration {
	if qT == nil || qT.QueryStat == nil {
		return 0
	}
	start := timestampToTime(qT.QueryStat.StartTime)
	end := timestampToTime(qT.QueryStat.EndTime)
	if start.IsZero() || end.IsZero() || end.Before(start) {
		return 0
	}
	return end.Sub(start)
}
