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

package clickhouse

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
)

// Destination table names (without the yagpcc database qualifier). These match
// the DDL in migrations/0001_init.up.sql and the file writer stream fan-out:
// sessions -> sessions_part, queries -> statements_part, segments ->
// segments_part.
const (
	TableSessions   = "sessions_part"
	TableStatements = "statements_part"
	TableSegments   = "segments_part"
)

// CDC bookkeeping columns present on every destination table. On a direct
// insert (no Kafka/Transfer in front) yagpcc fills them itself; see CDCMeta.
const (
	colTimestamp = "_timestamp"
	colPartition = "_partition"
	colOffset    = "_offset"
	colIdx       = "_idx"
	colRest      = "_rest"
)

// CDCMeta carries the change-data-capture columns for a directly inserted row.
// Partition is "direct", Offset is 0 and Idx is the row's position in its
// batch; Timestamp is the insertion time.
type CDCMeta struct {
	Timestamp time.Time
	Partition string
	Offset    uint64
	Idx       uint32
}

// valKind is the Go representation a JSON value is coerced into before it is
// appended to a ClickHouse batch. Nullable columns carry the same kind plus the
// nullable flag; an absent source yields SQL NULL (a nil interface value).
type valKind int

const (
	kindString valKind = iota
	kindBool
	kindU64
	kindU32
	kindI64
	kindI32
	kindF64
	kindDateTime
	kindStatusEnum // numeric QueryStatus -> proto enum name string
)

// column binds a destination column to a path inside the source JSON document.
// Each path element lists candidate keys because the session stream serialises
// proto messages with encoding/json (Go snake_case tags) while the query and
// segment streams use protojson (camelCase), so the same leaf can appear under
// either spelling.
type column struct {
	name     string
	kind     valKind
	nullable bool
	path     [][]string
}

// tableMapping is the ordered set of JSON-sourced columns for one destination
// table. The CDC columns and _rest are added by ColumnNames/BuildRow, so they
// are not listed here.
type tableMapping struct {
	table   string
	columns []column
}

// Mapping is the exported alias for a table's JSON→column mapping. It lets
// callers in other packages (the master ClickHouse writer) hold the *Mapping
// values returned by SessionsMapping/StatementsMapping/SegmentsMapping.
type Mapping = tableMapping

// Table returns the destination table name (without the database qualifier).
func (tm *tableMapping) Table() string { return tm.table }

// ColumnNames returns the full column list, in insert order: the four CDC
// columns, then the JSON-mapped columns, then _rest. It matches the order in
// which BuildRow emits values, so callers can build an explicit-column INSERT.
func (tm *tableMapping) ColumnNames() []string {
	names := make([]string, 0, len(tm.columns)+5)
	names = append(names, colTimestamp, colPartition, colOffset, colIdx)
	for i := range tm.columns {
		names = append(names, tm.columns[i].name)
	}
	names = append(names, colRest)
	return names
}

// BuildRow maps one parsed JSON document to a row aligned with ColumnNames.
// Values not consumed by any column are serialised into _rest; when everything
// is consumed _rest is NULL. A coercion failure aborts the row with an error so
// the caller can drop it and account a mapping_error.
func (tm *tableMapping) BuildRow(root map[string]any, meta CDCMeta) ([]any, error) {
	rest := deepCloneMap(root)
	values := make([]any, 0, len(tm.columns)+5)
	values = append(values, meta.Timestamp, meta.Partition, meta.Offset, meta.Idx)

	for i := range tm.columns {
		c := &tm.columns[i]
		raw, present := takePath(rest, c.path)
		v, err := coerce(raw, present, c)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", c.name, err)
		}
		values = append(values, v)
	}

	pruneEmpty(rest)
	if len(rest) == 0 {
		values = append(values, nil)
	} else {
		b, err := json.Marshal(rest)
		if err != nil {
			return nil, fmt.Errorf("marshal _rest: %w", err)
		}
		values = append(values, string(b))
	}
	return values, nil
}

// MapRow parses one JSON document and delegates to BuildRow. It is the entry
// point used by the ClickHouse writer per row.
func MapRow(tm *tableMapping, data []byte, meta CDCMeta) ([]any, error) {
	var root map[string]any
	dec := json.NewDecoder(bytes.NewReader(data))
	// UseNumber keeps integers as json.Number so 64-bit ids (query_id/plan_id)
	// from the encoding/json session stream survive without float64 rounding.
	dec.UseNumber()
	if err := dec.Decode(&root); err != nil {
		return nil, fmt.Errorf("unmarshal source json: %w", err)
	}
	return tm.BuildRow(root, meta)
}

// takePath descends m following path (each level matched against candidate
// keys), returns the leaf value and whether it was found, and removes the leaf
// from m so that whatever remains can be serialised into _rest.
func takePath(m map[string]any, path [][]string) (any, bool) {
	cur := m
	for depth, aliases := range path {
		key, ok := firstKey(cur, aliases)
		if !ok {
			return nil, false
		}
		if depth == len(path)-1 {
			v := cur[key]
			delete(cur, key)
			return v, true
		}
		next, ok := cur[key].(map[string]any)
		if !ok {
			return nil, false
		}
		cur = next
	}
	return nil, false
}

func firstKey(m map[string]any, aliases []string) (string, bool) {
	for _, k := range aliases {
		if _, ok := m[k]; ok {
			return k, true
		}
	}
	return "", false
}

func deepCloneMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = cloneValue(v)
	}
	return out
}

func cloneValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		return deepCloneMap(t)
	case []any:
		s := make([]any, len(t))
		for i, e := range t {
			s[i] = cloneValue(e)
		}
		return s
	default:
		return v
	}
}

// pruneEmpty drops nested objects that became empty after their mapped leaves
// were removed, so _rest does not carry bare {} placeholders.
func pruneEmpty(m map[string]any) {
	for k, v := range m {
		if sub, ok := v.(map[string]any); ok {
			pruneEmpty(sub)
			if len(sub) == 0 {
				delete(m, k)
			}
		}
	}
}

// coerce converts a raw JSON value into the Go type ClickHouse expects for the
// column. Absent (or JSON null) yields NULL for nullable columns and the zero
// value otherwise. Numbers arrive as json.Number (UseNumber) or as decimal
// strings (protojson emits 64-bit ints as strings); a non-integer or
// out-of-range value for an integer column is a mapping error.
func coerce(raw any, present bool, c *column) (any, error) {
	if !present || raw == nil {
		return zeroFor(c), nil
	}
	switch c.kind {
	case kindString:
		return toString(raw)
	case kindStatusEnum:
		n, err := toInt32(raw)
		if err != nil {
			return nil, err
		}
		return pbc.QueryStatus(n).String(), nil
	case kindBool:
		return toBool(raw)
	case kindU64:
		n, err := toUint64(raw)
		if err != nil {
			// A negative value in a nullable unsigned column (e.g. ClientPort=-1) means "absent".
			if c.nullable && isNegativeInt(raw) {
				return nil, nil
			}
			return nil, err
		}
		return n, nil
	case kindU32:
		n, err := toUint32(raw)
		if err != nil {
			if c.nullable && isNegativeInt(raw) {
				return nil, nil
			}
			return nil, err
		}
		return n, nil
	case kindI64:
		return toInt64(raw)
	case kindI32:
		return toInt32(raw)
	case kindF64:
		return toFloat64(raw)
	case kindDateTime:
		t, ok, err := toTime(raw)
		if err != nil {
			return nil, err
		}
		if !ok {
			return zeroFor(c), nil
		}
		return t, nil
	default:
		return nil, fmt.Errorf("unknown kind %d", c.kind)
	}
}

func zeroFor(c *column) any {
	if c.nullable {
		return nil
	}
	switch c.kind {
	case kindString, kindStatusEnum:
		return ""
	case kindBool:
		return false
	case kindU64:
		return uint64(0)
	case kindU32:
		return uint32(0)
	case kindI64:
		return int64(0)
	case kindI32:
		return int32(0)
	case kindF64:
		return float64(0)
	case kindDateTime:
		return time.Time{}
	default:
		return nil
	}
}

func toString(raw any) (string, error) {
	switch v := raw.(type) {
	case string:
		return v, nil
	case bool:
		return strconv.FormatBool(v), nil
	case json.Number:
		return v.String(), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	default:
		return "", fmt.Errorf("cannot convert %T to string", raw)
	}
}

func toBool(raw any) (bool, error) {
	switch v := raw.(type) {
	case bool:
		return v, nil
	case string:
		return strconv.ParseBool(v)
	default:
		return false, fmt.Errorf("cannot convert %T to bool", raw)
	}
}

func toUint64(raw any) (uint64, error) {
	switch v := raw.(type) {
	case float64:
		// Defensive: MapRow decodes with UseNumber, so floats should not reach here.
		if v < 0 || v != math.Trunc(v) || v >= 1<<64 {
			return 0, fmt.Errorf("cannot convert %v to uint64", v)
		}
		return uint64(v), nil
	case json.Number:
		return uint64FromString(v.String())
	case string:
		return uint64FromString(v)
	default:
		return 0, fmt.Errorf("cannot convert %T to uint64", raw)
	}
}

// Producers emit integers as digit strings (protojson) or integer literals
// (encoding/json), so anything ParseUint rejects is corrupt data.
func uint64FromString(v string) (uint64, error) {
	if v == "" {
		return 0, nil
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("cannot convert %q to uint64", v)
	}
	return n, nil
}

func toUint32(raw any) (uint32, error) {
	n, err := toUint64(raw)
	if err != nil {
		return 0, err
	}
	if n > math.MaxUint32 {
		return 0, fmt.Errorf("value %d overflows uint32", n)
	}
	return uint32(n), nil
}

func toInt64(raw any) (int64, error) {
	switch v := raw.(type) {
	case float64:
		// Defensive: MapRow decodes with UseNumber, so floats should not reach here.
		if v != math.Trunc(v) || v < math.MinInt64 || v >= 1<<63 {
			return 0, fmt.Errorf("cannot convert %v to int64", v)
		}
		return int64(v), nil
	case json.Number:
		return int64FromString(v.String())
	case string:
		return int64FromString(v)
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", raw)
	}
}

func int64FromString(v string) (int64, error) {
	if v == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("cannot convert %q to int64", v)
	}
	return n, nil
}

func isNegativeInt(raw any) bool {
	n, err := toInt64(raw)
	return err == nil && n < 0
}

func toInt32(raw any) (int32, error) {
	n, err := toInt64(raw)
	if err != nil {
		return 0, err
	}
	if n < math.MinInt32 || n > math.MaxInt32 {
		return 0, fmt.Errorf("value %d overflows int32", n)
	}
	return int32(n), nil
}

func toFloat64(raw any) (float64, error) {
	switch v := raw.(type) {
	case float64:
		return v, nil
	case json.Number:
		return v.Float64()
	case string:
		if v == "" {
			return 0, nil
		}
		return strconv.ParseFloat(v, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", raw)
	}
}

// timeLayouts covers every timestamp spelling the streams emit: UTCTime for
// session collect_time, protojson RFC3339(Nano) for proto timestamps, and the
// offset-only layout produced by utils.GetTimeAsString.
var timeLayouts = []string{
	"2006-01-02T15:04:05.000000000-07:00",
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02T15:04:05-07:00",
}

// toTime parses a timestamp string; an empty string means "no timestamp"
// (ok=false) so the caller can store NULL.
func toTime(raw any) (time.Time, bool, error) {
	s, ok := raw.(string)
	if !ok {
		return time.Time{}, false, fmt.Errorf("cannot convert %T to time", raw)
	}
	if s == "" {
		return time.Time{}, false, nil
	}
	for _, layout := range timeLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), true, nil
		}
	}
	return time.Time{}, false, fmt.Errorf("cannot parse time %q", s)
}

func keys(a ...string) []string {
	seen := make(map[string]bool, len(a))
	out := make([]string, 0, len(a))
	for _, k := range a {
		if !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	return out
}

// metricLeaf describes one GPMetrics scalar shared by the total/last/query
// column blocks: the destination column suffix, its kind, and the camel/snake
// key aliases used by protojson and encoding/json respectively.
type metricLeaf struct {
	suffix       string
	kind         valKind
	camel, snake string
}

var systemStatLeaves = []metricLeaf{
	{"running_seconds", kindF64, "runningTimeSeconds", "runningTimeSeconds"},
	{"user_seconds", kindF64, "userTimeSeconds", "userTimeSeconds"},
	{"kernel_seconds", kindF64, "kernelTimeSeconds", "kernelTimeSeconds"},
	{"vsize", kindU64, "vsize", "vsize"},
	{"rss", kindU64, "rss", "rss"},
	{"vmsizekb", kindU64, "VmSizeKb", "VmSizeKb"},
	{"vmpeakkb", kindU64, "VmPeakKb", "VmPeakKb"},
	{"rchar", kindU64, "rchar", "rchar"},
	{"wchar", kindU64, "wchar", "wchar"},
	{"syscr", kindU64, "syscr", "syscr"},
	{"syscw", kindU64, "syscw", "syscw"},
	{"readbytes", kindU64, "readBytes", "read_bytes"},
	{"writebytes", kindU64, "writeBytes", "write_bytes"},
	{"cancelledwritebytes", kindU64, "cancelledWriteBytes", "cancelled_write_bytes"},
}

var instrumentationLeaves = []metricLeaf{
	{"ntuples", kindU64, "ntuples", "ntuples"},
	{"nloops", kindU64, "nloops", "nloops"},
	{"tuplecount", kindU64, "tuplecount", "tuplecount"},
	{"firsttuple", kindF64, "firsttuple", "firsttuple"},
	{"startup", kindF64, "startup", "startup"},
	{"totalcost", kindF64, "total", "total"},
	{"shared_blks_hit", kindU64, "sharedBlksHit", "shared_blks_hit"},
	{"shared_blks_read", kindU64, "sharedBlksRead", "shared_blks_read"},
	{"shared_blks_dirtied", kindU64, "sharedBlksDirtied", "shared_blks_dirtied"},
	{"shared_blks_written", kindU64, "sharedBlksWritten", "shared_blks_written"},
	{"local_blks_hit", kindU64, "localBlksHit", "local_blks_hit"},
	{"local_blks_read", kindU64, "localBlksRead", "local_blks_read"},
	{"local_blks_dirtied", kindU64, "localBlksDirtied", "local_blks_dirtied"},
	{"local_blks_written", kindU64, "localBlksWritten", "local_blks_written"},
	{"temp_blks_read", kindU64, "tempBlksRead", "temp_blks_read"},
	{"temp_blks_written", kindU64, "tempBlksWritten", "temp_blks_written"},
	{"blk_read_time", kindF64, "blkReadTime", "blk_read_time"},
	{"blk_write_time", kindF64, "blkWriteTime", "blk_write_time"},
}

var networkLeaves = []metricLeaf{
	{"totalbytes", kindU64, "totalBytes", "total_bytes"},
	{"tuplebytes", kindU64, "tupleBytes", "tuple_bytes"},
	{"chunks", kindU64, "chunks", "chunks"},
}

var interconnectLeaves = []metricLeaf{
	{"active_connections_num", 0, "activeConnectionsNum", "active_connections_num"},
	{"buffer_counting_time", 0, "bufferCountingTime", "buffer_counting_time"},
	{"capacity_counting_time", 0, "capacityCountingTime", "capacity_counting_time"},
	{"crc_errors", 0, "crcErrors", "crc_errors"},
	{"disordered_pkt_num", 0, "disorderedPktNum", "disordered_pkt_num"},
	{"duplicated_pkt_num", 0, "duplicatedPktNum", "duplicated_pkt_num"},
	{"mismatch_num", 0, "mismatchNum", "mismatch_num"},
	{"recv_ack_num", 0, "recvAckNum", "recv_ack_num"},
	{"recv_pkt_num", 0, "recvPktNum", "recv_pkt_num"},
	{"recv_queue_size_counting_time", 0, "recvQueueSizeCountingTime", "recv_queue_size_counting_time"},
	{"retransmits", 0, "retransmits", "retransmits"},
	{"snd_pkt_num", 0, "sndPktNum", "snd_pkt_num"},
	{"startup_cached_pkt_num", 0, "startupCachedPktNum", "startup_cached_pkt_num"},
	{"status_query_msg_num", 0, "statusQueryMsgNum", "status_query_msg_num"},
	{"total_buffers", 0, "totalBuffers", "total_buffers"},
	{"total_capacity", 0, "totalCapacity", "total_capacity"},
	{"total_recv_queue_size", 0, "totalRecvQueueSize", "total_recv_queue_size"},
}

// gpMetricColumns builds the metric columns rooted at a GPMetrics message.
// prefix is the column-name prefix ("total_"/"last_"/"query_"); root is the top
// key holding the message; icKind is the integer kind for the interconnect and
// inherited_calls counters (Int64 in sessions, UInt64 in statements/segments);
// spillBytesCol is the (prefix-specific) spill-bytes column name; and
// withStartupTime adds the *_startup_time column where the table has one.
func gpMetricColumns(prefix string, root []string, icKind valKind, spillBytesCol string, withStartupTime bool) []column {
	cols := make([]column, 0, 80)
	add := func(name string, kind valKind, path ...[]string) {
		cols = append(cols, column{name: name, kind: kind, nullable: true, path: append([][]string{root}, path...)})
	}

	for _, l := range systemStatLeaves {
		add(prefix+l.suffix, l.kind, keys("systemStat"), keys(l.camel, l.snake))
	}
	for _, l := range instrumentationLeaves {
		add(prefix+l.suffix, l.kind, keys("instrumentation"), keys(l.camel, l.snake))
	}
	add(prefix+"spill_filecount", kindU32, keys("spill"), keys("fileCount"))
	add(spillBytesCol, kindU64, keys("spill"), keys("totalBytes"))
	if withStartupTime {
		add(prefix+"startup_time", kindF64, keys("instrumentation"), keys("startupTime", "startup_time"))
	}
	for _, l := range networkLeaves {
		add(prefix+"sent_"+l.suffix, l.kind, keys("instrumentation"), keys("sent"), keys(l.camel, l.snake))
	}
	for _, l := range networkLeaves {
		add(prefix+"receive_"+l.suffix, l.kind, keys("instrumentation"), keys("received"), keys(l.camel, l.snake))
	}
	add(prefix+"inherited_time", kindF64, keys("instrumentation"), keys("inheritedTime", "inherited_time"))
	add(prefix+"inherited_calls", icKind, keys("instrumentation"), keys("inheritedCalls", "inherited_calls"))
	for _, l := range interconnectLeaves {
		add(prefix+"interconnect_"+l.suffix, icKind, keys("instrumentation"), keys("interconnect"), keys(l.camel, l.snake))
	}
	return cols
}

// col is a small constructor for a JSON-mapped column; each element of path is
// a set of key aliases tried in order.
func col(name string, kind valKind, nullable bool, path ...[]string) column {
	return column{name: name, kind: kind, nullable: nullable, path: path}
}

// SessionsMapping maps the SessionDataWrite JSON stream onto sessions_part.
func SessionsMapping() *tableMapping {
	cols := []column{
		col("cluster_id", kindString, false, keys("ClusterID")),
		col("collect_time", kindDateTime, false, keys("CollectTime")),
		col("hostname", kindString, false, keys("Hostname")),
		col("sess_id", kindU64, false, keys("GpStatInfo"), keys("SessID")),
		col("tm_id", kindU64, false, keys("GpStatInfo"), keys("TmID")),
		col("dat_id", kindU64, true, keys("GpStatInfo"), keys("DatID")),
		col("database", kindString, true, keys("GpStatInfo"), keys("Datname")),
		col("pid", kindU32, true, keys("GpStatInfo"), keys("Pid")),
		col("user_id", kindU64, true, keys("GpStatInfo"), keys("UsesysID")),
		col("user", kindString, true, keys("GpStatInfo"), keys("Usename")),
		col("application_name", kindString, true, keys("GpStatInfo"), keys("ApplicationName")),
		col("client_addr", kindString, true, keys("GpStatInfo"), keys("ClientAddr")),
		col("client_hostname", kindString, true, keys("GpStatInfo"), keys("ClientHostname")),
		col("client_port", kindU32, true, keys("GpStatInfo"), keys("ClientPort")),
		col("backend_start", kindDateTime, true, keys("GpStatInfo"), keys("BackendStart")),
		col("xact_start", kindDateTime, true, keys("GpStatInfo"), keys("XactStart")),
		col("query_start", kindDateTime, true, keys("GpStatInfo"), keys("QueryStart")),
		col("state_change", kindDateTime, true, keys("GpStatInfo"), keys("StateChange")),
		col("waiting", kindBool, false, keys("GpStatInfo"), keys("Waiting")),
		col("state", kindString, true, keys("GpStatInfo"), keys("State")),
		col("backend_xid", kindString, true, keys("GpStatInfo"), keys("BackendXid")),
		col("backend_xmin", kindString, true, keys("GpStatInfo"), keys("BackendXmin")),
		col("query", kindString, true, keys("GpStatInfo"), keys("Query")),
		col("waiting_reason", kindString, true, keys("GpStatInfo"), keys("WaitingReason")),
		col("rsgid", kindU64, true, keys("GpStatInfo"), keys("Rsgid")),
		col("rsgname", kindString, true, keys("GpStatInfo"), keys("Rsgname")),
		col("rsgqueueduration", kindString, true, keys("GpStatInfo"), keys("Rsgqueueduration")),
		col("blocked_by_sess_id", kindU64, true, keys("GpStatInfo"), keys("BlockedBySessID")),
		col("wait_mode", kindString, true, keys("GpStatInfo"), keys("WaitMode")),
		col("locked_item", kindString, true, keys("GpStatInfo"), keys("LockedItem")),
		col("locked_mode", kindString, true, keys("GpStatInfo"), keys("LockedMode")),
		col("query_status", kindStatusEnum, true, keys("RunningQueryStatus")),
		col("query_sess_id", kindU64, true, keys("RunningQuery"), keys("Ssid")),
		col("query_tm_id", kindU64, true, keys("RunningQuery"), keys("Tmid")),
		col("query_ccnt", kindU64, true, keys("RunningQuery"), keys("Ccnt")),
		col("query_id", kindU64, true, keys("RunningQueryInfo"), keys("QueryID")),
		col("plan_id", kindU64, true, keys("RunningQueryInfo"), keys("PlanID")),
		col("query_text", kindString, true, keys("RunningQueryInfo"), keys("QueryText")),
		col("plan_text", kindString, true, keys("RunningQueryInfo"), keys("PlanText")),
	}
	cols = append(cols, gpMetricColumns("total_", keys("TotalGPMetrics"), kindI64, "total_spill_totalbytes", true)...)
	cols = append(cols, gpMetricColumns("last_", keys("LongRunningGPMetrics"), kindI64, "last_spill_lastbytes", true)...)
	cols = append(cols, gpMetricColumns("query_", keys("QueryGPMetrics"), kindI64, "query_spill_querybytes", true)...)
	cols = append(cols,
		col("running_query_level", kindI64, true, keys("RunningQueryLevel")),
		col("running_query_slices", kindI64, true, keys("RunningQuerySlices")),
	)
	return &tableMapping{table: TableSessions, columns: cols}
}

// StatementsMapping maps the QueryStatWrite JSON stream onto statements_part.
func StatementsMapping() *tableMapping {
	cols := []column{
		col("cluster_id", kindString, false, keys("clusterId", "cluster_id")),
		col("collect_time", kindDateTime, false, keys("collectTime", "collect_time")),
		col("hostname", kindString, false, keys("hostname")),
		col("sess_id", kindU64, false, keys("queryKey", "query_key"), keys("ssid")),
		col("tm_id", kindU64, false, keys("queryKey", "query_key"), keys("tmid")),
		col("ccnt", kindU64, false, keys("queryKey", "query_key"), keys("ccnt")),
		col("generator", kindString, true, keys("queryInfo", "query_info"), keys("generator")),
		col("query_id", kindU64, false, keys("queryInfo", "query_info"), keys("queryId", "query_id")),
		col("plan_id", kindU64, false, keys("queryInfo", "query_info"), keys("planId", "plan_id")),
		col("query_text", kindString, true, keys("queryInfo", "query_info"), keys("queryText", "query_text")),
		col("plan_text", kindString, true, keys("queryInfo", "query_info"), keys("planText", "plan_text")),
		col("template_query_text", kindString, true, keys("queryInfo", "query_info"), keys("templateQueryText", "template_query_text")),
		col("template_plan_text", kindString, true, keys("queryInfo", "query_info"), keys("templatePlanText", "template_plan_text")),
		col("user_name", kindString, false, keys("queryInfo", "query_info"), keys("userName")),
		col("database_name", kindString, false, keys("queryInfo", "query_info"), keys("databaseName")),
		col("rsgname", kindString, true, keys("queryInfo", "query_info"), keys("rsgname")),
		col("stat_kind", kindString, true, keys("statKind", "stat_kind")),
		col("query_status", kindString, true, keys("queryStatus", "query_status")),
		col("start_time", kindDateTime, true, keys("startTime", "start_time")),
		col("end_time", kindDateTime, true, keys("endTime", "end_time")),
		col("completed", kindBool, true, keys("completed")),
		col("blocked_by_sess_id", kindU64, true, keys("blockedBySessId", "blocked_by_sess_id")),
		col("wait_mode", kindString, true, keys("waitMode", "wait_mode")),
		col("locked_item", kindString, true, keys("lockedItem", "locked_item")),
		col("locked_mode", kindString, true, keys("lockedMode", "locked_mode")),
		col("session_state", kindString, true, keys("sessionState", "session_state")),
	}
	cols = append(cols, gpMetricColumns("total_", keys("totalQueryMetrics", "total_query_metrics"), kindU64, "total_spill_totalbytes", true)...)
	agg := keys("aggregatedMetrics", "aggregated_metrics")
	cols = append(cols,
		col("calls", kindI64, true, agg, keys("calls")),
		col("min_time", kindF64, true, agg, keys("minTime", "min_time")),
		col("max_time", kindF64, true, agg, keys("maxTime", "max_time")),
		col("mean_time", kindF64, true, agg, keys("meanTime", "mean_time")),
		col("stddev_time", kindF64, true, agg, keys("stddevTime", "stddev_time")),
		col("total_time", kindF64, true, agg, keys("totalTime", "total_time")),
		col("query_slices", kindU64, true, keys("slices")),
	)
	return &tableMapping{table: TableStatements, columns: cols}
}

// SegmentsMapping maps the SegmentMetricsWrite JSON stream onto segments_part.
func SegmentsMapping() *tableMapping {
	cols := []column{
		col("cluster_id", kindString, false, keys("clusterId", "cluster_id")),
		col("collect_time", kindDateTime, false, keys("collectTime", "collect_time")),
		col("hostname", kindString, false, keys("hostname")),
		col("sess_id", kindU64, false, keys("queryKey", "query_key"), keys("ssid")),
		col("tm_id", kindU64, false, keys("queryKey", "query_key"), keys("tmid")),
		col("ccnt", kindU64, false, keys("queryKey", "query_key"), keys("ccnt")),
		col("dbid", kindI32, false, keys("segmentKey", "segment_key"), keys("dbid")),
		col("segindex", kindI32, false, keys("segmentKey", "segment_key"), keys("segindex")),
		col("generator", kindString, true, keys("queryInfo", "query_info"), keys("generator")),
		col("query_id", kindU64, true, keys("queryInfo", "query_info"), keys("queryId", "query_id")),
		col("plan_id", kindU64, true, keys("queryInfo", "query_info"), keys("planId", "plan_id")),
		col("query_text", kindString, true, keys("queryInfo", "query_info"), keys("queryText", "query_text")),
		col("plan_text", kindString, true, keys("queryInfo", "query_info"), keys("planText", "plan_text")),
		col("template_query_text", kindString, true, keys("queryInfo", "query_info"), keys("templateQueryText", "template_query_text")),
		col("template_plan_text", kindString, true, keys("queryInfo", "query_info"), keys("templatePlanText", "template_plan_text")),
		col("user_name", kindString, true, keys("queryInfo", "query_info"), keys("userName")),
		col("database_name", kindString, true, keys("queryInfo", "query_info"), keys("databaseName")),
		col("rsgname", kindString, true, keys("queryInfo", "query_info"), keys("rsgname")),
		col("query_status", kindString, true, keys("queryStatus", "query_status")),
		col("start_time", kindDateTime, true, keys("startTime", "start_time")),
		col("end_time", kindDateTime, true, keys("endTime", "end_time")),
	}
	cols = append(cols, gpMetricColumns("total_", keys("segmentMetrics", "segment_metrics"), kindU64, "total_spill_totalbytes", false)...)
	return &tableMapping{table: TableSegments, columns: cols}
}
