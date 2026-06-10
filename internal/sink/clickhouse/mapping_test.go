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
	"encoding/json"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
)

func TestQueryStatusToCH(t *testing.T) {
	cases := []struct {
		in   pbc.QueryStatus
		want string
	}{
		{pbc.QueryStatus_QUERY_STATUS_UNSPECIFIED, statusUnknown},
		{pbc.QueryStatus_QUERY_STATUS_SUBMIT, statusSubmit},
		{pbc.QueryStatus_QUERY_STATUS_START, statusStart},
		{pbc.QueryStatus_QUERY_STATUS_DONE, statusDone},
		{pbc.QueryStatus_QUERY_STATUS_QUERY_DONE, statusQueryDone},
		{pbc.QueryStatus_QUERY_STATUS_ERROR, statusError},
		{pbc.QueryStatus_QUERY_STATUS_CANCELLING, statusCancelling},
		{pbc.QueryStatus_QUERY_STATUS_CANCELED, statusCanceled},
		{pbc.QueryStatus_QUERY_STATUS_END, statusEnd},
		{pbc.QueryStatus(99), statusUnknown},
	}
	for _, tc := range cases {
		if got := queryStatusToCH(tc.in); got != tc.want {
			t.Errorf("queryStatusToCH(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestGeneratorToCH(t *testing.T) {
	cases := []struct {
		in   pbc.PlanGenerator
		want string
	}{
		{pbc.PlanGenerator_PLAN_GENERATOR_UNSPECIFIED, generatorUnknown},
		{pbc.PlanGenerator_PLAN_GENERATOR_PLANNER, generatorPlanner},
		{pbc.PlanGenerator_PLAN_GENERATOR_OPTIMIZER, generatorOptimizer},
		{pbc.PlanGenerator(42), generatorUnknown},
	}
	for _, tc := range cases {
		if got := generatorToCH(tc.in); got != tc.want {
			t.Errorf("generatorToCH(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestTimestampToTime(t *testing.T) {
	if got := timestampToTime((*timestamppb.Timestamp)(nil)); !got.IsZero() {
		t.Errorf("nil timestamp -> %v, want zero", got)
	}
	now := time.Unix(1700000000, 0).UTC()
	ts := timestamppb.New(now)
	if got := timestampToTime(ts); !got.Equal(now) {
		t.Errorf("timestamp -> %v, want %v", got, now)
	}
}

func TestNullableTime(t *testing.T) {
	if got := nullableTime(time.Time{}); got != nil {
		t.Errorf("zero time -> %v, want nil", got)
	}
	now := time.Now().UTC()
	got := nullableTime(now)
	if got == nil || !got.Equal(now) {
		t.Errorf("non-zero time -> %v, want %v", got, now)
	}
}

func TestPlanTreeJSON_Empty(t *testing.T) {
	if got := planTreeJSON(nil); got != "{}" {
		t.Errorf("planTreeJSON(nil) = %q, want %q", got, "{}")
	}
	if got := planTreeJSON(&pbc.QueryInfo{}); got != "{}" {
		t.Errorf("planTreeJSON(empty) = %q, want %q", got, "{}")
	}
}

func TestPlanTreeJSON_Populated(t *testing.T) {
	qi := &pbc.QueryInfo{
		Generator:   pbc.PlanGenerator_PLAN_GENERATOR_OPTIMIZER,
		PlanText:    "Seq Scan on t",
		AnalyzeText: "Seq Scan on t (cost=...)",
	}
	got := planTreeJSON(qi)
	var decoded map[string]string
	if err := json.Unmarshal([]byte(got), &decoded); err != nil {
		t.Fatalf("invalid JSON %q: %v", got, err)
	}
	if decoded["generator"] != generatorOptimizer {
		t.Errorf("generator = %q, want %q", decoded["generator"], generatorOptimizer)
	}
	if decoded["plan_text"] != "Seq Scan on t" {
		t.Errorf("plan_text = %q", decoded["plan_text"])
	}
	if decoded["analyze_text"] != "Seq Scan on t (cost=...)" {
		t.Errorf("analyze_text = %q", decoded["analyze_text"])
	}
}

func TestQueryTotals_AddSegment_Nil(t *testing.T) {
	var totals queryTotals
	totals.addSegment(nil)
	if (totals != queryTotals{}) {
		t.Errorf("addSegment(nil) mutated totals: %+v", totals)
	}
}

func TestQueryTotals_AddSegment_Sums(t *testing.T) {
	var totals queryTotals
	a := &pbc.GPMetrics{
		SystemStat: &pbc.SystemStat{
			UserTimeSeconds:   1.5,
			KernelTimeSeconds: 0.5,
			Rss:               1024,
			ReadBytes:         100,
			WriteBytes:        200,
		},
		Instrumentation: &pbc.MetricInstrumentation{
			Ntuples:        10,
			SharedBlksHit:  3,
			SharedBlksRead: 4,
			BlkReadTime:    0.25,
			BlkWriteTime:   0.75,
			Sent:           &pbc.NetworkStat{TotalBytes: 50},
			Received:       &pbc.NetworkStat{TotalBytes: 60},
		},
		Spill: &pbc.SpillInfo{FileCount: 1, TotalBytes: 4096},
	}
	b := &pbc.GPMetrics{
		SystemStat: &pbc.SystemStat{
			UserTimeSeconds:   0.5,
			KernelTimeSeconds: 0.5,
			Rss:               2048,
			ReadBytes:         100,
			WriteBytes:        200,
		},
		Instrumentation: &pbc.MetricInstrumentation{
			Ntuples:        20,
			SharedBlksHit:  1,
			SharedBlksRead: 2,
			Sent:           &pbc.NetworkStat{TotalBytes: 5},
		},
		Spill: &pbc.SpillInfo{FileCount: 2, TotalBytes: 1024},
	}
	totals.addSegment(a)
	totals.addSegment(b)

	if totals.cpuUserSec != 2.0 {
		t.Errorf("cpuUserSec = %v, want 2.0", totals.cpuUserSec)
	}
	if totals.cpuKernelSec != 1.0 {
		t.Errorf("cpuKernelSec = %v, want 1.0", totals.cpuKernelSec)
	}
	if totals.runningSec != 3.0 {
		t.Errorf("runningSec = %v, want 3.0", totals.runningSec)
	}
	if totals.rssBytes != 2048 {
		t.Errorf("rssBytes = %v, want 2048 (max across segments)", totals.rssBytes)
	}
	if totals.readBytes != 200 {
		t.Errorf("readBytes = %v, want 200", totals.readBytes)
	}
	if totals.writeBytes != 400 {
		t.Errorf("writeBytes = %v, want 400", totals.writeBytes)
	}
	if totals.ntuples != 30 {
		t.Errorf("ntuples = %v, want 30", totals.ntuples)
	}
	if totals.sharedBlksHit != 4 {
		t.Errorf("sharedBlksHit = %v, want 4", totals.sharedBlksHit)
	}
	if totals.sharedBlksRead != 6 {
		t.Errorf("sharedBlksRead = %v, want 6", totals.sharedBlksRead)
	}
	if totals.sentBytes != 55 {
		t.Errorf("sentBytes = %v, want 55", totals.sentBytes)
	}
	if totals.recvBytes != 60 {
		t.Errorf("recvBytes = %v, want 60", totals.recvBytes)
	}
	if totals.spillFiles != 3 {
		t.Errorf("spillFiles = %v, want 3", totals.spillFiles)
	}
	if totals.spillBytes != 5120 {
		t.Errorf("spillBytes = %v, want 5120", totals.spillBytes)
	}
}

func TestSegmentTuple_Layout(t *testing.T) {
	sm := &pbm.SegmentMetrics{
		SegmentKey: &pbc.SegmentKey{Dbid: 7, Segindex: 3},
		SegmentMetrics: &pbc.GPMetrics{
			SystemStat: &pbc.SystemStat{
				UserTimeSeconds:   1.1,
				KernelTimeSeconds: 0.2,
				Rss:               9999,
				ReadBytes:         11,
				WriteBytes:        22,
			},
			Instrumentation: &pbc.MetricInstrumentation{Ntuples: 33},
		},
	}
	got := segmentTuple(sm)
	want := []any{
		int32(7), int32(3),
		1.1, 0.2,
		uint64(9999), uint64(33),
		uint64(11), uint64(22),
	}
	if len(got) != len(want) {
		t.Fatalf("tuple length = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("tuple[%d] = %v (%T), want %v (%T)", i, got[i], got[i], want[i], want[i])
		}
	}
}

func TestSegmentTuple_NilFields(t *testing.T) {
	sm := &pbm.SegmentMetrics{}
	got := segmentTuple(sm)
	if len(got) != 8 {
		t.Fatalf("tuple length = %d, want 8", len(got))
	}
	wantZero := []any{int32(0), int32(0), 0.0, 0.0, uint64(0), uint64(0), uint64(0), uint64(0)}
	for i := range wantZero {
		if got[i] != wantZero[i] {
			t.Errorf("tuple[%d] = %v, want %v", i, got[i], wantZero[i])
		}
	}
}

func TestToQueryEventRow_Nil(t *testing.T) {
	if row := toQueryEventRow(nil, 1, "v"); row != nil {
		t.Errorf("toQueryEventRow(nil) = %v, want nil", row)
	}
	if row := toQueryEventRow(&pbm.TotalQueryData{}, 1, "v"); row != nil {
		t.Errorf("toQueryEventRow{empty} = %v, want nil", row)
	}
}

func TestToQueryEventRow_Full(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	end := start.Add(2 * time.Second)
	submit := start.Add(-1 * time.Second)
	collect := end

	qT := &pbm.TotalQueryData{
		QueryStat: &pbm.QueryStat{
			QueryStatus: pbc.QueryStatus_QUERY_STATUS_END,
			QueryKey:    &pbc.QueryKey{Tmid: 7, Ssid: 11, Ccnt: 3},
			QueryInfo: &pbc.QueryInfo{
				Generator:    pbc.PlanGenerator_PLAN_GENERATOR_PLANNER,
				QueryId:      111,
				PlanId:       222,
				QueryText:    "select 1",
				PlanText:     "Result",
				UserName:     "alice",
				DatabaseName: "warehouse",
				Rsgname:      "default_group",
				SubmitTime:   timestamppb.New(submit),
			},
			StartTime:   timestamppb.New(start),
			EndTime:     timestamppb.New(end),
			CollectTime: timestamppb.New(collect),
		},
		SegmentQueryMetrics: []*pbm.SegmentMetrics{
			{
				SegmentKey: &pbc.SegmentKey{Dbid: 1, Segindex: 0},
				SegmentMetrics: &pbc.GPMetrics{
					SystemStat: &pbc.SystemStat{
						UserTimeSeconds:   1.0,
						KernelTimeSeconds: 0.5,
						Rss:               2048,
						ReadBytes:         500,
						WriteBytes:        100,
					},
					Instrumentation: &pbc.MetricInstrumentation{
						Ntuples:        5,
						SharedBlksHit:  1,
						SharedBlksRead: 2,
						Sent:           &pbc.NetworkStat{TotalBytes: 11},
						Received:       &pbc.NetworkStat{TotalBytes: 22},
					},
				},
			},
		},
	}

	row := toQueryEventRow(qT, 1, "v0.1.0")
	if row == nil {
		t.Fatal("toQueryEventRow returned nil for full input")
	}
	const expectedColumns = 36
	if len(row) != expectedColumns {
		t.Fatalf("row length = %d, want %d", len(row), expectedColumns)
	}

	// event_time
	if got, ok := row[0].(time.Time); !ok || !got.Equal(collect) {
		t.Errorf("event_time = %v (%T), want %v", row[0], row[0], collect)
	}
	// status
	if row[1] != statusEnd {
		t.Errorf("status = %v, want %s", row[1], statusEnd)
	}
	// query_id / plan_id
	if row[2] != uint64(111) || row[3] != uint64(222) {
		t.Errorf("query/plan id mismatch: %v / %v", row[2], row[3])
	}
	// tmid / session_id / command_count
	if row[4] != int32(7) || row[5] != int32(11) || row[6] != int32(3) {
		t.Errorf("identity tuple mismatch: %v %v %v", row[4], row[5], row[6])
	}
	// submit_time / start_time / end_time as *time.Time
	if got, ok := row[7].(*time.Time); !ok || got == nil || !got.Equal(submit) {
		t.Errorf("submit_time = %v, want %v", row[7], submit)
	}
	if got, ok := row[8].(*time.Time); !ok || got == nil || !got.Equal(start) {
		t.Errorf("start_time = %v, want %v", row[8], start)
	}
	if got, ok := row[9].(*time.Time); !ok || got == nil || !got.Equal(end) {
		t.Errorf("end_time = %v, want %v", row[9], end)
	}
	// duration_ms = 2000
	if got, ok := row[10].(*uint64); !ok || got == nil || *got != 2000 {
		t.Errorf("duration_ms = %v, want *2000", row[10])
	}
	if row[11] != "alice" || row[12] != "warehouse" || row[13] != "default_group" {
		t.Errorf("user/db/rsg mismatch: %v %v %v", row[11], row[12], row[13])
	}
	if row[14] != generatorPlanner {
		t.Errorf("generator = %v", row[14])
	}
	if row[15] != "select 1" || row[16] != "Result" {
		t.Errorf("template_query / template_plan: %v / %v", row[15], row[16])
	}
	segs, ok := row[17].([][]any)
	if !ok || len(segs) != 1 || len(segs[0]) != 8 {
		t.Fatalf("segments shape unexpected: %v (%T)", row[17], row[17])
	}
	// totals: cpu_user, cpu_kernel, running, rss, read, write, ntuples, hit, read_blks
	if row[18] != 1.0 || row[19] != 0.5 || row[20] != 1.5 {
		t.Errorf("cpu totals mismatch: %v %v %v", row[18], row[19], row[20])
	}
	if row[21] != uint64(2048) {
		t.Errorf("total_rss_bytes = %v", row[21])
	}
	if row[22] != uint64(500) || row[23] != uint64(100) {
		t.Errorf("total_read/write_bytes mismatch: %v %v", row[22], row[23])
	}
	if row[24] != uint64(5) {
		t.Errorf("total_ntuples = %v", row[24])
	}
	// total_sent_bytes / total_recv_bytes
	if row[29] != uint64(11) || row[30] != uint64(22) {
		t.Errorf("net totals mismatch: %v %v", row[29], row[30])
	}
	// plan_tree must be valid JSON
	planTree, ok := row[33].(string)
	if !ok || planTree == "" {
		t.Errorf("plan_tree = %v (%T)", row[33], row[33])
	}
	var decoded map[string]string
	if err := json.Unmarshal([]byte(planTree), &decoded); err != nil {
		t.Errorf("plan_tree not JSON: %v", err)
	}
	if row[34] != "v0.1.0" {
		t.Errorf("yagpcc_version = %v", row[34])
	}
	if row[35] != uint32(1) {
		t.Errorf("schema_version = %v", row[35])
	}
}

func TestToQueryEventRow_NoQueryInfo_NoSegments(t *testing.T) {
	qT := &pbm.TotalQueryData{
		QueryStat: &pbm.QueryStat{
			QueryStatus: pbc.QueryStatus_QUERY_STATUS_SUBMIT,
			QueryKey:    &pbc.QueryKey{Tmid: 1, Ssid: 2, Ccnt: 3},
		},
	}
	row := toQueryEventRow(qT, 7, "v9")
	if row == nil {
		t.Fatal("expected row for QueryStat-only input")
	}
	// duration_ms must be nil because both StartTime and EndTime are missing.
	if row[10] != (*uint64)(nil) {
		t.Errorf("duration_ms = %v, want typed nil *uint64", row[10])
	}
	if row[7] != (*time.Time)(nil) || row[8] != (*time.Time)(nil) || row[9] != (*time.Time)(nil) {
		t.Errorf("expected nil submit/start/end times, got %v %v %v", row[7], row[8], row[9])
	}
	// segments is empty slice (NOT nil) so the driver still writes Array of size 0.
	segs, ok := row[17].([][]any)
	if !ok || len(segs) != 0 {
		t.Errorf("segments = %v, want empty [][]any", row[17])
	}
	// event_time fell back to time.Now() (CollectTime/Start/End all zero).
	got, ok := row[0].(time.Time)
	if !ok || got.IsZero() {
		t.Errorf("event_time fallback = %v, want non-zero", row[0])
	}
	if row[14] != generatorUnknown {
		t.Errorf("generator = %v", row[14])
	}
	if row[33] != "{}" {
		t.Errorf("plan_tree without QueryInfo = %v, want {}", row[33])
	}
	if row[35] != uint32(7) {
		t.Errorf("schema_version = %v, want 7", row[35])
	}
}

func TestQueryEventDuration(t *testing.T) {
	if d := queryEventDuration(nil); d != 0 {
		t.Errorf("nil -> %v, want 0", d)
	}
	if d := queryEventDuration(&pbm.TotalQueryData{}); d != 0 {
		t.Errorf("empty -> %v, want 0", d)
	}
	start := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(750 * time.Millisecond)
	qT := &pbm.TotalQueryData{
		QueryStat: &pbm.QueryStat{
			StartTime: timestamppb.New(start),
			EndTime:   timestamppb.New(end),
		},
	}
	if d := queryEventDuration(qT); d != 750*time.Millisecond {
		t.Errorf("duration = %v, want 750ms", d)
	}
	// inverted -> 0 (defensive)
	qT.QueryStat.EndTime = timestamppb.New(start.Add(-1 * time.Second))
	if d := queryEventDuration(qT); d != 0 {
		t.Errorf("inverted = %v, want 0", d)
	}
}

func TestToAggregatedRow_NilReturnsNil(t *testing.T) {
	if got := toAggregatedRow(nil); got != nil {
		t.Errorf("toAggregatedRow(nil) = %v, want nil", got)
	}
}

func TestToAggregatedRow_FullBucket(t *testing.T) {
	bucketTime := time.Date(2026, 5, 2, 12, 30, 0, 0, time.UTC)
	b := &AggregatedBucket{
		BucketTime:      bucketTime,
		QueryID:         42,
		PlanID:          7,
		User:            "analytics",
		Database:        "warehouse",
		ResourceGroup:   "rsg_etl",
		Executions:      9,
		TotalCPUSec:     1.25,
		TotalRunningSec: 5.5,
		TotalRSSBytes:   1 << 20,
		TotalIOBytes:    2 << 20,
		TotalNTuples:    10000,
		AvgDurationMs:   123.45,
		MaxDurationMs:   500,
	}
	row := toAggregatedRow(b)
	// 14 columns in yagpcc.aggregated_metrics — see 0001_init.up.sql.
	if len(row) != 14 {
		t.Fatalf("toAggregatedRow length = %d, want 14", len(row))
	}
	want := []any{
		bucketTime,
		uint64(42), uint64(7),
		"analytics", "warehouse", "rsg_etl",
		uint64(9),
		1.25, 5.5,
		uint64(1 << 20), uint64(2 << 20), uint64(10000),
		123.45, uint64(500),
	}
	for i, v := range want {
		if row[i] != v {
			t.Errorf("row[%d] = %v (%T), want %v (%T)", i, row[i], row[i], v, v)
		}
	}
}

func TestToAggregatedRow_ZeroBucket(t *testing.T) {
	// Zero bucket should still produce a 14-column row with zero values, not
	// a nil — only nil pointer means "drop".
	row := toAggregatedRow(&AggregatedBucket{})
	if len(row) != 14 {
		t.Fatalf("toAggregatedRow zero length = %d, want 14", len(row))
	}
	if !row[0].(time.Time).IsZero() {
		t.Errorf("zero bucket time = %v, want zero", row[0])
	}
}

func TestToSessionRow_NilReturnsNil(t *testing.T) {
	if got := toSessionRow(nil); got != nil {
		t.Errorf("toSessionRow(nil) = %v, want nil", got)
	}
}

func TestToSessionRow_FullSession(t *testing.T) {
	snap := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	backend := snap.Add(-2 * time.Hour)
	xact := snap.Add(-30 * time.Second)
	query := snap.Add(-15 * time.Second)
	stateChange := snap.Add(-5 * time.Second)
	s := &Session{
		SnapshotTime: snap,
		SessionID:    42,
		PID:          12345,
		User:         "alice",
		Database:     "warehouse",
		Application:  "psql",
		ClientAddr:   "10.0.0.1",
		BackendStart: backend,
		XactStart:    &xact,
		QueryStart:   &query,
		StateChange:  &stateChange,
		State:        "active",
		Waiting:      true,
		Query:        "SELECT 1",
	}
	row := toSessionRow(s)
	if len(row) != 14 {
		t.Fatalf("toSessionRow length = %d, want 14", len(row))
	}
	want := []any{
		snap,
		int32(42),
		int32(12345),
		"alice",
		"warehouse",
		"psql",
		"10.0.0.1",
		backend,
		&xact,
		&query,
		&stateChange,
		"active",
		uint8(1),
		"SELECT 1",
	}
	for i, v := range want {
		if !sessionRowFieldEqual(row[i], v) {
			t.Errorf("row[%d] = %v (%T), want %v (%T)", i, row[i], row[i], v, v)
		}
	}
}

func TestToSessionRow_NotWaitingEncodesZero(t *testing.T) {
	s := &Session{Waiting: false}
	row := toSessionRow(s)
	if row[12] != uint8(0) {
		t.Errorf("waiting (false) -> %v (%T), want uint8(0)", row[12], row[12])
	}
}

func TestToSessionRow_NullableTimesPreservedAsNil(t *testing.T) {
	s := &Session{
		SnapshotTime: time.Now().UTC(),
		SessionID:    1,
		PID:          2,
		BackendStart: time.Now().UTC(),
	}
	row := toSessionRow(s)
	if row[8] != (*time.Time)(nil) {
		t.Errorf("xact_start = %v (%T), want typed nil *time.Time", row[8], row[8])
	}
	if row[9] != (*time.Time)(nil) {
		t.Errorf("query_start = %v (%T), want typed nil *time.Time", row[9], row[9])
	}
	if row[10] != (*time.Time)(nil) {
		t.Errorf("state_change = %v (%T), want typed nil *time.Time", row[10], row[10])
	}
}

// sessionRowFieldEqual compares two row fields with pointer-equality fallback
// for *time.Time so the test does not fail on identical-but-distinct values.
func sessionRowFieldEqual(got, want any) bool {
	if gp, gok := got.(*time.Time); gok {
		wp, wok := want.(*time.Time)
		if !wok {
			return false
		}
		if gp == nil || wp == nil {
			return gp == nil && wp == nil
		}
		return gp.Equal(*wp)
	}
	return got == want
}
