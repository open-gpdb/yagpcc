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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
)

var testMeta = CDCMeta{
	Timestamp: time.Date(2026, 7, 16, 10, 0, 0, 0, time.UTC),
	Partition: "direct",
	Offset:    0,
	Idx:       5,
}

// rowMap zips ColumnNames with a mapped row so tests can assert by column name.
func rowMap(t *testing.T, tm *tableMapping, data []byte) map[string]any {
	t.Helper()
	values, err := MapRow(tm, data, testMeta)
	require.NoError(t, err)
	names := tm.ColumnNames()
	require.Len(t, values, len(names))
	out := make(map[string]any, len(names))
	for i, n := range names {
		out[n] = values[i]
	}
	return out
}

func restMap(t *testing.T, row map[string]any) map[string]any {
	t.Helper()
	raw, ok := row[colRest].(string)
	require.True(t, ok, "_rest must be a non-nil JSON string, got %T", row[colRest])
	var out map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &out))
	return out
}

const sessionGolden = `{
  "CollectTime": "2025-02-21T16:07:02.000000000+00:00",
  "ClusterID": "fxd",
  "Hostname": "host1",
  "RunningQueryStatus": 3,
  "GpStatInfo": {
    "SessID": 123, "TmID": 12345, "DatID": 1, "Datname": "db1",
    "Pid": 7, "UsesysID": 10, "Usename": "u1",
    "Waiting": true, "State": "active",
    "BackendStart": "2025-02-21T16:00:00Z",
    "WaitEvent": "LWLockNamed", "WaitEventType": "Lock"
  },
  "RunningQuery": {"Ssid": 123, "Tmid": 12345, "Ccnt": 1},
  "RunningQueryInfo": {"QueryID": 321, "PlanID": 111, "QueryText": "select 1", "PlanText": "plan"},
  "TotalGPMetrics": {
    "systemStat": {"runningTimeSeconds": 2.5, "rss": 1024, "read_bytes": 4096},
    "instrumentation": {
      "ntuples": 5, "shared_blks_hit": 9, "blk_read_time": 1.5,
      "sent": {"total_bytes": 100, "chunks": 3},
      "inherited_calls": 7,
      "interconnect": {"retransmits": 4, "recv_pkt_num": 8}
    },
    "spill": {"fileCount": 2, "totalBytes": 8192}
  },
  "LongRunningGPMetrics": {},
  "QueryGPMetrics": {},
  "AggregatedMetrics": {"calls": 7, "min_time": 100},
  "RunningQueryLevel": 0,
  "RunningQuerySlices": 10
}`

func TestSessionsMapping_Golden(t *testing.T) {
	row := rowMap(t, SessionsMapping(), []byte(sessionGolden))

	assert.Equal(t, testMeta.Timestamp, row[colTimestamp])
	assert.Equal(t, "direct", row[colPartition])
	assert.Equal(t, uint64(0), row[colOffset])
	assert.Equal(t, uint32(5), row[colIdx])

	assert.Equal(t, "fxd", row["cluster_id"])
	assert.Equal(t, time.Date(2025, 2, 21, 16, 7, 2, 0, time.UTC), row["collect_time"])
	assert.Equal(t, "host1", row["hostname"])
	assert.Equal(t, uint64(123), row["sess_id"])
	assert.Equal(t, uint64(12345), row["tm_id"])
	assert.Equal(t, uint64(1), row["dat_id"])
	assert.Equal(t, "db1", row["database"])
	assert.Equal(t, uint32(7), row["pid"])
	assert.Equal(t, uint64(10), row["user_id"])
	assert.Equal(t, "u1", row["user"])
	assert.Equal(t, true, row["waiting"])
	assert.Equal(t, "active", row["state"])
	assert.Equal(t, time.Date(2025, 2, 21, 16, 0, 0, 0, time.UTC), row["backend_start"])
	assert.Equal(t, pbc.QueryStatus(3).String(), row["query_status"])

	assert.Equal(t, uint64(123), row["query_sess_id"])
	assert.Equal(t, uint64(12345), row["query_tm_id"])
	assert.Equal(t, uint64(1), row["query_ccnt"])
	assert.Equal(t, uint64(321), row["query_id"])
	assert.Equal(t, uint64(111), row["plan_id"])
	assert.Equal(t, "select 1", row["query_text"])
	assert.Equal(t, "plan", row["plan_text"])

	// total_* metric block (icKind Int64 for sessions).
	assert.Equal(t, 2.5, row["total_running_seconds"])
	assert.Equal(t, uint64(1024), row["total_rss"])
	assert.Equal(t, uint64(4096), row["total_readbytes"])
	assert.Equal(t, uint64(5), row["total_ntuples"])
	assert.Equal(t, uint64(9), row["total_shared_blks_hit"])
	assert.Equal(t, 1.5, row["total_blk_read_time"])
	assert.Equal(t, uint64(100), row["total_sent_totalbytes"])
	assert.Equal(t, uint64(3), row["total_sent_chunks"])
	assert.Equal(t, int64(7), row["total_inherited_calls"])
	assert.Equal(t, int64(4), row["total_interconnect_retransmits"])
	assert.Equal(t, int64(8), row["total_interconnect_recv_pkt_num"])
	assert.Equal(t, uint32(2), row["total_spill_filecount"])
	assert.Equal(t, uint64(8192), row["total_spill_totalbytes"])
	assert.Equal(t, int64(10), row["running_query_slices"])

	// Absent optional fields become SQL NULL.
	assert.Nil(t, row["application_name"])
	assert.Nil(t, row["client_addr"])
	// last_/query_ blocks have no source in this fixture.
	assert.Nil(t, row["last_running_seconds"])
	assert.Nil(t, row["query_ntuples"])

	// Unmapped fields land in _rest.
	rest := restMap(t, row)
	gpsi, ok := rest["GpStatInfo"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "LWLockNamed", gpsi["WaitEvent"])
	assert.Equal(t, "Lock", gpsi["WaitEventType"])
	assert.Contains(t, rest, "AggregatedMetrics")
	// Fully consumed objects are pruned from _rest.
	assert.NotContains(t, rest, "TotalGPMetrics")
	assert.NotContains(t, rest, "LongRunningGPMetrics")
	assert.NotContains(t, rest, "RunningQueryStatus")
}

const statementGolden = `{
  "clusterId": "c1",
  "hostname": "h1",
  "collectTime": "2025-02-21T16:07:02+00:00",
  "queryKey": {"ssid": 1, "tmid": 123, "ccnt": 2},
  "queryInfo": {
    "generator": "PLAN_GENERATOR_OPTIMIZER",
    "queryId": "12", "planId": "321",
    "queryText": "select 1", "planText": "plan",
    "templateQueryText": "SELECT $1", "templatePlanText": "TPLAN",
    "userName": "bob", "databaseName": "db", "rsgname": "rg",
    "analyzeText": "", "submitTime": null, "startTime": null, "endTime": null
  },
  "statKind": "SK_PRECISE",
  "queryStatus": "QUERY_STATUS_DONE",
  "startTime": "",
  "endTime": "",
  "completed": true,
  "blockedBySessId": "0",
  "waitMode": "", "lockedItem": "", "lockedMode": "", "sessionState": "",
  "message": "hello",
  "slices": "10",
  "totalQueryMetrics": {
    "systemStat": {"runningTimeSeconds": 4, "readBytes": "2048"},
    "instrumentation": {
      "ntuples": "5", "sharedBlksHit": "9", "inheritedCalls": "10",
      "interconnect": {"retransmits": "10"}
    },
    "spill": {"fileCount": 1, "totalBytes": "512"}
  },
  "aggregatedMetrics": {
    "calls": "3", "minTime": 1, "maxTime": 4, "meanTime": 2, "stddevTime": 0.5, "totalTime": 6
  }
}`

func TestStatementsMapping_Golden(t *testing.T) {
	row := rowMap(t, StatementsMapping(), []byte(statementGolden))

	assert.Equal(t, "c1", row["cluster_id"])
	assert.Equal(t, time.Date(2025, 2, 21, 16, 7, 2, 0, time.UTC), row["collect_time"])
	assert.Equal(t, uint64(1), row["sess_id"])
	assert.Equal(t, uint64(123), row["tm_id"])
	assert.Equal(t, uint64(2), row["ccnt"])
	assert.Equal(t, "PLAN_GENERATOR_OPTIMIZER", row["generator"])
	assert.Equal(t, uint64(12), row["query_id"])
	assert.Equal(t, uint64(321), row["plan_id"])
	assert.Equal(t, "select 1", row["query_text"])
	assert.Equal(t, "SELECT $1", row["template_query_text"])
	assert.Equal(t, "TPLAN", row["template_plan_text"])
	assert.Equal(t, "bob", row["user_name"])
	assert.Equal(t, "db", row["database_name"])
	assert.Equal(t, "rg", row["rsgname"])
	assert.Equal(t, "SK_PRECISE", row["stat_kind"])
	assert.Equal(t, "QUERY_STATUS_DONE", row["query_status"])
	assert.Equal(t, true, row["completed"])
	assert.Equal(t, uint64(0), row["blocked_by_sess_id"])
	// Empty top-level timestamps map to NULL.
	assert.Nil(t, row["start_time"])
	assert.Nil(t, row["end_time"])

	// total_* block, protojson string-encoded integers coerced.
	assert.Equal(t, float64(4), row["total_running_seconds"])
	assert.Equal(t, uint64(2048), row["total_readbytes"])
	assert.Equal(t, uint64(5), row["total_ntuples"])
	assert.Equal(t, uint64(9), row["total_shared_blks_hit"])
	assert.Equal(t, uint64(10), row["total_inherited_calls"]) // icKind UInt64 here
	assert.Equal(t, uint64(10), row["total_interconnect_retransmits"])
	assert.Equal(t, uint32(1), row["total_spill_filecount"])
	assert.Equal(t, uint64(512), row["total_spill_totalbytes"])

	// aggregated metrics.
	assert.Equal(t, int64(3), row["calls"])
	assert.Equal(t, float64(1), row["min_time"])
	assert.Equal(t, float64(6), row["total_time"])
	assert.Equal(t, uint64(10), row["query_slices"])

	// message + analyzeText are unmapped and land in _rest.
	rest := restMap(t, row)
	assert.Equal(t, "hello", rest["message"])
	qi, ok := rest["queryInfo"].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, qi, "analyzeText")
}

const segmentGolden = `{
  "clusterId": "c1",
  "hostname": "h1",
  "collectTime": "2025-02-21T16:07:02+00:00",
  "queryKey": {"ssid": 1, "tmid": 123, "ccnt": 0},
  "segmentKey": {"dbid": 2, "segindex": 4},
  "queryInfo": {
    "queryId": "77", "planId": "88", "queryText": "select 2",
    "templateQueryText": "SELECT $1", "userName": "u", "databaseName": "d",
    "generator": "PLAN_GENERATOR_PLANNER"
  },
  "queryStatus": "QUERY_STATUS_DONE",
  "startTime": "", "endTime": "",
  "segmentMetrics": {
    "systemStat": {"runningTimeSeconds": 3, "rss": "64"},
    "instrumentation": {"ntuples": "11", "startup": 0.25}
  }
}`

func TestSegmentsMapping_Golden(t *testing.T) {
	row := rowMap(t, SegmentsMapping(), []byte(segmentGolden))

	assert.Equal(t, "c1", row["cluster_id"])
	assert.Equal(t, uint64(1), row["sess_id"])
	assert.Equal(t, uint64(0), row["ccnt"])
	assert.Equal(t, int32(2), row["dbid"])
	assert.Equal(t, int32(4), row["segindex"])
	assert.Equal(t, "PLAN_GENERATOR_PLANNER", row["generator"])
	assert.Equal(t, uint64(77), row["query_id"])
	assert.Equal(t, uint64(88), row["plan_id"])
	assert.Equal(t, "select 2", row["query_text"])
	assert.Equal(t, "SELECT $1", row["template_query_text"])
	assert.Equal(t, "u", row["user_name"])
	assert.Equal(t, "QUERY_STATUS_DONE", row["query_status"])

	assert.Equal(t, float64(3), row["total_running_seconds"])
	assert.Equal(t, uint64(64), row["total_rss"])
	assert.Equal(t, uint64(11), row["total_ntuples"])
	assert.Equal(t, 0.25, row["total_startup"])

	// segments_part has no *_startup_time column.
	names := SegmentsMapping().ColumnNames()
	assert.NotContains(t, names, "total_startup_time")
	// node_status has no JSON source, so it is not in the insert column list.
	assert.NotContains(t, names, "node_status")
}

func TestMapping_EmptyMessage(t *testing.T) {
	// An empty document must not error; required (non-null) columns get zero
	// values and every optional column is NULL, _rest is NULL.
	row := rowMap(t, StatementsMapping(), []byte(`{}`))
	assert.Equal(t, "", row["cluster_id"])
	assert.Equal(t, uint64(0), row["sess_id"])
	assert.Equal(t, time.Time{}, row["collect_time"])
	assert.Nil(t, row["query_text"])
	assert.Nil(t, row["total_ntuples"])
	assert.Nil(t, row[colRest])
}

func TestMapping_LargeUint64PreservesPrecision(t *testing.T) {
	// GPDB query/plan ids are 64-bit hashes that routinely exceed 2^53. The
	// session stream serialises them via encoding/json as bare JSON numbers, so
	// the mapper must decode them without float64 rounding.
	const bigID uint64 = 18446744073709551615 // math.MaxUint64
	doc := `{"RunningQueryInfo":{"QueryID":18446744073709551615,"PlanID":9007199254740993}}`
	row := rowMap(t, SessionsMapping(), []byte(doc))
	assert.Equal(t, bigID, row["query_id"])
	assert.Equal(t, uint64(9007199254740993), row["plan_id"])
}

func TestMapping_UnknownFieldGoesToRest(t *testing.T) {
	row := rowMap(t, SessionsMapping(), []byte(`{"ClusterID":"z","SomethingNew":42}`))
	assert.Equal(t, "z", row["cluster_id"])
	rest := restMap(t, row)
	assert.Equal(t, float64(42), rest["SomethingNew"])
	assert.NotContains(t, rest, "ClusterID")
}

func TestMapping_ColumnNamesShape(t *testing.T) {
	for _, tm := range []*tableMapping{SessionsMapping(), StatementsMapping(), SegmentsMapping()} {
		names := tm.ColumnNames()
		require.GreaterOrEqual(t, len(names), 6)
		assert.Equal(t, []string{colTimestamp, colPartition, colOffset, colIdx}, names[:4])
		assert.Equal(t, colRest, names[len(names)-1])
	}
}

func TestMapping_TemplateTextsPreserved(t *testing.T) {
	// Guards the #47 fix: template_query_text / template_plan_text must survive
	// the JSON round-trip into their columns.
	stmt := rowMap(t, StatementsMapping(), []byte(statementGolden))
	assert.Equal(t, "SELECT $1", stmt["template_query_text"])
	assert.Equal(t, "TPLAN", stmt["template_plan_text"])
	seg := rowMap(t, SegmentsMapping(), []byte(segmentGolden))
	assert.Equal(t, "SELECT $1", seg["template_query_text"])
}
