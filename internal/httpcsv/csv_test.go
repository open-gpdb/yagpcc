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

package httpcsv

import (
	"bytes"
	"encoding/csv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
)

func TestSessionStateHeaders(t *testing.T) {
	headers := sessionStateHeaders()
	assert.NotEmpty(t, headers)
	// Verify some key headers exist
	assert.Equal(t, "time", headers[0])
	assert.Equal(t, "cluster_id", headers[1])
	assert.Equal(t, "hostname", headers[2])
	assert.Equal(t, "session_key.sess_id", headers[3])
	assert.Equal(t, "session_key.tm_id", headers[4])
}

func TestGpMetricsFieldsNil(t *testing.T) {
	fields := gpMetricsFields(nil)
	assert.Len(t, fields, 42)
	for _, f := range fields {
		assert.Empty(t, f)
	}
}

func TestGpMetricsFieldsFull(t *testing.T) {
	m := &pbc.GPMetrics{
		SystemStat: &pbc.SystemStat{
			RunningTimeSeconds:  1.5,
			UserTimeSeconds:     1.0,
			KernelTimeSeconds:   0.5,
			Vsize:               1024,
			Rss:                 512,
			VmPeakKb:            2048,
			Rchar:               100,
			Wchar:               200,
			Syscr:               10,
			Syscw:               20,
			ReadBytes:           300,
			WriteBytes:          400,
			CancelledWriteBytes: 50,
		},
		Instrumentation: &pbc.MetricInstrumentation{
			Ntuples:           1000,
			Nloops:            5,
			Tuplecount:        500,
			Firsttuple:        0.1,
			Startup:           0.2,
			Total:             1.5,
			SharedBlksHit:     10,
			SharedBlksRead:    20,
			SharedBlksDirtied: 5,
			SharedBlksWritten: 3,
			LocalBlksHit:      1,
			LocalBlksRead:     2,
			LocalBlksDirtied:  0,
			LocalBlksWritten:  0,
			TempBlksRead:      0,
			TempBlksWritten:   0,
			BlkReadTime:       0.01,
			BlkWriteTime:      0.02,
			StartupTime:       0.05,
			InheritedCalls:    2,
			InheritedTime:     0.3,
			Sent: &pbc.NetworkStat{
				TotalBytes: 1000,
				TupleBytes: 800,
				Chunks:     10,
			},
			Received: &pbc.NetworkStat{
				TotalBytes: 2000,
				TupleBytes: 1600,
				Chunks:     20,
			},
		},
		Spill: &pbc.SpillInfo{
			FileCount:  3,
			TotalBytes: 4096,
		},
	}

	fields := gpMetricsFields(m)
	assert.Len(t, fields, 42)

	// Check system stat fields
	assert.Equal(t, "1.5", fields[0])  // RunningTimeSeconds
	assert.Equal(t, "1", fields[1])    // UserTimeSeconds
	assert.Equal(t, "0.5", fields[2])  // KernelTimeSeconds
	assert.Equal(t, "1024", fields[3]) // Vsize
	assert.Equal(t, "512", fields[4])  // Rss
	assert.Equal(t, "2048", fields[5]) // VmPeakKb

	// Check instrumentation fields
	assert.Equal(t, "1000", fields[13]) // Ntuples
	assert.Equal(t, "5", fields[14])    // Nloops

	// Check network sent
	assert.Equal(t, "1000", fields[34]) // Sent.TotalBytes
	assert.Equal(t, "800", fields[35])  // Sent.TupleBytes
	assert.Equal(t, "10", fields[36])   // Sent.Chunks

	// Check network received
	assert.Equal(t, "2000", fields[37]) // Received.TotalBytes
	assert.Equal(t, "1600", fields[38]) // Received.TupleBytes
	assert.Equal(t, "20", fields[39])   // Received.Chunks

	// Check spill
	assert.Equal(t, "3", fields[40])    // FileCount
	assert.Equal(t, "4096", fields[41]) // TotalBytes
}

func TestGpMetricsFieldsPartial(t *testing.T) {
	// Only system stat, no instrumentation or spill
	m := &pbc.GPMetrics{
		SystemStat: &pbc.SystemStat{
			RunningTimeSeconds: 2.0,
		},
	}

	fields := gpMetricsFields(m)
	assert.Len(t, fields, 42)
	assert.Equal(t, "2", fields[0])
	// Instrumentation fields should be empty
	for i := 13; i < 40; i++ {
		assert.Empty(t, fields[i], "field %d should be empty", i)
	}
	// Spill fields should be empty
	assert.Empty(t, fields[40])
	assert.Empty(t, fields[41])
}

func TestSessionStateToRecord(t *testing.T) {
	ts := timestamppb.Now()
	s := &pbc.SessionState{
		Time:      ts,
		ClusterId: "test-cluster",
		Hostname:  "master-host",
		SessionKey: &pbc.SessionKey{
			SessId: 42,
			TmId:   100,
		},
		SessionInfo: &pbc.SessionInfo{
			Pid:             1234,
			Database:        "testdb",
			User:            "testuser",
			ApplicationName: "testapp",
			ClientAddr:      "127.0.0.1",
			ClientHostname:  "localhost",
			ClientPort:      5432,
			BackendStart:    ts,
			State:           "active",
			Rsgname:         "default_group",
		},
		RunningQuery: &pbc.QueryKey{
			Tmid: 100,
			Ssid: 42,
			Ccnt: 1,
		},
		RunningQueryInfo: &pbc.QueryInfo{
			Generator:    pbc.PlanGenerator_PLAN_GENERATOR_OPTIMIZER,
			QueryId:      12345,
			PlanId:       67890,
			QueryText:    "SELECT 1",
			UserName:     "testuser",
			DatabaseName: "testdb",
			Rsgname:      "default_group",
		},
		RunningQueryStatus: pbc.QueryStatus_QUERY_STATUS_START,
		TotalMetrics: &pbc.GPMetrics{
			SystemStat: &pbc.SystemStat{
				RunningTimeSeconds: 10.5,
			},
		},
		RunningQueryLevel:  0,
		RunningQuerySlices: 3,
		AggregatedMetrics: &pbc.AggregatedMetrics{
			Calls:      5,
			MinTime:    0.1,
			MaxTime:    10.0,
			MeanTime:   2.5,
			StddevTime: 1.2,
			TotalTime:  12.5,
		},
	}

	record := sessionStateToRecord(s)
	headers := sessionStateHeaders()
	assert.Equal(t, len(headers), len(record), "record length should match headers length")

	// Check specific fields
	assert.Equal(t, "test-cluster", record[1])
	assert.Equal(t, "master-host", record[2])
	assert.Equal(t, "42", record[3])   // sess_id
	assert.Equal(t, "100", record[4])  // tm_id
	assert.Equal(t, "1234", record[5]) // pid
	assert.Equal(t, "testdb", record[6])
	assert.Equal(t, "testuser", record[7])
	assert.Equal(t, "testapp", record[8])
}

func TestSessionStateToRecordNilFields(t *testing.T) {
	s := &pbc.SessionState{
		ClusterId: "test",
		Hostname:  "host",
	}

	record := sessionStateToRecord(s)
	headers := sessionStateHeaders()
	assert.Equal(t, len(headers), len(record), "record length should match headers length even with nil fields")
}

func TestWriteSessionsCSV(t *testing.T) {
	sessions := []*pbc.SessionState{
		{
			ClusterId: "cluster1",
			Hostname:  "host1",
			SessionKey: &pbc.SessionKey{
				SessId: 1,
				TmId:   100,
			},
			SessionInfo: &pbc.SessionInfo{
				Pid:      10,
				Database: "db1",
				User:     "user1",
				State:    "active",
			},
		},
		{
			ClusterId: "cluster1",
			Hostname:  "host1",
			SessionKey: &pbc.SessionKey{
				SessId: 2,
				TmId:   100,
			},
			SessionInfo: &pbc.SessionInfo{
				Pid:      20,
				Database: "db2",
				User:     "user2",
				State:    "idle",
			},
		},
	}

	var buf bytes.Buffer
	err := WriteSessionsCSV(&buf, sessions)
	require.NoError(t, err)

	// Parse the CSV output
	reader := csv.NewReader(&buf)
	records, err := reader.ReadAll()
	require.NoError(t, err)

	// Header + 2 data rows
	assert.Len(t, records, 3)

	// Check header
	assert.Equal(t, "time", records[0][0])
	assert.Equal(t, "cluster_id", records[0][1])

	// Check first data row
	assert.Equal(t, "cluster1", records[1][1])
	assert.Equal(t, "host1", records[1][2])
	assert.Equal(t, "1", records[1][3])

	// Check second data row
	assert.Equal(t, "cluster1", records[2][1])
	assert.Equal(t, "2", records[2][3])
}

func TestWriteSessionsCSVEmpty(t *testing.T) {
	var buf bytes.Buffer
	err := WriteSessionsCSV(&buf, nil)
	require.NoError(t, err)

	reader := csv.NewReader(&buf)
	records, err := reader.ReadAll()
	require.NoError(t, err)

	// Only header row
	assert.Len(t, records, 1)
}

func TestWriteTotalSessionsStatCSV(t *testing.T) {
	stats := []*pbm.SessionStat{
		{State: "active", Count: 5},
		{State: "idle", Count: 10},
		{State: "idle in transaction", Count: 2},
	}

	var buf bytes.Buffer
	err := WriteTotalSessionsStatCSV(&buf, stats)
	require.NoError(t, err)

	reader := csv.NewReader(&buf)
	records, err := reader.ReadAll()
	require.NoError(t, err)

	// Header + 3 data rows
	assert.Len(t, records, 4)
	assert.Equal(t, "state", records[0][0])
	assert.Equal(t, "count", records[0][1])
	assert.Equal(t, "active", records[1][0])
	assert.Equal(t, "5", records[1][1])
	assert.Equal(t, "idle", records[2][0])
	assert.Equal(t, "10", records[2][1])
}

func TestWriteQueryDataCSV(t *testing.T) {
	data := &pbm.TotalQueryData{
		QueryStat: &pbm.QueryStat{
			ClusterId: "cluster1",
			Hostname:  "host1",
			QueryKey: &pbc.QueryKey{
				Tmid: 100,
				Ssid: 42,
				Ccnt: 1,
			},
			QueryInfo: &pbc.QueryInfo{
				Generator:    pbc.PlanGenerator_PLAN_GENERATOR_PLANNER,
				QueryId:      12345,
				QueryText:    "SELECT * FROM t",
				UserName:     "user1",
				DatabaseName: "db1",
			},
			StatKind:    pbm.StatKind_SK_PRECISE,
			QueryStatus: pbc.QueryStatus_QUERY_STATUS_DONE,
			Completed:   true,
			Slices:      5,
		},
	}

	var buf bytes.Buffer
	err := WriteQueryDataCSV(&buf, data)
	require.NoError(t, err)

	// Parse with FieldsPerRecord=-1 to allow variable column counts across sections
	reader := csv.NewReader(&buf)
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	require.NoError(t, err)

	// query_stat header + query_stat row + segment_metrics header = 3 rows
	// (no segment rows since data has none)
	assert.Len(t, records, 3)
	assert.Equal(t, "query_stat.cluster_id", records[0][0])
	assert.Equal(t, "cluster1", records[1][0])
	assert.Equal(t, "host1", records[1][1])
	// Segment metrics header row
	assert.Equal(t, "segment_metrics.cluster_id", records[2][0])
}

func TestWriteQueryDataCSVWithSegments(t *testing.T) {
	ts := timestamppb.Now()
	data := &pbm.TotalQueryData{
		QueryStat: &pbm.QueryStat{
			ClusterId: "cluster1",
			Hostname:  "host1",
		},
		SegmentQueryMetrics: []*pbm.SegmentMetrics{
			{
				ClusterId: "cluster1",
				Hostname:  "seg-host-0",
				SegmentKey: &pbc.SegmentKey{
					Dbid:     2,
					Segindex: 0,
				},
				QueryStatus: pbc.QueryStatus_QUERY_STATUS_DONE,
				StartTime:   ts,
				SegmentMetrics: &pbc.GPMetrics{
					SystemStat: &pbc.SystemStat{
						RunningTimeSeconds: 1.5,
					},
				},
			},
			{
				ClusterId: "cluster1",
				Hostname:  "seg-host-1",
				SegmentKey: &pbc.SegmentKey{
					Dbid:     3,
					Segindex: 1,
				},
				QueryStatus: pbc.QueryStatus_QUERY_STATUS_DONE,
			},
		},
	}

	var buf bytes.Buffer
	err := WriteQueryDataCSV(&buf, data)
	require.NoError(t, err)

	reader := csv.NewReader(&buf)
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	require.NoError(t, err)

	// query_stat header + query_stat row + segment_metrics header + 2 segment rows = 5
	assert.Len(t, records, 5)

	// Verify query_stat section
	assert.Equal(t, "query_stat.cluster_id", records[0][0])
	assert.Equal(t, "cluster1", records[1][0])

	// Verify segment_metrics section header
	assert.Equal(t, "segment_metrics.cluster_id", records[2][0])
	assert.Equal(t, "segment_metrics.segment_key.dbid", records[2][3])

	// Verify first segment row
	assert.Equal(t, "cluster1", records[3][0])
	assert.Equal(t, "seg-host-0", records[3][1])
	assert.Equal(t, "2", records[3][3]) // dbid
	assert.Equal(t, "0", records[3][4]) // segindex

	// Verify second segment row
	assert.Equal(t, "seg-host-1", records[4][1])
	assert.Equal(t, "3", records[4][3]) // dbid
	assert.Equal(t, "1", records[4][4]) // segindex
}

func TestWriteQueryDataCSVNil(t *testing.T) {
	var buf bytes.Buffer
	err := WriteQueryDataCSV(&buf, nil)
	require.NoError(t, err)

	reader := csv.NewReader(&buf)
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	require.NoError(t, err)

	// query_stat header + segment_metrics header = 2 rows (no data rows since data is nil)
	assert.Len(t, records, 2)
	assert.Equal(t, "query_stat.cluster_id", records[0][0])
	assert.Equal(t, "segment_metrics.cluster_id", records[1][0])
}

func TestQueryDataHeadersLength(t *testing.T) {
	headers := queryDataHeaders()
	// 25 query stat fields + 42 metrics fields + 6 aggregated metrics = 73
	assert.Len(t, headers, 73)
}

func TestSegmentMetricsHeadersLength(t *testing.T) {
	headers := segmentMetricsHeaders()
	// 8 scalar/key fields + 42 GPMetrics fields = 50
	assert.Len(t, headers, 50)
}

func TestSegmentMetricsToRecordNil(t *testing.T) {
	record := segmentMetricsToRecord(nil)
	assert.Len(t, record, 50)
}

func TestQueryStatToRecordNil(t *testing.T) {
	record := queryStatToRecord(nil)
	assert.Len(t, record, 73)
}

func TestSessionStateRecordConsistency(t *testing.T) {
	// Ensure that records with all nil sub-messages have the same length as headers
	s := &pbc.SessionState{}
	record := sessionStateToRecord(s)
	headers := sessionStateHeaders()
	assert.Equal(t, len(headers), len(record),
		"empty SessionState record length (%d) must match headers length (%d)",
		len(record), len(headers))
}

func TestCSVSpecialCharacters(t *testing.T) {
	// Test that CSV properly escapes special characters in query text
	sessions := []*pbc.SessionState{
		{
			ClusterId: "cluster1",
			Hostname:  "host1",
			SessionInfo: &pbc.SessionInfo{
				Database: "db with, comma",
				User:     "user\"with\"quotes",
				State:    "active",
			},
			RunningQueryInfo: &pbc.QueryInfo{
				QueryText: "SELECT * FROM t WHERE name = 'hello\nworld'",
			},
		},
	}

	var buf bytes.Buffer
	err := WriteSessionsCSV(&buf, sessions)
	require.NoError(t, err)

	// Parse back — CSV library should handle escaping
	reader := csv.NewReader(&buf)
	records, err := reader.ReadAll()
	require.NoError(t, err)

	assert.Len(t, records, 2) // header + 1 row
	// Find the database field (index 6 in SessionInfo)
	assert.Equal(t, "db with, comma", records[1][6])
	assert.Equal(t, "user\"with\"quotes", records[1][7])
}

func TestExtensionsHeaders(t *testing.T) {
	headers := extensionsHeaders()
	assert.NotEmpty(t, headers)
	// Verify the headers match what we expect
	assert.Equal(t, "database_name", headers[0])
	assert.Equal(t, "ext_name", headers[1])
	assert.Equal(t, "ext_owner", headers[2])
	assert.Equal(t, "ext_namespace", headers[3])
	assert.Equal(t, "ext_relocatable", headers[4])
	assert.Equal(t, "ext_version", headers[5])
	assert.Equal(t, "error", headers[6])
	assert.Len(t, headers, 7)
}

func TestWriteExtensionsCSV(t *testing.T) {
	// Test with a database that has extensions
	databases := []*pbm.DatabaseExtensionsInfo{
		{
			DatabaseName: "testdb1",
			Extensions: []*pbm.Extension{
				{
					Name:        "plpgsql",
					Owner:       "postgres",
					Namespace:   "pg_catalog",
					Relocatable: false,
					Version:     "1.0",
				},
				{
					Name:        "uuid-ossp",
					Owner:       "postgres",
					Namespace:   "public",
					Relocatable: true,
					Version:     "1.1",
				},
			},
			Error: "",
		},
		// Test with a database that has no extensions but no error
		{
			DatabaseName: "emptydb",
			Extensions:   []*pbm.Extension{},
			Error:        "",
		},
		// Test with a database that has an error
		{
			DatabaseName: "errordb",
			Extensions:   []*pbm.Extension{},
			Error:        "connection failed",
		},
	}

	var buf bytes.Buffer
	err := WriteExtensionsCSV(&buf, databases)
	require.NoError(t, err)

	reader := csv.NewReader(&buf)
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	require.NoError(t, err)

	// We should have:
	// 1 header row
	// 2 rows for the extensions in testdb1
	// 1 row for emptydb (no extensions, no error)
	// 1 row for errordb (no extensions, but with error)
	assert.Len(t, records, 5)

	// Check header row
	assert.Equal(t, "database_name", records[0][0])
	assert.Equal(t, "ext_name", records[0][1])
	assert.Equal(t, "ext_owner", records[0][2])
	assert.Equal(t, "ext_namespace", records[0][3])
	assert.Equal(t, "ext_relocatable", records[0][4])
	assert.Equal(t, "ext_version", records[0][5])
	assert.Equal(t, "error", records[0][6])

	// Check first extension row
	assert.Equal(t, "testdb1", records[1][0])
	assert.Equal(t, "plpgsql", records[1][1])
	assert.Equal(t, "postgres", records[1][2])
	assert.Equal(t, "pg_catalog", records[1][3])
	assert.Equal(t, "false", records[1][4])
	assert.Equal(t, "1.0", records[1][5])
	assert.Equal(t, "", records[1][6]) // error field from database

	// Check second extension row
	assert.Equal(t, "testdb1", records[2][0])
	assert.Equal(t, "uuid-ossp", records[2][1])
	assert.Equal(t, "postgres", records[2][2])
	assert.Equal(t, "public", records[2][3])
	assert.Equal(t, "true", records[2][4])
	assert.Equal(t, "1.1", records[2][5])
	assert.Equal(t, "", records[2][6]) // error field from database

	// Check empty database row
	assert.Equal(t, "emptydb", records[3][0])
	assert.Equal(t, "", records[3][1]) // ext_name
	assert.Equal(t, "", records[3][2]) // ext_owner
	assert.Equal(t, "", records[3][3]) // ext_namespace
	assert.Equal(t, "", records[3][4]) // ext_relocatable
	assert.Equal(t, "", records[3][5]) // ext_version
	assert.Equal(t, "", records[3][6]) // error

	// Check error database row
	assert.Equal(t, "errordb", records[4][0])
	assert.Equal(t, "", records[4][1])                  // ext_name
	assert.Equal(t, "", records[4][2])                  // ext_owner
	assert.Equal(t, "", records[4][3])                  // ext_namespace
	assert.Equal(t, "", records[4][4])                  // ext_relocatable
	assert.Equal(t, "", records[4][5])                  // ext_version
	assert.Equal(t, "connection failed", records[4][6]) // error
}

func TestWriteExtensionsCSVNil(t *testing.T) {
	var buf bytes.Buffer
	err := WriteExtensionsCSV(&buf, nil)
	require.NoError(t, err)

	reader := csv.NewReader(&buf)
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	require.NoError(t, err)

	// Only header row
	assert.Len(t, records, 1)
	assert.Equal(t, "database_name", records[0][0])
	assert.Equal(t, "ext_name", records[0][1])
	assert.Equal(t, "ext_owner", records[0][2])
	assert.Equal(t, "ext_namespace", records[0][3])
	assert.Equal(t, "ext_relocatable", records[0][4])
	assert.Equal(t, "ext_version", records[0][5])
	assert.Equal(t, "error", records[0][6])
}
