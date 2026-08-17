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

// integration_test.go drives the JSON→column mapping and the standalone
// production DDL against a real ClickHouse server, exercising the same
// PrepareBatch/Append/Send path the master ClickHouse writer uses. It is gated
// behind the `integration` build tag and the YAGPCC_CH_ADDR env var so the
// default `go test` run stays hermetic.
//
// Run locally against a throwaway server:
//
//	docker run --rm -d -p 9000:9000 clickhouse/clickhouse-server:24.3
//	YAGPCC_CH_ADDR=127.0.0.1:9000 go test -tags=integration -count=1 \
//	    -run TestClickHouseIntegration ./internal/sink/clickhouse/...
//
//go:build integration

package clickhouse

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/open-gpdb/yagpcc/internal/config"
)

const integrationDatabase = "yagpcc"

// openIntegrationConn returns a live driver.Conn or skips the test when
// YAGPCC_CH_ADDR is unset. CI provides the address through a ClickHouse service
// container; developers export it after `docker run`.
func openIntegrationConn(ctx context.Context, t *testing.T) driver.Conn {
	t.Helper()
	addr := os.Getenv("YAGPCC_CH_ADDR")
	if addr == "" {
		t.Skip("set YAGPCC_CH_ADDR (host:port) to run the ClickHouse integration test")
	}
	cfg := &config.ClickhouseConfig{
		Addrs:       []string{addr},
		Database:    "default",
		User:        "default",
		DialTimeout: 5 * time.Second,
		ReadTimeout: 30 * time.Second,
	}
	conn, err := NewClient(ctx, cfg)
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	if err := Ping(ctx, conn); err != nil {
		_ = conn.Close()
		t.Fatalf("ping clickhouse: %v", err)
	}
	return conn
}

// insertSQL renders the explicit-column INSERT header for tm, mirroring the
// master writer so appended values line up regardless of physical column order.
func insertSQL(tm *tableMapping) string {
	return fmt.Sprintf("INSERT INTO %s.%s (%s)",
		integrationDatabase, tm.Table(), strings.Join(tm.ColumnNames(), ", "))
}

// insertJSON maps each JSON document through tm and sends the whole slice as a
// single native batch, exactly as ClickHouseWriters.insertBatch does.
func insertJSON(ctx context.Context, t *testing.T, conn driver.Conn, tm *tableMapping, docs []string) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, insertSQL(tm))
	if err != nil {
		t.Fatalf("prepare batch for %s: %v", tm.Table(), err)
	}
	for i, doc := range docs {
		meta := CDCMeta{Timestamp: time.Now().UTC(), Partition: "direct", Offset: 0, Idx: uint32(i)}
		row, err := MapRow(tm, []byte(doc), meta)
		if err != nil {
			t.Fatalf("map row %d for %s: %v", i, tm.Table(), err)
		}
		if err := batch.Append(row...); err != nil {
			_ = batch.Abort()
			t.Fatalf("append row %d to %s: %v", i, tm.Table(), err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send batch for %s: %v", tm.Table(), err)
	}
}

func countRows(ctx context.Context, t *testing.T, conn driver.Conn, query string) uint64 {
	t.Helper()
	var n uint64
	if err := conn.QueryRow(ctx, query).Scan(&n); err != nil {
		t.Fatalf("count query %q: %v", query, err)
	}
	return n
}

// TestClickHouseIntegration applies the standalone DDL, inserts a batch of each
// stream mapped from representative JSON, and verifies the key columns, CDC
// bookkeeping, _rest routing, preserved template texts, and ReplacingMergeTree
// idempotency of a re-delivered batch.
func TestClickHouseIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	conn := openIntegrationConn(ctx, t)
	defer conn.Close()

	// Fresh schema for every run so counts are deterministic.
	if err := conn.Exec(ctx, "DROP DATABASE IF EXISTS yagpcc"); err != nil {
		t.Fatalf("drop database: %v", err)
	}
	if err := ApplyMigrations(ctx, conn, MigrateOptions{RetentionDays: 60, YagpccVersion: "integration"}); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if v, err := GetCurrentVersion(ctx, conn); err != nil || v != ExpectedSchemaVersion {
		t.Fatalf("schema version = %d, err = %v, want %d", v, err, ExpectedSchemaVersion)
	}

	// Use current, second-precision timestamps: the DDL's TTL is measured from
	// collect_time, and OPTIMIZE FINAL below would evict anything already past
	// retention. Reusing the same strings across re-insertion keeps the
	// ReplacingMergeTree ORDER BY keys stable so duplicates collapse.
	ts := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339)

	sessions := []string{
		sessionDoc(ts, 1001, ""),
		sessionDoc(ts, 1002, ""),
		sessionDoc(ts, 1003, "itest-wait"), // WaitEvent is unmapped -> _rest
	}
	statements := []string{
		statementDoc(ts, 2001, ""),
		statementDoc(ts, 2002, "hello"), // extra top-level field -> _rest
	}
	segments := []string{
		segmentDoc(ts, 3001),
		segmentDoc(ts, 3002),
	}

	insertJSON(ctx, t, conn, SessionsMapping(), sessions)
	insertJSON(ctx, t, conn, StatementsMapping(), statements)
	insertJSON(ctx, t, conn, SegmentsMapping(), segments)

	t.Run("row counts and CDC columns", func(t *testing.T) {
		if got := countRows(ctx, t, conn, "SELECT count() FROM yagpcc.sessions_part"); got != 3 {
			t.Errorf("sessions count = %d, want 3", got)
		}
		if got := countRows(ctx, t, conn, "SELECT count() FROM yagpcc.statements_part"); got != 2 {
			t.Errorf("statements count = %d, want 2", got)
		}
		if got := countRows(ctx, t, conn, "SELECT count() FROM yagpcc.segments_part"); got != 2 {
			t.Errorf("segments count = %d, want 2", got)
		}
		if got := countRows(ctx, t, conn,
			"SELECT count() FROM yagpcc.sessions_part WHERE _partition = 'direct' AND _offset = 0"); got != 3 {
			t.Errorf("sessions with direct CDC meta = %d, want 3", got)
		}
	})

	t.Run("key columns survive the round trip", func(t *testing.T) {
		var cluster, host string
		var sessID uint64
		if err := conn.QueryRow(ctx,
			"SELECT cluster_id, hostname, sess_id FROM yagpcc.sessions_part WHERE sess_id = 1001",
		).Scan(&cluster, &host, &sessID); err != nil {
			t.Fatalf("scan session key: %v", err)
		}
		if cluster != "itest" || host != "h1" || sessID != 1001 {
			t.Errorf("session key = (%q, %q, %d), want (itest, h1, 1001)", cluster, host, sessID)
		}
	})

	t.Run("_rest captures unmapped fields", func(t *testing.T) {
		if got := countRows(ctx, t, conn,
			"SELECT count() FROM yagpcc.sessions_part WHERE _rest IS NOT NULL"); got != 1 {
			t.Errorf("sessions with non-null _rest = %d, want 1", got)
		}
		var wait string
		if err := conn.QueryRow(ctx,
			"SELECT JSONExtractString(_rest, 'GpStatInfo', 'WaitEvent') FROM yagpcc.sessions_part WHERE sess_id = 1003",
		).Scan(&wait); err != nil {
			t.Fatalf("extract _rest WaitEvent: %v", err)
		}
		if wait != "itest-wait" {
			t.Errorf("_rest WaitEvent = %q, want itest-wait", wait)
		}
		var msg string
		if err := conn.QueryRow(ctx,
			"SELECT JSONExtractString(_rest, 'message') FROM yagpcc.statements_part WHERE sess_id = 2002",
		).Scan(&msg); err != nil {
			t.Fatalf("extract _rest message: %v", err)
		}
		if msg != "hello" {
			t.Errorf("_rest message = %q, want hello", msg)
		}
	})

	t.Run("template texts are preserved", func(t *testing.T) {
		var tq, tp string
		if err := conn.QueryRow(ctx,
			"SELECT template_query_text, template_plan_text FROM yagpcc.statements_part WHERE sess_id = 2001",
		).Scan(&tq, &tp); err != nil {
			t.Fatalf("scan template texts: %v", err)
		}
		if tq != "SELECT $1" || tp != "TPLAN" {
			t.Errorf("template texts = (%q, %q), want (SELECT $1, TPLAN)", tq, tp)
		}
	})

	t.Run("re-delivered batch is idempotent after OPTIMIZE FINAL", func(t *testing.T) {
		insertJSON(ctx, t, conn, SessionsMapping(), sessions)
		if got := countRows(ctx, t, conn, "SELECT count() FROM yagpcc.sessions_part"); got != 6 {
			t.Errorf("sessions count before merge = %d, want 6 (duplicates present)", got)
		}
		if err := conn.Exec(ctx, "OPTIMIZE TABLE yagpcc.sessions_part FINAL"); err != nil {
			t.Fatalf("optimize final: %v", err)
		}
		if got := countRows(ctx, t, conn, "SELECT count() FROM yagpcc.sessions_part FINAL"); got != 3 {
			t.Errorf("sessions count after dedup = %d, want 3", got)
		}
	})
}

// sessionDoc builds a session JSON document in the encoding/json (PascalCase)
// shape the session stream emits. A non-empty waitEvent adds an unmapped field
// that must land in _rest.
func sessionDoc(ts string, sessID uint64, waitEvent string) string {
	gpsi := fmt.Sprintf(`{"SessID": %d, "TmID": 0, "Datname": "db1", "Usename": "u1", "Waiting": false`, sessID)
	if waitEvent != "" {
		gpsi += fmt.Sprintf(`, "WaitEvent": %q`, waitEvent)
	}
	gpsi += "}"
	return fmt.Sprintf(`{
  "CollectTime": %q,
  "ClusterID": "itest",
  "Hostname": "h1",
  "RunningQueryStatus": 3,
  "GpStatInfo": %s,
  "RunningQuery": {"Ssid": %d, "Tmid": 0, "Ccnt": 1},
  "RunningQueryInfo": {"QueryID": 321, "PlanID": 111, "QueryText": "select 1", "PlanText": "plan"}
}`, ts, gpsi, sessID)
}

// statementDoc builds a statement JSON document in the protojson (camelCase)
// shape the query stream emits. A non-empty extra adds an unmapped top-level
// field that must land in _rest.
func statementDoc(ts string, ssid uint64, extra string) string {
	tail := ""
	if extra != "" {
		tail = fmt.Sprintf(`,
  "message": %q`, extra)
	}
	return fmt.Sprintf(`{
  "clusterId": "itest",
  "hostname": "h1",
  "collectTime": %q,
  "queryKey": {"ssid": %d, "tmid": 0, "ccnt": 2},
  "queryInfo": {
    "generator": "PLAN_GENERATOR_OPTIMIZER",
    "queryId": "12", "planId": "321",
    "queryText": "select 1", "planText": "plan",
    "templateQueryText": "SELECT $1", "templatePlanText": "TPLAN",
    "userName": "bob", "databaseName": "db", "rsgname": "rg"
  },
  "queryStatus": "QUERY_STATUS_DONE",
  "completed": true%s
}`, ts, ssid, tail)
}

// segmentDoc builds a segment JSON document in the protojson (camelCase) shape
// the segment stream emits.
func segmentDoc(ts string, ssid uint64) string {
	return fmt.Sprintf(`{
  "clusterId": "itest",
  "hostname": "h1",
  "collectTime": %q,
  "queryKey": {"ssid": %d, "tmid": 0, "ccnt": 2},
  "segmentKey": {"dbid": 2, "segindex": 4},
  "queryInfo": {"queryId": "12", "planId": "321"}
}`, ts, ssid)
}
