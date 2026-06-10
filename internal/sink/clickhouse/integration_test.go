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

// File: integration_test.go runs an end-to-end smoke test against a real
// ClickHouse server spun up via testcontainers-go. It is gated behind the
// `integration` build tag so the default `go test` run remains hermetic.
//
// Run locally with:
//
//	go test -tags=integration -v ./internal/sink/clickhouse/...
//
// Requires Docker on the host and network access to pull
// clickhouse/clickhouse-server.
//go:build integration

package clickhouse

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// clickhouseImage is the server image used by the integration test. The plan
// pins this to the version observability infra is provisioned with; bump it
// here and in greenplum-role-ubuntu/roles/clickhouse together.
const clickhouseImage = "clickhouse/clickhouse-server:26.3"

// integrationRetentionDays must be > 0 (ApplyMigrations rejects 0). Picked
// large enough that synthetic rows inserted with `now()` are not immediately
// removed by TTL — the dedicated TTL sub-test inserts an explicit old row.
const integrationRetentionDays = 30

// startClickhouseContainer launches ClickHouse, waits for the native port to
// accept connections, and returns the mapped host:port endpoint. The caller
// is responsible for terminating the container (defer Terminate).
func startClickhouseContainer(ctx context.Context, t *testing.T) (tc.Container, string) {
	t.Helper()
	req := tc.ContainerRequest{
		Image:        clickhouseImage,
		ExposedPorts: []string{"9000/tcp"},
		Env: map[string]string{
			"CLICKHOUSE_DB":                        "default",
			"CLICKHOUSE_USER":                      "default",
			"CLICKHOUSE_PASSWORD":                  "",
			"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1",
		},
		WaitingFor: wait.ForListeningPort("9000/tcp").WithStartupTimeout(2 * time.Minute),
	}
	c, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start clickhouse container: %v", err)
	}
	host, err := c.Host(ctx)
	if err != nil {
		_ = c.Terminate(ctx)
		t.Fatalf("container host: %v", err)
	}
	port, err := c.MappedPort(ctx, "9000/tcp")
	if err != nil {
		_ = c.Terminate(ctx)
		t.Fatalf("mapped port: %v", err)
	}
	return c, fmt.Sprintf("%s:%s", host, port.Port())
}

// openCH opens a clickhouse-go connection for the integration test. dial with
// LZ4 compression to mirror NewClient and prove the wire path works.
func openCH(t *testing.T, addr, user, password, db string) driver.Conn {
	t.Helper()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: db,
			Username: user,
			Password: password,
		},
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
		DialTimeout: 5 * time.Second,
		ReadTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	if err := conn.Ping(context.Background()); err != nil {
		_ = conn.Close()
		t.Fatalf("ping clickhouse: %v", err)
	}
	return conn
}

// provisionWriter creates the yagpcc_writer user with the privileges the plan
// declares (CREATE / ALTER / INSERT / SELECT on yagpcc.*). We do this through
// the bootstrap `default` user which keeps ACCESS MANAGEMENT thanks to
// CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1.
func provisionWriter(ctx context.Context, t *testing.T, admin driver.Conn) {
	t.Helper()
	stmts := []string{
		"DROP USER IF EXISTS yagpcc_writer",
		"CREATE USER yagpcc_writer IDENTIFIED BY 'test'",
		"GRANT CREATE, ALTER, INSERT, SELECT, DROP ON yagpcc.* TO yagpcc_writer",
	}
	for _, s := range stmts {
		if err := admin.Exec(ctx, s); err != nil {
			t.Fatalf("provision writer (%q): %v", s, err)
		}
	}
}

// TestSchemaApplyAndInsert is the single comprehensive integration scenario
// described in clickhouse-sink.md Task 17. It exercises:
//   - migration application from version 0 to ExpectedSchemaVersion;
//   - INSERTs into all three v1 tables with values that touch every
//     ClickHouse-specific column type the schema relies on;
//   - decoding via ClickHouse-specific functions (Enum8 filter,
//     LowCardinality GROUP BY, Array(Tuple) tuple-element extraction,
//     Nullable() NULL handling, JSONExtract on the plan_tree column,
//     TTL eviction after OPTIMIZE FINAL);
//   - schema verification after migrations (VerifySchema must succeed);
//   - idempotency of ApplyMigrations (re-running must not re-apply).
func TestSchemaApplyAndInsert(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires docker")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	container, addr := startClickhouseContainer(ctx, t)
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()
		if err := container.Terminate(shutdownCtx); err != nil {
			t.Logf("terminate container: %v", err)
		}
	}()

	admin := openCH(t, addr, "default", "", "default")
	defer admin.Close()
	provisionWriter(ctx, t, admin)

	conn := openCH(t, addr, "yagpcc_writer", "test", "default")
	defer conn.Close()

	t.Run("ApplyMigrations brings schema to expected version", func(t *testing.T) {
		if err := ApplyMigrations(ctx, conn, MigrateOptions{
			RetentionDays: integrationRetentionDays,
			YagpccVersion: "integration-test",
		}); err != nil {
			t.Fatalf("ApplyMigrations: %v", err)
		}
		v, err := GetCurrentVersion(ctx, conn)
		if err != nil {
			t.Fatalf("GetCurrentVersion: %v", err)
		}
		if v != ExpectedSchemaVersion {
			t.Fatalf("schema version = %d, want %d", v, ExpectedSchemaVersion)
		}
		expected := map[string]bool{
			"_yagpcc_meta":       false,
			"query_events":       false,
			"aggregated_metrics": false,
			"session_snapshots":  false,
		}
		var rows []struct {
			Name string `ch:"name"`
		}
		if err := conn.Select(ctx, &rows, "SELECT name FROM system.tables WHERE database = 'yagpcc'"); err != nil {
			t.Fatalf("list yagpcc tables: %v", err)
		}
		for _, r := range rows {
			if _, ok := expected[r.Name]; ok {
				expected[r.Name] = true
			}
		}
		for name, found := range expected {
			if !found {
				t.Errorf("missing expected table yagpcc.%s", name)
			}
		}
	})

	t.Run("INSERT query_events covers Enum8 / Array(Tuple) / Nullable / JSON", func(t *testing.T) {
		batch, err := conn.PrepareBatch(ctx, queryEventsInsertSQL)
		if err != nil {
			t.Fatalf("prepare query_events batch: %v", err)
		}
		now := time.Now().UTC().Truncate(time.Millisecond)
		statuses := []string{statusSubmit, statusStart, statusDone, statusError, statusCanceled}
		for i := 0; i < 100; i++ {
			status := statuses[i%len(statuses)]
			eventTime := now.Add(time.Duration(i) * time.Millisecond)
			startTime := eventTime.Add(-50 * time.Millisecond)
			endTime := eventTime
			duration := uint64(50)
			row := []any{
				eventTime,
				status,
				uint64(1000 + i),
				uint64(2000 + i),
				int32(1),
				int32(42 + i%5),
				int32(0),
				(*time.Time)(nil),
				&startTime,
				&endTime,
				&duration,
				fmt.Sprintf("user_%d", i%3),
				"yagpcc_db",
				"default_rsg",
				generatorPlanner,
				fmt.Sprintf("SELECT * FROM t WHERE id = %d", i),
				"Plan text",
				[][]any{
					{int32(1), int32(0), float64(0.5), float64(0.1), uint64(1024), uint64(100), uint64(2048), uint64(512)},
					{int32(1), int32(1), float64(0.4), float64(0.1), uint64(1024), uint64(100), uint64(2048), uint64(512)},
				},
				float64(0.9), float64(0.2), float64(1.1),
				uint64(2048), uint64(4096), uint64(1024), uint64(200),
				uint64(10), uint64(5),
				float64(0.01), float64(0.02),
				uint64(8192), uint64(4096),
				int32(0), int64(0),
				`{"generator":"PLANNER","plan_text":"Seq Scan on t","analyze_text":""}`,
				"integration-test",
				uint32(ExpectedSchemaVersion),
			}
			if err := batch.Append(row...); err != nil {
				t.Fatalf("append query_events row %d: %v", i, err)
			}
		}
		if err := batch.Send(); err != nil {
			t.Fatalf("send query_events batch: %v", err)
		}

		var doneCount uint64
		if err := conn.QueryRow(ctx,
			"SELECT count() FROM yagpcc.query_events WHERE status = 'DONE'",
		).Scan(&doneCount); err != nil {
			t.Fatalf("count DONE: %v", err)
		}
		if doneCount != 20 {
			t.Errorf("DONE count = %d, want 20", doneCount)
		}

		var users []struct {
			User  string `ch:"user"`
			Count uint64 `ch:"c"`
		}
		if err := conn.Select(ctx, &users,
			"SELECT user, count() AS c FROM yagpcc.query_events GROUP BY user ORDER BY user",
		); err != nil {
			t.Fatalf("group by user: %v", err)
		}
		if len(users) != 3 {
			t.Errorf("distinct users = %d, want 3", len(users))
		}

		var firstSegCPU float64
		if err := conn.QueryRow(ctx,
			"SELECT segments[1].3 FROM yagpcc.query_events WHERE query_id = 1000 LIMIT 1",
		).Scan(&firstSegCPU); err != nil {
			t.Fatalf("array(tuple) extract: %v", err)
		}
		if firstSegCPU != 0.5 {
			t.Errorf("segments[1].cpu_user_sec = %v, want 0.5", firstSegCPU)
		}

		var nullSubmits uint64
		if err := conn.QueryRow(ctx,
			"SELECT count() FROM yagpcc.query_events WHERE submit_time IS NULL",
		).Scan(&nullSubmits); err != nil {
			t.Fatalf("count nullable: %v", err)
		}
		if nullSubmits != 100 {
			t.Errorf("NULL submit_time count = %d, want 100", nullSubmits)
		}

		var planGenerator string
		if err := conn.QueryRow(ctx,
			"SELECT JSONExtractString(plan_tree, 'generator') FROM yagpcc.query_events WHERE query_id = 1000 LIMIT 1",
		).Scan(&planGenerator); err != nil {
			t.Fatalf("json extract: %v", err)
		}
		if planGenerator != "PLANNER" {
			t.Errorf("JSONExtract(plan_tree.generator) = %q, want PLANNER", planGenerator)
		}
	})

	t.Run("INSERT aggregated_metrics", func(t *testing.T) {
		batch, err := conn.PrepareBatch(ctx, aggregatedMetricsInsertSQL)
		if err != nil {
			t.Fatalf("prepare aggregated_metrics batch: %v", err)
		}
		bucket := time.Now().UTC().Truncate(time.Second)
		for i := 0; i < 10; i++ {
			row := toAggregatedRow(&AggregatedBucket{
				BucketTime:      bucket.Add(time.Duration(i) * time.Second),
				QueryID:         uint64(5000 + i),
				PlanID:          uint64(6000 + i),
				User:            "agg_user",
				Database:        "yagpcc_db",
				ResourceGroup:   "rsg_default",
				Executions:      10,
				TotalCPUSec:     1.5,
				TotalRunningSec: 2.0,
				TotalRSSBytes:   2048,
				TotalIOBytes:    4096,
				TotalNTuples:    1000,
				AvgDurationMs:   150.5,
				MaxDurationMs:   500,
			})
			if err := batch.Append(row...); err != nil {
				t.Fatalf("append agg row %d: %v", i, err)
			}
		}
		if err := batch.Send(); err != nil {
			t.Fatalf("send agg batch: %v", err)
		}

		var rowCount uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM yagpcc.aggregated_metrics").Scan(&rowCount); err != nil {
			t.Fatalf("count aggregated: %v", err)
		}
		if rowCount != 10 {
			t.Errorf("aggregated_metrics count = %d, want 10", rowCount)
		}
	})

	t.Run("INSERT session_snapshots", func(t *testing.T) {
		batch, err := conn.PrepareBatch(ctx, sessionSnapshotsInsertSQL)
		if err != nil {
			t.Fatalf("prepare session_snapshots batch: %v", err)
		}
		now := time.Now().UTC().Truncate(time.Millisecond)
		for i := 0; i < 5; i++ {
			backendStart := now.Add(-1 * time.Hour)
			queryStart := now.Add(-1 * time.Minute)
			row := toSessionRow(&Session{
				SnapshotTime: now,
				SessionID:    int32(7000 + i),
				PID:          int32(8000 + i),
				User:         "sess_user",
				Database:     "yagpcc_db",
				Application:  "psql",
				ClientAddr:   "127.0.0.1",
				BackendStart: backendStart,
				XactStart:    nil,
				QueryStart:   &queryStart,
				StateChange:  nil,
				State:        "active",
				Waiting:      i%2 == 0,
				Query:        fmt.Sprintf("SELECT %d", i),
			})
			if err := batch.Append(row...); err != nil {
				t.Fatalf("append session row %d: %v", i, err)
			}
		}
		if err := batch.Send(); err != nil {
			t.Fatalf("send session batch: %v", err)
		}

		var waitingCount uint64
		if err := conn.QueryRow(ctx,
			"SELECT count() FROM yagpcc.session_snapshots WHERE waiting = 1",
		).Scan(&waitingCount); err != nil {
			t.Fatalf("count waiting: %v", err)
		}
		if waitingCount != 3 {
			t.Errorf("waiting=1 count = %d, want 3", waitingCount)
		}
	})

	t.Run("TTL eviction removes rows older than retention", func(t *testing.T) {
		old := time.Now().UTC().Add(-time.Duration(integrationRetentionDays+1) * 24 * time.Hour)
		batch, err := conn.PrepareBatch(ctx, queryEventsInsertSQL)
		if err != nil {
			t.Fatalf("prepare ttl batch: %v", err)
		}
		row := []any{
			old, statusDone,
			uint64(99999), uint64(99999), int32(1), int32(42), int32(0),
			(*time.Time)(nil), (*time.Time)(nil), (*time.Time)(nil), (*uint64)(nil),
			"ttl_user", "yagpcc_db", "default_rsg", generatorPlanner,
			"old", "old plan",
			[][]any{},
			float64(0), float64(0), float64(0),
			uint64(0), uint64(0), uint64(0), uint64(0),
			uint64(0), uint64(0),
			float64(0), float64(0),
			uint64(0), uint64(0),
			int32(0), int64(0),
			"{}", "integration-test", uint32(ExpectedSchemaVersion),
		}
		if err := batch.Append(row...); err != nil {
			t.Fatalf("append ttl row: %v", err)
		}
		if err := batch.Send(); err != nil {
			t.Fatalf("send ttl batch: %v", err)
		}
		if err := conn.Exec(ctx, "OPTIMIZE TABLE yagpcc.query_events FINAL"); err != nil {
			t.Fatalf("optimize final: %v", err)
		}
		var ttlCount uint64
		if err := conn.QueryRow(ctx,
			"SELECT count() FROM yagpcc.query_events WHERE query_id = 99999",
		).Scan(&ttlCount); err != nil {
			t.Fatalf("count ttl: %v", err)
		}
		if ttlCount != 0 {
			t.Errorf("expected TTL to drop expired row, got count = %d", ttlCount)
		}
	})

	t.Run("VerifySchema succeeds after migrations", func(t *testing.T) {
		if err := VerifySchema(ctx, conn); err != nil {
			t.Errorf("VerifySchema: %v", err)
		}
	})

	t.Run("ApplyMigrations is idempotent", func(t *testing.T) {
		var beforeRows uint64
		if err := conn.QueryRow(ctx,
			"SELECT count() FROM yagpcc._yagpcc_meta WHERE direction = 'up'",
		).Scan(&beforeRows); err != nil {
			t.Fatalf("count meta before: %v", err)
		}
		if err := ApplyMigrations(ctx, conn, MigrateOptions{
			RetentionDays: integrationRetentionDays,
			YagpccVersion: "integration-test",
		}); err != nil {
			t.Fatalf("re-apply: %v", err)
		}
		var afterRows uint64
		if err := conn.QueryRow(ctx,
			"SELECT count() FROM yagpcc._yagpcc_meta WHERE direction = 'up'",
		).Scan(&afterRows); err != nil {
			t.Fatalf("count meta after: %v", err)
		}
		if beforeRows != afterRows {
			t.Errorf("meta rows changed across re-apply: before=%d after=%d", beforeRows, afterRows)
		}
		v, err := GetCurrentVersion(ctx, conn)
		if err != nil {
			t.Fatalf("GetCurrentVersion: %v", err)
		}
		if v != ExpectedSchemaVersion {
			t.Errorf("version drifted: got %d want %d", v, ExpectedSchemaVersion)
		}
	})
}
