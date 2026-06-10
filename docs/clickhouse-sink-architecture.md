# ClickHouse sink architecture

This document describes the `internal/sink/clickhouse/` package — yagpcc's
optional sink that streams query history from a master-role agent into a
ClickHouse cluster for long-term, ad-hoc SQL analysis.

It complements:

- `docs/vm-vs-ch.md` — why yagpcc uses ClickHouse alongside VictoriaMetrics
  and what data goes where.
- `docs/plans/clickhouse-sink.md` — original implementation plan, tasks and
  open questions.

## Scope

The sink runs only on the master role (next to `archiver.go`/`statwriter.go`)
and is opt-in via `clickhouse.enabled: true`. When disabled the orchestrator
is a no-op and yagpcc behaves exactly as before.

When enabled it persists three first-class tables (plus one bookkeeping
table) under the `yagpcc` database in ClickHouse:

| Table | Purpose |
|-------|---------|
| `yagpcc.query_events` | One row per query status change (SUBMIT/START/DONE/ERROR/CANCELLING/CANCELED/END). Holds the full plan tree as JSON, per-segment metrics as `Array(Tuple(...))` and aggregated SystemStat/Instrumentation totals. |
| `yagpcc.aggregated_metrics` | `SummingMergeTree` rollups keyed by `(query_id, plan_id, user, database, resource_group, bucket_time)`. Mirrors the in-memory `AggregatedStorage` flush. |
| `yagpcc.session_snapshots` | Periodic snapshot of `pg_stat_activity` (default every 10s). |
| `yagpcc._yagpcc_meta` | Schema-version ledger (`version`, `applied_at`, `direction='up'\|'down'`). Used by both auto-migrate and verify-schema modes. |

The DDL lives in `internal/sink/clickhouse/migrations/0001_init.up.sql` and
is embedded into the binary through `//go:embed`. The retention TTL is a
`text/template` placeholder rendered from `clickhouse.retention_days` at
startup.

## Package layout

```
internal/sink/clickhouse/
├── client.go         NewClient + TLS config + Ping
├── writer.go         ClickhouseWriter orchestrator (lifecycle, Submit, FlushAggregates)
├── tables.go         QueryEventWriter, AggregatedWriter, SessionSnapshotWriter
├── mapping.go        proto/storage → CH row converters (toQueryEventRow, etc.)
├── buffer.go         thread-safe ring buffer (drop_oldest | block)
├── migrations.go     ParseMigrations, RenderTemplate, ApplyMigrations, GetCurrentVersion
├── schema.go         VerifySchema, DumpSchema, DumpMigration
├── metrics.go        Prometheus collectors + per-table hook factories
├── migrations/       0001_init.{up,down}.sql (embedded via go:embed)
└── testdata/         fixtures for unit tests
```

Boundaries deliberately keep the sink isolated from yagpcc's hot-path
storage:

- `mapping.go` is the only file that imports `pbm.TotalQueryData` and
  storage types; all other files operate on sink-local row types.
- The orchestrator exposes a small `ClickhouseSink` interface to
  `internal/master/background.go` (Submit + FlushAggregates) so the master
  can swap a fake in tests without depending on the real CH client.
- Per-table writers depend on a `batchPreparer` interface — a subset of
  `driver.Conn` — so unit tests use hand-rolled fakes instead of standing
  up a real server. A compile-time assertion in `writer.go` keeps the
  interface in sync with `clickhouse-go/v2`.

## Data flow

```
yagp-hooks-collector ──UDS──▶ yagpcc segments ──gRPC pull──▶ master/background.go
                                                                  │
                              ┌───────────────────────────────────┤
                              ▼                                   ▼
               ArchiveOrAggregate (existing)            AggregatedStorage cycle hook
                              │                                   │
                              ▼                                   ▼
                  ClickhouseWriter.Submit              ClickhouseWriter.FlushAggregates
                              │                                   │
                              ▼                                   ▼
              QueryEventWriter.Write              AggregatedWriter.FlushBuckets
              (filter + Buffer.Append)                      │
                              │                             │
                              ▼                             ▼
                   periodic Flush(ctx)             INSERT yagpcc.aggregated_metrics
                              │                             (SummingMergeTree)
                              ▼
                INSERT yagpcc.query_events                 SessionSnapshotWriter (own ticker)
                                                                  │
                                                                  ▼
                                                INSERT yagpcc.session_snapshots
```

`archiver.go` and `statwriter.go` keep working unchanged; the JSON archive
becomes a manual disaster-recovery source rather than the only persistence
path. Both can be enabled at the same time, only one, or neither.

### Hot-path entry points

- `Submit(qT *pbm.TotalQueryData)` — called from `ArchiveOrAggregate` for
  every query (short or long). Filters out queries whose duration is below
  `clickhouse.min_duration_ms` for terminal statuses, then enqueues into
  `QueryEventWriter`'s buffer. Non-terminal events bypass the filter so the
  status timeline stays intact.
- `FlushAggregates(ctx, buckets)` — invoked from the cycle hook attached to
  `storage.AggregatedStorage.SetCycleHook`. The hook converts mature
  `AggBucketSnapshot` rows into `clickhouse.AggregatedBucket` and forwards
  them. There is no buffer here: aggregated rows arrive already grouped.

### Background loops

`Run(ctx)` starts up to two goroutines:

- A `time.NewTicker(cfg.FlushInterval)` periodically drains the
  `query_events` buffer and runs an `INSERT` batch.
- `SessionSnapshotWriter.Run(ctx)` ticks at `session_snapshot_interval_sec`,
  pulls active sessions from the provider injected by the master and
  inserts them.

`ctx.Done()` triggers a final flush guarded by a 30s timeout
(`shutdownFlushTimeout`) so a stuck CH cannot block master shutdown.

## Schema lifecycle

```
                          ┌─────────────────────────────────────────────┐
                          │ embedded migrations (//go:embed)            │
                          │   0001_init.up.sql / 0001_init.down.sql     │
                          │   ExpectedSchemaVersion = 1                 │
                          └────────────────────┬────────────────────────┘
                                               │
                                               ▼
                              ┌────────────────────────────────────────┐
                              │ schema_management (yagpcc.yaml)        │
                              │   auto         → ApplyMigrations       │
                              │   verify_only  → VerifySchema          │
                              │   dump_only    → DumpSchema → stdout   │
                              └────────────────────────────────────────┘
                                               │
                                               ▼
                              ┌────────────────────────────────────────┐
                              │ yagpcc._yagpcc_meta                    │
                              │   one row per applied migration        │
                              │   (version, applied_at, direction)     │
                              └────────────────────────────────────────┘
```

`schema_management` modes:

- `auto` — applies pending migrations on startup. Required CH grants:
  `CREATE, ALTER, INSERT, SELECT ON yagpcc.*` (plus `DROP` if you ever run
  the `down` direction manually).
- `verify_only` — read-only check. On mismatch the writer logs, sets
  `yagpcc_ch_schema_mismatch=1`, closes the connection and self-disables
  (Submit/FlushAggregates become no-ops). yagpcc keeps running so JSON
  archive can carry on as fallback.
- `dump_only` — prints the rendered DDL to stdout and self-disables. The
  `--dump-only` mode is meant for offline review; the corresponding CLI
  flags are described below.

`ApplyMigrations` walks `current+1 .. ExpectedSchemaVersion`, executes the
rendered `.up.sql` for each step and inserts a row into `_yagpcc_meta`.
ClickHouse does not support multi-statement DDL transactions; each migration
is therefore expected to be self-contained, and an interrupted run records
the last successful version so an operator can decide how to recover.

A `current > ExpectedSchemaVersion` is fail-fast: the binary is older than
the schema and refuses to start in `auto` mode.

### Adding a new migration

1. Create `internal/sink/clickhouse/migrations/000N_<name>.up.sql` and
   `000N_<name>.down.sql` (zero-padded version, lower-case name).
2. Use `{{.RetentionDays}}` (and any future template params added to
   `RenderTemplate`) when you need values from config.
3. Bump `ExpectedSchemaVersion` in `migrations.go` to `N`.
4. Update `internal/sink/clickhouse/migrations_test.go` and
   `schema_test.go` if the test fixtures depend on the version count.
5. Document the change in this file and in `docs/plans/clickhouse-sink.md`
   under "Schema (DDL миграции ...)" if the schema has changed.

Migrations are append-only by version. Editing an applied `.up.sql` after a
release is not supported — write a new migration instead.

## Failure modes

| Scenario | Behaviour |
|----------|-----------|
| ClickHouse unreachable on startup (`Ping` fails) | `clickhouse.New` returns an error; with `enabled: true` yagpcc fail-fasts. |
| ClickHouse unreachable while running | `Flush` errors are absorbed by the goroutine; `yagpcc_ch_unreachable=1` and a warn-level log are emitted. The buffer keeps filling under the configured overflow policy. |
| Buffer overflow | `drop_oldest` evicts the oldest row and increments `yagpcc_ch_dropped_rows_total{reason="buffer_full"}`. `block` causes `QueryEventWriter.Write` (and therefore `ArchiveOrAggregate`) to wait until the next `Drain`. |
| Schema older than binary | `auto`: migration is applied and the run continues. `verify_only`: returns `ErrSchemaUpgradeRequired`; sink self-disables and JSON archive remains the only persistence. |
| Schema newer than binary | `auto`: refuses to start (downgrade not allowed). `verify_only`: returns `ErrSchemaDowngradeRequired`; same self-disable as above. |
| `min_duration_ms` filter | Terminal events shorter than the threshold are dropped with `yagpcc_ch_dropped_rows_total{reason="filter"}`. |
| Mapping error (unexpected proto shape) | Row is dropped with `yagpcc_ch_dropped_rows_total{reason="mapping_error"}`; the rest of the batch is unaffected. |
| Insert error | Whole batch fails: `yagpcc_ch_inserts_total{status="error"}` and `yagpcc_ch_dropped_rows_total{reason="insert_error"}` increment. The ring buffer continues collecting new rows. |

### Delivery guarantees

The sink is **at-most-once** for ClickHouse. Once a row is dropped due to
overflow or insert error it is gone from CH. The JSON archive keeps the
data on local disk, so a manual replay tool against the archive is the
recovery story (see "Out of scope" in the implementation plan for the
spill-to-disk follow-up).

## Configuration reference

Full schema lives in `internal/config/config.go` under `ClickhouseConfig`.
The yagpcc.yaml section:

```yaml
clickhouse:
  enabled: false
  addrs: ["clickhouse-mon:9000"]
  database: yagpcc
  user: yagpcc_writer
  # password is read from YAGPCC_CH_PASSWORD env var

  schema_management: auto      # auto | verify_only | dump_only
  retention_days: 30           # rendered into the TTL clause of every table

  batch_size: 10000            # rows per INSERT batch (Drain limit)
  flush_interval: 10s          # query_events batch cadence
  buffer_max_rows: 100000      # ring buffer capacity
  on_buffer_overflow: drop_oldest   # drop_oldest | block
  async_insert: true           # CH-side async_insert=1, wait_for_async_insert=0

  min_duration_ms: 100         # drop terminal events shorter than this
  session_snapshot_interval_sec: 10

  dial_timeout: 5s
  read_timeout: 30s

  tls:
    enabled: false
    ca_file: /etc/yagpcc/ssl/ch_ca.crt
    insecure_skip_verify: false

  sinks:
    query_events: true
    aggregated_metrics: true
    session_snapshots: true
    plan_nodes: false          # reserved for v2
```

Independent toggling of `archiver.enabled` and `clickhouse.enabled` is
supported; the two paths do not share state.

## CLI commands

`cmd/server/schema_cli.go` recognises four mutually-exclusive flags before
the main app starts. They never read live cluster state — only
`yagpcc.yaml` and the embedded migrations.

| Flag | Effect | Exit code |
|------|--------|-----------|
| `--dump-schema` | Print all up-migrations rendered with `retention_days` from config (default 30 if no config). | `0` |
| `--dump-migration --from=N --to=M` | Print SQL to migrate between versions. `from < to` ⇒ ups; `from > to` ⇒ downs in reverse; `from == to` ⇒ empty. | `0` |
| `--migrate-only` | Load config, connect, `ApplyMigrations`, exit. | `0` on success, `2` on load/validate/connect/apply error. |
| `--verify-schema` | Load config, connect, `VerifySchema`, exit. | `0` on match, `2` on upgrade/downgrade required or connect error. |

`schema_management: dump_only` in the YAML achieves the same as
`--dump-schema` but goes through the normal startup path; once the dump is
printed the binary self-disables the sink and the master continues to run
without ClickHouse.

## Prometheus metrics

Registered via `Metrics.NewMetrics(prometheus.Registerer)`; the orchestrator
passes `prometheus.DefaultRegisterer` so they appear on the existing
`/metrics` endpoint.

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `yagpcc_ch_inserts_total` | counter | `table`, `status` (`success`\|`error`) | One increment per `INSERT` call. |
| `yagpcc_ch_buffer_size` | gauge | `table` | Current buffer length after each Drain. |
| `yagpcc_ch_batch_duration_seconds` | histogram (0.01 → 30s) | `table` | Wall time of `PrepareBatch + Append + Send`. |
| `yagpcc_ch_dropped_rows_total` | counter | `table`, `reason` (`buffer_full`\|`filter`\|`mapping_error`\|`insert_error`) | Rows that did not reach ClickHouse. |
| `yagpcc_ch_schema_mismatch` | gauge | — | `1` while `verify_only` reports a mismatch. |
| `yagpcc_ch_unreachable` | gauge | — | `1` while the latest flush failed. Cleared on the first successful flush. |

Suggested alerts (vmalert):

- `yagpcc_ch_unreachable == 1 for 5m` — CH ingestion broken.
- `yagpcc_ch_schema_mismatch == 1` — operator action needed (run
  `--migrate-only` after upgrading the binary).
- `rate(yagpcc_ch_dropped_rows_total{reason="buffer_full"}[5m]) > 0` —
  consider raising `buffer_max_rows` or `batch_size`.

## Testing

- Unit tests cover ~91% of the package via hand-rolled fakes for
  `MigrationConn`, `batchPreparer` and `orchestratorConn` (subsets of
  `driver.Conn`). No real ClickHouse required: `go test ./internal/sink/clickhouse/...`.
- One integration scenario lives in `integration_test.go` behind
  `//go:build integration` and uses `testcontainers-go` to spin up
  `clickhouse-server:26.3`. It exercises `ApplyMigrations`, all three
  table inserts, CH-specific features (Enum8, LowCardinality, Array(Tuple),
  Nullable, JSON column, TTL) and migration idempotency.
- CI runs the integration job in `.github/workflows/test.yaml` after the
  regular `tests` job.

Run locally:

```bash
go test ./internal/sink/clickhouse/...
go test -tags=integration -v -count=1 -timeout=10m ./internal/sink/clickhouse/...
```

## Related documents

- `docs/plans/clickhouse-sink.md` — the implementation plan with task
  breakdown and decisions.
- `docs/plans/clickhouse-sink-questions.md` — architecture Q&A used to seed
  the plan.
- `docs/vm-vs-ch.md` — VictoriaMetrics vs ClickHouse role split.
- `greenplum-role-ubuntu/docs/plans/observability-stack.md` — how the CH
  cluster itself is provisioned (separate repo).
