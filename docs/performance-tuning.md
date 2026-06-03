# Performance tuning

This guide covers the main configuration knobs, environment variables, and diagnostic tools available for tuning yagpcc performance. Every option listed here can be set in `yagpcc.yaml` unless stated otherwise.

---

## Memory control with GOMEMLIMIT

Go 1.19+ honours the `GOMEMLIMIT` environment variable (or its runtime equivalent). Setting it caps the total amount of memory the Go runtime will try to use before triggering more aggressive garbage collection.

```bash
# Limit the Go heap to 512 MiB
export GOMEMLIMIT=512MiB
./devbin/yagpcc
```

Useful when yagpcc runs on a host shared with Greenplum processes and you want to prevent the agent from consuming too much RAM. The runtime will GC more frequently rather than exceed the limit.

> **Tip:** Combine `GOMEMLIMIT` with `maximum_stored_queries` (see below) for predictable memory usage.

---

## Logging and output files

yagpcc uses [zap](https://github.com/uber-go/zap) in production mode for structured JSON logging. The log destination and level are configured under the `app.logging` key:

```yaml
app:
  logging:
    level: debug    # debug, info, warn, error
    file: stdout    # default — write to stdout
```

### Log destination

| `file` value | Behaviour |
|--------------|-----------|
| `stdout` (default) | Logs go to standard output. When running under systemd or a process manager, they are captured by the journal or redirected to a file by the service unit. |
| A file path (e.g. `/var/log/yagpcc/server.log`) | Logs are written directly to the specified file. The directory must exist and be writable by the yagpcc process. |

### Standard output and standard error

Besides the structured log, Go itself may write to **stderr** in certain situations (e.g. runtime panics, fatal errors from `log.Fatal`, or unrecovered goroutine crashes). These messages bypass zap and go directly to the process stderr stream.

When running yagpcc as a systemd service, both stdout and stderr are typically captured by `journalctl`:

```bash
# View recent yagpcc logs from the journal
journalctl -u yagpcc -n 200 --no-pager

# Follow logs in real time
journalctl -u yagpcc -f

# Filter by priority (errors only)
journalctl -u yagpcc -p err
```

When running manually, redirect both streams to a file if needed:

```bash
./devbin/yagpcc > /var/log/yagpcc/output.log 2>&1
```

### Archiver output files

The master role writes historical data (sessions, queries, segment metrics) to JSON files via the archiver. These are configured under `arch_config`:

```yaml
arch_config:
  sessions_file: sessions.json       # default
  queries_file: queries.json         # default
  segments_file: segments.json       # default
  plan_detail_file: plan_details.json # default
  max_file_size: 419430400           # 400 MiB — files rotate when this size is exceeded
```

These files grow over time and are rotated when they exceed `max_file_size`. Monitor their disk usage, especially on clusters with high query throughput.

### Log level tuning for performance

Setting `level: debug` produces verbose output (every session refresh, segment pull, and procfs cycle is logged). For production, use `info` or `warn` to reduce I/O overhead:

```yaml
app:
  logging:
    level: info
```

---

## Hooks collector — disabling nested query forwarding

The **yagp-hooks-collector** extension inside Greenplum can send telemetry for nested (internal) queries. On busy clusters this can generate a large volume of messages over UDS. If you do not need nested-query visibility, disable it in the hooks-collector configuration to reduce CPU and memory pressure on both the extension and yagpcc.

Refer to the yagp-hooks-collector documentation for the exact parameter name (typically a GUC like `yagp_hooks_collector.send_nested_queries`).

---

## Tuning stored queries — `maximum_stored_queries`

```yaml
maximum_stored_queries: 50000   # default
```

Controls the upper bound on how many running-query records yagpcc keeps in memory. Lowering this value reduces RAM usage but may cause recently started short queries to be evicted before they complete. Raising it allows tracking more concurrent queries at the cost of higher memory.

### Garbage collection and archiving

When the number of stored queries reaches `maximum_stored_queries`, a garbage collection (GC) cycle runs and frees **50 %** of the storage capacity. Queries are evicted in the following order:

1. **Running (non-completed) queries** — oldest first.
2. **Completed queries** — oldest first.

Completed queries evicted by GC are **not silently dropped**. Instead, they are forwarded to the archiver pipeline so their data is preserved in the archive files (sessions, queries, segment metrics). Only running queries that have not yet received a completion status are lost during GC.

This means that even under heavy load, completed query data is retained for historical analysis as long as the archiver is keeping up.

> **Tip:** Monitor the `gc_runs`, `gc_deleted_queries`, and `gc_archived_queries` Prometheus metrics (see below) to understand how often GC fires and how many queries are being archived vs. dropped.

---

## Short-query aggregation — `short_agg_interval`

```yaml
short_agg_interval: 10m   # default
```

Queries shorter than `minimum_query_duration_sec` are aggregated into per-user buckets. `short_agg_interval` defines the time window for these buckets. A shorter interval produces more granular aggregation but increases the number of stored aggregation records; a longer interval reduces record count but lowers time resolution.

---

## Procfs gathering — `procfs_enabled` / `procfs_refresh_interval`

```yaml
procfs_enabled: true              # default — set to false to disable
procfs_refresh_interval: 60s      # default
```

When enabled, the master periodically queries each segment host for `/proc/<pid>/stat`, `/proc/<pid>/status`, `/proc/<pid>/io` data of running Greenplum processes. This provides per-session CPU, RSS, and I/O metrics with 5/15/30-minute exponential moving averages.

**Disable** procfs gathering (`procfs_enabled: false`) if:

- You do not need per-process resource metrics.
- The cluster has many segment hosts and the extra gRPC calls add noticeable load.
- You are running on a non-Linux platform where `/proc` is unavailable.

**Adjust the interval** to balance freshness vs. load. A shorter interval gives more responsive EMA values but increases gRPC traffic to segment hosts.

---

## Segment pull parallelism — `segment_pull_threads`

```yaml
segment_pull_threads: 15   # default
```

Number of goroutines that concurrently pull query metrics from segment-host yagpcc instances. Increasing this value speeds up data collection on large clusters (many segment hosts) but uses more network connections and CPU on the master. Decreasing it reduces master load at the cost of slower data freshness.

**Rule of thumb:** Set to roughly the number of distinct segment hosts, capped at a reasonable concurrency limit (e.g. 30–50).

---

## Segment pull frequency — `segment_pull_rate_sec`

```yaml
segment_pull_rate_sec: 2   # default (seconds between pull cycles)
```

Controls how often the master enqueues segment hosts for data pulling. A lower value means more frequent pulls and fresher data, but higher network and CPU usage on both master and segments. A higher value reduces load but increases the staleness of segment-level metrics.

---

## Session snapshot frequency — `session_send_metric_interval`

```yaml
session_send_metric_interval: 60s   # default
```

How often session snapshots are written to the archiver channel. Lowering this produces more frequent session records in the archive files (useful for fine-grained historical analysis) but increases I/O and archive file growth. Raising it reduces archive volume.

---

## Prometheus metrics

yagpcc exposes Prometheus metrics on the **instrumentation** HTTP endpoint. By default this listens on port `1433`.

```yaml
app:
  instrumentation:
    addr: "[::1]:1433"
```

### Scrape endpoint

```
GET http://<host>:1433/metrics
```

### Available metric types

| Metric | Type | Description |
|--------|------|-------------|
| `new_sessions` | Counter | Total sessions created since startup. |
| `new_queries` | Counter | Total queries received since startup. |
| `deleted_sessions` | Counter | Total sessions removed. |
| `deleted_queries` | Counter | Total queries removed after archival. |
| `new_aggregated_queries` | Counter | Short queries aggregated. |
| `dropped_queries` | Counter | Queries dropped (e.g. storage full). |
| `failed_queries` | Counter | Queries that ended with an error status. |
| `gc_runs` | Counter | Total number of storage garbage collection cycles. |
| `gc_deleted_queries` | Counter | Total queries evicted by GC (both running and completed). |
| `gc_archived_queries` | Counter | Completed queries forwarded to the archiver during GC (subset of `gc_deleted_queries`). |
| `total_sessions` | Gauge | Current number of tracked sessions. |
| `total_queries` | Gauge | Current number of tracked running queries. |
| `aggregated_queries` | Gauge | Current number of aggregated query buckets. |
| `queries_in_flight` | Gauge | Queries currently being processed. |
| `time` | Histogram | Internal handler latencies, labeled by `method` (e.g. `processSegment`, `RefreshSessions`, `RefreshQueries`, `RefreshProcfs`, `SendSessionMetrics`). |
| `query` | Histogram | Query duration distribution. |
| `slice` | Histogram | Slice-level duration distribution. |
| `query_statuses` | Counter (vec) | Query completion statuses, labeled by `status`. |
| `executing_query` | Gauge (vec) | Currently executing queries bucketed by elapsed time (le labels). Recalculated on scrape. |

### Prometheus configuration example

```yaml
# prometheus.yml
scrape_configs:
  - job_name: yagpcc
    scrape_interval: 15s
    static_configs:
      - targets:
          - "master-host:1433"
          - "segment-host-1:1433"
          - "segment-host-2:1433"
```

### Key metrics to watch

- **`total_sessions`** and **`total_queries`** — if these grow unboundedly, check `maximum_stored_queries` and `clear_deleted_sessions`.
- **`time{method=processSegment}`** — high p99 indicates slow segment pulls; consider increasing `segment_pull_threads` or investigating network latency.
- **`time{method=RefreshProcfs}`** — if this approaches `procfs_refresh_interval`, procfs gathering is too slow; increase the interval or reduce `segment_pull_threads` for procfs.
- **`dropped_queries`** — non-zero means the running-queries storage is full; raise `maximum_stored_queries`.
- **`gc_runs`** — how often GC fires. Frequent GC means the storage is consistently near capacity.
- **`gc_archived_queries`** — completed queries saved during GC. If this is high, consider raising `maximum_stored_queries`.
- **`executing_query`** — shows the distribution of currently running query durations; useful for spotting long-running query buildup.

---

## Profiling with pprof

yagpcc includes a built-in Go pprof debug server that can be activated **on demand** via a Unix signal. This avoids the security risk of leaving pprof endpoints permanently open.

### Configuration

```yaml
debug_port: 6060       # port for the pprof HTTP server (0 = disabled)
debug_minutes: 10      # how long the server stays up after activation
```

### Activating pprof

Send `SIGUSR2` to the yagpcc process:

```bash
kill -USR2 $(pidof yagpcc)
```

The debug HTTP server starts on `[::1]:<debug_port>` and automatically shuts down after `debug_minutes`. Sending `SIGUSR2` again while the server is running extends the timer.

### Using pprof

Once activated, the following endpoints are available:

```bash
# Interactive web UI (requires graphviz for SVG)
go tool pprof -http=:8080 http://localhost:6060/debug/pprof/heap

# 30-second CPU profile
go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30

# Goroutine dump
curl http://localhost:6060/debug/pprof/goroutine?debug=2

# Heap profile (text)
go tool pprof -text http://localhost:6060/debug/pprof/heap

# Allocs profile (total allocations, useful for GC pressure)
go tool pprof -text http://localhost:6060/debug/pprof/allocs

# Execution trace (5 seconds)
curl -o trace.out http://localhost:6060/debug/pprof/trace?seconds=5
go tool trace trace.out

# Block profile (contention)
curl http://localhost:6060/debug/pprof/block?debug=1

# Mutex profile
curl http://localhost:6060/debug/pprof/mutex?debug=1
```

### Common profiling scenarios

**High CPU usage:**

```bash
go tool pprof -http=:8080 http://localhost:6060/debug/pprof/profile?seconds=30
```

Look at the flame graph for hot functions. Common culprits: JSON serialization in archivers, lock contention in storage, or excessive segment pulls.

**High memory usage:**

```bash
go tool pprof -http=:8080 http://localhost:6060/debug/pprof/heap
```

Check `inuse_space` for current allocations and `alloc_space` for cumulative. If `total_queries` is high, consider lowering `maximum_stored_queries`.

**Goroutine leaks:**

```bash
curl http://localhost:6060/debug/pprof/goroutine?debug=2 | head -100
```

Look for goroutines stuck in channel operations or network I/O. A growing goroutine count may indicate segment hosts that are unreachable (connections timing out).

---

## Quick reference — all tuning parameters

| Parameter | YAML key | Default | Effect |
|-----------|----------|---------|--------|
| Go memory limit | `GOMEMLIMIT` env var | unlimited | Caps Go heap; triggers more aggressive GC |
| Log level | `app.logging.level` | debug | Controls log verbosity; use `info` or `warn` in production to reduce I/O |
| Log destination | `app.logging.file` | stdout | Where structured logs are written (`stdout` or a file path) |
| Archive max file size | `arch_config.max_file_size` | 400 MiB | Archiver JSON files rotate when exceeding this size |
| Stored queries cap | `maximum_stored_queries` | 50000 | Max running-query records in memory |
| Short-query aggregation window | `short_agg_interval` | 10m | Time bucket size for short-query aggregation |
| Procfs gathering | `procfs_enabled` | true | Enable/disable per-process resource stats |
| Procfs refresh rate | `procfs_refresh_interval` | 60s | How often procfs data is collected |
| Segment pull workers | `segment_pull_threads` | 15 | Concurrent goroutines pulling from segments |
| Segment pull cycle | `segment_pull_rate_sec` | 2 | Seconds between segment pull cycles |
| Session snapshot rate | `session_send_metric_interval` | 60s | How often session snapshots are archived |
| Pprof port | `debug_port` | 0 (disabled) | Port for on-demand pprof server |
| Pprof duration | `debug_minutes` | 0 | Minutes pprof stays active after SIGUSR2 |

---

## Related documentation

- [Service architecture](service-architecture.md) — Roles, interfaces, ports, data flow.
- [Per-process resource statistics](proc-stats-flow.md) — Procfs data flow and EMA calculations.
- [API description](API.md) — gRPC and CSV HTTP API reference.
