# Per-process resource statistics

This document describes how yagpcc collects per-process Linux procfs data
(`/proc/<pid>/{stat,status,io,cmdline}`) for every Greenplum / Cloudberry
backend across the cluster, attributes it to the running session, and
exposes 5-minute rolling deltas on the master via the existing
`LastMetrics` field on `SessionState`.

For the broader picture see [Architecture overview](./architecture.md) and
[Service architecture](./service-architecture.md). For the user-facing API
see [API description](./API.md).

---

## 1. Schema and per-session cluster-wide flow

The data flow has three stages: PID discovery on the master, a per-host
gRPC fan-out to segment-host yagpccs, and snapshot storage + diff-based
aggregation back on the master.

### 1.1 PID discovery (master, via libpq)

The master yagpcc already maintains a background list of every Greenplum
backend (master + every segment) by polling
`gp_dist_random('pg_stat_activity') UNION ALL pg_stat_activity` from the
Greenplum master.

| Item | Location |
|------|----------|
| Cloudberry/GP6 query text | `cloudberryAllSessionsQuery` / `gp6AllSessionsQuery` in [internal/gp/stat_activity/lister.go](../internal/gp/stat_activity/lister.go) |
| Polling cadence | `WithBackgroundAllSessionsCollectionInterval` (default `60s`, see `newBackgroundAllSessions` in [internal/gp/stat_activity/lister.go](../internal/gp/stat_activity/lister.go)) |
| Cache TTL | `WithBackgroundAllSessionsCacheTTL` (default `600s`) |
| Row type | `SessionPid{GpSegmentId, Pid, SessId, BackendType}` in [internal/gp/stat_activity/models.go](../internal/gp/stat_activity/models.go) |
| Read accessor | `Lister.ListAllSessions(ctx)` |

A row of `SessionPid` carries everything we need to address a single
process from the master: its hosting segment id, its OS pid, the
Greenplum `sess_id` it belongs to, and (on Cloudberry) `backend_type`
(`client backend`, `walwriter`, …).

`gp_segment_id` is then resolved to a hostname via the existing segment
topology that the master pulls from `gp_segment_configuration` (see
[internal/master/background.go](../internal/master/background.go)).

### 1.2 Per-host fan-out (master → segment-host yagpcc, gRPC)

The master groups the latest `[]SessionPid` by the segment-host that
owns each `gp_segment_id` and issues one or more `GetPidProcStat` calls
per host. This is implemented in
[`ProcfsGatherStorage`](../internal/master/procfs_gather.go), which is
separate from the existing `segChan` puller machinery used for
`GetMetricQueries`.

The fan-out works as follows:

1. [`GatherProcfsStat()`](../internal/master/procfs_gather.go) calls
   `ListAllSessions(ctx)` to get the current PID list.
2. [`getJobsMap()`](../internal/master/procfs_gather.go) groups sessions
   by hostname (resolved via
   [`GetHostnameForSegindex()`](../internal/storage/config_storage.go)).
3. An `errgroup` launches one goroutine per host, each calling
   [`processProcfsRequests()`](../internal/master/procfs_gather.go).
4. Each goroutine opens a gRPC connection to the segment-host yagpcc and
   sends `GetPidProcStat` requests, batching up to `jobsPerQuery = 1000`
   `SegmentProcess` entries per RPC call to stay within message size
   limits.
5. Results from all hosts are collected under a mutex and returned as a
   flat `[]*GpPidProcInfo`.

Request and response messages are defined in:

```
// api/proto/agent_segment/yagpcc_get_service.proto

service GetQueryInfo {
    rpc GetMetricQueries (GetQueriesInfoReq) returns (GetQueriesInfoResponse) {}
    rpc GetPidProcStat   (GetPidProcInfoReq) returns (GetPidProcInfoResponse) {}
}

message SegmentProcess {
    int64 gp_segment_id = 1;
    int64 sess_id       = 2;
    int64 pid           = 3;
}

message GetPidProcInfoReq {
    repeated SegmentProcess segment_process = 1;
}

message GetPidProcInfoResponse {
    repeated GpPidProcInfo pid_proc_data = 1;
}
```

`GpPidProcInfo` (defined in
[api/proto/common/yagpcc_metrics.proto](../api/proto/common/yagpcc_metrics.proto))
carries the primary key (`gp_segment_id`, `sess_id`, `pid`), the process
`cmdline`, the parsed Greenplum command count (`ccnt`), and the parsed
`ProcStat` (from `/proc/<pid>/stat`), `ProcStatus` (from
`/proc/<pid>/status`), `ProcIO` (from `/proc/<pid>/io`), and `ProcSpill`
(from `gp_workfile_usage_per_query`, injected on the master — see §1.3):

```
message GpPidProcInfo {
    int64       gp_segment_id = 1;
    int64       sess_id       = 2;
    int64       pid           = 3;
    string      cmdline       = 4;
    string      state         = 5;
    ProcStat    proc_stat     = 6;
    ProcStatus  proc_status   = 7;
    ProcIO      proc_io       = 8;
    ProcSpill   proc_spill    = 9;   // injected from workfile_usage (master side)
    int32       ccnt          = 10;  // parsed from cmdline on the segment side
}

message ProcIO {
    int64 rchar                  = 1;  // bytes read via read-like syscalls
    int64 wchar                  = 2;  // bytes written via write-like syscalls
    int64 syscr                  = 3;  // read syscall count
    int64 syscw                  = 4;  // write syscall count
    int64 read_bytes             = 5;  // bytes fetched from storage
    int64 write_bytes            = 6;  // bytes sent to storage
    int64 cancelled_write_bytes  = 7;  // writes never persisted
}

message ProcSpill {
    int64 size  = 1;   // bytes spilled to disk by this PID (point-in-time snapshot)
    int64 files = 2;   // number of spill files created by this PID (snapshot)
}
```

All `ProcIO` fields are cumulative kernel counters. The master computes
**deltas** between two snapshots to derive interval-based metrics (see
§1.5). The numeric fields in `ProcStat`, `ProcStatus`, and `ProcIO` are
intentionally **signed** (`int32` / `int64` rather than the `uint*` types
used by `prometheus/procfs`) so the same layout can also carry deltas
without needing a parallel signed-delta schema. Counter values themselves
never exceed `2^63` for any realistic process lifetime, so the conversion
is lossless.

`ProcSpill` is different: it is a **point-in-time snapshot** (the current
number of bytes and files in `gp_toolkit.gp_workfile_usage_per_query` at
query time), not a monotonically increasing counter. It is therefore not
diffed — `ProcfsDiff` passes through the `last` value unchanged, and
aggregation simply sums the latest values across all PIDs in the session.

### 1.3 Workfile usage enrichment (master side, before storage)

Spill-file statistics (`ProcSpill`) are **not** collected via procfs on
the segment hosts. Instead, the master queries
`gp_toolkit.gp_workfile_usage_per_query` once per `RefreshProcfs` cycle
and **enriches** the just-gathered `[]*GpPidProcInfo` before storing them
in the ring buffer.

| Item | Location |
|------|----------|
| SQL query | `workfileUsageQuery` in [internal/gp/workfile_usage/lister.go](../internal/gp/workfile_usage/lister.go) |
| Row type | `WorkfileUsageEntry{Pid, SessID, CommandCnt, SegID, Size, NumFiles}` in [internal/gp/workfile_usage/models.go](../internal/gp/workfile_usage/models.go) |
| Background lister | [`workfile_usage.Lister`](../internal/gp/workfile_usage/lister.go) — started in `InitBG()`, polls every `defaultCollectionInterval` (2 s) |
| Enrichment function | [`enrichWithWorkfileUsage(procInfos, usageEntries)`](../internal/master/background.go) |
| Call site | [`RefreshProcfs()`](../internal/master/background.go) — called immediately after `GatherProcfsStat()` and before `RegisterProcfsStatWithDataQuality()` |

The enrichment step works as follows:

1. `RefreshProcfs` reads the current snapshot from the workfile-usage
   lister via `List(ctx)`.
2. It builds a lookup map keyed by `workfileUsageKey{SegID, Pid}`.
3. For each `GpPidProcInfo` returned by `GatherProcfsStat()`,
   `enrichWithWorkfileUsage()` looks up the corresponding entry in the
   map. If found, it sets `proc.ProcSpill = &ProcSpill{Size: e.Size, Files: e.NumFiles}`.
4. If no matching entry exists (the PID has no active spill), `ProcSpill`
   remains `nil`.

The workfile-usage lister itself queries the Greenplum master over a
regular database connection; unlike procfs data, spill-file statistics
are maintained by the database engine and are available directly from the
master without a per-host fan-out.

### 1.4 Segment side (stateless)

The segment-host yagpcc keeps **no local state** for proc-stats. On every
`GetPidProcStat` call it:

1. Iterates the `SegmentProcess` entries from the request.
2. For each `(gp_segment_id, sess_id, pid)` reads
   `/proc/<pid>/stat`, `/proc/<pid>/status`, `/proc/<pid>/io`,
   `/proc/<pid>/cmdline` via
   [`GetPidProcInfo()`](../internal/utils/procfs.go). While reading
   `cmdline`, it parses the `cmd<N>` token once and stores it as
   `GpPidProcInfo.ccnt`, so later query-level lookups do not repeatedly
   parse command lines from the in-memory procfs storage.
3. Skips entries where the process has already exited
   (`ErrProcessNotFound` / `ENOENT`) so that the master can detect
   process disappearance from the missing key alone.
4. If some PIDs error but at least one succeeds, returns the partial
   result. Only returns an error if **all** PIDs fail.
5. Returns the assembled `[]GpPidProcInfo`.

The handler is implemented in
[`GetQueryInfoServer.GetPidProcStat()`](../internal/grpc/get_query_info.go)
and is registered on both segment and master roles in
[`NewApp()`](../internal/app/app.go).

Note that `/proc/<pid>/io` counters (`read_bytes`, `write_bytes`,
`rchar`, `wchar`, `syscr`, `syscw`, `cancelled_write_bytes`) are also
surfaced through the `SystemStat` portion of the hook-collected
`GPMetrics` documented in
[API.md → SystemStat (procfs)](./API.md#systemstat-procfs). That path
delivers data only when the `yagp-hooks-collector` extension is loaded
in Greenplum and only for queries it can hook. `GetPidProcStat` is the
authoritative path for per-tick procfs sampling and works for every
backend in `pg_stat_activity` (including system processes that
`yagp-hooks-collector` never sees).

### 1.5 Master aggregation (snapshot-pair diffing, per session)

The master does **not** use EMA (exponential moving averages). Instead,
it stores raw snapshots in a ring buffer and computes deltas between any
two snapshots on demand.

The flow is driven by two independent ticker goroutines launched in
[`InitBG()`](../internal/master/background.go):

**Gather loop** — [`RefreshProcfs()`](../internal/master/background.go):

1. Fires every `procfs_refresh_interval` (config).
2. Creates a [`ProcfsGatherStorage`](../internal/master/procfs_gather.go)
   and calls `GatherProcfsStat()` to fan out to all segment hosts.
3. Calls `enrichWithWorkfileUsage()` to inject `ProcSpill` data from the
   workfile-usage lister into the gathered results (see §1.3).
4. Calls
   [`ProcfsStorage.RegisterProcfsStatWithDataQuality(time.Now(), result, expectedHosts, respondedHosts)`](../internal/storage/procfs_storage.go)
   to append the gathered `[]*GpPidProcInfo` as a new timestamped
   snapshot in the ring buffer together with procfs gather quality
   metadata.
5. `RegisterProcfsStatWithDataQuality` builds two maps for fast access:
   - `pidProcData` (`ProcMap`): keyed by `ProcKey{GpSegmentId, SessId, Pid}` → `*ProcStat`
   - `pidProcIndex` (`ProcIndexMap`): keyed by `ProcIndexKey{SessId}` → `[]*ProcIndexData{GpSegmentId, Pid}`
   - each `ProcStat` now also stores `ProcSpill` from the enrichment step
   - each snapshot stores `hostsExpected` and `hostsResponded`, later used
     to fill `DataQuality` on procfs-backed query-running-metrics responses.
6. Calls `TidyUpProcfsStat()` to trim the ring buffer to
   `maximumStoredPoints` (default 30).

**Session refresh loop** — [`RefreshSessions()`](../internal/master/background.go):

1. Fires every `session_refresh_interval`.
2. Calls
   [`TryRefreshSessionsFromGP()`](../internal/master/background.go)
   which, after refreshing the session list, calls
   [`RecalculateProcfsUsage()`](../internal/gp/sessions.go).
3. `RecalculateProcfsUsage()` collects all session IDs and calls
   [`ProcfsStorage.GetProcfsSessions(sessIds)`](../internal/storage/procfs_storage.go).
4. `GetProcfsSessions()` calls `get5Min()` to find the snapshot nearest
   to 5 minutes ago and pairs it with the latest snapshot.
5. For each session, [`getProcfsSession()`](../internal/storage/procfs_storage.go)
   iterates all `(GpSegmentId, Pid)` entries from the session's index in
   the **latest** snapshot, computes per-process deltas via
   [`ProcfsDiff()`](../internal/storage/procfs_group.go), and aggregates
   across segments via
   [`GroupProcfsMetrics()`](../internal/storage/procfs_group.go) using
   `AggSegmentHost` aggregation.
6. The aggregated `GpPidProcInfo` is converted to `GPMetrics` via
   [`procfsStatToLastStat()`](../internal/gp/sessions.go) and written
   into `SessionData.LongRunningGPMetrics`.

This means each session's `LastMetrics` (exposed on `SessionState` via
the existing `GetGPSessions` / `GetGPQuery` RPCs) contains the
**5-minute delta** of cluster-wide procfs counters, plus the **current
spill-file totals**, for that session:

| `LastMetrics` field | Source | Semantics |
|--------------------|--------|-----------|
| `SystemStat.UserTimeSeconds` | `Σ_segments Δ(ProcStat.Utime)` | 5-min CPU delta |
| `SystemStat.KernelTimeSeconds` | `Σ_segments Δ(ProcStat.Stime)` | 5-min kernel time delta |
| `SystemStat.Vsize` | per-host sum of `ProcStat.Vsize` (latest) | snapshot |
| `SystemStat.Rss` | per-host sum of `ProcStat.Rss` (latest) | snapshot |
| `SystemStat.Rchar` | `Σ_segments Δ(ProcIO.Rchar)` | 5-min IO delta |
| `SystemStat.Wchar` | `Σ_segments Δ(ProcIO.Wchar)` | 5-min IO delta |
| `SystemStat.Syscr` | `Σ_segments Δ(ProcIO.Syscr)` | 5-min IO delta |
| `SystemStat.Syscw` | `Σ_segments Δ(ProcIO.Syscw)` | 5-min IO delta |
| `SystemStat.ReadBytes` | `Σ_segments Δ(ProcIO.ReadBytes)` | 5-min IO delta |
| `SystemStat.WriteBytes` | `Σ_segments Δ(ProcIO.WriteBytes)` | 5-min IO delta |
| `SystemStat.CancelledWriteBytes` | `Σ_segments Δ(ProcIO.CancelledWriteBytes)` | 5-min IO delta |
| `Spill.FileCount` | `Σ_pids ProcSpill.Files` (latest snapshot) | current total |
| `Spill.TotalBytes` | `Σ_pids ProcSpill.Size` (latest snapshot) | current total |

The `Δ` notation means `nonNegativeDiff(first, last)` — if the counter
decreased (PID reuse or counter reset), the diff is clamped to zero.

Memory metrics (`Vsize`, `Rss`) use `AggSegmentHost` aggregation: within
a single host, values from multiple processes are summed into an
intermediate map keyed by `(MetricName, Hostname)`, then the per-host
total is used as the final value. This avoids double-counting when
multiple segments on the same host report the same process.

Spill metrics (`Spill.FileCount`, `Spill.TotalBytes`) are **not diffed**.
`ProcSpill` is a point-in-time snapshot from `gp_workfile_usage_per_query`,
so `ProcfsDiff` passes the `last` value through unchanged. The per-session
sum (`Σ_pids`) in `GroupProcfsMetrics` therefore reflects the current
total spill across all worker PIDs for the session at the time of the most
recent `RefreshProcfs` cycle. If a session has no active spill,
`ProcSpill` is `nil` for all its PIDs and `LastMetrics.Spill` will be
`nil` in the response.

### 1.6 End-to-end sequence

```mermaid
sequenceDiagram
    participant GP as Greenplum master (libpq)
    participant WU as workfile_usage lister
    participant M as Master yagpcc
    participant PS as ProcfsStorage (ring buffer)
    participant SS as SessionsStorage
    participant SH as Segment-host yagpcc
    participant Procfs as /proc on segment host

    Note over M: RefreshProcfs ticker fires
    M->>GP: gp_dist_random('pg_stat_activity') UNION pg_stat_activity
    GP-->>M: rows of (gp_segment_id, pid, sess_id, backend_type)
    Note over M: group by hostname,<br/>build GetPidProcInfoReq per host
    loop per segment host (parallel via errgroup)
        M->>SH: GetPidProcStat(SegmentProcess[])
        SH->>Procfs: read /proc/<pid>/{stat,status,io,cmdline}
        Procfs-->>SH: raw procfs data
        SH-->>M: GpPidProcInfo[]
    end
    M->>WU: List(ctx) — read cached workfile_usage snapshot
    WU-->>M: []WorkfileUsageEntry{SegID, Pid, Size, NumFiles}
    Note over M: enrichWithWorkfileUsage():<br/>match (SegID, Pid) → set ProcSpill on each GpPidProcInfo
    M->>PS: RegisterProcfsStatWithDataQuality(time.Now(), enriched, expectedHosts, respondedHosts)
    Note over PS: append snapshot (includes ProcSpill + DataQuality source counters),<br/>trim ring buffer

    Note over M: RefreshSessions ticker fires
    M->>SS: TryRefreshSessionsFromGP()
    SS->>PS: GetProcfsSessions(sessIds)
    Note over PS: get5Min() → find nearest<br/>snapshot to 5min ago
    Note over PS: for each session:<br/>ProcfsDiff(old, new) — ProcSpill taken from last<br/>GroupProcfsMetrics(AggSegmentHost) — sum ProcSpill across PIDs
    PS-->>SS: map[sessId]*GpPidProcInfo (ProcSpill = Σ per-PID spill)
    Note over SS: procfsStatToLastStat() →<br/>SpillInfo{FileCount, TotalBytes} → LongRunningGPMetrics.Spill
```

---

## 2. Storage architecture on the master

### 2.1 Ring-buffer snapshot store

Instead of maintaining per-process delta state and computing EMA rolling
averages, the master stores **raw snapshots** in a fixed-size ring buffer.
Any two snapshots can be diffed on demand to produce deltas over the
corresponding time window.

This design is simple, stateless between ticks, and naturally handles:

- **Missed ticks** — the ring buffer just has fewer snapshots; the
  nearest-snapshot search still works.
- **PID reuse** — `nonNegativeDiff()` clamps negative deltas to zero,
  so a counter reset produces a zero delta rather than a spike.
- **Process disappearance** — if a PID is absent from the latest
  snapshot, it simply doesn't contribute to the session's aggregated
  metrics.

### 2.2 Storage types

```go
// internal/storage/procfs_storage.go

type ProcKey struct {
    GpSegmentId int64
    SessId      int64
    Pid         int64
}

type ProcIndexKey struct {
    SessId int64
}

type ProcIndexData struct {
    GpSegmentId int64
    Pid         int64
}

type ProcStat struct {
    Cmdline    string
    State      string
    Ccnt       int32
    ProcStat   *pbc.ProcStat
    ProcStatus *pbc.ProcStatus
    ProcIO     *pbc.ProcIO
    ProcSpill  *pbc.ProcSpill  // injected from workfile_usage; nil when no active spill
}

type ProcMap      map[ProcKey]*ProcStat
type ProcIndexMap map[ProcIndexKey][]*ProcIndexData

type ProcfsStatType struct {
    statTime       time.Time
    pidProcData    ProcMap       // primary: (seg, sess, pid) → stats
    pidProcIndex   ProcIndexMap  // secondary: sess → [(seg, pid), ...]
    hostsExpected  int64         // number of segment hosts targeted by the procfs gather
    hostsResponded int64         // number of segment hosts that returned procfs data successfully
}

type ProcfsStorage struct {
    mx                  *sync.RWMutex
    procfsStat          []ProcfsStatType  // ring buffer, newest last
    maximumStoredPoints int               // default 30
}
```

Each snapshot (`ProcfsStatType`) contains:

- `statTime` — when the snapshot was taken.
- `pidProcData` — a map from `ProcKey{GpSegmentId, SessId, Pid}` to the
  parsed procfs data for that process.
- `pidProcIndex` — a secondary index from `ProcIndexKey{SessId}` to the
  list of `(GpSegmentId, Pid)` pairs belonging to that session, enabling
  efficient per-session lookups.
- `hostsExpected` / `hostsResponded` — procfs gather quality counters.
  They record how many segment hosts were targeted and how many responded
  successfully; query-level procfs APIs use them to fill `DataQuality`.

### 2.3 Snapshot lookup

[`getNearestNTimeUnlocked(d)`](../internal/storage/procfs_storage.go)
searches the ring buffer for the snapshot whose age (relative to the
newest snapshot) is closest to duration `d`. Since snapshots are stored
in chronological order, the search walks backwards from the newest entry
and stops as soon as the absolute difference starts growing (early exit).

Convenience wrappers:

| Method | Window |
|--------|--------|
| `get5Min()` | 5 minutes |
| `get15Min()` | 15 minutes |
| `get30Min()` | 30 minutes |

Currently, `GetProcfsSessions()` uses `get5Min()` to produce the
per-session deltas exposed via `LastMetrics`.

### 2.4 Diff and aggregation

**Per-process diff** —
[`ProcfsDiff(first, last)`](../internal/storage/procfs_group.go) produces
a `GpPidProcInfo` where:

- Snapshot fields (`Pid`, `Comm`, `State`, `Cmdline`, `ProcStatus`,
  `ProcSpill`, …) are taken from `last`.
- Cumulative counters (`Utime`, `Stime`, `MinFlt`, `MajFlt`, `Rchar`,
  `WriteBytes`, …) are diffed via `nonNegativeDiff(first, last)` — if
  `last < first` (counter reset / PID reuse), the result is `0`.
- `ProcSpill` is taken verbatim from `last` (it is a snapshot, not a
  cumulative counter, so no diff is computed).

**Per-session aggregation** —
[`GroupProcfsMetrics(dest, source, aggKind, segHostname, intermediateResults)`](../internal/storage/procfs_group.go)
merges one process's diff into a session-level accumulator:

- CPU counters (`Utime`, `Stime`, `Cutime`, `Cstime`, `GuestTime`,
  `CguestTime`) are **summed** across all segments.
- IO counters (`Rchar`, `Wchar`, `Syscr`, `Syscw`, `ReadBytes`,
  `WriteBytes`, `CancelledWriteBytes`) are **summed** across all
  segments.
- Memory gauges (`Vsize`, `Rss`) use an intermediate map keyed by
  `(MetricName, Hostname)` to first sum within each host, then either
  take the per-host value (`AggSegmentHost`) or the max across hosts
  (`AggMax`). The per-session path uses `AggSegmentHost`.
- Spill gauges (`ProcSpill.Size`, `ProcSpill.Files`) are **summed**
  across all PIDs in the session. On the first merge the source value is
  cloned; on subsequent merges the fields are added. A nil `ProcSpill`
  on a source entry is silently skipped.

### 2.5 Lifecycle / eviction

- **Ring buffer trimming** —
  [`TidyUpProcfsStat()`](../internal/storage/procfs_storage.go) is called
  after every `RegisterProcfsStatWithDataQuality()`. If the buffer exceeds
  `maximumStoredPoints`, the oldest snapshots are discarded.
- **No per-PID GC** — individual processes are not tracked across ticks.
  If a PID disappears from a snapshot, it simply won't appear in the
  diff. If a session disappears from `pg_stat_activity`, it is removed
  from `SessionsStorage` by `RefreshSessionList()` (when
  `clearDeletedSessions` is enabled), and its procfs data naturally
  stops being queried.

### 2.6 Exposure via existing gRPC / HTTP UI surfaces

Procfs data is exposed through the existing `SessionState.LastMetrics`
field and through the query runtime matrix API:

1. [`RecalculateProcfsUsage()`](../internal/gp/sessions.go) iterates all
   sessions and calls `GetProcfsSessions()`.
2. For each session with procfs data,
   [`procfsStatToLastStat()`](../internal/gp/sessions.go) converts the
   aggregated `GpPidProcInfo` into a `GPMetrics` with a populated
   `SystemStat` and, when spill data is present, a `SpillInfo`:

   ```go
   result.SystemStat = &pbc.SystemStat{
       UserTimeSeconds:   float64(procfsStat.ProcStat.Utime),
       KernelTimeSeconds: float64(procfsStat.ProcStat.Stime),
       Vsize:             uint64(procfsStat.ProcStat.Vsize),
       VmSizeKb:          uint64(procfsStat.ProcStat.Vsize) / 1024,
       Rss:               uint64(procfsStat.ProcStat.Rss),
       // ProcIO fields mapped to SystemStat IO fields...
   }
   // SpillInfo is only set when ProcSpill was present in the snapshot
   if procfsStat.ProcSpill != nil {
       result.Spill = &pbc.SpillInfo{
           FileCount:  int32(procfsStat.ProcSpill.Files),
           TotalBytes: procfsStat.ProcSpill.Size,
       }
   }
   ```

3. This is written to `SessionData.LongRunningGPMetrics`, which is
   exposed as `LastMetrics` on `SessionState` in the existing
   `GetGPSessions` / `GetGPQuery` RPCs.

Consumers calling `GetGPSessions` or `GetGPQuery` see the 5-minute
cluster-wide CPU / RSS / IO deltas for each session in `LastMetrics`,
alongside any hook-collected metrics in `TotalMetrics` and
`QueryMetrics`. If the session is currently producing spill files,
`LastMetrics.Spill` will also be populated with the current aggregate
spill byte count and file count across all its worker PIDs.

`GetGPQueryRunningMatrics` exposes per-cell procfs runtime data for a
single query. The master filters procfs entries by `(SessId, Ccnt)` using
the stored `GpPidProcInfo.ccnt`, aggregates PIDs into `CellMetrics`, and
returns `RuntimeMetrics` for each `(slice_id, segindex)` cell. The
`RuntimeMetrics.state` field carries the aggregated procfs process state;
the UI uses it to render idle cells as gray and active cells from green to
red based on CPU skew within the slice. Query-level `DataQuality` is
filled from `hostsExpected` / `hostsResponded` stored in the latest procfs
snapshot.

### 2.7 Spill-info data flow summary

```
gp_workfile_usage_per_query
        │  (polled by workfile_usage.Lister, every 2 s)
        ▼
WorkfileUsageEntry{SegID, Pid, Size, NumFiles}
        │
        │  enrichWithWorkfileUsage() — keyed by (SegID, Pid)
        ▼
GpPidProcInfo.ProcSpill{Size, Files}   ← per-PID snapshot
        │
        │  RegisterProcfsStatWithDataQuality() → stored in ProcStat.ProcSpill and snapshot DataQuality counters
        ▼
Ring buffer snapshot (ProcfsStatType)
        │
        │  ProcfsDiff(first, last) — ProcSpill taken from last (no diff)
        ▼
per-PID diff (ProcSpill = latest snapshot value)
        │
        │  GroupProcfsMetrics() — sum ProcSpill.Size and .Files across all PIDs
        ▼
session-level GpPidProcInfo.ProcSpill = Σ_pids (Size, Files)
        │
        │  procfsStatToLastStat()
        ▼
GPMetrics.Spill = SpillInfo{
    FileCount:  int32(ProcSpill.Files),
    TotalBytes: ProcSpill.Size,
}
        │
        │  written to SessionData.LongRunningGPMetrics
        ▼
SessionState.LastMetrics.Spill  (returned by GetGPSessions / GetGPQuery)
```

### 2.8 Configuration

| Knob | Default | Defined in |
|------|---------|------------|
| `procfs_refresh_interval` | per yagpcc.yaml | [internal/config/config.go](../internal/config/config.go) — `ProcfsRefreshInterval` |
| `segment_pull_threads` | per yagpcc.yaml | [internal/config/config.go](../internal/config/config.go) — used as `nPullers` for errgroup concurrency |
| `max_message_size` | per yagpcc.yaml | [internal/config/config.go](../internal/config/config.go) — gRPC max receive message size |
| `maximumStoredPoints` | `30` | [internal/storage/procfs_storage.go](../internal/storage/procfs_storage.go) — `WithMaximumStoredPoints()` option |
| `WithBackgroundAllSessionsCollectionInterval` | `60s` | [internal/gp/stat_activity/lister.go](../internal/gp/stat_activity/lister.go) — PID list refresh cadence |
| `WithBackgroundAllSessionsCacheTTL` | `600s` | same |
| `defaultCollectionInterval` (workfile_usage) | `2s` | [internal/gp/workfile_usage/lister.go](../internal/gp/workfile_usage/lister.go) — spill data refresh cadence |
| `defaultCacheTTL` (workfile_usage) | `180s` | same — max age before `List()` returns a stale error |

---

## 3. Out of scope / future work

The following are **not** part of the current implementation and may be
considered as future enhancements:

- **EMA-based rolling averages** — the current implementation uses
  simple snapshot-pair diffing over a fixed window (5 min). A future
  enhancement could add exponential moving averages with configurable
  time constants (e.g. 5 / 15 / 30 min) for smoother `top`-style
  output.
- **`GetClusterTop` RPC** — a dedicated RPC returning cluster-wide
  rollup metrics sorted by resource usage.
- **`ProcAvg` proto message** — a dedicated message for per-session
  rolling averages on `SessionState`.
- **Configurable diff windows** — currently hardcoded to 5 minutes in
  `GetProcfsSessions()`. Making the window configurable (or exposing
  5 / 15 / 30 min variants) would allow consumers to choose their
  preferred time horizon.
