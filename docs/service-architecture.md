# Service architecture

This document describes the **services**, **interfaces**, and **data flows** of the yagpcc (Yet Another Greenplum Command Center) system. For a high-level overview and diagram, see [Architecture overview](architecture.md). For the gRPC API reference, see [API description](API.md).

---

## Components

| Component | Description |
|-----------|--------------|
| **Greenplum** | The database cluster: one Master node and multiple Segment nodes. Queries are sent to the Master and executed in parallel on Segments. |
| **yagp-hooks-collector** | A Greenplum extension that runs inside each Master and Segment process. It uses query execution hooks to capture telemetry (metrics, plan info, timing) and sends it to the local yagpcc agent. |
| **yagpcc** | This project. A Go service that runs in two **roles**: **segment** (on each Segment host) and **master** (on the Master host). It receives telemetry, stores it, and (in master role) aggregates it and exposes it to consumers. |

---

## Roles and deployment

### yagpcc in segment role

- **Where:** One instance per **Segment host** (shared by all Segment processes on that host).
- **Config:** `role: segment` in `yagpcc.yaml`. Example: `cmd/server/yagpcc_segment.yaml`.
- **Responsibilities:**
  - Listen on a **Unix Domain Socket (UDS)** for gRPC calls from **yagp-hooks-collector** (same host).
  - Accept telemetry via **SetQueryInfo.SetMetricQuery** and store it in memory.
  - Listen on a **TCP port** (e.g. `listen_port: 1432`) for gRPC calls from the **master** yagpcc.
  - Serve **GetQueryInfo.GetMetricQueries** so the master can pull query/metric data.

### yagpcc in master role

- **Where:** One instance on the **Master host**.
- **Config:** `role: master` in `yagpcc.yaml`, plus `master_connection` and segment-pull settings. Example: `cmd/server/yagpcc_master.yaml`.
- **Responsibilities:**
  - Same as segment for **local** telemetry: UDS + **SetQueryInfo.SetMetricQuery**, store in memory.
  - Connect to the Greenplum **Master** via **libpq** to get cluster topology (Segment hosts and ports).
  - Periodically **pull** from each Segment host’s yagpcc over TCP gRPC (**GetQueryInfo.GetMetricQueries**).
  - **Merge** segment data with local Master data, build aggregates (sessions, queries, metrics).
  - Expose **GetGPInfo** and **ActionService** (sessions, queries, terminate, move to resource group) to external consumers over TCP gRPC.

---

## Interfaces

### 1. yagp-hooks-collector → local yagpcc (UDS, gRPC)

| Aspect | Detail |
|--------|--------|
| **Transport** | Unix Domain Socket (UDS). |
| **Protocol** | gRPC. |
| **Service** | **SetQueryInfo** (segment proto: `api/proto/agent_segment/yagpcc_set_service.proto`). |
| **RPC** | **SetMetricQuery**: sends query status, keys, segment key, metrics (e.g. `SetQueryReq`). |
| **Direction** | Collector (inside Greenplum) → yagpcc (same host). |
| **Used by** | Both segment and master yagpcc (for local Greenplum processes). |

Statistics are pushed as the query runs and when it finishes. If yagpcc is down or slow, the hook may drop data or timeout so that user queries are not blocked.

### 2. Master yagpcc → Greenplum Master (libpq)

| Aspect | Detail |
|--------|--------|
| **Transport** | TCP (libpq / PostgreSQL wire protocol). |
| **Purpose** | Get cluster configuration: list of Segment hosts and their ports (e.g. from `gp_segment_configuration`). |
| **Config** | `master_connection` in yagpcc.yaml (addrs, user, password, sslmode, etc.). |

Used to discover where to pull segment data from.

### 3. Master yagpcc → Segment-host yagpcc (TCP, gRPC)

| Aspect | Detail |
|--------|--------|
| **Transport** | TCP (e.g. `listen_port` on each Segment host). |
| **Protocol** | gRPC. |
| **Service** | **GetQueryInfo** (segment proto: `api/proto/agent_segment/yagpcc_get_service.proto`). |
| **RPC** | **GetMetricQueries**: master sends optional filters (query keys, time range); segment returns **GetQueriesInfoResponse** (e.g. `QueryData` with metrics). |
| **Direction** | Master yagpcc → segment yagpcc (pull). |
| **Frequency** | Configured by `segment_pull_rate_sec` and `segment_pull_threads` on the master. |
| **RPC** | **GetPidProcStat**: master sends a `GetPidProcInfoReq` listing `(gp_segment_id, sess_id, pid)` triples discovered via `gp_dist_random('pg_stat_activity')` on the Greenplum master; segment-host yagpcc reads `/proc/<pid>/{stat,status,io,cmdline}` and returns `GetPidProcInfoResponse` containing `GpPidProcInfo` per live process. Stateless on the segment. |
| **Aggregation** | Master sums deltas across segments per running query (cluster-wide CPU/RSS/IO right now) and feeds 5/15/30-minute EMAs per session and a single cluster-wide rollup. See [Per-process resource statistics](./proc-stats-flow.md). |

Master aggregates this with its own stored data to form a cluster-wide view.

### 4. External consumers → Master yagpcc (TCP, gRPC)

| Aspect | Detail |
|--------|--------|
| **Transport** | TCP (same `listen_port` as used for segment pull, or a dedicated endpoint depending on deployment). |
| **Protocol** | gRPC. |
| **Services** | **GetGPInfo** (sessions, queries, totals), **ActionService** (terminate query/session, move query to resource group). |
| **Proto** | `api/proto/agent_master/yagpcc_get_service.proto`, `api/proto/agent_master/yagpcc_action_service.proto`. |
| **Direction** | External clients → master yagpcc. |

See [API description](API.md) for full request/response and metrics.

---

## Data flow summary

```
Greenplum (Master + Segments)
    → [hooks] → yagp-hooks-collector
                    → [UDS gRPC SetQueryInfo] → local yagpcc (segment or master)

Master-host yagpcc
    ← [libpq] Greenplum Master (topology + gp_dist_random pg_stat_activity for PIDs)
    ← [TCP gRPC GetQueryInfo.GetMetricQueries] Segment-host yagpcc instances
    ← [TCP gRPC GetQueryInfo.GetPidProcStat] Segment-host yagpcc instances (procfs per PID)
    → [in-memory merge, EMA 5/15/30-min, aggregate]
    → [TCP gRPC GetGPInfo / ActionService] External consumers
```

- **Segment hosts:** Each segment-host yagpcc only has **local** data (its Segments). Data is **partial** per query.
- **Master host:** Master yagpcc has **local** data (Master process) plus **pulled** data from all Segment hosts. It **aggregates** (e.g. sum metrics, join by query/session keys) and serves the **full** view to consumers.

---

## Storage and lifecycle

- **Segment yagpcc:** Keeps received metrics in **memory** (and may support clearing after send via GetMetricQueries options). No persistent database in this project.
- **Master yagpcc:** Keeps aggregated sessions, queries, and metrics in **memory**. External systems (e.g. dashboards, historical store) consume via GetGPInfo and may persist data themselves.

---

## Related documentation

- [Architecture overview](architecture.md) — High-level diagram and flow (including Mermaid).
- [API description](API.md) — GetGPInfo and ActionService RPCs, messages, and metrics.
- [Per-process resource statistics](proc-stats-flow.md) — Procfs (`GetPidProcStat`) data flow and 5/15/30-minute top-style averages.
- [README](../README.md) — Build, configuration, and run.
