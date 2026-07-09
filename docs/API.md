# YAGPCC API Description

This document describes the gRPC API exposed by the yagpcc (Yet Another Greenplum Command Center) agent master, based on the definitions in `api/proto/agent_master/*.proto` and the common types in `api/proto/common/`.

For **detailed metric descriptions** and command center parameter reference, see [Yandex Cloud: Managed Greenplum — Command Center parameters](https://yandex.cloud/en/docs/managed-greenplum/concepts/command-center-parameters).

---

## Overview

The agent master exposes two gRPC services:

| Service        | Proto file                  | Purpose                          |
|----------------|-----------------------------|----------------------------------|
| **GetGPInfo**  | `yagpcc_get_service.proto`  | Read sessions, queries, and stats |
| **ActionService** | `yagpcc_action_service.proto` | Terminate sessions/queries, move query to resource group |

All definitions use package `yagpcc` and depend on `api/proto/common/yagpcc_metrics.proto` and `api/proto/common/yagpcc_session.proto`.

---

## GetGPInfo service

Service for querying Greenplum sessions, queries, and aggregate statistics.

### RPCs

| RPC | Request | Response | Description |
|-----|---------|----------|-------------|
| **GetGPSessions** | `GetGPSessionsReq` | `GetGPSessionsResponse` | List sessions with optional filters, fields, and pagination. Can show top or last query per session. |
| **GetGPQueries** | `GetGPQueriesReq` | `GetGPSessionsResponse` | List sessions filtered and ordered like GetGPSessions, used for query-centric listing. |
| **GetGPQuery** | `GetGPQueryReq` | `GetGPQueryResponse` | Get full query data (query stat + per-segment metrics) by query key. |
| **GetGPSession** | `GetGPSessionReq` | `GetGPSessionResponse` | Get a single session by session key. |
| **GetTotalSessionsStat** | `GetTotalSessionsReq` | `GetTotalSessionsResponse` | Get aggregate session count by state. |
| **GetGPExtensions** | `GetGPExtensionsReq` | `GetGPExtensionsResponse` | Get installed extensions for all databases. |
| **ListDatabases** | `ListDatabasesReq` | `ListDatabasesResponse` | List available databases. |
| **GetGPQueryRunningMatrics** | `GetGPQueryRunningMatricsReq` | `GetGPQueryRunningMatricsResponse` | Get procfs runtime metrics matrix (slice × segment) for a running query. |
| **GetGPHostsRunningQueries** | `GetGPHostsRunningQueriesReq` | `GetGPHostsRunningQueriesResponse` | Get aggregated host-level statistics for all hosts with running queries. |
| **GetGPHostRunningQueries** | `GetGPHostRunningQueriesReq` | `GetGPHostRunningQueriesResponse` | Get per-query details for a single host. |
| **GetGpPidProcInfo** | `GetGpPidProcInfoReq` | `GpPidProcInfo` | Get procfs process info for a specific segment/slice of a query. |

### Request/response types

#### GetGPSessionsReq

| Field | Type | Description |
|-------|------|-------------|
| `field` | `repeated SessionFieldWrapper` | Fields to return and sort order. |
| `filter` | `repeated SessionFilter` | Filters (host, user, database, state, etc.). |
| `page_size` | `int64` | Page size for pagination. |
| `page_token` | `string` | Token for next page (from previous response). |
| `show_query_type` | `RunningQueryType` | `RQT_TOP` = top query, `RQT_LAST` = last executed query. |

#### GetGPSessionsResponse

| Field | Type | Description |
|-------|------|-------------|
| `sessions_state` | `repeated SessionState` | Session records. |
| `next_page_token` | `string` | Token for next page; empty if no more. |

#### GetGPQueriesReq

| Field | Type | Description |
|-------|------|-------------|
| `field` | `repeated SessionFieldWrapper` | Fields and sort order. |
| `filter` | `repeated SessionFilter` | Filters. |
| `page_size` | `int64` | Page size. |
| `page_token` | `string` | Pagination token. |

#### GetGPQueryReq / GetGPQueryResponse

- **GetGPQueryReq**: `query_key` (`QueryKey`) — identifies the query.
- **GetGPQueryResponse**: `queries_data` (`TotalQueryData`) — `QueryStat` plus `repeated SegmentMetrics`.

#### GetGPSessionReq / GetGPSessionResponse

- **GetGPSessionReq**: `session_key` (`SessionKey`).
- **GetGPSessionResponse**: `sessions_state` (`SessionState`).

#### GetTotalSessionsReq / GetTotalSessionsResponse

- **GetTotalSessionsReq**: empty.
- **GetTotalSessionsResponse**: `sessions_stat` — `repeated SessionStat` (each has `state` string and `count` double).

#### GetGPExtensionsReq / GetGPExtensionsResponse

- **GetGPExtensionsReq**: optional `database_name` (empty = all databases).
- **GetGPExtensionsResponse**: `databases` — `repeated DatabaseExtensionsInfo`.

#### ListDatabasesReq / ListDatabasesResponse

- **ListDatabasesReq**: empty.
- **ListDatabasesResponse**: `databases` — `repeated DatabaseInfo` (each has `name` string).

#### GetGPQueryRunningMatricsReq / GetGPQueryRunningMatricsResponse

- **GetGPQueryRunningMatricsReq**: `query_key` (`QueryKey`).
- **GetGPQueryRunningMatricsResponse**:
  - `slice_id` (`repeated int64`) — all slice IDs observed.
  - `segindex` (`repeated int32`) — all segment indices observed.
  - `cell_metrics` (`repeated CellMetrics`) — one entry per (slice, segment) cell.
  - `skew` (`Skew`) — CPU-time skew across segments.
  - `data_quality` (`DataQuality`) — completeness/freshness metadata.

**CellMetrics**:

| Field | Type | Description |
|-------|------|-------------|
| `slice_id` | `int64` | Slice ID. |
| `segindex` | `int32` | Segment index. |
| `runtime_metrics` | `RuntimeMetrics` | Process-level runtime metrics for this cell. |

#### GetGPHostsRunningQueriesReq / GetGPHostsRunningQueriesResponse

- **GetGPHostsRunningQueriesReq**: empty.
- **GetGPHostsRunningQueriesResponse**: `hosts` — `repeated RunningHostInfo`.

**RunningHostInfo**:

| Field | Type | Description |
|-------|------|-------------|
| `host_name` | `string` | Hostname. |
| `segindex` | `repeated int32` | Segment indices on this host. |
| `active_queries` | `int64` | Number of unique active (ssid, ccnt) pairs. |
| `active_slices` | `int64` | Number of unique active slices. |
| `cpu_usage` | `double` | Sum of CPU time (utime + stime) across all query processes, in clock ticks. |
| `memory_usage` | `int64` | Sum of RSS (resident set size) across all query processes, in bytes. |
| `disk_usage` | `int64` | Sum of disk I/O (read_bytes + write_bytes) across all query processes, in bytes. |
| `spill_bytes` | `int64` | Sum of spill bytes across all query processes. |
| `skew` | `Skew` | CPU-time skew across segments on this host. |
| `data_quality` | `DataQuality` | Completeness/freshness of the data. |
| `avg5` | `double` | 5-minute system load average (from `/proc/loadavg`). |
| `disk_reads` | `int64` | Completed disk read operations (from `/proc/stat`). |
| `disk_writes` | `int64` | Completed disk write operations (from `/proc/stat`). |
| `total_sessions` | `int64` | Number of unique sessions (ssid values) on this host, including idle ones. For master processes (segindex == -1), the local hostname is used. |

#### GetGPHostRunningQueriesReq / GetGPHostRunningQueriesResponse

- **GetGPHostRunningQueriesReq**: `host_name` (string).
- **GetGPHostRunningQueriesResponse**:
  - `host_name` (`string`) — hostname.
  - `segindex` (`repeated int32`) — segment indices on this host.
  - `queries` (`repeated RunningQueryInfo`) — per-query details.
  - `data_quality` (`DataQuality`).
  - `next_page_token` (`string`) — pagination token.

**RunningQueryInfo**:

| Field | Type | Description |
|-------|------|-------------|
| `query_key` | `QueryKey` | Query identifier. |
| `user_name` | `string` | Database user. |
| `db_name` | `string` | Database name. |
| `query_text` | `string` | Query text. |
| `runtime_metrics` | `RuntimeMetrics` | Process-level runtime metrics. |

#### GetGpPidProcInfoReq / GpPidProcInfo

- **GetGpPidProcInfoReq**: `query_key` (`QueryKey`), `segindex` (`int32`), `slice_id` (`int64`).
- **GpPidProcInfo** (see "Common types" section below for full field list).

### Enums used by GetGPInfo

- **RunningQueryType**: `RQT_UNSPECIFIED`, `RQT_TOP` (show top query), `RQT_LAST` (show last executed query).
- **StatKind**: `SK_UNSPECIFIED`, `SK_PRECISE` (per-query metrics), `SK_AGGREGATED` (interval-aggregated metrics; interval in `start_time`/`end_time`).
- **SortOrder**: `SORT_ORDER_UNSPECIFIED`, `SORT_ASC`, `SORT_DESC`.

### Query and segment data (GetGPInfo)

#### TotalQueryData

| Field | Type | Description |
|-------|------|-------------|
| `query_stat` | `QueryStat` | Master query statistics. |
| `segment_query_metrics` | `repeated SegmentMetrics` | Per-segment metrics for this query. |

#### QueryStat

| Field | Type | Description |
|-------|------|-------------|
| `cluster_id` | `string` | Cluster identifier. |
| `hostname` | `string` | Master hostname. |
| `collect_time` | `Timestamp` | When stats were collected. |
| `query_key` | `QueryKey` | Query identifier. |
| `query_info` | `QueryInfo` | Query attributes. |
| `stat_kind` | `StatKind` | Precise vs aggregated. |
| `query_status` | `QueryStatus` | Status enum. |
| `start_time` | `Timestamp` | Query start. |
| `end_time` | `Timestamp` | Query end. |
| `completed` | `bool` | Whether query completed. |
| `total_query_metrics` | `GPMetrics` | Sum of segment metrics. |
| `aggregated_metrics` | `AggregatedMetrics` | Set when `stat_kind == SK_AGGREGATED` (short queries). |
| `blocked_by_sess_id` | `int64` | Session ID blocking this query. |
| `wait_mode` | `string` | Desired lock mode. |
| `locked_item` | `string` | Locked relation or object type. |
| `locked_mode` | `string` | Lock mode held by blocking session. |
| `session_state` | `string` | Session state. |
| `message` | `string` | Error message for failed queries. |
| `slices` | `int64` | Number of registered slices. |

#### AggregatedMetrics (for SK_AGGREGATED)

| Field | Type | Description |
|-------|------|-------------|
| `calls` | `int64` | Number of executions. |
| `min_time` | `double` | Min execution time. |
| `max_time` | `double` | Max execution time. |
| `mean_time` | `double` | Mean execution time. |
| `stddev_time` | `double` | Stddev execution time. |
| `total_time` | `double` | Total execution time. |

#### SegmentMetrics

| Field | Type | Description |
|-------|------|-------------|
| `cluster_id` | `string` | Cluster ID. |
| `hostname` | `string` | Host. |
| `collect_time` | `Timestamp` | Collection time. |
| `segment_key` | `SegmentKey` | Segment identifier. |
| `query_status` | `QueryStatus` | Status. |
| `start_time` | `Timestamp` | Start. |
| `end_time` | `Timestamp` | End. |
| `segment_metrics` | `GPMetrics` | Metrics for this segment. |
| `query_key` | `QueryKey` | When used outside TotalQueryData. |
| `query_info` | `QueryInfo` | When used outside TotalQueryData. |

---

## ActionService

Service for terminating sessions/queries and moving a query to a resource group.

### RPCs

| RPC | Request | Response | Description |
|-----|---------|----------|-------------|
| **MoveQueryToResourceGroup** | `MoveQueryToResourceGroupRequest` | `google.protobuf.Empty` | Move the given query to the specified resource group. |
| **TerminateQuery** | `TerminateQueryRequest` | `TerminateResponse` | Terminate a query by query key. |
| **TerminateSession** | `TerminateSessionRequest` | `TerminateResponse` | Terminate a session by session key. |

### Request/response types

#### MoveQueryToResourceGroupRequest

| Field | Type | Description |
|-------|------|-------------|
| `query_key` | `QueryKey` | Query to move. |
| `resource_group_name` | `string` | Target resource group name. |

#### TerminateQueryRequest

| Field | Type | Description |
|-------|------|-------------|
| `query_key` | `QueryKey` | Query to terminate. |

#### TerminateSessionRequest

| Field | Type | Description |
|-------|------|-------------|
| `session_key` | `SessionKey` | Session to terminate. |

#### TerminateResponse

| Field | Type | Description |
|-------|------|-------------|
| `status_code` | `TerminateResponseStatusCode` | `SUCCESS` or `ERROR`. |
| `status_text` | `string` | Human-readable status. |

**TerminateResponseStatusCode**: `TERMINATE_RESPONSE_STATUS_CODE_UNSPECIFIED`, `TERMINATE_RESPONSE_STATUS_CODE_SUCCESS`, `TERMINATE_RESPONSE_STATUS_CODE_ERROR`.

---

## Common types (session and keys)

Defined in `api/proto/common/yagpcc_session.proto`.

### SessionKey

Cluster-wide unique session identifier.

| Field | Type | Description |
|-------|------|-------------|
| `sess_id` | `int64` | Session ID from `pg_stat_activity`. |
| `tm_id` | `int64` | Epoch of postmaster start; ensures uniqueness. |

### SessionInfo

Session attributes (aligned with `pg_stat_activity`).

| Field | Type | Description |
|-------|------|-------------|
| `pid` | `int64` | Backend process ID. |
| `database` | `string` | Database name. |
| `user` | `string` | Username. |
| `application_name` | `string` | Application name. |
| `client_addr` | `string` | Client IP. |
| `client_hostname` | `string` | Client hostname (if log_hostname enabled). |
| `client_port` | `int64` | Client port. |
| `backend_start` | `Timestamp` | Backend start time. |
| `xact_start` | `Timestamp` | Transaction start. |
| `query_start` | `Timestamp` | Query start. |
| `state_change` | `Timestamp` | Last state change. |
| `waiting_reason` | `string` | e.g. lock, replication, resgroup. |
| `waiting` | `bool` | Waiting on lock. |
| `state` | `string` | Session state. |
| `backend_xid` | `string` | Top-level transaction ID. |
| `backend_xmin` | `string` | Session xmin. |
| `rsgid` | `int64` | Resource group OID. |
| `rsgname` | `string` | Resource group name. |
| `rsgqueueduration` | `string` | Time queued in resource group. |
| `blocked_by_sess_id` | `int64` | Session ID blocking this one. |
| `wait_mode` | `string` | Desired lock mode. |
| `locked_item` | `string` | Locked relation/object. |
| `locked_mode` | `string` | Lock mode held by blocker. |

### SessionState

Full session snapshot returned by GetGPSessions / GetGPSession.

| Field | Type | Description |
|-------|------|-------------|
| `time` | `Timestamp` | Collection time. |
| `cluster_id` | `string` | Cluster ID. |
| `hostname` | `string` | Master hostname. |
| `session_key` | `SessionKey` | Session identifier. |
| `session_info` | `SessionInfo` | Session attributes. |
| `running_query` | `QueryKey` | Current query key. |
| `running_query_info` | `QueryInfo` | Current query info. |
| `running_query_status` | `QueryStatus` | Current query status. |
| `total_metrics` | `GPMetrics` | Session-wide aggregated metrics. |
| `last_metrics` | `GPMetrics` | Session metrics in last 1h window. |
| `query_metrics` | `GPMetrics` | Metrics for current query. |
| `running_query_level` | `int64` | Nesting level of current query. |
| `running_queries_stack` | `repeated QueryDesc` | Stack of nested queries. |
| `blocked_session_level` | `int64` | Blocked level if blocked. |
| `running_query_error` | `string` | Error for current query. |
| `running_query_slices` | `int64` | Number of slices for current query. |

### QueryDesc

| Field | Type | Description |
|-------|------|-------------|
| `query_key` | `QueryKey` | Query identifier. |
| `query_info` | `QueryInfo` | Query attributes. |
| `query_status` | `QueryStatus` | Status. |
| `query_level` | `int64` | Nesting level. |
| `query_start` | `Timestamp` | Query start. |

---

## Common types (metrics and query keys)

Defined in `api/proto/common/yagpcc_metrics.proto`.

### QueryKey

Cluster-wide unique query identifier.

| Field | Type | Description |
|-------|------|-------------|
| `tmid` | `int32` | Distributed transaction timestamp (unix). |
| `ssid` | `int32` | Session ID from `pg_stat_activity`. |
| `ccnt` | `int32` | Command count (`gp_command_count`). |

### SegmentKey

| Field | Type | Description |
|-------|------|-------------|
| `dbid` | `int32` | Database id. |
| `segindex` | `int32` | Content: -1 = master, 0..n-1 = segment; primary and mirror share segindex. |

### QueryStatus

| Value | Description |
|-------|-------------|
| `QUERY_STATUS_UNSPECIFIED` | Unknown. |
| `QUERY_STATUS_SUBMIT` | Submitted. |
| `QUERY_STATUS_START` | Started. |
| `QUERY_STATUS_DONE` | Done. |
| `QUERY_STATUS_QUERY_DONE` | Query done. |
| `QUERY_STATUS_ERROR` | Error. |
| `QUERY_STATUS_CANCELLING` | Cancelling. |
| `QUERY_STATUS_CANCELED` | Canceled. |
| `QUERY_STATUS_END` | End. |

### QueryInfo

| Field | Type | Description |
|-------|------|-------------|
| `generator` | `PlanGenerator` | GPORCA vs legacy planner. |
| `query_id` | `uint64` | Normalized query hash. |
| `plan_id` | `uint64` | Normalized plan hash. |
| `query_text` | `string` | Raw query text. |
| `plan_text` | `string` | Raw plan text. |
| `userName` | `string` | User. |
| `databaseName` | `string` | Database. |
| `rsgname` | `string` | Resource group. |
| `analyze_text` | `string` | EXPLAIN ANALYZE text. |
| `submit_time` | `Timestamp` | Submit time. |
| `start_time` | `Timestamp` | Start time. |
| `end_time` | `Timestamp` | End time. |

**PlanGenerator**: `PLAN_GENERATOR_UNSPECIFIED`, `PLAN_GENERATOR_PLANNER` (legacy), `PLAN_GENERATOR_OPTIMIZER` (GPORCA).

### GPMetrics

Container for system and Greenplum statistics (session or query).

| Field | Type | Description |
|-------|------|-------------|
| `systemStat` | `SystemStat` | Procfs-based system stats. |
| `instrumentation` | `MetricInstrumentation` | Plan node / buffer / interconnect stats. |
| `spill` | `SpillInfo` | Spill file stats. |

### GpPidProcInfo

Process-level information collected from `/proc` filesystem for each Greenplum backend process.

| Field | Type | Description |
|-------|------|-------------|
| `gp_segment_id` | `int64` | Segment ID. |
| `sess_id` | `int64` | Session ID. |
| `pid` | `int64` | Process ID. |
| `cmdline` | `string` | Full command line. |
| `state` | `string` | Process state (e.g. "running", "idle"). |
| `proc_stat` | `ProcStat` | Contents of `/proc/pid/stat`. |
| `proc_status` | `ProcStatus` | Contents of `/proc/pid/status`. |
| `proc_io` | `ProcIO` | Contents of `/proc/pid/io`. |
| `proc_spill` | `ProcSpill` | Spill file information. |
| `ccnt` | `int32` | Greenplum command count parsed from cmdline (-1 if not available). |
| `slice_id` | `int64` | Greenplum slice ID parsed from cmdline (-1 if not available). |

### RuntimeMetrics

Aggregated runtime metrics for a single process or cell in the running-metrics matrix.

| Field | Type | Description |
|-------|------|-------------|
| `utime` | `int64` | User-mode CPU time in clock ticks. |
| `stime` | `int64` | Kernel-mode CPU time in clock ticks. |
| `vm_peak` | `int64` | Peak virtual memory size. |
| `vm_rss` | `int64` | Resident set size (RssAnon + RssFile + RssShmem). |
| `proc_io` | `ProcIO` | I/O statistics from `/proc/pid/io`. |
| `proc_spill` | `ProcSpill` | Spill file statistics. |
| `state` | `string` | Aggregated process state for this cell. |

### DataQuality

Metadata about completeness and freshness of collected data.

| Field | Type | Description |
|-------|------|-------------|
| `segments_expected` | `int64` | Number of segments expected to send data. |
| `segments_received` | `int64` | Number of segments that actually sent data. |
| `is_partial` | `bool` | True when data is incomplete. |
| `freshness_ms` | `int64` | Milliseconds since data was received. |

### Skew

CPU-time skew information across segments.

| Field | Type | Description |
|-------|------|-------------|
| `skew` | `double` | Skew ratio (max / mean CPU time). |
| `segindex` | `int32` | Segment index with the highest CPU time. |

---

## Metrics reference

Metric semantics and aggregation are documented in `api/proto/common/yagpcc_metrics.proto`. The Yandex Cloud documentation [Command Center parameters](https://yandex.cloud/en/docs/managed-greenplum/concepts/command-center-parameters) provides the full parameter and metric reference for the managed Greenplum command center; the following aligns with the proto and common command-center categories.

### SystemStat (procfs)

| Field | Source | Description | Aggregation |
|-------|--------|-------------|-------------|
| `runningTimeSeconds` | derived | User + kernel CPU time | sum |
| `userTimeSeconds` | `/proc/pid/stat` utime | CPU time in user space | sum |
| `kernelTimeSeconds` | `/proc/pid/stat` stime | CPU time in kernel space | sum |
| `vsize` | `/proc/pid/stat` vsize | Virtual memory size (bytes) | max |
| `rss` | `/proc/pid/stat` rss | Physical memory (RAM) | max |
| `VmPeakKb` | `/proc/pid/status` VmPeak | Peak virtual memory | max |
| `rchar` | `/proc/pid/io` rchar | Bytes read (read-like syscalls) | sum |
| `wchar` | `/proc/pid/io` wchar | Bytes written (write-like syscalls) | sum |
| `syscr` | `/proc/pid/io` syscr | Read syscalls | sum |
| `syscw` | `/proc/pid/io` syscw | Write syscalls | sum |
| `read_bytes` | `/proc/pid/io` read_bytes | Bytes read from storage | sum |
| `write_bytes` | `/proc/pid/io` write_bytes | Bytes written to storage | sum |
| `cancelled_write_bytes` | `/proc/pid/io` cancelled_write_bytes | Writes not persisted | sum |

### MetricInstrumentation

| Field | Description | Aggregation |
|-------|-------------|-------------|
| `ntuples` | Tuples produced by plan nodes | sum |
| `nloops` | Plan node execution count | sum |
| `tuplecount` | Tuples emitted in current cycle | sum |
| `firsttuple` | Time to first tuple | sum |
| `startup` | Startup (cost estimation) time | sum |
| `total` | Total execution time | sum |
| `shared_blks_hit` / `_read` / `_dirtied` / `_written` | Shared buffer blocks | sum |
| `local_blks_hit` / `_read` / `_dirtied` / `_written` | Local buffer blocks | sum |
| `temp_blks_read` / `temp_blks_written` | Temp blocks | sum |
| `blk_read_time` / `blk_write_time` | Block I/O time | sum |
| `sent` / `received` | `NetworkStat` (total_bytes, tuple_bytes, chunks) for Motion layer | sum |
| `startup_time` | Startup (planning + queue) time | sum |
| `inherited_calls` | Nested (sub-)query executions | sum |
| `inherited_time` | Time in nested queries | sum |
| `interconnect` | `InterconnectStat` | Network stat for Interconnect layer |

### InterconnectStat

| Field | Description | Aggregation |
|-------|-------------|-------------|
| `total_recv_queue_size` | Receive queue size | max |
| `total_capacity` | Send queue capacity | max |
| `total_buffers` | Buffers before send | min |
| `active_connections_num` | Outgoing connections | max |
| `retransmits` | Retransmits | sum |
| `snd_pkt_num` / `recv_pkt_num` | Packets sent/received | sum |
| `mismatch_num` / `crc_errors` | Bad packets | sum |
| `disordered_pkt_num` / `duplicated_pkt_num` | Order/dup packets | sum |
| `recv_ack_num` / `status_query_msg_num` | ACKs and status messages | sum |
| `startup_cached_pkt_num` | Cached future packets (when gp_interconnect_cache_future_packets=on) | sum |
| Counting-time fields | Counts of when queue/buffer/capacity was computed | sum |

### SpillInfo

| Field | Description | Aggregation |
|-------|-------------|-------------|
| `fileCount` | Number of spill files | sum |
| `totalBytes` | Total spill size | sum |

### NetworkStat (inside instrumentation.sent/received)

| Field | Description | Aggregation |
|-------|-------------|-------------|
| `total_bytes` | Total bytes (with headers) | sum |
| `tuple_bytes` | Tuple payload bytes | sum |
| `chunks` | Chunk count | sum |

---

## Session fields and filters (GetGPSessions / GetGPQueries)

### SessionFieldWrapper

Used to request and sort by a session/query field.

| Field | Type | Description |
|-------|------|-------------|
| `field_name` | `SessionField` | Field to return/sort by. |
| `order` | `SortOrder` | ASC/DESC. |

### SessionField (selection of main values)

Session and query attributes used as field names:

- **Session**: `SESSION_FIELD_KEY`, `SESSION_FIELD_HOST`, `SESSION_FIELD_PID`, `SESSION_FIELD_DATABASE`, `SESSION_FIELD_USER`, `SESSION_FIELD_APPLICATION_NAME`, `SESSION_FIELD_CLIENT_ADDR`, `SESSION_FIELD_CLIENT_HOSTNAME`, `SESSION_FIELD_CLIENT_PORT`, `SESSION_FIELD_BACKEND_START`, `SESSION_FIELD_XACT_START`, `SESSION_FIELD_QUERY_START`, `SESSION_FIELD_STATE_CHANGE`, `SESSION_FIELD_WAITING_REASON`, `SESSION_FIELD_WAITING`, `SESSION_FIELD_STATE`, `SESSION_FIELD_BACKEND_XID`, `SESSION_FIELD_BACKEND_XMIN`, `SESSION_FIELD_RSGID`, `SESSION_FIELD_RSGNAME`, `SESSION_FIELD_RSGQUEUEDURATION`, `SESSION_FIELD_BLOCKED_BY`, `SESSION_FIELD_BLOCKED_REASON`, `SESSION_FIELD_RUNNING_QUERY`, `SESSION_FIELD_RUNNING_QUERY_STATUS`.
- **Query**: `QUERY_GENERATOR`, `QUERY_QUERY_ID`, `QUERY_PLAN_ID`, `QUERY_QUERY_TEXT`, `QUERY_PLAN_TEXT`, `QUERY_TEMPLATE_QUERY_TEXT`, `QUERY_TEMPLATE_PLAN_TEXT`, `QUERY_USERNAME`, `QUERY_DATABASENAME`, `QUERY_RSGNAME`.
- **Metrics (total/last/query)**: e.g. `TOTAL_RUNNINGTIMESECONDS`, `TOTAL_USERTIMESECONDS`, `TOTAL_KERNELTIMESECONDS`, `TOTAL_VSIZE`, `TOTAL_RSS`, `TOTAL_*` / `LAST_*` / `QUERY_*` for the same metric names (including spill, interconnect, network sent/recv). See proto `SessionField` enum for the full list (TOTAL_*, LAST_*, QUERY_*).

### SessionFilter

| Field | Type | Description |
|-------|------|-------------|
| `field_name` | `SessionFilterEnum` | Filter type. |
| `value` | `string` | Filter value. |

**SessionFilterEnum**: `SESSION_FILTER_ENUM_UNSPECIFIED`, `SESSION_FILTER_HOST`, `SESSION_FILTER_USER`, `SESSION_FILTER_DATABASE`, `SESSION_FILTER_APPLICATION_NAME`, `SESSION_FILTER_CLIENT_HOSTNAME`, `SESSION_FILTER_STATE`, `SESSION_FILTER_RSGNAME`, `SESSION_FILTER_SESS_ID`, `SESSION_FILTER_TM_ID`.

### DatabaseExtensionsInfo

Information about extensions in a specific database.

| Field | Type | Description |
|-------|------|-------------|
| `database_name` | `string` | Name of the database. |
| `extensions` | `repeated PgExtensionInfo` | List of extensions in this database. |
| `error` | `string` | Error message if extension query failed for this database. |

### PgExtensionInfo

Information about a single PostgreSQL extension.

| Field | Type | Description |
|-------|------|-------------|
| `ext_name` | `string` | Extension name. |
| `ext_owner` | `string` | Extension owner. |
| `ext_namespace` | `string` | Extension namespace. |
| `ext_relocatable` | `bool` | Whether the extension is relocatable. |
| `ext_version` | `string` | Extension version. |

---

## CSV HTTP API

The master yagpcc also exposes an **HTTP service** that mirrors the gRPC **GetGPInfo** service but returns data in **CSV format**. This is useful for quick data export, spreadsheet integration, and scripting with tools like `curl`.

The CSV HTTP server listens on the port configured by `csv_port` (default **1440**) and binds to `[::1]` (localhost IPv6). It is available only on the **master** role.

All endpoints accept only **GET** requests. Business logic (filtering, sorting, pagination) is delegated to the same `GetMasterInfoServer` implementation used by gRPC, so results are identical.

### Endpoints

| Endpoint | gRPC equivalent | Description |
|----------|-----------------|-------------|
| `GET /csv/sessions` | `GetGPSessions` | List sessions with optional filters, sorting, and pagination. |
| `GET /csv/session` | `GetGPSession` | Get a single session by session key. |
| `GET /csv/queries` | `GetGPQueries` | List sessions in query-centric mode. |
| `GET /csv/query` | `GetGPQuery` | Get full query data (query stat + per-segment metrics) by query key. |
| `GET /csv/total_sessions_stat` | `GetTotalSessionsStat` | Get aggregate session count by state. |
| `GET /csv/extensions` | `GetGPExtensions` | Get installed extensions for all databases. |

### Query parameters

#### GET /csv/sessions

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `show_system` | bool | `false` | Include system sessions. |
| `show_query_type` | string | `RQT_UNSPECIFIED` | `RQT_TOP` (top query) or `RQT_LAST` (last executed query). |
| `hide_empty_queries` | bool | `false` | Hide sessions with no running query. |
| `page_size` | int | `100` | Number of sessions per page. |
| `page_token` | string | | Token from previous response for next page. |
| `sort` | string | | Repeatable. Format: `FIELD_NAME:ASC` or `FIELD_NAME:DESC`. Field names match `SessionField` enum (e.g. `TOTAL_RUNNINGTIMESECONDS:DESC`). |
| `filter_host` | string | | Filter by hostname. |
| `filter_user` | string | | Filter by username. |
| `filter_database` | string | | Filter by database. |
| `filter_application_name` | string | | Filter by application name. |
| `filter_client_hostname` | string | | Filter by client hostname. |
| `filter_state` | string | | Filter by session state. |
| `filter_rsgname` | string | | Filter by resource group name. |
| `filter_sess_id` | string | | Filter by session ID. |
| `filter_tm_id` | string | | Filter by transaction manager ID. |

#### GET /csv/session

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `sess_id` | int64 | **yes** | Session ID to look up. |

#### GET /csv/queries

Same parameters as `/csv/sessions` except `show_system`, `show_query_type`, and `hide_empty_queries` are not applicable.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `page_size` | int | `100` | Number of results per page. |
| `page_token` | string | | Pagination token. |
| `sort` | string | | Repeatable. Same format as `/csv/sessions`. |
| `filter_*` | string | | Same filter parameters as `/csv/sessions`. |

#### GET /csv/query

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `ssid` | int32 | **yes** | Session ID component of the query key. |
| `ccnt` | int32 | **yes** | Command count component of the query key. |

#### GET /csv/total_sessions_stat

No parameters. Returns aggregate session counts by state.

### Response format

All endpoints return `Content-Type: text/csv; charset=utf-8` with a `Content-Disposition: attachment` header.

- **Sessions endpoints** (`/csv/sessions`, `/csv/session`, `/csv/queries`): Each row is a flattened `SessionState` with ~220 columns covering session info, query info, and all metrics (total, last, query). If pagination produces a next page, the response includes an `X-Next-Page-Token` header.
- **Query endpoint** (`/csv/query`): Returns `TotalQueryData` as two sections in the same CSV stream. The first section has a `QueryStat` header row followed by the query statistics row. The second section starts immediately after with a `SegmentMetrics` header row followed by one row per segment in `segment_query_metrics`.
- **Total sessions stat** (`/csv/total_sessions_stat`): Two columns: `state` and `count`.
- **Extensions** (`/csv/extensions`): Each row represents an extension in a database with columns: `database_name`, `ext_name`, `ext_owner`, `ext_namespace`, `ext_relocatable`, `ext_version`, `error`.

### Example usage

```bash
# List all sessions sorted by CPU time descending
curl 'http://[::1]:1440/csv/sessions?sort=TOTAL_RUNNINGTIMESECONDS:DESC'

# Get a specific session
curl 'http://[::1]:1440/csv/session?sess_id=42'

# Get query details
curl 'http://[::1]:1440/csv/query?ssid=42&ccnt=1'

# Get session state summary
curl 'http://[::1]:1440/csv/total_sessions_stat'

# Paginate through sessions
curl 'http://[::1]:1440/csv/sessions?page_size=50'
# Use X-Next-Page-Token header from response for next page:
curl 'http://[::1]:1440/csv/sessions?page_size=50&page_token=TOKEN_FROM_HEADER'

# Get extensions for all databases
curl 'http://[::1]:1440/csv/extensions'
```

### Error responses

| HTTP Status | Condition |
|-------------|-----------|
| `400 Bad Request` | Missing or invalid required parameters. |
| `405 Method Not Allowed` | Non-GET request. |
| `500 Internal Server Error` | gRPC method returned an error. |

### Configuration

Set `csv_port` in `yagpcc.yaml` to enable the CSV HTTP server. Set to `0` to disable.

```yaml
csv_port: 1440   # default
```

The `extensions_cache_ttl_sec` parameter controls how long extension information is cached (default: 900 seconds = 15 minutes):

```yaml
extensions_cache_ttl_sec: 900   # default
```

---

## JSON HTTP API (Web UI)

The JSON HTTP API serves the same data as the CSV and gRPC APIs but returns JSON responses,
designed for consumption by the browser-based web UI. It is served on the port configured by
`ui_port` (disabled by default, set to `1441` to enable).

All responses use `Content-Type: application/json; charset=utf-8`. Protobuf messages are
serialized using `protojson` with camelCase field names and zero-value fields included.

### Read Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/sessions` | List sessions (same query params as CSV: `show_system`, `show_query_type`, `hide_empty_queries`, `page_size`, `page_token`, `sort`, `filter_*`) |
| `GET` | `/api/session/{sess_id}` | Get a single session by session ID |
| `GET` | `/api/queries` | List queries (query params: `page_size`, `page_token`, `sort`, `filter_*`) |
| `GET` | `/api/query/{ssid}/{ccnt}` | Get a single query by SSID and CCNT |
| `GET` | `/api/query/{ssid}/{ccnt}/running-metrics` | Get procfs runtime metrics matrix cells for a query. Returns `cellMetrics[]` with `sliceId`, `segindex`, and `runtimeMetrics` (`utime`, `stime`, `vmPeak`, `vmRss`, `state`, `procIo`, `procSpill`), plus `skew` and `dataQuality`. Idle cells can be rendered separately using `runtimeMetrics.state == "idle"`. |
| `GET` | `/api/hosts/running-queries` | Get aggregated host-level statistics for all hosts with running queries. Returns `hosts[]` with `hostName`, `segindex[]`, `activeQueries`, `activeSlices`, `cpuUsage`, `memoryUsage`, `diskUsage`, `spillBytes`, `skew`, `dataQuality`, `avg5`, `diskReads`, `diskWrites`, `totalSessions`. |
| `GET` | `/api/stats/sessions` | Get session state statistics (counts by state) |
| `GET` | `/api/extensions` | List extensions (optional `database_name` param) |
| `GET` | `/api/databases` | List available databases |

### Action Endpoints

| Method | Path | Request Body | Description |
|--------|------|-------------|-------------|
| `POST` | `/api/action/terminate-session` | `{"sess_id": 123, "tm_id": 0}` | Terminate a session |
| `POST` | `/api/action/terminate-query` | `{"ssid": 42, "ccnt": 1}` | Terminate a query |
| `POST` | `/api/action/terminate-sessions` | `{"database": "mydb", "username": "user", "query_id": 0}` | Bulk terminate sessions |
| `POST` | `/api/action/move-query-rsg` | `{"ssid": 42, "ccnt": 1, "resource_group_name": "rg1"}` | Move query to resource group |

### Static Assets

All non-API paths serve the embedded SPA frontend. Unknown paths fall back to `index.html`
for client-side routing.

### Error Responses

| HTTP Status | Condition |
|-------------|-----------|
| `400 Bad Request` | Missing or invalid required parameters / invalid JSON body. |
| `405 Method Not Allowed` | Wrong HTTP method (e.g., POST to a GET endpoint). |
| `500 Internal Server Error` | gRPC method returned an error. |
| `503 Service Unavailable` | Master or action server not initialized. |

### Configuration

Set `ui_port` in `yagpcc.yaml` to enable the JSON HTTP / Web UI server. Set to `0` to disable (default).

```yaml
ui_port: 1441
```

---

## Proto file layout

```
api/proto/
├── agent_master/
│   ├── yagpcc_get_service.proto   # GetGPInfo
│   └── yagpcc_action_service.proto # ActionService
└── common/
    ├── yagpcc_metrics.proto       # GPMetrics, QueryKey, SegmentKey, QueryInfo, GpPidProcInfo, RuntimeMetrics, DataQuality, Skew
    └── yagpcc_session.proto      # SessionKey, SessionInfo, SessionState, QueryDesc
```

All timestamps use `google.protobuf.Timestamp` (RFC 3339 / Unix time).
