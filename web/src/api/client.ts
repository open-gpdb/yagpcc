/**
 * API client for the yagpcc JSON REST API.
 *
 * All endpoints are relative to the current origin (same-origin requests).
 * During development, Vite proxies /api/* to the Go backend.
 */

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

async function request<T>(url: string, init?: RequestInit): Promise<T> {
  const resp = await fetch(url, init);
  if (!resp.ok) {
    let msg = resp.statusText;
    try {
      const body = (await resp.json()) as { error?: string };
      if (body.error) msg = body.error;
    } catch {
      // ignore parse errors
    }
    throw new ApiError(resp.status, msg);
  }
  return resp.json() as Promise<T>;
}

function get<T>(url: string): Promise<T> {
  return request<T>(url);
}

function post<T>(url: string, body: unknown): Promise<T> {
  return request<T>(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

// ─── Read endpoints ──────────────────────────────────────────────

export interface GetSessionsParams {
  showSystem?: boolean;
  showQueryType?: string;
  pageSize?: number;
  pageToken?: string;
  sort?: string[];
  filters?: Record<string, string>;
}

export function getSessions(params: GetSessionsParams = {}) {
  const q = new URLSearchParams();
  if (params.showSystem) q.set("show_system", "true");
  if (params.showQueryType) q.set("show_query_type", params.showQueryType);
  if (params.pageSize) q.set("page_size", String(params.pageSize));
  if (params.pageToken) q.set("page_token", params.pageToken);
  params.sort?.forEach((s) => q.append("sort", s));
  if (params.filters) {
    for (const [k, v] of Object.entries(params.filters)) {
      q.set(k, v);
    }
  }
  const qs = q.toString();
  return get<SessionsResponse>(`/api/sessions${qs ? "?" + qs : ""}`);
}

export function getSession(sessId: number) {
  return get<SessionResponse>(`/api/session/${sessId}`);
}

export interface GetQueriesParams {
  pageSize?: number;
  pageToken?: string;
  sort?: string[];
  filters?: Record<string, string>;
}

export function getQueries(params: GetQueriesParams = {}) {
  const q = new URLSearchParams();
  if (params.pageSize) q.set("page_size", String(params.pageSize));
  if (params.pageToken) q.set("page_token", params.pageToken);
  params.sort?.forEach((s) => q.append("sort", s));
  if (params.filters) {
    for (const [k, v] of Object.entries(params.filters)) {
      q.set(k, v);
    }
  }
  const qs = q.toString();
  return get<QueriesResponse>(`/api/queries${qs ? "?" + qs : ""}`);
}

export function getQuery(ssid: number, ccnt: number) {
  return get<QueryResponse>(`/api/query/${ssid}/${ccnt}`);
}

export function getQueryRunningMetrics(ssid: number, ccnt: number) {
  return get<QueryRunningMetricsResponse>(`/api/query/${ssid}/${ccnt}/running-metrics`);
}

export function getSessionStats() {
  return get<SessionStatsResponse>("/api/stats/sessions");
}

export function getExtensions(databaseName?: string) {
  const q = databaseName ? `?database_name=${encodeURIComponent(databaseName)}` : "";
  return get<ExtensionsResponse>(`/api/extensions${q}`);
}

export function getDatabases() {
  return get<DatabasesResponse>("/api/databases");
}

// ─── Action endpoints ────────────────────────────────────────────

export function terminateSession(sessId: number, tmId?: number) {
  return post<TerminateResponse>("/api/action/terminate-session", {
    sess_id: sessId,
    tm_id: tmId ?? 0,
  });
}

export function terminateQuery(ssid: number, ccnt: number) {
  return post<TerminateResponse>("/api/action/terminate-query", {
    ssid,
    ccnt,
  });
}

export function terminateSessions(params: {
  database?: string;
  username?: string;
  queryId?: number;
}) {
  return post<TerminateResponses>("/api/action/terminate-sessions", {
    database: params.database ?? "",
    username: params.username ?? "",
    query_id: params.queryId ?? 0,
  });
}

export function moveQueryToResourceGroup(
  ssid: number,
  ccnt: number,
  resourceGroupName: string,
) {
  return post<{ status: string }>("/api/action/move-query-rsg", {
    ssid,
    ccnt,
    resource_group_name: resourceGroupName,
  });
}

// ─── Response types ──────────────────────────────────────────────
// These mirror the JSON output from the Go backend handlers.

export interface SessionKey {
  sessId: string; // int64 as string
  tmId: string;
}

export interface SessionInfo {
  // Session identification
  sessionKey: SessionKey;
  host: string;
  clusterId: string;
  collectTime: string;

  // pg_stat_activity fields
  pid: number;
  user: string;
  database: string;
  applicationName: string;
  clientAddr: string;
  clientHostname: string;
  clientPort: number;
  backendStart: string;
  xactStart: string;
  queryStart: string;
  stateChange: string;
  waitingReason: string;
  waiting: boolean;
  state: string;
  backendXid: string;
  backendXmin: string;
  rsgId: number;
  rsgName: string;
  rsgQueueDuration: string;
  blockedBySessId: number;
  waitMode: string;
  lockedItem: string;
  lockedMode: string;
  waitEventType: string;
  waitEvent: string;

  // Computed fields
  totalRunningTimeSeconds: number;

  // Running query info
  runningQuery: QueryKey | null;
  runningQueryStatus: string;
  runningQueryText: string;
  runningQueryLevel: number;
  runningQuerySlices: number;
  runningQueryError: string;
  blockedSessionLevel: number;

  // Running query detailed info
  runningQueryInfo: QueryDetailInfo | null;

  // Metrics
  totalMetrics: GPMetrics | null;
  lastMetrics: GPMetrics | null;
  queryMetrics: GPMetrics | null;

  // Aggregated metrics
  aggregatedMetrics: AggregatedMetrics | null;

  // Nested queries
  queries: QueryDesc[];
}

export interface QueryDesc {
  queryKey: QueryKey;
  queryText: string;
  queryStart: string;
  queryDurationSeconds: number;
  status: string;
}

export interface QueryKey {
  ssid: number;
  ccnt: number;
}

export interface QueryDetailInfo {
  generator: string;
  queryId: number;
  planId: number;
  queryText: string;
  planText: string;
  userName: string;
  databaseName: string;
  rsgname: string;
  analyzeText: string;
  submitTime: string;
  startTime: string;
  endTime: string;
}

export interface QueryInfo {
  queryKey: QueryKey;
  queryText: string;
  queryStart: string;
  queryDurationSeconds: number;
  status: string;
  sessionKey: SessionKey;
  user: string;
  database: string;
  rsgName: string;
  host: string;
  pid: number;
  state: string;
  waitEventType: string;
  waitEvent: string;
  runningQueryLevel: number;
  runningQuerySlices: number;
  runningQueryError: string;
  metrics: GPMetrics | null;

  // QueryStat fields
  clusterId: string;
  collectTime: string;
  statKind: string;
  startTime: string;
  endTime: string;
  completed: boolean;
  message: string;
  blockedBySessId: number;
  waitMode: string;
  lockedItem: string;
  lockedMode: string;
  sessionState: string;
  slices: number;

  // Detailed query info
  queryInfo: QueryDetailInfo | null;
  aggregatedMetrics: AggregatedMetrics | null;

  // Segment metrics
  segmentMetrics: SegmentMetricsInfo[];
}

export interface NetworkStat {
  totalBytes: number;
  tupleBytes: number;
  chunks: number;
}

export interface InterconnectStat {
  totalRecvQueueSize: number;
  recvQueueSizeCountingTime: number;
  totalCapacity: number;
  capacityCountingTime: number;
  totalBuffers: number;
  bufferCountingTime: number;
  activeConnectionsNum: number;
  retransmits: number;
  startupCachedPktNum: number;
  mismatchNum: number;
  crcErrors: number;
  sndPktNum: number;
  recvPktNum: number;
  disorderedPktNum: number;
  duplicatedPktNum: number;
  recvAckNum: number;
  statusQueryMsgNum: number;
}

export interface SystemStat {
  runningTimeSeconds: number;
  userTimeSeconds: number;
  kernelTimeSeconds: number;
  vsize: number;
  rss: number;
  vmPeakKb: number;
  rchar: number;
  wchar: number;
  syscr: number;
  syscw: number;
  readBytes: number;
  writeBytes: number;
  cancelledWriteBytes: number;
}

export interface Instrumentation {
  ntuples: number;
  nloops: number;
  tuplecount: number;
  firsttuple: number;
  startup: number;
  total: number;
  sharedBlksHit: number;
  sharedBlksRead: number;
  sharedBlksDirtied: number;
  sharedBlksWritten: number;
  localBlksHit: number;
  localBlksRead: number;
  localBlksDirtied: number;
  localBlksWritten: number;
  tempBlksRead: number;
  tempBlksWritten: number;
  blkReadTime: number;
  blkWriteTime: number;
  startupTime: number;
  inheritedCalls: number;
  inheritedTime: number;
  sent: NetworkStat | null;
  received: NetworkStat | null;
  interconnect: InterconnectStat | null;
}

export interface SpillInfo {
  fileCount: number;
  totalBytes: number;
}

export interface GPMetrics {
  // Summary fields
  cpuUsage: number;
  memoryUsage: number;
  diskRead: number;
  diskWrite: number;
  networkSent: number;
  networkReceived: number;

  // Detailed fields
  systemStat: SystemStat | null;
  instrumentation: Instrumentation | null;
  spill: SpillInfo | null;
}

export interface AggregatedMetrics {
  calls: number;
  minTime: number;
  maxTime: number;
  meanTime: number;
  stddevTime: number;
  totalTime: number;
}

export interface ProcIO {
  rchar: number;
  wchar: number;
  syscr: number;
  syscw: number;
  readBytes: number;
  writeBytes: number;
  cancelledWriteBytes: number;
}

export interface ProcSpill {
  size: number;
  files: number;
}

export interface RuntimeMetrics {
  utime: number;
  stime: number;
  vmPeak: number;
  vmRss: number;
  state: string;
  procIo: ProcIO | null;
  procSpill: ProcSpill | null;
}

export interface SkewInfo {
  skew: number;
  segindex: number;
}

export interface DataQuality {
  segmentsExpected: number;
  segmentsReceived: number;
  isPartial: boolean;
  freshnessMs: number;
}

export interface RuntimeMetricsCell {
  sliceId: number;
  segindex: number;
  runtimeMetrics: RuntimeMetrics | null;
}

export interface QueryRunningMetricsResponse {
  sliceId: number[];
  segindex: number[];
  runtimeMetrics: RuntimeMetricsCell[];
  skew: SkewInfo | null;
  dataQuality: DataQuality | null;
}

export interface SegmentKey {
  dbid: number;
  segindex: number;
}

export interface SegmentMetricsInfo {
  segmentKey: SegmentKey | null;
  clusterId: string;
  hostname: string;
  collectTime: string;
  queryStatus: string;
  startTime: string;
  endTime: string;
  metrics: GPMetrics | null;
}

export interface SessionStat {
  state: string;
  count: number;
}

export interface SessionsResponse {
  sessions: SessionInfo[];
  nextPageToken: string;
  totalCount: string;
}

export interface SessionResponse {
  session: SessionInfo;
}

export interface QueriesResponse {
  queries: QueryInfo[];
  nextPageToken: string;
  totalCount: string;
}

export interface QueryResponse {
  query: QueryInfo;
}

export interface SessionStatsResponse {
  stats: SessionStat[];
}

export interface PgExtensionInfo {
  name: string;
  defaultVersion: string;
  installedVersion: string;
  comment: string;
}

export interface DatabaseExtensionsInfo {
  databaseName: string;
  extensions: PgExtensionInfo[];
}

export interface ExtensionsResponse {
  databases: DatabaseExtensionsInfo[];
}

export interface DatabasesResponse {
  databases: string[];
}

export interface TerminateResponse {
  statusCode: string;
  statusText: string;
}

// ---------------------------------------------------------------------------
// Hosts running queries
// ---------------------------------------------------------------------------

export interface RunningHostInfo {
  hostName: string;
  segindex: number[];
  activeQueries: number;
  activeSlices: number;
  cpuUsage: number;
  memoryUsage: number;
  diskUsage: number;
  spillBytes: number;
  skew: SkewInfo | null;
  dataQuality: DataQuality | null;
  avg5: number;
  diskReads: number;
  diskWrites: number;
  totalSessions: number;
}

export interface HostsRunningQueriesResponse {
  hosts: RunningHostInfo[];
}

export interface HostRunningQueryInfo {
  queryKey: QueryKey | null;
  userName: string;
  dbName: string;
  queryText: string;
  runtimeMetrics: RuntimeMetrics | null;
  skew: SkewInfo | null;
  dataQuality: DataQuality | null;
  state: string;
  isIdle: boolean;
}

export interface HostRunningQueriesResponse {
  hostName: string;
  segindex: number[];
  queries: HostRunningQueryInfo[];
  dataQuality: DataQuality | null;
  nextPageToken: string;
}

export function getHostsRunningQueries() {
  return get<HostsRunningQueriesResponse>("/api/hosts/running-queries");
}

export function getHostRunningQueries(
  hostName: string,
  params: { pageSize?: number; pageToken?: string } = {},
) {
  const q = new URLSearchParams();
  if (params.pageSize) q.set("page_size", String(params.pageSize));
  if (params.pageToken) q.set("page_token", params.pageToken);
  const qs = q.toString();
  return get<HostRunningQueriesResponse>(
    `/api/hosts/${encodeURIComponent(hostName)}/running-queries${qs ? `?${qs}` : ""}`,
  );
}

export interface TerminateResponses {
  terminateResponse: TerminateResponse[];
}
