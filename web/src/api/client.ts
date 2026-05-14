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
  hideEmptyQueries?: boolean;
  pageSize?: number;
  pageToken?: string;
  sort?: string[];
  filters?: Record<string, string>;
}

export function getSessions(params: GetSessionsParams = {}) {
  const q = new URLSearchParams();
  if (params.showSystem) q.set("show_system", "true");
  if (params.showQueryType) q.set("show_query_type", params.showQueryType);
  if (params.hideEmptyQueries) q.set("hide_empty_queries", "true");
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
  return post<TerminateResponse>("/api/action/terminate-sessions", {
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
// These mirror the protojson output from the Go backend.

export interface SessionKey {
  sessId: string; // int64 as string in protojson
  tmId: string;
}

export interface SessionInfo {
  sessionKey: SessionKey;
  host: string;
  user: string;
  database: string;
  applicationName: string;
  clientHostname: string;
  state: string;
  rsgName: string;
  waitEventType: string;
  waitEvent: string;
  backendStart: string;
  xactStart: string;
  queryStart: string;
  stateChange: string;
  totalRunningTimeSeconds: number;
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
  metrics: GPMetrics | null;
}

export interface GPMetrics {
  cpuUsage: number;
  memoryUsage: number;
  diskRead: number;
  diskWrite: number;
  networkSent: number;
  networkReceived: number;
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
  success: boolean;
  message: string;
}
