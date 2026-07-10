import { useCallback, useEffect, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { Table, Card, Typography, Button, Space, Tag, Tooltip } from "antd";
import { ReloadOutlined } from "@ant-design/icons";
import { useApi } from "../hooks/useApi";
import {
  getHostRunningQueries,
  getHostsRunningQueries,
  type DataQuality,
  type HostRunningQueriesResponse,
  type HostRunningQueryInfo,
  type RunningHostInfo,
} from "../api/client";
import ErrorAlert from "../components/ErrorAlert";
import { useTheme } from "../contexts/ThemeContext";
import { getColors, FONT_MONO } from "../theme";

const { Title } = Typography;
const HOST_DETAIL_PAGE_SIZE = 50;

/** Format bytes into a human-readable string. */
function formatBytes(value?: number | null): string {
  if (!value && value !== 0) return "—";
  const v = Number(value);
  if (v === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.min(Math.floor(Math.log(v) / Math.log(1024)), units.length - 1);
  return `${(v / Math.pow(1024, i)).toFixed(i ? 1 : 0)} ${units[i]}`;
}

function formatDataQualityTag(dq: DataQuality | null | undefined) {
  if (!dq) return "—";
  const { segmentsReceived, segmentsExpected, freshnessMs } = dq;
  const pct =
    segmentsExpected > 0
      ? Math.round((segmentsReceived / segmentsExpected) * 100)
      : 100;
  const color = pct === 100 ? "green" : pct > 50 ? "orange" : "red";
  const ageSec = Math.round(freshnessMs / 1000);
  return (
    <Tag color={color} title={`${ageSec}s ago`}>
      {segmentsReceived}/{segmentsExpected} ({pct}%, {ageSec}s)
    </Tag>
  );
}

function firstWord(value?: string | null): string {
  const trimmed = (value ?? "").trim();
  if (!trimmed) return "—";
  return trimmed.split(/\s+/, 1)[0] ?? "—";
}

function queryPreview(value?: string | null): string {
  const trimmed = (value ?? "").trim();
  if (!trimmed) return "—";
  return trimmed.length > 100 ? `${trimmed.slice(0, 100)}…` : trimmed;
}

/** Render a color-coded CPU usage badge. */
function CpuUsageBadge({ value }: { value: number }) {
  const { mode } = useTheme();
  const c = getColors(mode);
  const pct = Math.max(0, Math.min(Math.round(value * 100), 100));
  let color = c.green;
  if (pct > 80) color = c.red;
  else if (pct > 50) color = c.yellow;
  return <Tag color={color}>{pct}%</Tag>;
}

function HostQueriesExpandedRow({ hostName }: { hostName: string }) {
  const navigate = useNavigate();
  const [data, setData] = useState<HostRunningQueriesResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const [page, setPage] = useState(1);
  const [pageTokens, setPageTokens] = useState<string[]>([""]);

  const load = useCallback(() => {
    const pageToken = pageTokens[page - 1] ?? "";
    setLoading(true);
    setError(null);
    getHostRunningQueries(hostName, {
      pageSize: HOST_DETAIL_PAGE_SIZE,
      pageToken,
    })
      .then((response) => {
        setData(response);
        if (response.nextPageToken) {
          setPageTokens((prev) => {
            if (prev[page] === response.nextPageToken) return prev;
            const next = prev.slice(0, page);
            next[page] = response.nextPageToken;
            return next;
          });
        }
      })
      .catch((err: unknown) => {
        setError(err instanceof Error ? err : new Error(String(err)));
      })
      .finally(() => {
        setLoading(false);
      });
  }, [hostName, page, pageTokens]);

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [hostName, page]);

  const columns = [
    {
      title: "Session",
      dataIndex: ["queryKey", "ssid"],
      width: 110,
      render: (v: number | undefined) =>
        v !== undefined ? (
          <Link to={`/session/${v}`} onClick={(e) => e.stopPropagation()}>
            {v}
          </Link>
        ) : "—",
    },
    {
      title: "CCNT",
      dataIndex: ["queryKey", "ccnt"],
      width: 90,
      render: (_: unknown, row: HostRunningQueryInfo) => {
        const ssid = row.queryKey?.ssid;
        const ccnt = row.queryKey?.ccnt;
        if (ccnt === undefined || ccnt <= 0) return <Tag>Unset</Tag>;
        if (ssid === undefined) return ccnt;
        return (
          <Link to={`/query/${ssid}/${ccnt}`} onClick={(e) => e.stopPropagation()}>
            {ccnt}
          </Link>
        );
      },
    },
    {
      title: "User",
      dataIndex: "userName",
      width: 120,
      render: (v: string) => v || "—",
    },
    {
      title: "State",
      dataIndex: "state",
      width: 120,
      render: (v: string, row: HostRunningQueryInfo) => (
        <Space size={4}>
          {row.isIdle && <Tag color="default">Idle process</Tag>}
          <Tooltip title={v || "—"}>
            <span>{firstWord(v)}</span>
          </Tooltip>
        </Space>
      ),
    },
    {
      title: "Database",
      dataIndex: "dbName",
      width: 120,
      render: (v: string) => v || "—",
    },
    {
      title: "Query",
      dataIndex: "queryText",
      width: 360,
      render: (v: string, row: HostRunningQueryInfo) => {
        if (row.isIdle) return <Typography.Text type="secondary">—</Typography.Text>;
        return (
          <Tooltip title={v || "—"}>
            <Typography.Text style={{ fontFamily: FONT_MONO }}>{queryPreview(v)}</Typography.Text>
          </Tooltip>
        );
      },
    },
    {
      title: "CPU ticks",
      width: 110,
      render: (_: unknown, row: HostRunningQueryInfo) =>
        (row.runtimeMetrics?.utime ?? 0) + (row.runtimeMetrics?.stime ?? 0),
    },
    {
      title: "Memory RSS",
      width: 120,
      render: (_: unknown, row: HostRunningQueryInfo) => formatBytes(row.runtimeMetrics?.vmRss ?? 0),
    },
    {
      title: "Disk R/W",
      width: 150,
      render: (_: unknown, row: HostRunningQueryInfo) => {
        const read = row.runtimeMetrics?.procIo?.readBytes ?? 0;
        const write = row.runtimeMetrics?.procIo?.writeBytes ?? 0;
        return `${formatBytes(read)} / ${formatBytes(write)}`;
      },
    },
    {
      title: "Spill",
      width: 110,
      render: (_: unknown, row: HostRunningQueryInfo) => formatBytes(row.runtimeMetrics?.procSpill?.size ?? 0),
    },
    {
      title: "Skew",
      dataIndex: ["skew", "skew"],
      width: 90,
      render: (v: number | undefined) => (v !== undefined ? v.toFixed(2) : "—"),
    },
    {
      title: "Data Quality",
      dataIndex: "dataQuality",
      width: 160,
      render: (dq: DataQuality | null) => formatDataQualityTag(dq),
    },
  ];

  const hasNext = Boolean(data?.nextPageToken);
  return (
    <div style={{ padding: 16 }}>
      <ErrorAlert error={error} />
      <Space style={{ marginBottom: 12 }}>
        <Button icon={<ReloadOutlined />} onClick={load} loading={loading}>
          Refresh details
        </Button>
        <Typography.Text type="secondary">
          {data?.hostName ?? hostName} · {data?.queries?.length ?? 0} rows
        </Typography.Text>
      </Space>
      <Table
        loading={loading}
        dataSource={data?.queries ?? []}
        columns={columns}
        rowKey={(row) => `${row.queryKey?.ssid ?? ""}-${row.queryKey?.ccnt ?? ""}-${row.isIdle}`}
        size="small"
        pagination={false}
        scroll={{ x: 1500 }}
        onRow={(row) => ({
          onClick: () => {
            const ssid = row.queryKey?.ssid;
            if (ssid === undefined) return;
            const params = new URLSearchParams({ hostname: hostName, ssid: String(ssid) });
            const ccnt = row.queryKey?.ccnt;
            if (ccnt !== undefined && ccnt > 0) params.set("ccnt", String(ccnt));
            navigate(`/procfs/pid-proc-info?${params.toString()}`);
          },
          style: { cursor: row.queryKey?.ssid !== undefined ? "pointer" : "default" },
        })}
      />
      <Space style={{ marginTop: 12 }}>
        <Button
          disabled={page <= 1 || loading}
          onClick={() => setPage((prev) => Math.max(1, prev - 1))}
        >
          Previous
        </Button>
        <Typography.Text>Page {page}</Typography.Text>
        <Button disabled={!hasNext || loading} onClick={() => setPage((prev) => prev + 1)}>
          Next
        </Button>
      </Space>
    </div>
  );
}

export default function HostsPage() {
  const { data, loading, error, refresh } = useApi(
    () => getHostsRunningQueries(),
    [],
  );

  const columns = [
    {
      title: "Host",
      dataIndex: "hostName",
      fixed: "left" as const,
      width: 200,
      render: (v: string) => (
        <span style={{ fontFamily: FONT_MONO }} title={v}>
          {v}
        </span>
      ),
      sorter: (a: RunningHostInfo, b: RunningHostInfo) =>
        a.hostName.localeCompare(b.hostName),
    },
    {
      title: "Segments",
      dataIndex: "segindex",
      width: 120,
      render: (segs: number[]) => (
        <Tag color="blue">{segs.length}</Tag>
      ),
      sorter: (a: RunningHostInfo, b: RunningHostInfo) =>
        a.segindex.length - b.segindex.length,
    },
    {
      title: "CPU %",
      dataIndex: "cpuUsage",
      width: 100,
      render: (v: number) => <CpuUsageBadge value={v} />,
      sorter: (a: RunningHostInfo, b: RunningHostInfo) =>
        a.cpuUsage - b.cpuUsage,
    },
    {
      title: "Load 5m",
      dataIndex: "avg5",
      width: 100,
      render: (v: number) => v.toFixed(2),
      sorter: (a: RunningHostInfo, b: RunningHostInfo) =>
        a.avg5 - b.avg5,
    },
    {
      title: "Total Sessions",
      dataIndex: "totalSessions",
      width: 130,
      sorter: (a: RunningHostInfo, b: RunningHostInfo) =>
        a.totalSessions - b.totalSessions,
    },
    {
      title: "Active Queries",
      dataIndex: "activeQueries",
      width: 130,
      sorter: (a: RunningHostInfo, b: RunningHostInfo) =>
        a.activeQueries - b.activeQueries,
    },
    {
      title: "Active Slices",
      dataIndex: "activeSlices",
      width: 130,
      sorter: (a: RunningHostInfo, b: RunningHostInfo) =>
        a.activeSlices - b.activeSlices,
    },
    {
      title: "Memory RSS",
      dataIndex: "memoryUsage",
      width: 120,
      render: (v: number) => formatBytes(v),
      sorter: (a: RunningHostInfo, b: RunningHostInfo) =>
        a.memoryUsage - b.memoryUsage,
    },
    {
      title: "Disk Reads",
      dataIndex: "diskReads",
      width: 120,
      render: (v: number) => formatBytes(v),
      sorter: (a: RunningHostInfo, b: RunningHostInfo) =>
        a.diskReads - b.diskReads,
    },
    {
      title: "Disk Writes",
      dataIndex: "diskWrites",
      width: 120,
      render: (v: number) => formatBytes(v),
      sorter: (a: RunningHostInfo, b: RunningHostInfo) =>
        a.diskWrites - b.diskWrites,
    },
    {
      title: "Spill",
      dataIndex: "spillBytes",
      width: 120,
      render: (v: number) => formatBytes(v),
      sorter: (a: RunningHostInfo, b: RunningHostInfo) =>
        a.spillBytes - b.spillBytes,
    },
    {
      title: "Skew",
      dataIndex: ["skew", "skew"],
      width: 100,
      render: (v: number | undefined) => (v !== undefined ? v.toFixed(2) : "—"),
      sorter: (a: RunningHostInfo, b: RunningHostInfo) =>
        (a.skew?.skew ?? 0) - (b.skew?.skew ?? 0),
    },
    {
      title: "Data Quality",
      dataIndex: "dataQuality",
      width: 160,
      render: (dq: DataQuality | null) => formatDataQualityTag(dq),
    },
  ];

  return (
    <div>
      <Title level={3}>Hosts</Title>
      <ErrorAlert error={error} />

      <Card style={{ marginBottom: 16 }}>
        <Space>
          <Button icon={<ReloadOutlined />} onClick={refresh}>
            Refresh
          </Button>
        </Space>
      </Card>

      <Card>
        <Table
          loading={loading}
          dataSource={data?.hosts ?? []}
          columns={columns}
          rowKey="hostName"
          size="small"
          pagination={false}
          scroll={{ x: 2000 }}
          expandable={{
            expandedRowRender: (record: RunningHostInfo) => (
              <HostQueriesExpandedRow hostName={record.hostName} />
            ),
          }}
        />
      </Card>
    </div>
  );
}
