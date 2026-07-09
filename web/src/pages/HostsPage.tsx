import { Table, Card, Typography, Button, Space, Tag } from "antd";
import { ReloadOutlined } from "@ant-design/icons";
import { useApi } from "../hooks/useApi";
import { getHostsRunningQueries, type RunningHostInfo } from "../api/client";
import ErrorAlert from "../components/ErrorAlert";
import { useTheme } from "../contexts/ThemeContext";
import { getColors, FONT_MONO } from "../theme";

const { Title } = Typography;

/** Format bytes into a human-readable string. */
function formatBytes(value?: number | null): string {
  if (!value && value !== 0) return "—";
  const v = Number(value);
  if (v === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.min(Math.floor(Math.log(v) / Math.log(1024)), units.length - 1);
  return `${(v / Math.pow(1024, i)).toFixed(i ? 1 : 0)} ${units[i]}`;
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
      width: 140,
      render: (dq: { segmentsExpected: number; segmentsReceived: number } | null) => {
        if (!dq) return "—";
        const { segmentsReceived, segmentsExpected } = dq;
        const pct =
          segmentsExpected > 0
            ? Math.round((segmentsReceived / segmentsExpected) * 100)
            : 100;
        const color = pct === 100 ? "green" : pct > 50 ? "orange" : "red";
        return (
          <Tag color={color}>
            {segmentsReceived}/{segmentsExpected} ({pct}%)
          </Tag>
        );
      },
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
            expandedRowRender: () => (
              <div style={{ padding: 16 }}>
                <Typography.Text type="secondary">
                  Per-query breakdown coming soon
                </Typography.Text>
              </div>
            ),
          }}
        />
      </Card>
    </div>
  );
}
