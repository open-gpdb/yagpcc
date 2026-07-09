import { useParams, useNavigate } from "react-router-dom";
import {
  Card,
  Descriptions,
  Typography,
  Spin,
  Button,
  Space,
  Modal,
  message,
  Input,
  Table,
  Tag,
  Alert,
  Empty,
} from "antd";
import { ArrowLeftOutlined, StopOutlined, SwapOutlined } from "@ant-design/icons";
import { Fragment, useState } from "react";
import { useApi } from "../hooks/useApi";
import {
  getQuery,
  getQueryRunningMetrics,
  terminateQuery,
  moveQueryToResourceGroup,
  type QueryRunningMetricsResponse,
  type RuntimeMetrics,
  type RuntimeMetricsCell,
  type SegmentMetricsInfo,
} from "../api/client";
import ErrorAlert from "../components/ErrorAlert";
import QueryStatusBadge from "../components/QueryStatusBadge";
import { GPMetricsCard, AggregatedMetricsCard } from "../components/MetricsDisplay";
import { useTheme } from "../contexts/ThemeContext";
import { FONT_MONO, getCodeBlockStyle, getColors, type ThemeColors } from "../theme";

const { Title, Paragraph, Text } = Typography;

function formatBytes(value?: number | null): string {
  if (!value) return "0 B";
  if (value >= 1073741824) return `${(value / 1073741824).toFixed(2)} GB`;
  if (value >= 1048576) return `${(value / 1048576).toFixed(2)} MB`;
  if (value >= 1024) return `${(value / 1024).toFixed(2)} KB`;
  return `${value} B`;
}

function runtimeCPU(metrics?: RuntimeMetrics | null): number {
  return (metrics?.utime ?? 0) + (metrics?.stime ?? 0);
}

function isIdleRuntime(metrics?: RuntimeMetrics | null): boolean {
  return (metrics?.state ?? "").trim().toLowerCase() === "idle";
}

function runtimeCellColor(metrics: RuntimeMetrics | null | undefined, maxCPU: number, colors: ThemeColors): string {
  if (!metrics || isIdleRuntime(metrics)) return colors.bgHover;
  const cpu = runtimeCPU(metrics);
  if (cpu <= 0 || maxCPU <= 0) return "#d7edca";
  const ratio = Math.min(1, Math.max(0, cpu / maxCPU));
  if (ratio >= 0.85) return "#e7b1ad";
  if (ratio >= 0.6) return "#ffd8a8";
  if (ratio >= 0.25) return "#fbf2bd";
  return "#d7edca";
}

function RuntimeMetricsMatrix({ ssid, ccnt }: { ssid: number; ccnt: number }) {
  const { mode } = useTheme();
  const colors = getColors(mode);
  const [selectedCell, setSelectedCell] = useState<RuntimeMetricsCell | null>(null);
  const { data, loading, error } = useApi(
    () => getQueryRunningMetrics(ssid, ccnt),
    [ssid, ccnt],
  );

  const response = data as QueryRunningMetricsResponse | null;
  const cells = response?.runtimeMetrics ?? [];
  const cellMap = new Map<string, RuntimeMetricsCell>();
  for (const cell of cells) {
    cellMap.set(`${cell.sliceId}:${cell.segindex}`, cell);
  }

  const segindexes = Array.from(new Set(response?.segindex ?? [])).sort((a, b) => a - b);
  const sliceIds = Array.from(new Set(response?.sliceId ?? [])).sort((a, b) => a - b);

  const selectedMetrics = selectedCell?.runtimeMetrics ?? null;
  const maxCPUBySlice = new Map<number, number>();
  for (const cell of cells) {
    if (isIdleRuntime(cell.runtimeMetrics)) continue;
    const cpu = runtimeCPU(cell.runtimeMetrics);
    maxCPUBySlice.set(cell.sliceId, Math.max(maxCPUBySlice.get(cell.sliceId) ?? 0, cpu));
  }

  return (
    <Card title="Runtime metrics matrix" style={{ marginBottom: 16 }}>
      <ErrorAlert error={error} />
      <Spin spinning={loading}>
        {cells.length === 0 ? (
          <Empty description="No runtime metrics from procfs yet" />
        ) : (
          <div style={{ display: "grid", gridTemplateColumns: "minmax(0, 1fr) 360px", gap: 24 }}>
            <div style={{ overflowX: "auto" }}>
              <Text type="secondary">
                Rows are slices; columns are segment indexes.
              </Text>
              <div
                style={{
                  display: "grid",
                  gridTemplateColumns: `120px repeat(${segindexes.length}, 76px)`,
                  gap: 8,
                  alignItems: "center",
                  fontFamily: FONT_MONO,
                  marginTop: 12,
                }}
              >
                <div />
                {segindexes.map((segindex) => (
                  <div key={`h-${segindex}`} style={{ textAlign: "center", color: colors.textSecondary }}>
                    {segindex}
                  </div>
                ))}
                {sliceIds.map((sliceId) => (
                  <Fragment key={`row-${sliceId}`}>
                    <div key={`slice-${sliceId}`} style={{ textAlign: "right", paddingRight: 8 }}>
                      {sliceId}{sliceId === 0 ? " (coord.)" : ""}
                    </div>
                    {segindexes.map((segindex) => {
                      const cell = cellMap.get(`${sliceId}:${segindex}`);
                      const cpu = runtimeCPU(cell?.runtimeMetrics);
                      const active = selectedCell?.sliceId === sliceId && selectedCell?.segindex === segindex;
                      return (
                        <button
                          key={`${sliceId}-${segindex}`}
                          type="button"
                          disabled={!cell}
                          onClick={() => cell && setSelectedCell(cell)}
                          style={{
                            height: 56,
                            borderRadius: 6,
                            border: active ? `3px solid ${colors.red}` : `1px solid ${colors.border}`,
                            background: cell ? runtimeCellColor(cell.runtimeMetrics, maxCPUBySlice.get(sliceId) ?? 0, colors) : colors.bgHover,
                            color: cell && !isIdleRuntime(cell.runtimeMetrics) ? "#2c5f1d" : colors.textSecondary,
                            cursor: cell ? "pointer" : "not-allowed",
                            fontFamily: FONT_MONO,
                            fontSize: 18,
                            opacity: cell ? 1 : 0.6,
                          }}
                          title={cell ? `slice ${sliceId}, segindex ${segindex}` : "No data"}
                        >
                          {cell ? cpu.toFixed(0) : "—"}
                        </button>
                      );
                    })}
                  </Fragment>
                ))}
              </div>

              <Space wrap style={{ marginTop: 16 }}>
                <Tag color="#d7edca">low skew</Tag>
                <Tag color="#fbf2bd">25% of slice max</Tag>
                <Tag color="#ffd8a8">60% of slice max</Tag>
                <Tag color="#e7b1ad">85% of slice max</Tag>
                <Tag>idle / no data</Tag>
              </Space>
            </div>

            <div
              style={{
                border: `1px solid ${colors.border}`,
                borderRadius: 8,
                padding: 16,
                background: colors.bgElevated,
              }}
            >
              <Space direction="vertical" size={12} style={{ width: "100%" }}>
                <div>
                  <Text strong>
                    skew {response?.skew?.skew?.toFixed(2) ?? "0.00"} on segment {response?.skew?.segindex ?? 0}
                  </Text>
                  <br />
                  <Text>
                    received {response?.dataQuality?.segmentsReceived ?? 0} of {response?.dataQuality?.segmentsExpected ?? 0} hosts
                  </Text>
                  <br />
                  <Text type="secondary">
                    updated {((response?.dataQuality?.freshnessMs ?? 0) / 1000).toFixed(1)} seconds ago
                  </Text>
                </div>

                <Alert
                  type={response?.dataQuality?.isPartial ? "warning" : "success"}
                  showIcon
                  message={response?.dataQuality?.isPartial ? "Partial procfs data" : "Procfs data is complete"}
                />

                {selectedCell ? (
                  <Descriptions
                    bordered
                    size="small"
                    column={1}
                    title={`Cell: segindex ${selectedCell.segindex}, slice ${selectedCell.sliceId}`}
                  >
                    <Descriptions.Item label="State">{selectedMetrics?.state || "unknown"}</Descriptions.Item>
                    <Descriptions.Item label="CPU (utime+stime)">{runtimeCPU(selectedMetrics).toFixed(0)}</Descriptions.Item>
                    <Descriptions.Item label="utime">{selectedMetrics?.utime ?? 0}</Descriptions.Item>
                    <Descriptions.Item label="stime">{selectedMetrics?.stime ?? 0}</Descriptions.Item>
                    <Descriptions.Item label="RSS">{formatBytes(selectedMetrics?.vmRss)}</Descriptions.Item>
                    <Descriptions.Item label="VmPeak">{formatBytes(selectedMetrics?.vmPeak)}</Descriptions.Item>
                    <Descriptions.Item label="Read bytes">{formatBytes(selectedMetrics?.procIo?.readBytes)}</Descriptions.Item>
                    <Descriptions.Item label="Write bytes">{formatBytes(selectedMetrics?.procIo?.writeBytes)}</Descriptions.Item>
                    <Descriptions.Item label="rchar">{formatBytes(selectedMetrics?.procIo?.rchar)}</Descriptions.Item>
                    <Descriptions.Item label="wchar">{formatBytes(selectedMetrics?.procIo?.wchar)}</Descriptions.Item>
                    <Descriptions.Item label="syscr">{selectedMetrics?.procIo?.syscr ?? 0}</Descriptions.Item>
                    <Descriptions.Item label="syscw">{selectedMetrics?.procIo?.syscw ?? 0}</Descriptions.Item>
                    <Descriptions.Item label="Spill">
                      {formatBytes(selectedMetrics?.procSpill?.size)} ({selectedMetrics?.procSpill?.files ?? 0} files)
                    </Descriptions.Item>
                  </Descriptions>
                ) : (
                  <Text type="secondary">Click a matrix cell to see runtime statistics.</Text>
                )}
              </Space>
            </div>
          </div>
        )}
      </Spin>
    </Card>
  );
}

export default function QueryDetailPage() {
  const { ssid, ccnt } = useParams<{ ssid: string; ccnt: string }>();
  const { mode } = useTheme();
  const cbStyle = getCodeBlockStyle(mode);
  const c = getColors(mode);
  const navigate = useNavigate();
  const [moveRsgVisible, setMoveRsgVisible] = useState(false);
  const [rsgName, setRsgName] = useState("");

  const { data, loading, error, refresh } = useApi(
    () => getQuery(Number(ssid), Number(ccnt)),
    [ssid, ccnt],
  );

  const query = data?.query;

  const handleTerminate = () => {
    Modal.confirm({
      title: "Terminate Query",
      content: `Terminate query ${ssid}/${ccnt}?`,
      okText: "Terminate",
      okType: "danger",
      onOk: async () => {
        try {
          await terminateQuery(Number(ssid), Number(ccnt));
          message.success("Query terminated");
          refresh();
        } catch (err) {
          message.error(`Failed: ${err instanceof Error ? err.message : String(err)}`);
        }
      },
    });
  };

  const handleMoveRsg = async () => {
    if (!rsgName.trim()) {
      message.warning("Please enter a resource group name");
      return;
    }
    try {
      await moveQueryToResourceGroup(Number(ssid), Number(ccnt), rsgName.trim());
      message.success(`Moved to resource group "${rsgName.trim()}"`);
      setMoveRsgVisible(false);
      setRsgName("");
      refresh();
    } catch (err) {
      message.error(`Failed: ${err instanceof Error ? err.message : String(err)}`);
    }
  };

  return (
    <div>
      <Space style={{ marginBottom: 16 }}>
        <Button icon={<ArrowLeftOutlined />} onClick={() => navigate("/queries")}>
          Back to Queries
        </Button>
      </Space>

      <Title level={3}>
        Query {ssid}/{ccnt}
      </Title>
      <ErrorAlert error={error} />

      <Spin spinning={loading}>
        {query && (
          <>
            <RuntimeMetricsMatrix ssid={Number(ssid)} ccnt={Number(ccnt)} />

            <Card
              title="Query Info"
              extra={
                <Space>
                  <Button
                    icon={<SwapOutlined />}
                    onClick={() => setMoveRsgVisible(true)}
                  >
                    Move to RSG
                  </Button>
                  <Button danger icon={<StopOutlined />} onClick={handleTerminate}>
                    Terminate
                  </Button>
                </Space>
              }
              style={{ marginBottom: 16 }}
            >
              <Descriptions bordered column={{ xs: 1, sm: 2, md: 3 }} size="small">
                <Descriptions.Item label="SSID">
                  <span className="mono">{query.queryKey?.ssid}</span>
                </Descriptions.Item>
                <Descriptions.Item label="CCNT">
                  <span className="mono">{query.queryKey?.ccnt}</span>
                </Descriptions.Item>
                <Descriptions.Item label="Status">
                  <QueryStatusBadge status={query.status ?? ""} />
                </Descriptions.Item>
                <Descriptions.Item label="User">{query.user}</Descriptions.Item>
                <Descriptions.Item label="Database">{query.database}</Descriptions.Item>
                <Descriptions.Item label="Resource Group">
                  {query.rsgName}
                </Descriptions.Item>
                <Descriptions.Item label="Host">{query.host}</Descriptions.Item>
                <Descriptions.Item label="Cluster ID">{query.clusterId}</Descriptions.Item>
                <Descriptions.Item label="Session ID">
                  <Button
                    type="link"
                    className="mono"
                    onClick={() => navigate(`/session/${query.sessionKey?.sessId}`)}
                  >
                    {query.sessionKey?.sessId}
                  </Button>
                </Descriptions.Item>
                <Descriptions.Item label="Duration">
                  {(query.queryDurationSeconds ?? 0).toFixed(1)}s
                </Descriptions.Item>
                <Descriptions.Item label="Query Start">
                  {query.queryStart}
                </Descriptions.Item>
                <Descriptions.Item label="Stat Kind">
                  {query.statKind}
                </Descriptions.Item>
                <Descriptions.Item label="Collect Time">
                  {query.collectTime}
                </Descriptions.Item>
                <Descriptions.Item label="Start Time">
                  {query.startTime}
                </Descriptions.Item>
                <Descriptions.Item label="End Time">
                  {query.endTime}
                </Descriptions.Item>
                <Descriptions.Item label="Completed">
                  <Tag color={query.completed ? c.green : c.primary}>
                    {query.completed ? "Yes" : "No"}
                  </Tag>
                </Descriptions.Item>
                <Descriptions.Item label="Session State">
                  {query.sessionState}
                </Descriptions.Item>
                <Descriptions.Item label="Slices">
                  {query.slices}
                </Descriptions.Item>
                <Descriptions.Item label="Blocked By Session">
                  {query.blockedBySessId ? (
                    <Button
                      type="link"
                      className="mono"
                      onClick={() => navigate(`/session/${query.blockedBySessId}`)}
                    >
                      {query.blockedBySessId}
                    </Button>
                  ) : (
                    ""
                  )}
                </Descriptions.Item>
                <Descriptions.Item label="Wait Mode">
                  {query.waitMode}
                </Descriptions.Item>
                <Descriptions.Item label="Locked Item">
                  {query.lockedItem}
                </Descriptions.Item>
                <Descriptions.Item label="Locked Mode">
                  {query.lockedMode}
                </Descriptions.Item>
                <Descriptions.Item label="Message">
                  {query.message}
                </Descriptions.Item>
              </Descriptions>
            </Card>

            {/* Detailed Query Info */}
            {query.queryInfo && (
              <Card title="Query Details" style={{ marginBottom: 16 }}>
                <Descriptions bordered column={{ xs: 1, sm: 2, md: 3 }} size="small">
                  <Descriptions.Item label="Generator">
                    {query.queryInfo.generator}
                  </Descriptions.Item>
                  <Descriptions.Item label="Query ID">
                    <span className="mono">{query.queryInfo.queryId || ""}</span>
                  </Descriptions.Item>
                  <Descriptions.Item label="Plan ID">
                    <span className="mono">{query.queryInfo.planId || ""}</span>
                  </Descriptions.Item>
                  <Descriptions.Item label="User">
                    {query.queryInfo.userName}
                  </Descriptions.Item>
                  <Descriptions.Item label="Database">
                    {query.queryInfo.databaseName}
                  </Descriptions.Item>
                  <Descriptions.Item label="Resource Group">
                    {query.queryInfo.rsgname}
                  </Descriptions.Item>
                  <Descriptions.Item label="Submit Time">
                    {query.queryInfo.submitTime}
                  </Descriptions.Item>
                  <Descriptions.Item label="Start Time">
                    {query.queryInfo.startTime}
                  </Descriptions.Item>
                  <Descriptions.Item label="End Time">
                    {query.queryInfo.endTime}
                  </Descriptions.Item>
                </Descriptions>
              </Card>
            )}

            <Card title="Query Text" style={{ marginBottom: 16 }}>
              <Paragraph>
                <pre style={cbStyle}>
                  {query.queryText || "(empty)"}
                </pre>
              </Paragraph>
            </Card>

            {query.queryInfo?.planText && (
              <Card title="Plan Text" style={{ marginBottom: 16 }}>
                <pre style={cbStyle}>
                  {query.queryInfo.planText}
                </pre>
              </Card>
            )}

            {query.queryInfo?.analyzeText && (
              <Card title="Explain Analyze" style={{ marginBottom: 16 }}>
                <pre style={cbStyle}>
                  {query.queryInfo.analyzeText}
                </pre>
              </Card>
            )}

            <GPMetricsCard title="Total Query Metrics" metrics={query.metrics} />
            <AggregatedMetricsCard title="Aggregated Metrics" metrics={query.aggregatedMetrics} />

            {/* Segment Metrics Table */}
            {query.segmentMetrics && query.segmentMetrics.length > 0 && (
              <Card title={`Segment Metrics (${query.segmentMetrics.length})`} style={{ marginBottom: 16 }}>
                <Table
                  dataSource={query.segmentMetrics}
                  rowKey={(r: SegmentMetricsInfo) =>
                    `${r.segmentKey?.dbid ?? 0}-${r.segmentKey?.segindex ?? 0}-${r.hostname ?? ""}`
                  }
                  size="small"
                  pagination={query.segmentMetrics.length > 20 ? { pageSize: 20 } : false}
                  scroll={{ x: 1200 }}
                  columns={[
                    {
                      title: "Segment",
                      key: "segment",
                      fixed: "left" as const,
                      width: 120,
                      render: (_: unknown, r: SegmentMetricsInfo) =>
                        r.segmentKey
                          ? `dbid=${r.segmentKey.dbid} seg=${r.segmentKey.segindex}`
                          : "N/A",
                    },
                    {
                      title: "Hostname",
                      dataIndex: "hostname",
                      width: 150,
                      ellipsis: true,
                    },
                    {
                      title: "Status",
                      dataIndex: "queryStatus",
                      width: 120,
                      render: (v: string) => <QueryStatusBadge status={v ?? ""} />,
                    },
                    {
                      title: "Start Time",
                      dataIndex: "startTime",
                      width: 200,
                      ellipsis: true,
                    },
                    {
                      title: "End Time",
                      dataIndex: "endTime",
                      width: 200,
                      ellipsis: true,
                    },
                    {
                      title: "CPU (s)",
                      key: "cpu",
                      width: 100,
                      render: (_: unknown, r: SegmentMetricsInfo) =>
                        r.metrics?.cpuUsage?.toFixed(2) ?? "N/A",
                    },
                    {
                      title: "Memory",
                      key: "memory",
                      width: 100,
                      render: (_: unknown, r: SegmentMetricsInfo) => {
                        const v = r.metrics?.memoryUsage;
                        if (!v) return "N/A";
                        if (v > 1073741824) return `${(v / 1073741824).toFixed(2)} GB`;
                        if (v > 1048576) return `${(v / 1048576).toFixed(2)} MB`;
                        if (v > 1024) return `${(v / 1024).toFixed(2)} KB`;
                        return `${v} B`;
                      },
                    },
                    {
                      title: "Disk Read",
                      key: "diskRead",
                      width: 100,
                      render: (_: unknown, r: SegmentMetricsInfo) => {
                        const v = r.metrics?.diskRead;
                        if (!v) return "N/A";
                        if (v > 1073741824) return `${(v / 1073741824).toFixed(2)} GB`;
                        if (v > 1048576) return `${(v / 1048576).toFixed(2)} MB`;
                        if (v > 1024) return `${(v / 1024).toFixed(2)} KB`;
                        return `${v} B`;
                      },
                    },
                    {
                      title: "Disk Write",
                      key: "diskWrite",
                      width: 100,
                      render: (_: unknown, r: SegmentMetricsInfo) => {
                        const v = r.metrics?.diskWrite;
                        if (!v) return "N/A";
                        if (v > 1073741824) return `${(v / 1073741824).toFixed(2)} GB`;
                        if (v > 1048576) return `${(v / 1048576).toFixed(2)} MB`;
                        if (v > 1024) return `${(v / 1024).toFixed(2)} KB`;
                        return `${v} B`;
                      },
                    },
                    {
                      title: "Net Sent",
                      key: "netSent",
                      width: 100,
                      render: (_: unknown, r: SegmentMetricsInfo) => {
                        const v = r.metrics?.networkSent;
                        if (!v) return "N/A";
                        if (v > 1073741824) return `${(v / 1073741824).toFixed(2)} GB`;
                        if (v > 1048576) return `${(v / 1048576).toFixed(2)} MB`;
                        if (v > 1024) return `${(v / 1024).toFixed(2)} KB`;
                        return `${v} B`;
                      },
                    },
                    {
                      title: "Net Recv",
                      key: "netRecv",
                      width: 100,
                      render: (_: unknown, r: SegmentMetricsInfo) => {
                        const v = r.metrics?.networkReceived;
                        if (!v) return "N/A";
                        if (v > 1073741824) return `${(v / 1073741824).toFixed(2)} GB`;
                        if (v > 1048576) return `${(v / 1048576).toFixed(2)} MB`;
                        if (v > 1024) return `${(v / 1024).toFixed(2)} KB`;
                        return `${v} B`;
                      },
                    },
                  ]}
                />
              </Card>
            )}
          </>
        )}
      </Spin>

      <Modal
        title="Move Query to Resource Group"
        open={moveRsgVisible}
        onOk={handleMoveRsg}
        onCancel={() => {
          setMoveRsgVisible(false);
          setRsgName("");
        }}
        okText="Move"
      >
        <p>Enter the target resource group name:</p>
        <Input
          placeholder="Resource group name"
          value={rsgName}
          onChange={(e) => setRsgName(e.target.value)}
          onPressEnter={handleMoveRsg}
        />
      </Modal>
    </div>
  );
}
