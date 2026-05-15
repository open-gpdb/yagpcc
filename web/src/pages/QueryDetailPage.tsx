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
} from "antd";
import { ArrowLeftOutlined, StopOutlined, SwapOutlined } from "@ant-design/icons";
import { useState } from "react";
import { useApi } from "../hooks/useApi";
import {
  getQuery,
  terminateQuery,
  moveQueryToResourceGroup,
  type SegmentMetricsInfo,
} from "../api/client";
import ErrorAlert from "../components/ErrorAlert";
import QueryStatusBadge from "../components/QueryStatusBadge";
import { GPMetricsCard, AggregatedMetricsCard } from "../components/MetricsDisplay";
import { codeBlockStyle, TV } from "../theme";

const { Title, Paragraph } = Typography;

export default function QueryDetailPage() {
  const { ssid, ccnt } = useParams<{ ssid: string; ccnt: string }>();
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
                  <Tag color={query.completed ? TV.green : TV.primary}>
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
                <pre style={codeBlockStyle}>
                  {query.queryText || "(empty)"}
                </pre>
              </Paragraph>
            </Card>

            {query.queryInfo?.planText && (
              <Card title="Plan Text" style={{ marginBottom: 16 }}>
                <pre style={codeBlockStyle}>
                  {query.queryInfo.planText}
                </pre>
              </Card>
            )}

            {query.queryInfo?.analyzeText && (
              <Card title="Explain Analyze" style={{ marginBottom: 16 }}>
                <pre style={codeBlockStyle}>
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
