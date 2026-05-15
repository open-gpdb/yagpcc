import { useParams, useNavigate } from "react-router-dom";
import {
  Card,
  Descriptions,
  Typography,
  Spin,
  Table,
  Button,
  Space,
  Modal,
  message,
} from "antd";
import { ArrowLeftOutlined, StopOutlined } from "@ant-design/icons";
import { useApi } from "../hooks/useApi";
import {
  getSession,
  terminateSession,
  terminateQuery,
  type QueryDesc,
} from "../api/client";
import ErrorAlert from "../components/ErrorAlert";
import SessionStateBadge from "../components/SessionStateBadge";
import QueryStatusBadge from "../components/QueryStatusBadge";
import { GPMetricsCard, AggregatedMetricsCard } from "../components/MetricsDisplay";
import { codeBlockStyle } from "../theme";

const { Title } = Typography;

export default function SessionDetailPage() {
  const { sessId } = useParams<{ sessId: string }>();
  const navigate = useNavigate();

  const { data, loading, error, refresh } = useApi(
    () => getSession(Number(sessId)),
    [sessId],
  );

  const session = data?.session;

  const handleTerminateSession = () => {
    Modal.confirm({
      title: "Terminate Session",
      content: `Terminate session ${sessId}?`,
      okText: "Terminate",
      okType: "danger",
      onOk: async () => {
        try {
          await terminateSession(Number(sessId));
          message.success("Session terminated");
          navigate("/sessions");
        } catch (err) {
          message.error(`Failed: ${err instanceof Error ? err.message : String(err)}`);
        }
      },
    });
  };

  const handleTerminateQuery = (ssid: number, ccnt: number) => {
    Modal.confirm({
      title: "Terminate Query",
      content: `Terminate query ${ssid}/${ccnt}?`,
      okText: "Terminate",
      okType: "danger",
      onOk: async () => {
        try {
          await terminateQuery(ssid, ccnt);
          message.success("Query terminated");
          refresh();
        } catch (err) {
          message.error(`Failed: ${err instanceof Error ? err.message : String(err)}`);
        }
      },
    });
  };

  return (
    <div>
      <Space style={{ marginBottom: 16 }}>
        <Button icon={<ArrowLeftOutlined />} onClick={() => navigate("/sessions")}>
          Back to Sessions
        </Button>
      </Space>

      <Title level={3}>Session {sessId}</Title>
      <ErrorAlert error={error} />

      <Spin spinning={loading}>
        {session && (
          <>
            <Card
              title="Session Info"
              extra={
                <Button danger icon={<StopOutlined />} onClick={handleTerminateSession}>
                  Terminate Session
                </Button>
              }
              style={{ marginBottom: 16 }}
            >
              <Descriptions bordered column={{ xs: 1, sm: 2, md: 3 }} size="small">
                <Descriptions.Item label="Session ID">
                  <span className="mono">{session.sessionKey?.sessId}</span>
                </Descriptions.Item>
                <Descriptions.Item label="TM ID">
                  <span className="mono">{session.sessionKey?.tmId}</span>
                </Descriptions.Item>
                <Descriptions.Item label="PID">
                  <span className="mono">{session.pid}</span>
                </Descriptions.Item>
                <Descriptions.Item label="State">
                  <SessionStateBadge state={session.state ?? ""} />
                </Descriptions.Item>
                <Descriptions.Item label="User">{session.user}</Descriptions.Item>
                <Descriptions.Item label="Database">{session.database}</Descriptions.Item>
                <Descriptions.Item label="Host">{session.host}</Descriptions.Item>
                <Descriptions.Item label="Cluster ID">{session.clusterId}</Descriptions.Item>
                <Descriptions.Item label="Collect Time">{session.collectTime}</Descriptions.Item>
                <Descriptions.Item label="Application">
                  {session.applicationName}
                </Descriptions.Item>
                <Descriptions.Item label="Client Address">
                  {session.clientAddr}
                </Descriptions.Item>
                <Descriptions.Item label="Client Hostname">
                  {session.clientHostname}
                </Descriptions.Item>
                <Descriptions.Item label="Client Port">
                  {session.clientPort}
                </Descriptions.Item>
                <Descriptions.Item label="Resource Group">
                  {session.rsgName}
                </Descriptions.Item>
                <Descriptions.Item label="Resource Group ID">
                  {session.rsgId || ""}
                </Descriptions.Item>
                <Descriptions.Item label="RSG Queue Duration">
                  {session.rsgQueueDuration}
                </Descriptions.Item>
                <Descriptions.Item label="Wait Event Type">
                  {session.waitEventType}
                </Descriptions.Item>
                <Descriptions.Item label="Wait Event">
                  {session.waitEvent}
                </Descriptions.Item>
                <Descriptions.Item label="Waiting">
                  {session.waiting ? "Yes" : "No"}
                </Descriptions.Item>
                <Descriptions.Item label="Waiting Reason">
                  {session.waitingReason}
                </Descriptions.Item>
                <Descriptions.Item label="Running Time">
                  {(session.totalRunningTimeSeconds ?? 0).toFixed(1)}s
                </Descriptions.Item>
                <Descriptions.Item label="Backend Start">
                  {session.backendStart}
                </Descriptions.Item>
                <Descriptions.Item label="Transaction Start">
                  {session.xactStart}
                </Descriptions.Item>
                <Descriptions.Item label="Query Start">
                  {session.queryStart}
                </Descriptions.Item>
                <Descriptions.Item label="State Change">
                  {session.stateChange}
                </Descriptions.Item>
                <Descriptions.Item label="Backend XID">
                  {session.backendXid}
                </Descriptions.Item>
                <Descriptions.Item label="Backend XMIN">
                  {session.backendXmin}
                </Descriptions.Item>
                <Descriptions.Item label="Blocked By Session">
                  {session.blockedBySessId ? (
                    <Button
                      type="link"
                      className="mono"
                      onClick={() => navigate(`/session/${session.blockedBySessId}`)}
                    >
                      {session.blockedBySessId}
                    </Button>
                  ) : (
                    ""
                  )}
                </Descriptions.Item>
                <Descriptions.Item label="Wait Mode">
                  {session.waitMode}
                </Descriptions.Item>
                <Descriptions.Item label="Locked Item">
                  {session.lockedItem}
                </Descriptions.Item>
                <Descriptions.Item label="Locked Mode">
                  {session.lockedMode}
                </Descriptions.Item>
                <Descriptions.Item label="Running Query Status">
                  {session.runningQueryStatus}
                </Descriptions.Item>
                <Descriptions.Item label="Running Query Level">
                  {session.runningQueryLevel}
                </Descriptions.Item>
                <Descriptions.Item label="Running Query Slices">
                  {session.runningQuerySlices}
                </Descriptions.Item>
                <Descriptions.Item label="Running Query Error">
                  {session.runningQueryError}
                </Descriptions.Item>
                <Descriptions.Item label="Blocked Session Level">
                  {session.blockedSessionLevel || ""}
                </Descriptions.Item>
              </Descriptions>
            </Card>

            {session.runningQueryText && (
              <Card title="Running Query Text" style={{ marginBottom: 16 }}>
                <pre style={codeBlockStyle}>
                  {session.runningQueryText}
                </pre>
              </Card>
            )}

            {session.runningQueryInfo && (
              <Card title="Running Query Details" style={{ marginBottom: 16 }}>
                <Descriptions bordered column={{ xs: 1, sm: 2, md: 3 }} size="small">
                  <Descriptions.Item label="Generator">
                    {session.runningQueryInfo.generator}
                  </Descriptions.Item>
                  <Descriptions.Item label="Query ID">
                    <span className="mono">{session.runningQueryInfo.queryId || ""}</span>
                  </Descriptions.Item>
                  <Descriptions.Item label="Plan ID">
                    <span className="mono">{session.runningQueryInfo.planId || ""}</span>
                  </Descriptions.Item>
                  <Descriptions.Item label="User">
                    {session.runningQueryInfo.userName}
                  </Descriptions.Item>
                  <Descriptions.Item label="Database">
                    {session.runningQueryInfo.databaseName}
                  </Descriptions.Item>
                  <Descriptions.Item label="Resource Group">
                    {session.runningQueryInfo.rsgname}
                  </Descriptions.Item>
                  <Descriptions.Item label="Submit Time">
                    {session.runningQueryInfo.submitTime}
                  </Descriptions.Item>
                  <Descriptions.Item label="Start Time">
                    {session.runningQueryInfo.startTime}
                  </Descriptions.Item>
                  <Descriptions.Item label="End Time">
                    {session.runningQueryInfo.endTime}
                  </Descriptions.Item>
                </Descriptions>
                {session.runningQueryInfo.planText && (
                  <>
                    <Typography.Title level={5} style={{ marginTop: 12 }}>
                      Plan Text
                    </Typography.Title>
                    <pre style={codeBlockStyle}>
                      {session.runningQueryInfo.planText}
                    </pre>
                  </>
                )}
                {session.runningQueryInfo.analyzeText && (
                  <>
                    <Typography.Title level={5} style={{ marginTop: 12 }}>
                      Explain Analyze
                    </Typography.Title>
                    <pre style={codeBlockStyle}>
                      {session.runningQueryInfo.analyzeText}
                    </pre>
                  </>
                )}
              </Card>
            )}

            <GPMetricsCard title="Query Metrics (Current Query)" metrics={session.queryMetrics} />
            <GPMetricsCard title="Total Metrics (Session Lifetime)" metrics={session.totalMetrics} />
            <GPMetricsCard title="Last Metrics (1h Window)" metrics={session.lastMetrics} />
            <AggregatedMetricsCard title="Aggregated Metrics" metrics={session.aggregatedMetrics} />

            <Card title={`Queries (${session.queries?.length ?? 0})`}>
              <Table
                dataSource={session.queries ?? []}
                rowKey={(r) => `${r.queryKey?.ssid ?? 0}-${r.queryKey?.ccnt ?? 0}`}
                size="small"
                pagination={false}
                columns={[
                  {
                    title: "SSID/CCNT",
                    render: (_: unknown, r: QueryDesc) => (
                      <Button
                        type="link"
                        className="mono"
                        onClick={() =>
                          navigate(`/query/${r.queryKey?.ssid ?? 0}/${r.queryKey?.ccnt ?? 0}`)
                        }
                      >
                        {r.queryKey?.ssid}/{r.queryKey?.ccnt}
                      </Button>
                    ),
                  },
                  {
                    title: "Query Text",
                    dataIndex: "queryText",
                    ellipsis: true,
                    render: (v: string) => (
                      <span className="mono truncate" title={v}>
                        {v}
                      </span>
                    ),
                  },
                  {
                    title: "Status",
                    dataIndex: "status",
                    render: (v: string) => <QueryStatusBadge status={v ?? ""} />,
                  },
                  {
                    title: "Duration (s)",
                    dataIndex: "queryDurationSeconds",
                    render: (v: number) => (v ?? 0).toFixed(1),
                  },
                  {
                    title: "Query Start",
                    dataIndex: "queryStart",
                    ellipsis: true,
                  },
                  {
                    title: "Actions",
                    key: "actions",
                    render: (_: unknown, r: QueryDesc) => (
                      <Button
                        danger
                        size="small"
                        icon={<StopOutlined />}
                        onClick={() =>
                          handleTerminateQuery(r.queryKey?.ssid ?? 0, r.queryKey?.ccnt ?? 0)
                        }
                      >
                        Kill
                      </Button>
                    ),
                  },
                ]}
              />
            </Card>
          </>
        )}
      </Spin>
    </div>
  );
}
