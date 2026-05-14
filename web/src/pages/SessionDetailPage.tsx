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
                <pre
                  style={{
                    background: "#f6f6f6",
                    padding: 16,
                    borderRadius: 6,
                    overflow: "auto",
                    maxHeight: 300,
                    fontSize: 13,
                    fontFamily: "'SF Mono', 'Fira Code', monospace",
                  }}
                >
                  {session.runningQueryText}
                </pre>
              </Card>
            )}

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
