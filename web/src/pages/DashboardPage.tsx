import { Card, Col, Row, Statistic, Spin, Typography, Table } from "antd";
import {
  UserOutlined,
  ThunderboltOutlined,
  PauseCircleOutlined,
  ClockCircleOutlined,
} from "@ant-design/icons";
import { useApi } from "../hooks/useApi";
import { getSessionStats, getSessions, type SessionStat } from "../api/client";
import ErrorAlert from "../components/ErrorAlert";
import SessionStateBadge from "../components/SessionStateBadge";

const { Title } = Typography;

const stateIcons: Record<string, React.ReactNode> = {
  active: <ThunderboltOutlined style={{ color: "#52c41a" }} />,
  idle: <PauseCircleOutlined style={{ color: "#8c8c8c" }} />,
  "idle in transaction": <ClockCircleOutlined style={{ color: "#faad14" }} />,
};

export default function DashboardPage() {
  const stats = useApi(() => getSessionStats(), []);
  const recentSessions = useApi(
    () => getSessions({ pageSize: 10, sort: ["TOTAL_RUNNINGTIMESECONDS:DESC"] }),
    [],
  );

  const totalSessions =
    stats.data?.stats?.reduce((sum: number, s: SessionStat) => sum + (s.count ?? 0), 0) ?? 0;

  return (
    <div>
      <Title level={3}>Dashboard</Title>

      <ErrorAlert error={stats.error} />
      <ErrorAlert error={recentSessions.error} />

      {/* Session state summary cards */}
      <Spin spinning={stats.loading}>
        <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
          <Col xs={24} sm={12} md={6}>
            <Card>
              <Statistic
                title="Total Sessions"
                value={totalSessions}
                prefix={<UserOutlined />}
              />
            </Card>
          </Col>
          {stats.data?.stats?.map((s: SessionStat) => {
            const normalized = s.state
              ?.toLowerCase()
              .replace(/session_status_/i, "")
              .replace(/_/g, " ");
            return (
              <Col xs={24} sm={12} md={6} key={s.state}>
                <Card>
                  <Statistic
                    title={normalized || s.state}
                    value={s.count ?? 0}
                    prefix={stateIcons[normalized ?? ""] ?? <UserOutlined />}
                  />
                </Card>
              </Col>
            );
          })}
        </Row>
      </Spin>

      {/* Recent long-running sessions */}
      <Card title="Top Sessions by Running Time" style={{ marginBottom: 24 }}>
        <Table
          loading={recentSessions.loading}
          dataSource={recentSessions.data?.sessions ?? []}
          rowKey={(r) => `${r.sessionKey?.sessId ?? ""}-${r.sessionKey?.tmId ?? ""}`}
          pagination={false}
          size="small"
          columns={[
            {
              title: "Session ID",
              dataIndex: ["sessionKey", "sessId"],
              render: (v: string) => <span className="mono">{v}</span>,
            },
            { title: "User", dataIndex: "user" },
            { title: "Database", dataIndex: "database" },
            { title: "Host", dataIndex: "host" },
            {
              title: "State",
              dataIndex: "state",
              render: (v: string) => <SessionStateBadge state={v ?? ""} />,
            },
            {
              title: "Running Time (s)",
              dataIndex: "totalRunningTimeSeconds",
              render: (v: number) => (v ?? 0).toFixed(1),
              sorter: (a, b) =>
                (a.totalRunningTimeSeconds ?? 0) - (b.totalRunningTimeSeconds ?? 0),
              defaultSortOrder: "descend",
            },
          ]}
        />
      </Card>
    </div>
  );
}
