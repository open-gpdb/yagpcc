import { useState, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import {
  Table,
  Card,
  Typography,
  Button,
  Space,
  Input,
  Select,
  Switch,
  Modal,
  message,
} from "antd";
import { ReloadOutlined, StopOutlined } from "@ant-design/icons";
import { useApi } from "../hooks/useApi";
import {
  getSessions,
  terminateSession,
  type SessionInfo,
  type GetSessionsParams,
} from "../api/client";
import ErrorAlert from "../components/ErrorAlert";
import SessionStateBadge from "../components/SessionStateBadge";

const { Title } = Typography;
const { Search } = Input;

export default function SessionsPage() {
  const navigate = useNavigate();
  const [params, setParams] = useState<GetSessionsParams>({
    pageSize: 50,
    showSystem: false,
    hideEmptyQueries: false,
    sort: ["TOTAL_RUNNINGTIMESECONDS:DESC"],
  });
  const [filterUser, setFilterUser] = useState("");
  const [filterDatabase, setFilterDatabase] = useState("");
  const [filterState, setFilterState] = useState("");

  const buildParams = useCallback((): GetSessionsParams => {
    const filters: Record<string, string> = {};
    if (filterUser) filters["filter_user"] = filterUser;
    if (filterDatabase) filters["filter_database"] = filterDatabase;
    if (filterState) filters["filter_state"] = filterState;
    return { ...params, filters };
  }, [params, filterUser, filterDatabase, filterState]);

  const { data, loading, error, refresh } = useApi(
    () => getSessions(buildParams()),
    [params, filterUser, filterDatabase, filterState],
  );

  const handleTerminate = (sessId: string) => {
    Modal.confirm({
      title: "Terminate Session",
      content: `Are you sure you want to terminate session ${sessId}?`,
      okText: "Terminate",
      okType: "danger",
      onOk: async () => {
        try {
          await terminateSession(Number(sessId));
          message.success(`Session ${sessId} terminated`);
          refresh();
        } catch (err) {
          message.error(`Failed to terminate: ${err instanceof Error ? err.message : String(err)}`);
        }
      },
    });
  };

  const columns = [
    {
      title: "Session ID",
      dataIndex: ["sessionKey", "sessId"],
      render: (v: string, _record: SessionInfo) => (
        <Button
          type="link"
          className="mono"
          onClick={() => navigate(`/session/${v}`)}
        >
          {v}
        </Button>
      ),
    },
    { title: "User", dataIndex: "user", ellipsis: true },
    { title: "Database", dataIndex: "database", ellipsis: true },
    { title: "Host", dataIndex: "host", ellipsis: true },
    { title: "App", dataIndex: "applicationName", ellipsis: true },
    {
      title: "State",
      dataIndex: "state",
      render: (v: string) => <SessionStateBadge state={v ?? ""} />,
    },
    {
      title: "Running (s)",
      dataIndex: "totalRunningTimeSeconds",
      render: (v: number) => (v ?? 0).toFixed(1),
      sorter: true,
      width: 120,
    },
    {
      title: "Queries",
      dataIndex: "queries",
      render: (v: unknown[]) => v?.length ?? 0,
      width: 80,
    },
    {
      title: "Actions",
      key: "actions",
      width: 100,
      render: (_: unknown, record: SessionInfo) => (
        <Button
          danger
          size="small"
          icon={<StopOutlined />}
          onClick={() => handleTerminate(record.sessionKey?.sessId ?? "0")}
        >
          Kill
        </Button>
      ),
    },
  ];

  return (
    <div>
      <Title level={3}>Sessions</Title>
      <ErrorAlert error={error} />

      <Card style={{ marginBottom: 16 }}>
        <Space wrap>
          <Search
            placeholder="Filter by user"
            allowClear
            onSearch={setFilterUser}
            style={{ width: 180 }}
          />
          <Search
            placeholder="Filter by database"
            allowClear
            onSearch={setFilterDatabase}
            style={{ width: 180 }}
          />
          <Select
            placeholder="Filter by state"
            allowClear
            style={{ width: 200 }}
            onChange={(v) => setFilterState(v ?? "")}
            options={[
              { value: "SESSION_STATUS_ACTIVE", label: "Active" },
              { value: "SESSION_STATUS_IDLE", label: "Idle" },
              { value: "SESSION_STATUS_IDLE_IN_TRANSACTION", label: "Idle in Transaction" },
              {
                value: "SESSION_STATUS_IDLE_IN_TRANSACTION_ABORTED",
                label: "Idle in Txn (Aborted)",
              },
            ]}
          />
          <Switch
            checkedChildren="System"
            unCheckedChildren="System"
            checked={params.showSystem}
            onChange={(v) => setParams((p) => ({ ...p, showSystem: v }))}
          />
          <Switch
            checkedChildren="Hide Empty"
            unCheckedChildren="Hide Empty"
            checked={params.hideEmptyQueries}
            onChange={(v) => setParams((p) => ({ ...p, hideEmptyQueries: v }))}
          />
          <Button icon={<ReloadOutlined />} onClick={refresh}>
            Refresh
          </Button>
        </Space>
      </Card>

      <Card>
        <Table
          loading={loading}
          dataSource={data?.sessions ?? []}
          columns={columns}
          rowKey={(r) => `${r.sessionKey?.sessId ?? ""}-${r.sessionKey?.tmId ?? ""}`}
          size="small"
          pagination={{
            total: Number(data?.totalCount ?? 0),
            pageSize: params.pageSize ?? 50,
            showSizeChanger: true,
            showTotal: (total) => `${total} sessions`,
          }}
          scroll={{ x: 1000 }}
        />
      </Card>
    </div>
  );
}
