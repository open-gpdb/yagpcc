import { useState, useCallback, useEffect } from "react";
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
  const [page, setPage] = useState(1);
  const [pageTokens, setPageTokens] = useState<Record<number, string>>({ 1: "" });

  const resetPagination = useCallback(() => {
    setPage(1);
    setPageTokens({ 1: "" });
  }, []);

  const buildParams = useCallback((): GetSessionsParams => {
    const filters: Record<string, string> = {};
    if (filterUser) filters["filter_user"] = filterUser;
    if (filterDatabase) filters["filter_database"] = filterDatabase;
    if (filterState) filters["filter_state"] = filterState;
    return { ...params, pageToken: pageTokens[page] ?? "", filters };
  }, [params, filterUser, filterDatabase, filterState, page, pageTokens]);

  const { data, loading, error, refresh } = useApi(
    () => getSessions(buildParams()),
    [params, filterUser, filterDatabase, filterState, page, pageTokens],
  );

  useEffect(() => {
    const nextToken = data?.nextPageToken;
    if (!nextToken) return;
    setPageTokens((prev) => {
      if (prev[page + 1] === nextToken) return prev;
      return { ...prev, [page + 1]: nextToken };
    });
  }, [data?.nextPageToken, page]);

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
      fixed: "left" as const,
      width: 110,
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
    { title: "PID", dataIndex: "pid", width: 80 },
    { title: "User", dataIndex: "user", width: 120, ellipsis: true },
    { title: "Database", dataIndex: "database", width: 120, ellipsis: true },
    { title: "Host", dataIndex: "host", width: 140, ellipsis: true },
    { title: "App", dataIndex: "applicationName", width: 140, ellipsis: true },
    {
      title: "State",
      dataIndex: "state",
      width: 120,
      render: (v: string) => <SessionStateBadge state={v ?? ""} />,
    },
    {
      title: "Running (s)",
      dataIndex: "totalRunningTimeSeconds",
      render: (v: number) => (v ?? 0).toFixed(1),
      sorter: true,
      width: 110,
    },
    { title: "Client Addr", dataIndex: "clientAddr", width: 130, ellipsis: true },
    { title: "Client Host", dataIndex: "clientHostname", width: 140, ellipsis: true },
    { title: "Client Port", dataIndex: "clientPort", width: 100 },
    { title: "Wait Event Type", dataIndex: "waitEventType", width: 140, ellipsis: true },
    { title: "Wait Event", dataIndex: "waitEvent", width: 140, ellipsis: true },
    {
      title: "Waiting",
      dataIndex: "waiting",
      width: 80,
      render: (v: boolean) => (v ? "Yes" : "No"),
    },
    { title: "Waiting Reason", dataIndex: "waitingReason", width: 140, ellipsis: true },
    { title: "RSG Name", dataIndex: "rsgName", width: 130, ellipsis: true },
    { title: "RSG Queue Duration", dataIndex: "rsgQueueDuration", width: 150, ellipsis: true },
    { title: "Backend Start", dataIndex: "backendStart", width: 180, ellipsis: true },
    { title: "Xact Start", dataIndex: "xactStart", width: 180, ellipsis: true },
    { title: "Query Start", dataIndex: "queryStart", width: 180, ellipsis: true },
    { title: "State Change", dataIndex: "stateChange", width: 180, ellipsis: true },
    { title: "Backend XID", dataIndex: "backendXid", width: 120, ellipsis: true },
    { title: "Backend XMIN", dataIndex: "backendXmin", width: 120, ellipsis: true },
    {
      title: "Blocked By",
      dataIndex: "blockedBySessId",
      width: 110,
      render: (v: number) => (v ? String(v) : ""),
    },
    { title: "Wait Mode", dataIndex: "waitMode", width: 120, ellipsis: true },
    { title: "Locked Item", dataIndex: "lockedItem", width: 140, ellipsis: true },
    { title: "Locked Mode", dataIndex: "lockedMode", width: 120, ellipsis: true },
    {
      title: "Running Query",
      dataIndex: "runningQueryText",
      width: 200,
      ellipsis: true,
      render: (v: string) => (
        <span className="mono truncate" title={v}>
          {v}
        </span>
      ),
    },
    { title: "Query Status", dataIndex: "runningQueryStatus", width: 120, ellipsis: true },
    { title: "Query Level", dataIndex: "runningQueryLevel", width: 100 },
    { title: "Query Slices", dataIndex: "runningQuerySlices", width: 110 },
    { title: "Query Error", dataIndex: "runningQueryError", width: 160, ellipsis: true },
    {
      title: "Blocked Level",
      dataIndex: "blockedSessionLevel",
      width: 120,
      render: (v: number) => (v ? String(v) : ""),
    },
    {
      title: "Queries",
      dataIndex: "queries",
      width: 80,
      render: (v: unknown[]) => v?.length ?? 0,
    },
    { title: "TM ID", dataIndex: ["sessionKey", "tmId"], width: 100 },
    {
      title: "Actions",
      key: "actions",
      fixed: "right" as const,
      width: 80,
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
            onSearch={(v) => {
              setFilterUser(v);
              resetPagination();
            }}
            style={{ width: 180 }}
          />
          <Search
            placeholder="Filter by database"
            allowClear
            onSearch={(v) => {
              setFilterDatabase(v);
              resetPagination();
            }}
            style={{ width: 180 }}
          />
          <Select
            placeholder="Filter by state"
            allowClear
            style={{ width: 200 }}
            onChange={(v) => {
              setFilterState(v ?? "");
              resetPagination();
            }}
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
            onChange={(v) => {
              setParams((p) => ({ ...p, showSystem: v }));
              resetPagination();
            }}
          />
          <Switch
            checkedChildren="Hide Empty"
            unCheckedChildren="Hide Empty"
            checked={params.hideEmptyQueries}
            onChange={(v) => {
              setParams((p) => ({ ...p, hideEmptyQueries: v }));
              resetPagination();
            }}
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
            current: page,
            total: Number(data?.totalCount ?? 0),
            pageSize: params.pageSize ?? 50,
            showSizeChanger: true,
            showTotal: (total) => `${total} sessions`,
            onChange: (nextPage, nextPageSize) => {
              if (nextPageSize !== (params.pageSize ?? 50)) {
                setParams((p) => ({ ...p, pageSize: nextPageSize }));
                resetPagination();
                return;
              }
              if (nextPage === 1 || pageTokens[nextPage] !== undefined) {
                setPage(nextPage);
              } else {
                message.info("Please move to pages sequentially.");
              }
            },
          }}
          scroll={{ x: 4000 }}
        />
      </Card>
    </div>
  );
}
