import { useState, useCallback, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { Table, Card, Typography, Button, Space, Input, Modal, message } from "antd";
import { ReloadOutlined, StopOutlined } from "@ant-design/icons";
import { useApi } from "../hooks/useApi";
import {
  getQueries,
  terminateQuery,
  type QueryInfo,
  type GetQueriesParams,
} from "../api/client";
import ErrorAlert from "../components/ErrorAlert";
import QueryStatusBadge from "../components/QueryStatusBadge";

const { Title } = Typography;
const { Search } = Input;

export default function QueriesPage() {
  const navigate = useNavigate();
  const [params, setParams] = useState<GetQueriesParams>({
    pageSize: 50,
  });
  const [filterUser, setFilterUser] = useState("");
  const [filterDatabase, setFilterDatabase] = useState("");
  const [page, setPage] = useState(1);
  const [pageTokens, setPageTokens] = useState<Record<number, string>>({ 1: "" });

  const resetPagination = useCallback(() => {
    setPage(1);
    setPageTokens({ 1: "" });
  }, []);

  const buildParams = useCallback((): GetQueriesParams => {
    const filters: Record<string, string> = {};
    if (filterUser) filters["filter_user"] = filterUser;
    if (filterDatabase) filters["filter_database"] = filterDatabase;
    return { ...params, pageToken: pageTokens[page] ?? "", filters };
  }, [params, filterUser, filterDatabase, page, pageTokens]);

  const { data, loading, error, refresh } = useApi(
    () => getQueries(buildParams()),
    [params, filterUser, filterDatabase, page, pageTokens],
  );

  useEffect(() => {
    const nextToken = data?.nextPageToken;
    if (!nextToken) return;
    setPageTokens((prev) => {
      if (prev[page + 1] === nextToken) return prev;
      return { ...prev, [page + 1]: nextToken };
    });
  }, [data?.nextPageToken, page]);

  const handleTerminate = (ssid: number, ccnt: number) => {
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

  const columns = [
    {
      title: "SSID/CCNT",
      fixed: "left" as const,
      render: (_: unknown, r: QueryInfo) => (
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
      width: 120,
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
    { title: "User", dataIndex: "user", width: 120, ellipsis: true },
    { title: "Database", dataIndex: "database", width: 120, ellipsis: true },
    {
      title: "Status",
      dataIndex: "status",
      width: 120,
      render: (v: string) => <QueryStatusBadge status={v ?? ""} />,
    },
    {
      title: "Duration (s)",
      dataIndex: "queryDurationSeconds",
      width: 110,
      render: (v: number) => (v ?? 0).toFixed(1),
      sorter: (a: QueryInfo, b: QueryInfo) =>
        (a.queryDurationSeconds ?? 0) - (b.queryDurationSeconds ?? 0),
    },
    { title: "Resource Group", dataIndex: "rsgName", width: 140, ellipsis: true },
    { title: "Host", dataIndex: "host", width: 140, ellipsis: true },
    { title: "PID", dataIndex: "pid", width: 80 },
    { title: "Session State", dataIndex: "state", width: 120, ellipsis: true },
    { title: "Query Start", dataIndex: "queryStart", width: 180, ellipsis: true },
    { title: "Wait Event Type", dataIndex: "waitEventType", width: 140, ellipsis: true },
    { title: "Wait Event", dataIndex: "waitEvent", width: 140, ellipsis: true },
    { title: "Query Level", dataIndex: "runningQueryLevel", width: 100 },
    { title: "Query Slices", dataIndex: "runningQuerySlices", width: 110 },
    { title: "Query Error", dataIndex: "runningQueryError", width: 160, ellipsis: true },
    {
      title: "Session ID",
      dataIndex: ["sessionKey", "sessId"],
      width: 110,
      render: (v: string) => (
        <Button
          type="link"
          className="mono"
          onClick={() => navigate(`/session/${v}`)}
        >
          {v}
        </Button>
      ),
    },
    {
      title: "Actions",
      key: "actions",
      fixed: "right" as const,
      width: 80,
      render: (_: unknown, r: QueryInfo) => (
        <Button
          danger
          size="small"
          icon={<StopOutlined />}
          onClick={() => handleTerminate(r.queryKey?.ssid ?? 0, r.queryKey?.ccnt ?? 0)}
        >
          Kill
        </Button>
      ),
    },
  ];

  return (
    <div>
      <Title level={3}>Queries</Title>
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
          <Button icon={<ReloadOutlined />} onClick={refresh}>
            Refresh
          </Button>
        </Space>
      </Card>

      <Card>
        <Table
          loading={loading}
          dataSource={data?.queries ?? []}
          columns={columns}
          rowKey={(r) => `${r.queryKey?.ssid ?? 0}-${r.queryKey?.ccnt ?? 0}`}
          size="small"
          pagination={{
            current: page,
            total: Number(data?.totalCount ?? 0),
            pageSize: params.pageSize ?? 50,
            showSizeChanger: true,
            showTotal: (total: number) => `${total} queries`,
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
          scroll={{ x: 2400 }}
        />
      </Card>
    </div>
  );
}
