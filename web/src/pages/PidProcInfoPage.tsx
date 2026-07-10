import { useCallback, useMemo, useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { Button, Card, Space, Table, Typography } from "antd";
import { ArrowLeftOutlined, ReloadOutlined } from "@ant-design/icons";
import { useApi } from "../hooks/useApi";
import {
  getPidProcInfo,
  type GpPidProcInfo,
  type PidProcInfoResponse,
} from "../api/client";
import ErrorAlert from "../components/ErrorAlert";
import { FONT_MONO } from "../theme";

const { Title, Text } = Typography;
const PAGE_SIZE = 50;

function numberValue(value?: string | number | null): number {
  if (value === undefined || value === null) return 0;
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function cpu(row: GpPidProcInfo): number {
  return (row.procStat?.utime ?? 0) + (row.procStat?.stime ?? 0);
}

function formatBytes(value?: number | null): string {
  if (!value) return "0 B";
  if (value >= 1073741824) return `${(value / 1073741824).toFixed(2)} GB`;
  if (value >= 1048576) return `${(value / 1048576).toFixed(2)} MB`;
  if (value >= 1024) return `${(value / 1024).toFixed(2)} KB`;
  return `${value} B`;
}

export default function PidProcInfoPage() {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const [page, setPage] = useState(1);
  const [pageTokens, setPageTokens] = useState<string[]>([""]);

  const params = useMemo(() => {
    const hostname = searchParams.get("hostname") || undefined;
    const ssidRaw = searchParams.get("ssid") || "0";
    const ccntRaw = searchParams.get("ccnt");
    const segindexRaw = searchParams.get("segindex");
    const sliceIDRaw = searchParams.get("slice_id");
    return {
      hostname,
      ssid: Number(ssidRaw),
      ccnt: ccntRaw !== null ? Number(ccntRaw) : undefined,
      segindex: segindexRaw !== null ? Number(segindexRaw) : undefined,
      sliceId: sliceIDRaw !== null ? Number(sliceIDRaw) : undefined,
    };
  }, [searchParams]);

  const load = useCallback(() => {
    return getPidProcInfo({
      ...params,
      pageSize: PAGE_SIZE,
      pageToken: pageTokens[page - 1] ?? "",
    }).then((response) => {
      if (response.nextPageToken) {
        setPageTokens((prev) => {
          if (prev[page] === response.nextPageToken) return prev;
          const next = prev.slice(0, page);
          next[page] = response.nextPageToken;
          return next;
        });
      }
      return response;
    });
  }, [page, pageTokens, params]);

  const { data, loading, error, refresh } = useApi<PidProcInfoResponse>(load, [load]);
  const rows = data?.pidProcData ?? [];
  const hasNext = Boolean(data?.nextPageToken);

  const columns = [
    {
      title: "PID",
      dataIndex: "pid",
      width: 110,
      render: (v: string) => <Text style={{ fontFamily: FONT_MONO }}>{v}</Text>,
    },
    {
      title: "Segindex",
      dataIndex: "gpSegmentId",
      width: 100,
      render: (v: string) => v,
      sorter: (a: GpPidProcInfo, b: GpPidProcInfo) => numberValue(a.gpSegmentId) - numberValue(b.gpSegmentId),
    },
    {
      title: "Session",
      dataIndex: "sessId",
      width: 110,
      render: (v: string) => v,
    },
    {
      title: "CCNT",
      dataIndex: "ccnt",
      width: 90,
      render: (v: number) => (v >= 0 ? v : "—"),
    },
    {
      title: "Slice",
      dataIndex: "sliceId",
      width: 90,
      render: (v: string) => v,
    },
    {
      title: "State",
      dataIndex: "state",
      width: 110,
      render: (v: string) => v || "—",
    },
    {
      title: "CPU",
      width: 90,
      render: (_: unknown, row: GpPidProcInfo) => cpu(row),
      sorter: (a: GpPidProcInfo, b: GpPidProcInfo) => cpu(a) - cpu(b),
      defaultSortOrder: "descend" as const,
    },
    {
      title: "RSS",
      width: 120,
      render: (_: unknown, row: GpPidProcInfo) => formatBytes(row.procStatus?.vmRss ?? 0),
    },
    {
      title: "Read",
      width: 120,
      render: (_: unknown, row: GpPidProcInfo) => formatBytes(row.procIo?.readBytes ?? 0),
    },
    {
      title: "Write",
      width: 120,
      render: (_: unknown, row: GpPidProcInfo) => formatBytes(row.procIo?.writeBytes ?? 0),
    },
    {
      title: "Spill",
      width: 140,
      render: (_: unknown, row: GpPidProcInfo) => `${formatBytes(row.procSpill?.size ?? 0)} (${row.procSpill?.files ?? 0})`,
    },
    {
      title: "Command line",
      dataIndex: "cmdline",
      width: 420,
      render: (v: string) => <Text style={{ fontFamily: FONT_MONO }}>{v || "—"}</Text>,
    },
  ];

  return (
    <Space direction="vertical" size="large" style={{ width: "100%" }}>
      <Space>
        <Button icon={<ArrowLeftOutlined />} onClick={() => navigate(-1)}>
          Back
        </Button>
        <Button icon={<ReloadOutlined />} onClick={refresh} loading={loading}>
          Refresh
        </Button>
      </Space>
      <Card>
        <Title level={3}>Process details</Title>
        <Text type="secondary">
          host {params.hostname ?? "—"}, ssid {params.ssid}
          {params.ccnt !== undefined ? `, ccnt ${params.ccnt}` : ""}
          {params.segindex !== undefined ? `, segindex ${params.segindex}` : ""}
          {params.sliceId !== undefined ? `, slice ${params.sliceId}` : ""}
        </Text>
      </Card>
      <ErrorAlert error={error} />
      <Table
        loading={loading}
        dataSource={rows}
        columns={columns}
        rowKey={(row) => `${row.gpSegmentId}-${row.sessId}-${row.pid}-${row.sliceId}`}
        pagination={false}
        scroll={{ x: 1600 }}
      />
      <Space>
        <Button disabled={page <= 1 || loading} onClick={() => setPage((prev) => Math.max(1, prev - 1))}>
          Previous
        </Button>
        <Text>Page {page}</Text>
        <Button disabled={!hasNext || loading} onClick={() => setPage((prev) => prev + 1)}>
          Next
        </Button>
      </Space>
    </Space>
  );
}
