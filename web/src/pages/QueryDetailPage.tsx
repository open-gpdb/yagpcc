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
} from "antd";
import { ArrowLeftOutlined, StopOutlined, SwapOutlined } from "@ant-design/icons";
import { useState } from "react";
import { useApi } from "../hooks/useApi";
import {
  getQuery,
  terminateQuery,
  moveQueryToResourceGroup,
} from "../api/client";
import ErrorAlert from "../components/ErrorAlert";
import QueryStatusBadge from "../components/QueryStatusBadge";

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
                <Descriptions.Item label="PID">
                  {query.pid || ""}
                </Descriptions.Item>
                <Descriptions.Item label="Session State">
                  {query.state}
                </Descriptions.Item>
                <Descriptions.Item label="Wait Event Type">
                  {query.waitEventType}
                </Descriptions.Item>
                <Descriptions.Item label="Wait Event">
                  {query.waitEvent}
                </Descriptions.Item>
                <Descriptions.Item label="Query Level">
                  {query.runningQueryLevel}
                </Descriptions.Item>
                <Descriptions.Item label="Query Slices">
                  {query.runningQuerySlices}
                </Descriptions.Item>
                <Descriptions.Item label="Query Error">
                  {query.runningQueryError}
                </Descriptions.Item>
              </Descriptions>
            </Card>

            <Card title="Query Text" style={{ marginBottom: 16 }}>
              <Paragraph>
                <pre
                  style={{
                    background: "#f6f6f6",
                    padding: 16,
                    borderRadius: 6,
                    overflow: "auto",
                    maxHeight: 400,
                    fontSize: 13,
                    fontFamily: "'SF Mono', 'Fira Code', monospace",
                  }}
                >
                  {query.queryText || "(empty)"}
                </pre>
              </Paragraph>
            </Card>

            {query.metrics && (
              <Card title="Metrics">
                <Descriptions bordered column={{ xs: 1, sm: 2, md: 3 }} size="small">
                  <Descriptions.Item label="CPU Usage">
                    {query.metrics.cpuUsage?.toFixed(2) ?? "N/A"}
                  </Descriptions.Item>
                  <Descriptions.Item label="Memory Usage">
                    {query.metrics.memoryUsage?.toFixed(2) ?? "N/A"}
                  </Descriptions.Item>
                  <Descriptions.Item label="Disk Read">
                    {query.metrics.diskRead?.toFixed(2) ?? "N/A"}
                  </Descriptions.Item>
                  <Descriptions.Item label="Disk Write">
                    {query.metrics.diskWrite?.toFixed(2) ?? "N/A"}
                  </Descriptions.Item>
                  <Descriptions.Item label="Network Sent">
                    {query.metrics.networkSent?.toFixed(2) ?? "N/A"}
                  </Descriptions.Item>
                  <Descriptions.Item label="Network Received">
                    {query.metrics.networkReceived?.toFixed(2) ?? "N/A"}
                  </Descriptions.Item>
                </Descriptions>
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
