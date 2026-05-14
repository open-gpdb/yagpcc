import { useState } from "react";
import { Table, Card, Typography, Select, Spin, Space, Button } from "antd";
import { ReloadOutlined } from "@ant-design/icons";
import { useApi } from "../hooks/useApi";
import {
  getExtensions,
  getDatabases,
  type PgExtensionInfo,
  type DatabaseExtensionsInfo,
} from "../api/client";
import ErrorAlert from "../components/ErrorAlert";

const { Title } = Typography;

export default function ExtensionsPage() {
  const [selectedDb, setSelectedDb] = useState<string | undefined>(undefined);

  const databases = useApi(() => getDatabases(), []);
  const extensions = useApi(() => getExtensions(selectedDb), [selectedDb]);

  const allExtensions: (PgExtensionInfo & { databaseName: string })[] = [];
  extensions.data?.databases?.forEach((db: DatabaseExtensionsInfo) => {
    db.extensions?.forEach((ext: PgExtensionInfo) => {
      allExtensions.push({ ...ext, databaseName: db.databaseName });
    });
  });

  return (
    <div>
      <Title level={3}>Extensions</Title>
      <ErrorAlert error={databases.error} />
      <ErrorAlert error={extensions.error} />

      <Card style={{ marginBottom: 16 }}>
        <Space wrap>
          <Select
            placeholder="All databases"
            allowClear
            style={{ width: 250 }}
            value={selectedDb}
            onChange={setSelectedDb}
            loading={databases.loading}
            options={databases.data?.databases?.map((db: string) => ({
              value: db,
              label: db,
            }))}
          />
          <Button icon={<ReloadOutlined />} onClick={extensions.refresh}>
            Refresh
          </Button>
        </Space>
      </Card>

      <Card>
        <Spin spinning={extensions.loading}>
          <Table
            dataSource={allExtensions}
            rowKey={(r) => `${r.databaseName}-${r.name}`}
            size="small"
            pagination={{ pageSize: 50, showSizeChanger: true }}
            columns={[
              { title: "Database", dataIndex: "databaseName", width: 180 },
              { title: "Extension", dataIndex: "name", width: 200 },
              {
                title: "Installed Version",
                dataIndex: "installedVersion",
                width: 150,
              },
              {
                title: "Default Version",
                dataIndex: "defaultVersion",
                width: 150,
              },
              {
                title: "Description",
                dataIndex: "comment",
                ellipsis: true,
              },
            ]}
          />
        </Spin>
      </Card>
    </div>
  );
}
