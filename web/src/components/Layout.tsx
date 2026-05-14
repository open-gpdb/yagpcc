import { Outlet, useNavigate, useLocation } from "react-router-dom";
import { Layout as AntLayout, Menu } from "antd";
import {
  DashboardOutlined,
  UserOutlined,
  CodeOutlined,
  AppstoreOutlined,
} from "@ant-design/icons";

const { Header, Content, Footer } = AntLayout;

const menuItems = [
  { key: "/", icon: <DashboardOutlined />, label: "Dashboard" },
  { key: "/sessions", icon: <UserOutlined />, label: "Sessions" },
  { key: "/queries", icon: <CodeOutlined />, label: "Queries" },
  { key: "/extensions", icon: <AppstoreOutlined />, label: "Extensions" },
];

export default function Layout() {
  const navigate = useNavigate();
  const location = useLocation();

  // Determine which menu item is active based on current path
  const selectedKey =
    menuItems.find(
      (item) => item.key !== "/" && location.pathname.startsWith(item.key),
    )?.key ?? "/";

  return (
    <AntLayout style={{ minHeight: "100vh" }}>
      <Header
        style={{
          display: "flex",
          alignItems: "center",
          padding: "0 24px",
          background: "#001529",
        }}
      >
        <div
          style={{
            color: "#fff",
            fontSize: 18,
            fontWeight: 700,
            marginRight: 40,
            cursor: "pointer",
            whiteSpace: "nowrap",
          }}
          onClick={() => navigate("/")}
        >
          🐘 YAGPCC
        </div>
        <Menu
          theme="dark"
          mode="horizontal"
          selectedKeys={[selectedKey]}
          items={menuItems}
          onClick={({ key }) => navigate(key)}
          style={{ flex: 1, minWidth: 0 }}
        />
      </Header>
      <Content style={{ padding: "24px 24px", background: "#f5f5f5" }}>
        <Outlet />
      </Content>
      <Footer style={{ textAlign: "center", color: "#999" }}>
        YAGPCC — Yet Another Greenplum Command Center
      </Footer>
    </AntLayout>
  );
}
