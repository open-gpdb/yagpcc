import { useState } from "react";
import { Outlet, useNavigate, useLocation } from "react-router-dom";
import { Layout as AntLayout, Menu, Button } from "antd";
import {
  DashboardOutlined,
  UserOutlined,
  CodeOutlined,
  AppstoreOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
} from "@ant-design/icons";
import { TV } from "../theme";

const { Sider, Content, Footer } = AntLayout;

const menuItems = [
  { key: "/", icon: <DashboardOutlined />, label: "Dashboard" },
  { key: "/sessions", icon: <UserOutlined />, label: "Sessions" },
  { key: "/queries", icon: <CodeOutlined />, label: "Queries" },
  { key: "/extensions", icon: <AppstoreOutlined />, label: "Extensions" },
];

export default function Layout() {
  const navigate = useNavigate();
  const location = useLocation();
  const [collapsed, setCollapsed] = useState(false);

  // Determine which menu item is active based on current path
  const selectedKey =
    menuItems.find(
      (item) => item.key !== "/" && location.pathname.startsWith(item.key),
    )?.key ?? "/";

  return (
    <AntLayout style={{ minHeight: "100vh" }}>
      <Sider
        width={TV.sidebarWidth}
        collapsedWidth={TV.sidebarCollapsedWidth}
        collapsible
        collapsed={collapsed}
        onCollapse={setCollapsed}
        trigger={null}
        style={{
          background: TV.bgSidebar,
          borderRight: `1px solid ${TV.border}`,
          overflow: "auto",
          height: "100vh",
          position: "fixed",
          left: 0,
          top: 0,
          bottom: 0,
          zIndex: 10,
        }}
      >
        {/* Logo / Title */}
        <div
          style={{
            padding: collapsed ? "20px 0" : "20px 16px",
            textAlign: collapsed ? "center" : "left",
            borderBottom: `1px solid ${TV.border}`,
            marginBottom: 8,
            cursor: "pointer",
          }}
          onClick={() => navigate("/")}
        >
          <span
            style={{
              color: TV.cyan,
              fontSize: collapsed ? 20 : 16,
              fontWeight: 700,
              fontFamily: TV.fontMono,
              whiteSpace: "nowrap",
              letterSpacing: collapsed ? 0 : 1,
            }}
          >
            {collapsed ? "🐘" : "🐘 YAGPCC"}
          </span>
        </div>

        {/* Navigation menu */}
        <Menu
          theme="dark"
          mode="inline"
          selectedKeys={[selectedKey]}
          items={menuItems}
          onClick={({ key }) => navigate(key)}
          style={{
            background: "transparent",
            borderRight: "none",
            fontFamily: TV.fontMono,
          }}
        />

        {/* Collapse toggle at bottom */}
        <div
          style={{
            position: "absolute",
            bottom: 0,
            width: "100%",
            borderTop: `1px solid ${TV.border}`,
            padding: "12px 0",
            textAlign: "center",
          }}
        >
          <Button
            type="text"
            icon={collapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
            onClick={() => setCollapsed(!collapsed)}
            style={{
              color: TV.textSecondary,
              fontSize: 16,
              width: "100%",
            }}
          />
        </div>
      </Sider>

      {/* Main content area */}
      <AntLayout
        style={{
          marginLeft: collapsed ? TV.sidebarCollapsedWidth : TV.sidebarWidth,
          transition: "margin-left 0.2s",
          background: TV.bgMain,
        }}
      >
        <Content
          style={{
            padding: 24,
            minHeight: "calc(100vh - 48px)",
            background: TV.bgMain,
          }}
        >
          <Outlet />
        </Content>
        <Footer
          style={{
            textAlign: "center",
            color: TV.textSecondary,
            background: "transparent",
            padding: "12px 24px",
            fontFamily: TV.fontMono,
            fontSize: 12,
            borderTop: `1px solid ${TV.border}`,
          }}
        >
          YAGPCC — Yet Another Greenplum Command Center
        </Footer>
      </AntLayout>
    </AntLayout>
  );
}
