import { useState } from "react";
import { Outlet, useNavigate, useLocation } from "react-router-dom";
import { Layout as AntLayout, Menu, Button, Tooltip } from "antd";
import {
  DashboardOutlined,
  UserOutlined,
  CodeOutlined,
  AppstoreOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
  SunOutlined,
  MoonOutlined,
} from "@ant-design/icons";
import { useTheme } from "../contexts/ThemeContext";
import { getColors, SIDEBAR_WIDTH, SIDEBAR_COLLAPSED_WIDTH, FONT_MONO } from "../theme";

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
  const { mode, toggle } = useTheme();
  const c = getColors(mode);

  // Determine which menu item is active based on current path
  const selectedKey =
    menuItems.find(
      (item) => item.key !== "/" && location.pathname.startsWith(item.key),
    )?.key ?? "/";

  return (
    <AntLayout style={{ minHeight: "100vh" }}>
      <Sider
        width={SIDEBAR_WIDTH}
        collapsedWidth={SIDEBAR_COLLAPSED_WIDTH}
        collapsible
        collapsed={collapsed}
        onCollapse={setCollapsed}
        trigger={null}
        style={{
          background: c.bgSidebar,
          borderRight: `1px solid ${c.border}`,
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
            borderBottom: `1px solid ${c.border}`,
            marginBottom: 8,
            cursor: "pointer",
          }}
          onClick={() => navigate("/")}
        >
          <span
            style={{
              color: c.cyan,
              fontSize: collapsed ? 20 : 16,
              fontWeight: 700,
              fontFamily: FONT_MONO,
              whiteSpace: "nowrap",
              letterSpacing: collapsed ? 0 : 1,
            }}
          >
            {collapsed ? "🐘" : "🐘 YAGPCC"}
          </span>
        </div>

        {/* Navigation menu */}
        <Menu
          theme={mode === "dark" ? "dark" : "light"}
          mode="inline"
          selectedKeys={[selectedKey]}
          items={menuItems}
          onClick={({ key }) => navigate(key)}
          style={{
            background: "transparent",
            borderRight: "none",
            fontFamily: FONT_MONO,
          }}
        />

        {/* Bottom controls: theme toggle + collapse toggle */}
        <div
          style={{
            position: "absolute",
            bottom: 0,
            width: "100%",
            borderTop: `1px solid ${c.border}`,
            padding: "8px 0",
            textAlign: "center",
            display: "flex",
            flexDirection: collapsed ? "column" : "row",
            justifyContent: "center",
            gap: 4,
          }}
        >
          <Tooltip title={mode === "dark" ? "Switch to light theme" : "Switch to dark theme"}>
            <Button
              type="text"
              icon={mode === "dark" ? <SunOutlined /> : <MoonOutlined />}
              onClick={toggle}
              style={{
                color: c.textSecondary,
                fontSize: 16,
                flex: collapsed ? undefined : 1,
              }}
            />
          </Tooltip>
          <Button
            type="text"
            icon={collapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
            onClick={() => setCollapsed(!collapsed)}
            style={{
              color: c.textSecondary,
              fontSize: 16,
              flex: collapsed ? undefined : 1,
            }}
          />
        </div>
      </Sider>

      {/* Main content area */}
      <AntLayout
        style={{
          marginLeft: collapsed ? SIDEBAR_COLLAPSED_WIDTH : SIDEBAR_WIDTH,
          transition: "margin-left 0.2s",
          background: c.bgMain,
        }}
      >
        <Content
          style={{
            padding: 24,
            minHeight: "calc(100vh - 48px)",
            background: c.bgMain,
          }}
        >
          <Outlet />
        </Content>
        <Footer
          style={{
            textAlign: "center",
            color: c.textSecondary,
            background: "transparent",
            padding: "12px 24px",
            fontFamily: FONT_MONO,
            fontSize: 12,
            borderTop: `1px solid ${c.border}`,
          }}
        >
          YAGPCC — Yet Another Greenplum Command Center
        </Footer>
      </AntLayout>
    </AntLayout>
  );
}
