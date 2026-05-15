import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { ConfigProvider, theme } from "antd";
import App from "./App";
import { ThemeProvider, useTheme } from "./contexts/ThemeContext";
import { getColors, FONT_MONO } from "./theme";
import "./index.css";

function ThemedApp() {
  const { mode } = useTheme();
  const c = getColors(mode);
  const isDark = mode === "dark";

  return (
    <ConfigProvider
      theme={{
        algorithm: isDark ? theme.darkAlgorithm : theme.defaultAlgorithm,
        token: {
          colorPrimary: c.primary,
          colorBgContainer: c.bgCard,
          colorBgLayout: c.bgMain,
          colorBgElevated: c.bgElevated,
          colorBorder: c.border,
          colorBorderSecondary: c.border,
          colorText: c.textPrimary,
          colorTextSecondary: c.textSecondary,
          colorTextTertiary: c.textSecondary,
          colorTextQuaternary: c.textSecondary,
          borderRadius: 4,
          fontFamily: FONT_MONO,
          fontSize: 13,
          colorBgSpotlight: c.bgHover,
          colorLink: c.primary,
          colorLinkHover: c.cyan,
          colorSuccess: c.green,
          colorWarning: c.yellow,
          colorError: c.red,
          colorInfo: c.primary,
        },
        components: {
          Layout: {
            siderBg: c.bgSidebar,
            bodyBg: c.bgMain,
            headerBg: c.bgSidebar,
            footerBg: "transparent",
          },
          Menu: isDark
            ? {
                darkItemBg: "transparent",
                darkItemSelectedBg: c.bgSelected,
                darkItemHoverBg: c.bgSidebarHover,
                darkItemColor: c.textSecondary,
                darkItemSelectedColor: c.cyan,
              }
            : {
                itemBg: "transparent",
                itemSelectedBg: c.bgSelected,
                itemHoverBg: c.bgSidebarHover,
                itemColor: c.textSecondary,
                itemSelectedColor: c.primary,
              },
          Card: {
            colorBgContainer: c.bgCard,
            colorBorderSecondary: c.border,
          },
          Table: {
            colorBgContainer: c.bgCard,
            headerBg: isDark ? c.bgSidebar : c.bgCode,
            headerColor: c.textHeading,
            rowHoverBg: c.bgHover,
            borderColor: c.border,
            headerBorderRadius: 4,
          },
          Descriptions: {
            colorBgContainer: c.bgCard,
            labelBg: isDark ? c.bgSidebar : c.bgCode,
            colorSplit: c.border,
          },
          Modal: {
            contentBg: c.bgCard,
            headerBg: c.bgCard,
            footerBg: c.bgCard,
          },
          Input: {
            colorBgContainer: isDark ? c.bgCode : "#ffffff",
            colorBorder: c.border,
            activeBorderColor: c.primary,
            hoverBorderColor: c.borderActive,
          },
          Select: {
            colorBgContainer: isDark ? c.bgCode : "#ffffff",
            colorBorder: c.border,
            optionSelectedBg: c.bgSelected,
          },
          InputNumber: {
            colorBgContainer: isDark ? c.bgCode : "#ffffff",
            colorBorder: c.border,
          },
          Switch: {
            colorPrimary: c.primary,
          },
          Collapse: {
            colorBgContainer: c.bgCard,
            colorBorder: c.border,
            headerBg: isDark ? c.bgSidebar : c.bgCode,
          },
          Statistic: {
            colorTextDescription: c.textSecondary,
          },
          Tag: {
            colorBgContainer: isDark ? c.bgCode : c.bgCode,
            colorBorder: c.border,
          },
          Spin: {
            colorPrimary: c.cyan,
          },
          Alert: {
            colorInfoBg: c.bgCode,
            colorInfoBorder: c.border,
            colorWarningBg: c.alertWarningBg,
            colorWarningBorder: c.alertWarningBorder,
          },
          Pagination: {
            colorBgContainer: c.bgCard,
          },
        },
      }}
    >
      <BrowserRouter>
        <App />
      </BrowserRouter>
    </ConfigProvider>
  );
}

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <ThemeProvider>
      <ThemedApp />
    </ThemeProvider>
  </React.StrictMode>,
);
