import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { ConfigProvider, theme } from "antd";
import App from "./App";
import { TV } from "./theme";
import "./index.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <ConfigProvider
      theme={{
        algorithm: theme.darkAlgorithm,
        token: {
          colorPrimary: TV.primary,
          colorBgContainer: TV.bgCard,
          colorBgLayout: TV.bgMain,
          colorBgElevated: TV.bgElevated,
          colorBorder: TV.border,
          colorBorderSecondary: TV.border,
          colorText: TV.textPrimary,
          colorTextSecondary: TV.textSecondary,
          colorTextTertiary: TV.textSecondary,
          colorTextQuaternary: TV.textSecondary,
          borderRadius: 4,
          fontFamily: TV.fontMono,
          fontSize: 13,
          colorBgSpotlight: TV.bgHover,
          colorLink: TV.primary,
          colorLinkHover: TV.cyan,
          colorSuccess: TV.green,
          colorWarning: TV.yellow,
          colorError: TV.red,
          colorInfo: TV.primary,
        },
        components: {
          Layout: {
            siderBg: TV.bgSidebar,
            bodyBg: TV.bgMain,
            headerBg: TV.bgSidebar,
            footerBg: "transparent",
          },
          Menu: {
            darkItemBg: "transparent",
            darkItemSelectedBg: TV.bgSelected,
            darkItemHoverBg: TV.bgSidebarHover,
            darkItemColor: TV.textSecondary,
            darkItemSelectedColor: TV.cyan,
          },
          Card: {
            colorBgContainer: TV.bgCard,
            colorBorderSecondary: TV.border,
          },
          Table: {
            colorBgContainer: TV.bgCard,
            headerBg: TV.bgSidebar,
            headerColor: TV.textHeading,
            rowHoverBg: TV.bgHover,
            borderColor: TV.border,
            headerBorderRadius: 4,
          },
          Descriptions: {
            colorBgContainer: TV.bgCard,
            labelBg: TV.bgSidebar,
            colorSplit: TV.border,
          },
          Modal: {
            contentBg: TV.bgCard,
            headerBg: TV.bgCard,
            footerBg: TV.bgCard,
          },
          Input: {
            colorBgContainer: TV.bgCode,
            colorBorder: TV.border,
            activeBorderColor: TV.primary,
            hoverBorderColor: TV.borderActive,
          },
          Select: {
            colorBgContainer: TV.bgCode,
            colorBorder: TV.border,
            optionSelectedBg: TV.bgSelected,
          },
          InputNumber: {
            colorBgContainer: TV.bgCode,
            colorBorder: TV.border,
          },
          Switch: {
            colorPrimary: TV.primary,
          },
          Collapse: {
            colorBgContainer: TV.bgCard,
            colorBorder: TV.border,
            headerBg: TV.bgSidebar,
          },
          Statistic: {
            colorTextDescription: TV.textSecondary,
          },
          Tag: {
            colorBgContainer: TV.bgCode,
            colorBorder: TV.border,
          },
          Spin: {
            colorPrimary: TV.cyan,
          },
          Alert: {
            colorInfoBg: TV.bgCode,
            colorInfoBorder: TV.border,
            colorWarningBg: "#2a2520",
            colorWarningBorder: "#4a3a28",
          },
          Pagination: {
            colorBgContainer: TV.bgCard,
          },
        },
      }}
    >
      <BrowserRouter>
        <App />
      </BrowserRouter>
    </ConfigProvider>
  </React.StrictMode>,
);
