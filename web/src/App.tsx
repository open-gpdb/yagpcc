import { Routes, Route } from "react-router-dom";
import Layout from "./components/Layout";
import DashboardPage from "./pages/DashboardPage";
import SessionsPage from "./pages/SessionsPage";
import SessionDetailPage from "./pages/SessionDetailPage";
import QueriesPage from "./pages/QueriesPage";
import QueryDetailPage from "./pages/QueryDetailPage";
import ExtensionsPage from "./pages/ExtensionsPage";
import HostsPage from "./pages/HostsPage";
import PidProcInfoPage from "./pages/PidProcInfoPage";

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route path="/" element={<DashboardPage />} />
        <Route path="/sessions" element={<SessionsPage />} />
        <Route path="/session/:sessId" element={<SessionDetailPage />} />
        <Route path="/queries" element={<QueriesPage />} />
        <Route path="/query/:ssid/:ccnt" element={<QueryDetailPage />} />
        <Route path="/hosts" element={<HostsPage />} />
        <Route path="/procfs/pid-proc-info" element={<PidProcInfoPage />} />
        <Route path="/extensions" element={<ExtensionsPage />} />
      </Route>
    </Routes>
  );
}
