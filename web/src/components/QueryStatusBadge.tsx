import { Tag } from "antd";

const statusColors: Record<string, string> = {
  running: "processing",
  finished: "success",
  error: "error",
  cancelled: "warning",
  idle: "default",
};

interface Props {
  status: string;
}

export default function QueryStatusBadge({ status }: Props) {
  const normalized = status.toLowerCase().replace(/query_status_/i, "");
  const color = statusColors[normalized] ?? "default";
  return <Tag color={color}>{normalized || "unknown"}</Tag>;
}
