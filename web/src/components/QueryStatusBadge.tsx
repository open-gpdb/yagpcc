import { Tag } from "antd";
import { queryStatusColors } from "../theme";

interface Props {
  status: string;
}

export default function QueryStatusBadge({ status }: Props) {
  const normalized = status.toLowerCase().replace(/query_status_/i, "");
  const color = queryStatusColors[normalized] ?? queryStatusColors["idle"];
  return <Tag color={color}>{normalized || "unknown"}</Tag>;
}
