import { Tag } from "antd";
import { sessionStateColors } from "../theme";

interface Props {
  state: string;
}

export default function SessionStateBadge({ state }: Props) {
  const normalized = state.toLowerCase().replace(/session_status_/i, "").replace(/_/g, " ");
  const color = sessionStateColors[normalized] ?? sessionStateColors["idle"];
  return <Tag color={color}>{normalized || "unknown"}</Tag>;
}
