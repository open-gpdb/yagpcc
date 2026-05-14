import { Tag } from "antd";

const stateColors: Record<string, string> = {
  active: "green",
  idle: "default",
  "idle in transaction": "orange",
  "idle in transaction (aborted)": "red",
  fastpath: "blue",
  disabled: "default",
};

interface Props {
  state: string;
}

export default function SessionStateBadge({ state }: Props) {
  const normalized = state.toLowerCase().replace(/session_status_/i, "").replace(/_/g, " ");
  const color = stateColors[normalized] ?? "default";
  return <Tag color={color}>{normalized || "unknown"}</Tag>;
}
