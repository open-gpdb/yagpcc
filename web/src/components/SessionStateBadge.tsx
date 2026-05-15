import { Tag } from "antd";
import { useTheme } from "../contexts/ThemeContext";
import { getSessionStateColors } from "../theme";

interface Props {
  state: string;
}

export default function SessionStateBadge({ state }: Props) {
  const { mode } = useTheme();
  const colors = getSessionStateColors(mode);
  const normalized = state.toLowerCase().replace(/session_status_/i, "").replace(/_/g, " ");
  const color = colors[normalized] ?? colors["idle"];
  return <Tag color={color}>{normalized || "unknown"}</Tag>;
}
