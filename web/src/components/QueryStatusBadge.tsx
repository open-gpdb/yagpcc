import { Tag } from "antd";
import { useTheme } from "../contexts/ThemeContext";
import { getQueryStatusColors } from "../theme";

interface Props {
  status: string;
}

export default function QueryStatusBadge({ status }: Props) {
  const { mode } = useTheme();
  const colors = getQueryStatusColors(mode);
  const normalized = status.toLowerCase().replace(/query_status_/i, "");
  const color = colors[normalized] ?? colors["idle"];
  return <Tag color={color}>{normalized || "unknown"}</Tag>;
}
