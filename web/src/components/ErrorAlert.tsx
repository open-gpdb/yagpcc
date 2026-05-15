import { Alert } from "antd";

interface Props {
  error: Error | null;
}

export default function ErrorAlert({ error }: Props) {
  if (!error) return null;
  return (
    <Alert
      type="error"
      message="Error"
      description={error.message}
      showIcon
      closable
      style={{ marginBottom: 16 }}
    />
  );
}
