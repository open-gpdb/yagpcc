import { Card, Descriptions, Collapse } from "antd";
import type {
  GPMetrics,
  AggregatedMetrics,
} from "../api/client";

const { Panel } = Collapse;

/** Format a number: if it looks like a float show 2 decimals, otherwise show as-is. */
function fmt(v: number | undefined | null): string {
  if (v === undefined || v === null) return "N/A";
  if (Number.isInteger(v)) return v.toLocaleString();
  return v.toFixed(2);
}

function fmtBytes(v: number | undefined | null): string {
  if (v === undefined || v === null) return "N/A";
  if (v === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.min(Math.floor(Math.log(v) / Math.log(1024)), units.length - 1);
  return `${(v / Math.pow(1024, i)).toFixed(2)} ${units[i]}`;
}

export function GPMetricsCard({
  title,
  metrics,
}: {
  title: string;
  metrics: GPMetrics | null | undefined;
}) {
  if (!metrics) return null;

  return (
    <Card title={title} style={{ marginBottom: 16 }}>
      {/* Summary */}
      <Descriptions bordered column={{ xs: 1, sm: 2, md: 3 }} size="small" title="Summary">
        <Descriptions.Item label="CPU Usage (s)">{fmt(metrics.cpuUsage)}</Descriptions.Item>
        <Descriptions.Item label="Memory (RSS)">{fmtBytes(metrics.memoryUsage)}</Descriptions.Item>
        <Descriptions.Item label="Disk Read">{fmtBytes(metrics.diskRead)}</Descriptions.Item>
        <Descriptions.Item label="Disk Write">{fmtBytes(metrics.diskWrite)}</Descriptions.Item>
        <Descriptions.Item label="Network Sent">{fmtBytes(metrics.networkSent)}</Descriptions.Item>
        <Descriptions.Item label="Network Received">{fmtBytes(metrics.networkReceived)}</Descriptions.Item>
      </Descriptions>

      <Collapse ghost style={{ marginTop: 12 }}>
        {/* System Stat */}
        {metrics.systemStat && (
          <Panel header="System Statistics (procfs)" key="systemStat">
            <Descriptions bordered column={{ xs: 1, sm: 2, md: 3 }} size="small">
              <Descriptions.Item label="Running Time (s)">{fmt(metrics.systemStat.runningTimeSeconds)}</Descriptions.Item>
              <Descriptions.Item label="User Time (s)">{fmt(metrics.systemStat.userTimeSeconds)}</Descriptions.Item>
              <Descriptions.Item label="Kernel Time (s)">{fmt(metrics.systemStat.kernelTimeSeconds)}</Descriptions.Item>
              <Descriptions.Item label="Virtual Memory">{fmtBytes(metrics.systemStat.vsize)}</Descriptions.Item>
              <Descriptions.Item label="RSS">{fmtBytes(metrics.systemStat.rss)}</Descriptions.Item>
              <Descriptions.Item label="VM Peak (KB)">{fmt(metrics.systemStat.vmPeakKb)}</Descriptions.Item>
              <Descriptions.Item label="Read Chars">{fmtBytes(metrics.systemStat.rchar)}</Descriptions.Item>
              <Descriptions.Item label="Write Chars">{fmtBytes(metrics.systemStat.wchar)}</Descriptions.Item>
              <Descriptions.Item label="Read Syscalls">{fmt(metrics.systemStat.syscr)}</Descriptions.Item>
              <Descriptions.Item label="Write Syscalls">{fmt(metrics.systemStat.syscw)}</Descriptions.Item>
              <Descriptions.Item label="Read Bytes (disk)">{fmtBytes(metrics.systemStat.readBytes)}</Descriptions.Item>
              <Descriptions.Item label="Write Bytes (disk)">{fmtBytes(metrics.systemStat.writeBytes)}</Descriptions.Item>
              <Descriptions.Item label="Cancelled Write Bytes">{fmtBytes(metrics.systemStat.cancelledWriteBytes)}</Descriptions.Item>
            </Descriptions>
          </Panel>
        )}

        {/* Instrumentation */}
        {metrics.instrumentation && (
          <Panel header="Instrumentation (Plan Nodes)" key="instrumentation">
            <Descriptions bordered column={{ xs: 1, sm: 2, md: 3 }} size="small">
              <Descriptions.Item label="Tuples">{fmt(metrics.instrumentation.ntuples)}</Descriptions.Item>
              <Descriptions.Item label="Loops">{fmt(metrics.instrumentation.nloops)}</Descriptions.Item>
              <Descriptions.Item label="Tuple Count">{fmt(metrics.instrumentation.tuplecount)}</Descriptions.Item>
              <Descriptions.Item label="First Tuple (s)">{fmt(metrics.instrumentation.firsttuple)}</Descriptions.Item>
              <Descriptions.Item label="Startup (s)">{fmt(metrics.instrumentation.startup)}</Descriptions.Item>
              <Descriptions.Item label="Total (s)">{fmt(metrics.instrumentation.total)}</Descriptions.Item>
              <Descriptions.Item label="Startup Time (s)">{fmt(metrics.instrumentation.startupTime)}</Descriptions.Item>
              <Descriptions.Item label="Inherited Calls">{fmt(metrics.instrumentation.inheritedCalls)}</Descriptions.Item>
              <Descriptions.Item label="Inherited Time (s)">{fmt(metrics.instrumentation.inheritedTime)}</Descriptions.Item>
            </Descriptions>

            <Descriptions bordered column={{ xs: 1, sm: 2, md: 3 }} size="small" title="Block I/O" style={{ marginTop: 8 }}>
              <Descriptions.Item label="Shared Blks Hit">{fmt(metrics.instrumentation.sharedBlksHit)}</Descriptions.Item>
              <Descriptions.Item label="Shared Blks Read">{fmt(metrics.instrumentation.sharedBlksRead)}</Descriptions.Item>
              <Descriptions.Item label="Shared Blks Dirtied">{fmt(metrics.instrumentation.sharedBlksDirtied)}</Descriptions.Item>
              <Descriptions.Item label="Shared Blks Written">{fmt(metrics.instrumentation.sharedBlksWritten)}</Descriptions.Item>
              <Descriptions.Item label="Local Blks Hit">{fmt(metrics.instrumentation.localBlksHit)}</Descriptions.Item>
              <Descriptions.Item label="Local Blks Read">{fmt(metrics.instrumentation.localBlksRead)}</Descriptions.Item>
              <Descriptions.Item label="Local Blks Dirtied">{fmt(metrics.instrumentation.localBlksDirtied)}</Descriptions.Item>
              <Descriptions.Item label="Local Blks Written">{fmt(metrics.instrumentation.localBlksWritten)}</Descriptions.Item>
              <Descriptions.Item label="Temp Blks Read">{fmt(metrics.instrumentation.tempBlksRead)}</Descriptions.Item>
              <Descriptions.Item label="Temp Blks Written">{fmt(metrics.instrumentation.tempBlksWritten)}</Descriptions.Item>
              <Descriptions.Item label="Blk Read Time (s)">{fmt(metrics.instrumentation.blkReadTime)}</Descriptions.Item>
              <Descriptions.Item label="Blk Write Time (s)">{fmt(metrics.instrumentation.blkWriteTime)}</Descriptions.Item>
            </Descriptions>

            {(metrics.instrumentation.sent || metrics.instrumentation.received) && (
              <Descriptions bordered column={{ xs: 1, sm: 2, md: 3 }} size="small" title="Network" style={{ marginTop: 8 }}>
                {metrics.instrumentation.sent && (
                  <>
                    <Descriptions.Item label="Sent Total Bytes">{fmtBytes(metrics.instrumentation.sent.totalBytes)}</Descriptions.Item>
                    <Descriptions.Item label="Sent Tuple Bytes">{fmtBytes(metrics.instrumentation.sent.tupleBytes)}</Descriptions.Item>
                    <Descriptions.Item label="Sent Chunks">{fmt(metrics.instrumentation.sent.chunks)}</Descriptions.Item>
                  </>
                )}
                {metrics.instrumentation.received && (
                  <>
                    <Descriptions.Item label="Recv Total Bytes">{fmtBytes(metrics.instrumentation.received.totalBytes)}</Descriptions.Item>
                    <Descriptions.Item label="Recv Tuple Bytes">{fmtBytes(metrics.instrumentation.received.tupleBytes)}</Descriptions.Item>
                    <Descriptions.Item label="Recv Chunks">{fmt(metrics.instrumentation.received.chunks)}</Descriptions.Item>
                  </>
                )}
              </Descriptions>
            )}

            {metrics.instrumentation.interconnect && (
              <Descriptions bordered column={{ xs: 1, sm: 2, md: 3 }} size="small" title="Interconnect" style={{ marginTop: 8 }}>
                <Descriptions.Item label="Active Connections">{fmt(metrics.instrumentation.interconnect.activeConnectionsNum)}</Descriptions.Item>
                <Descriptions.Item label="Retransmits">{fmt(metrics.instrumentation.interconnect.retransmits)}</Descriptions.Item>
                <Descriptions.Item label="Send Packets">{fmt(metrics.instrumentation.interconnect.sndPktNum)}</Descriptions.Item>
                <Descriptions.Item label="Recv Packets">{fmt(metrics.instrumentation.interconnect.recvPktNum)}</Descriptions.Item>
                <Descriptions.Item label="Disordered Packets">{fmt(metrics.instrumentation.interconnect.disorderedPktNum)}</Descriptions.Item>
                <Descriptions.Item label="Duplicated Packets">{fmt(metrics.instrumentation.interconnect.duplicatedPktNum)}</Descriptions.Item>
                <Descriptions.Item label="CRC Errors">{fmt(metrics.instrumentation.interconnect.crcErrors)}</Descriptions.Item>
                <Descriptions.Item label="Mismatch Count">{fmt(metrics.instrumentation.interconnect.mismatchNum)}</Descriptions.Item>
                <Descriptions.Item label="Recv Ack Count">{fmt(metrics.instrumentation.interconnect.recvAckNum)}</Descriptions.Item>
                <Descriptions.Item label="Status Query Msgs">{fmt(metrics.instrumentation.interconnect.statusQueryMsgNum)}</Descriptions.Item>
                <Descriptions.Item label="Startup Cached Pkts">{fmt(metrics.instrumentation.interconnect.startupCachedPktNum)}</Descriptions.Item>
                <Descriptions.Item label="Total Recv Queue Size">{fmt(metrics.instrumentation.interconnect.totalRecvQueueSize)}</Descriptions.Item>
                <Descriptions.Item label="Recv Queue Counting Time">{fmt(metrics.instrumentation.interconnect.recvQueueSizeCountingTime)}</Descriptions.Item>
                <Descriptions.Item label="Total Capacity">{fmt(metrics.instrumentation.interconnect.totalCapacity)}</Descriptions.Item>
                <Descriptions.Item label="Capacity Counting Time">{fmt(metrics.instrumentation.interconnect.capacityCountingTime)}</Descriptions.Item>
                <Descriptions.Item label="Total Buffers">{fmt(metrics.instrumentation.interconnect.totalBuffers)}</Descriptions.Item>
                <Descriptions.Item label="Buffer Counting Time">{fmt(metrics.instrumentation.interconnect.bufferCountingTime)}</Descriptions.Item>
              </Descriptions>
            )}
          </Panel>
        )}

        {/* Spill */}
        {metrics.spill && (
          <Panel header="Spill Files" key="spill">
            <Descriptions bordered column={{ xs: 1, sm: 2 }} size="small">
              <Descriptions.Item label="File Count">{fmt(metrics.spill.fileCount)}</Descriptions.Item>
              <Descriptions.Item label="Total Bytes">{fmtBytes(metrics.spill.totalBytes)}</Descriptions.Item>
            </Descriptions>
          </Panel>
        )}
      </Collapse>
    </Card>
  );
}

export function AggregatedMetricsCard({
  title,
  metrics,
}: {
  title: string;
  metrics: AggregatedMetrics | null | undefined;
}) {
  if (!metrics) return null;

  return (
    <Card title={title} style={{ marginBottom: 16 }}>
      <Descriptions bordered column={{ xs: 1, sm: 2, md: 3 }} size="small">
        <Descriptions.Item label="Calls">{fmt(metrics.calls)}</Descriptions.Item>
        <Descriptions.Item label="Total Time (s)">{fmt(metrics.totalTime)}</Descriptions.Item>
        <Descriptions.Item label="Mean Time (s)">{fmt(metrics.meanTime)}</Descriptions.Item>
        <Descriptions.Item label="Min Time (s)">{fmt(metrics.minTime)}</Descriptions.Item>
        <Descriptions.Item label="Max Time (s)">{fmt(metrics.maxTime)}</Descriptions.Item>
        <Descriptions.Item label="Stddev Time (s)">{fmt(metrics.stddevTime)}</Descriptions.Item>
      </Descriptions>
    </Card>
  );
}
