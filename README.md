# yagpcc

**yagpcc** (Yet Another Greenplum Command Center) is a diagnostic and monitoring agent for [Greenplum](https://greenplum.org/) clusters. It collects query and session telemetry from the database (via the **yagp-hooks-collector** extension), aggregates it across Master and Segment hosts, and exposes it over gRPC for real-time and historical use.

## Features

- Collects query and session telemetry from the database (via the **yagp-hooks-collector** extension).
- Aggregates it across Master and Segment hosts.
- Exposes it over gRPC for real-time and historical use.
- Provides an HTTP CSV export API mirroring the gRPC GetGPInfo service for easy scripting and spreadsheet integration.
- **Web UI** — a browser-based Command Center for monitoring sessions, queries, cluster health, and managing resources (terminate sessions/queries, move queries between resource groups).

## Documentation

| Document | Description |
|----------|-------------|
| [**Service architecture**](docs/service-architecture.md) | Services, roles, interfaces (UDS, TCP, libpq, HTTP), data flow, and default listen ports. |
| [Architecture overview](docs/architecture.md) | High-level design and system diagram (with Mermaid). |
| [API description](docs/API.md) | gRPC API reference (GetGPInfo, ActionService), CSV HTTP API, messages, and metrics. |
| [Per-process resource statistics](docs/proc-stats-flow.md) | Procfs (`GetPidProcStat`) data flow per running query and proposed master-only 5/15/30-minute top-style averages (per-session and cluster-wide rollup). |
| [**Performance tuning**](docs/performance-tuning.md) | Memory limits (`GOMEMLIMIT`), config knobs (procfs, segment pull, stored queries, aggregation), Prometheus metrics reference, and pprof profiling guide. |

## Building

**Prerequisites:**
- Go 1.25+ (see `go.mod`).
- protoc compiler (see https://protobuf.dev/installation/ `apt install -y protobuf-compiler`)
- protoc-gen-go, use https://protobuf.dev/reference/go/go-generated/ `go install google.golang.org/protobuf/cmd/protoc-gen-go@latest`
- protoc-gen-go-grpc, use `go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest`
- Node.js 18+ and npm (only needed for building the web UI)

Build the binary (generates protos and outputs to `devbin/yagpcc`):

```bash
make build
```

Build with the web UI embedded:

```bash
make build-all
```

Or build the UI separately and then the Go binary:

```bash
make build-ui    # builds web/ → copies dist to internal/httpui/dist
make build       # compiles Go binary with embedded UI assets
```

Or build without regenerating protos:

```bash
go build -o devbin/yagpcc ./cmd/server
```

The binary is produced at `devbin/yagpcc`.

## Configuration

The application reads **`yagpcc.yaml`** from the current working directory. A valid config file is required to run.

Config files differ by role:

- **Master** — runs on the Greenplum master host: connects to the cluster, pulls data from segments, and exposes the aggregated gRPC API. Requires `role: master` and `master_connection` (and typically `segment_pull_rate_sec`, `segment_pull_threads`).
- **Segment** — runs on each segment host: collects local metrics and serves them to the master. Requires `role: segment` and `listen_port`.

Example configs are in **`cmd/server/`**:

| File | Role | Use as |
|------|------|--------|
| `cmd/server/yagpcc_master.yaml` | master | Template for master node: set `master_connection.addrs`, `master_connection.password`, and optionally `sslrootcert`. |
| `cmd/server/yagpcc_segment.yaml` | segment | Template for segment nodes. |

Copy or adapt the right file to `yagpcc.yaml` in the directory from which you will run the binary (see [Running](#running)).

Minimal structure:

**Master** (`yagpcc.yaml`):

```yaml
role: master
listen_port: 1432
segment_pull_rate_sec: 3
segment_pull_threads: 2
master_connection:
  addrs:
    - host1:6432
    - host2:6432
  sslmode: allow
  password: "your_password"
app:
  logging:
    level: debug
```

**Segment** (`yagpcc.yaml`):

```yaml
role: segment
listen_port: 1432
app:
  logging:
    level: debug
```

Adjust `listen_port`, logging, and other options as needed (see `internal/config/config.go` for full options).

### Web UI

To enable the web UI, add `ui_port` to the **master** config:

```yaml
ui_port: 1441
```

The UI is disabled by default (`ui_port: 0`). When enabled, the web UI is available at `http://[::1]:1441/`.

## Running

1. Use a config file for the correct role (master or segment) and save it as **`yagpcc.yaml`** in the directory where you will run the binary.
2. From that directory, run:

```bash
./devbin/yagpcc
```

The binary expects `yagpcc.yaml` in the current working directory; it does not take a config path argument.
