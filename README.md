# yagpcc

**yagpcc** (Yet Another Greenplum Command Center) is a diagnostic and monitoring agent for [Greenplum](https://greenplum.org/) clusters. It collects query and session telemetry from the database (via the **yagp-hooks-collector** extension), aggregates it across Master and Segment hosts, and exposes it over gRPC for real-time and historical use.

## Features

- Collects query and session telemetry from the database (via the **yagp-hooks-collector** extension).
- Aggregates it across Master and Segment hosts.
- Exposes it over gRPC for real-time and historical use.

## Documentation

| Document | Description |
|----------|-------------|
| [**Service architecture**](docs/service-architecture.md) | Services, roles, interfaces (UDS, TCP, libpq), and data flow. |
| [Architecture overview](docs/architecture.md) | High-level design and system diagram (with Mermaid). |
| [API description](docs/API.md) | gRPC API reference (GetGPInfo, ActionService, messages, metrics). |
| [Per-process resource statistics](docs/proc-stats-flow.md) | Procfs (`GetPidProcStat`) data flow per running query and proposed master-only 5/15/30-minute top-style averages (per-session and cluster-wide rollup). |

## Building

**Prerequisites:** 
- Go 1.25+ (see `go.mod`).
- protoc compiler (see https://protobuf.dev/installation/ `apt install -y protobuf-compiler`)
- protoc-gen-go, use https://protobuf.dev/reference/go/go-generated/ `go install google.golang.org/protobuf/cmd/protoc-gen-go@latest`
- protoc-gen-go-grpc, use `go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest`

Build the binary (generates protos and outputs to `devbin/yagpcc`):

```bash
make build
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

## Running

1. Use a config file for the correct role (master or segment) and save it as **`yagpcc.yaml`** in the directory where you will run the binary.
2. From that directory, run:

```bash
./devbin/yagpcc
```

The binary expects `yagpcc.yaml` in the current working directory; it does not take a config path argument.
