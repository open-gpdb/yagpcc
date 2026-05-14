# AGENTS.md — Developer & AI-Agent Guide

## Project Overview

**yagpcc** (Yet Another Greenplum Command Center) is a diagnostic and monitoring agent for
[Greenplum](https://greenplum.org/) clusters written in Go.  
It collects query/session telemetry via the **yagp-hooks-collector** database extension,
aggregates data across Master and Segment hosts, and exposes it over **gRPC** (plus an HTTP/CSV
export API).

| Aspect | Detail |
|--------|--------|
| Language | Go 1.25+ |
| Module path | `github.com/open-gpdb/yagpcc` |
| Entry point | `cmd/server/main.go` |
| Binary output | `devbin/yagpcc` |
| Test framework | [Ginkgo v2](https://onsi.github.io/ginkgo/) + Gomega |
| Proto toolchain | [Buf v2](https://buf.build/) with `protoc-gen-go` / `protoc-gen-go-grpc` |

For architecture details see `docs/service-architecture.md` and `docs/architecture.md`.

---

## Proto Files

All `.proto` definitions live under **`api/proto/`**, organised by service domain:

```
api/proto/
├── agent_master/          # Master-facing gRPC services
│   ├── yagpcc_action_service.proto
│   └── yagpcc_get_service.proto
├── agent_segment/         # Segment-facing gRPC services
│   ├── yagpcc_control_service.proto
│   ├── yagpcc_get_service.proto
│   └── yagpcc_set_service.proto
└── common/                # Shared message types
    ├── yagpcc_metrics.proto
    └── yagpcc_session.proto
```

Generated `*.pb.go` and `*_grpc.pb.go` files are committed alongside the `.proto` sources
(same directories).  
Generation is configured by `buf.yaml` (module root, includes `api/proto`) and
`buf.gen.yaml` (plugins & output options).

### Regenerating Proto Stubs

```bash
make genproto
```

This installs the pinned `protoc-gen-go` / `protoc-gen-go-grpc` versions and runs
`buf generate`.  You need the `buf` CLI installed (`brew install bufbuild/buf/buf` or
see <https://buf.build/docs/installation>).

---

## Building

```bash
make build        # generates protos → builds binary to devbin/yagpcc
```

To build **without** regenerating protos (e.g. when stubs are already up-to-date):

```bash
go build -o devbin/yagpcc ./cmd/server
```

### Prerequisites

| Tool | Install hint |
|------|-------------|
| Go 1.25+ | <https://go.dev/dl/> |
| `buf` CLI | `brew install bufbuild/buf/buf` or <https://buf.build/docs/installation> |
| `protoc-gen-go` | installed automatically by `make genproto` |
| `protoc-gen-go-grpc` | installed automatically by `make genproto` |

---

## Running Tests

**Use the Makefile target as the primary way to run the full test suite:**

```bash
make unittest
```

This executes all Ginkgo tests with the `-race` detector enabled:

```
go run github.com/onsi/ginkgo/v2/ginkgo@v2.28.0 run --race --github-output ./...
```

> **Note:** Always prefer `make unittest` over invoking `go test` directly — it ensures
> the correct Ginkgo CLI version and flags are used.

### Running a Single Package's Tests

If you need to iterate on a specific package:

```bash
go run github.com/onsi/ginkgo/v2/ginkgo@v2.28.0 run --race ./internal/storage/...
```

Or with plain `go test` (loses Ginkgo-specific output formatting):

```bash
go test -race ./internal/storage/...
```

---

## Linting & Formatting

```bash
make fmt           # gofmt all Go files
make lint          # golangci-lint (requires golangci-lint installed)
```

---

## Makefile Targets Summary

| Target | What it does |
|--------|-------------|
| `make genproto` | Install protoc plugins, run `buf generate` |
| `make build` | `genproto` → compile binary to `devbin/yagpcc` |
| `make unittest` | Run all Ginkgo tests with race detector |
| `make fmt` | Format Go source files |
| `make lint` | Run `golangci-lint` |
| `make build-ui` | Build frontend (web/) and copy dist to `internal/httpui/dist` |
| `make build-all` | `genproto` → `build-ui` → compile binary with embedded UI |
| `make package` | Build a Debian package |

---

## Configuration (for running the binary)

The binary reads `yagpcc.yaml` from the current working directory.  
Example configs for **master** and **segment** roles are in `cmd/server/`:

- `cmd/server/yagpcc_master.yaml`
- `cmd/server/yagpcc_segment.yaml`

See `README.md` for full configuration reference.
