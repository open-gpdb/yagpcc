// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright
// ownership. The ASF licenses this file to You under the Apache
// License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the
// License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

// Schema CLI flags wire ClickHouse schema management commands into yagpcc:
// inspecting and applying embedded migrations without starting the master
// process. Each command exits the process with an explicit code so they slot
// into operator scripts and CI pipelines:
//
//   - --dump-schema           print cumulative DDL, never connects to CH
//   - --dump-migration        print SQL for from→to transition, never connects
//   - --migrate-only          load config, connect, ApplyMigrations, exit
//   - --verify-schema         load config, connect, VerifySchema, exit
//
// Exit codes:
//
//	0 — success
//	2 — failure (config error, connection error, schema mismatch, etc.)
//
// The dump commands work without a config file: RetentionDays defaults to
// the same value as DefaultClickhouseConfig() so embedded SQL can be rendered
// in isolation. When a config-path is supplied, RetentionDays is read from it
// instead so the printed DDL matches what --migrate-only would actually run.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/spf13/pflag"

	"github.com/open-gpdb/yagpcc/internal/config"
	"github.com/open-gpdb/yagpcc/internal/sink/clickhouse"
)

const (
	schemaCLIExitOK   = 0
	schemaCLIExitFail = 2

	flagNameDumpSchema    = "dump-schema"
	flagNameDumpMigration = "dump-migration"
	flagNameMigrateOnly   = "migrate-only"
	flagNameVerifySchema  = "verify-schema"
	flagNameFrom          = "from"
	flagNameTo            = "to"
)

// schemaFlags captures the parsed CLI flags relevant to schema management.
// `from` is `-1` when not set so we can distinguish "not provided" from the
// caller asking for version 0.
type schemaFlags struct {
	dumpSchema    bool
	dumpMigration bool
	migrateOnly   bool
	verifySchema  bool
	from          int
	to            int
	configPath    string
}

// registerSchemaCLIFlags wires the schema flags into the supplied flag set.
// Defaults for --from / --to are -1 so commands can detect "not set".
func registerSchemaCLIFlags(set *pflag.FlagSet, f *schemaFlags) {
	set.BoolVar(&f.dumpSchema, flagNameDumpSchema, false,
		"print cumulative ClickHouse DDL to stdout and exit (no connection required)")
	set.BoolVar(&f.dumpMigration, flagNameDumpMigration, false,
		"print SQL to migrate ClickHouse schema between --from and --to and exit (no connection required)")
	set.BoolVar(&f.migrateOnly, flagNameMigrateOnly, false,
		"load config, connect to ClickHouse, apply pending migrations and exit")
	set.BoolVar(&f.verifySchema, flagNameVerifySchema, false,
		"load config, connect to ClickHouse, verify schema version and exit")
	set.IntVar(&f.from, flagNameFrom, -1, "source schema version for --dump-migration")
	set.IntVar(&f.to, flagNameTo, -1, "target schema version for --dump-migration")
}

// schemaCommandRequested reports whether any of the schema-management CLI
// flags were set. The normal startup path runs only when no schema command is
// requested.
func (f *schemaFlags) schemaCommandRequested() bool {
	return f.dumpSchema || f.dumpMigration || f.migrateOnly || f.verifySchema
}

// schemaCLIDeps lets tests substitute the config loader, the ClickHouse
// migration connector and stdout/stderr without touching globals.
type schemaCLIDeps struct {
	stdout       io.Writer
	stderr       io.Writer
	loadConfig   func(configFile string) (*config.Config, error)
	openConn     func(ctx context.Context, cfg *config.ClickhouseConfig) (clickhouse.MigrationConn, func() error, error)
	applyMigrate func(ctx context.Context, conn clickhouse.MigrationConn, opts clickhouse.MigrateOptions) error
	verifySchema func(ctx context.Context, conn clickhouse.MigrationConn) error
}

// runSchemaCLI dispatches schema management commands. It returns the exit
// code; the caller is expected to call os.Exit. Dispatch order: dumps first
// (they do not need a config), then migrate-only, then verify-schema. Mixing
// flags is rejected because the command semantics differ enough that running
// them silently in some order would surprise operators.
func runSchemaCLI(ctx context.Context, f schemaFlags, deps schemaCLIDeps) int {
	if !f.schemaCommandRequested() {
		return schemaCLIExitOK
	}
	if mixed := exclusiveFlagsViolation(f); mixed != "" {
		_, _ = fmt.Fprintln(deps.stderr, mixed)
		return schemaCLIExitFail
	}

	switch {
	case f.dumpSchema:
		return runDumpSchema(f, deps)
	case f.dumpMigration:
		return runDumpMigration(f, deps)
	case f.migrateOnly:
		return runMigrateOnly(ctx, f, deps)
	case f.verifySchema:
		return runVerifySchema(ctx, f, deps)
	}
	return schemaCLIExitOK
}

// exclusiveFlagsViolation returns a human-readable error string when more
// than one schema command is requested simultaneously. The flags are mutually
// exclusive on purpose: their exit semantics differ (dump prints SQL,
// migrate/verify report status), and combining them would obscure that.
func exclusiveFlagsViolation(f schemaFlags) string {
	count := 0
	for _, set := range []bool{f.dumpSchema, f.dumpMigration, f.migrateOnly, f.verifySchema} {
		if set {
			count++
		}
	}
	if count > 1 {
		return fmt.Sprintf(
			"only one of --%s, --%s, --%s, --%s may be supplied",
			flagNameDumpSchema, flagNameDumpMigration, flagNameMigrateOnly, flagNameVerifySchema,
		)
	}
	return ""
}

// retentionDaysFor returns the retention setting used to render embedded
// migrations. If a config path is supplied and loadable, the value comes from
// the loaded config so dumps match what --migrate-only would apply; otherwise
// the default from DefaultClickhouseConfig is used so the dump commands stay
// usable on hosts that have no yagpcc.yaml yet.
func retentionDaysFor(f schemaFlags, deps schemaCLIDeps) int {
	if f.configPath == "" || deps.loadConfig == nil {
		return config.DefaultClickhouseConfig().RetentionDays
	}
	cfg, err := deps.loadConfig(f.configPath)
	if err != nil || cfg == nil {
		return config.DefaultClickhouseConfig().RetentionDays
	}
	if cfg.Clickhouse.RetentionDays > 0 {
		return cfg.Clickhouse.RetentionDays
	}
	return config.DefaultClickhouseConfig().RetentionDays
}

func runDumpSchema(f schemaFlags, deps schemaCLIDeps) int {
	opts := clickhouse.DumpOptions{RetentionDays: retentionDaysFor(f, deps)}
	out, err := clickhouse.DumpSchema(opts)
	if err != nil {
		_, _ = fmt.Fprintf(deps.stderr, "dump-schema: %v\n", err)
		return schemaCLIExitFail
	}
	if _, err := io.WriteString(deps.stdout, out); err != nil {
		_, _ = fmt.Fprintf(deps.stderr, "dump-schema: write stdout: %v\n", err)
		return schemaCLIExitFail
	}
	return schemaCLIExitOK
}

func runDumpMigration(f schemaFlags, deps schemaCLIDeps) int {
	if f.from < 0 || f.to < 0 {
		_, _ = fmt.Fprintf(deps.stderr,
			"--%s requires --%s and --%s (both >= 0)\n",
			flagNameDumpMigration, flagNameFrom, flagNameTo,
		)
		return schemaCLIExitFail
	}
	opts := clickhouse.DumpOptions{RetentionDays: retentionDaysFor(f, deps)}
	out, err := clickhouse.DumpMigration(f.from, f.to, opts)
	if err != nil {
		_, _ = fmt.Fprintf(deps.stderr, "dump-migration: %v\n", err)
		return schemaCLIExitFail
	}
	if _, err := io.WriteString(deps.stdout, out); err != nil {
		_, _ = fmt.Fprintf(deps.stderr, "dump-migration: write stdout: %v\n", err)
		return schemaCLIExitFail
	}
	return schemaCLIExitOK
}

// loadCHConfig is the shared entry point for the connect-needing commands. It
// loads the file, applies env overrides (so YAGPCC_CH_PASSWORD works the same
// way as for the master process) and validates the ClickHouse connectivity
// fields. The `enabled` flag is intentionally ignored: operators frequently
// want to run --migrate-only or --verify-schema while the sink itself is
// still off (bootstrap, dry-run on a fresh cluster).
func loadCHConfig(f schemaFlags, deps schemaCLIDeps) (*config.ClickhouseConfig, error) {
	if f.configPath == "" {
		return nil, errors.New("--config-path is required for this command")
	}
	if deps.loadConfig == nil {
		return nil, errors.New("internal: no config loader wired")
	}
	cfg, err := deps.loadConfig(f.configPath)
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	if cfg == nil {
		return nil, errors.New("load config: returned nil")
	}
	cfg.Clickhouse.ApplyEnvOverrides()
	// Force-enable for the validator: connectivity checks (addrs, password,
	// retention_days, etc.) are gated on enabled=true, but the CLI commands
	// always need them. The original cfg.Clickhouse.Enabled is not propagated
	// — the returned struct is consumed only for opening a connection and
	// rendering retention.
	chCopy := cfg.Clickhouse
	chCopy.Enabled = true
	if err := chCopy.Validate(); err != nil {
		return nil, fmt.Errorf("validate clickhouse config: %w", err)
	}
	return &chCopy, nil
}

func runMigrateOnly(ctx context.Context, f schemaFlags, deps schemaCLIDeps) int {
	chCfg, err := loadCHConfig(f, deps)
	if err != nil {
		_, _ = fmt.Fprintf(deps.stderr, "migrate-only: %v\n", err)
		return schemaCLIExitFail
	}
	conn, closeFn, err := deps.openConn(ctx, chCfg)
	if err != nil {
		_, _ = fmt.Fprintf(deps.stderr, "migrate-only: open clickhouse: %v\n", err)
		return schemaCLIExitFail
	}
	defer func() {
		if closeFn != nil {
			_ = closeFn()
		}
	}()
	opts := clickhouse.MigrateOptions{RetentionDays: chCfg.RetentionDays}
	if err := deps.applyMigrate(ctx, conn, opts); err != nil {
		_, _ = fmt.Fprintf(deps.stderr, "migrate-only: %v\n", err)
		return schemaCLIExitFail
	}
	_, _ = fmt.Fprintln(deps.stdout, "migrate-only: ok")
	return schemaCLIExitOK
}

func runVerifySchema(ctx context.Context, f schemaFlags, deps schemaCLIDeps) int {
	chCfg, err := loadCHConfig(f, deps)
	if err != nil {
		_, _ = fmt.Fprintf(deps.stderr, "verify-schema: %v\n", err)
		return schemaCLIExitFail
	}
	conn, closeFn, err := deps.openConn(ctx, chCfg)
	if err != nil {
		_, _ = fmt.Fprintf(deps.stderr, "verify-schema: open clickhouse: %v\n", err)
		return schemaCLIExitFail
	}
	defer func() {
		if closeFn != nil {
			_ = closeFn()
		}
	}()
	if err := deps.verifySchema(ctx, conn); err != nil {
		_, _ = fmt.Fprintf(deps.stderr, "verify-schema: %v\n", err)
		return schemaCLIExitFail
	}
	_, _ = fmt.Fprintln(deps.stdout, "verify-schema: ok")
	return schemaCLIExitOK
}

// maybeRunDumpOnlyFromConfig handles the
// `clickhouse.schema_management: dump_only` configuration mode at startup.
// When the loaded config requests dump_only, the binary prints the cumulative
// DDL to stdout and exits — the master process is intentionally not started.
// Returns (exitCode, true) to signal "the caller should os.Exit" or
// (_, false) to indicate "no dump_only config mode, proceed with normal
// startup". A missing or unparseable config is treated as "not dump_only" so
// operators can still run yagpcc without a yaml on disk for testing.
func maybeRunDumpOnlyFromConfig(f schemaFlags, deps schemaCLIDeps) (int, bool) {
	if f.configPath == "" || deps.loadConfig == nil {
		return 0, false
	}
	cfg, err := deps.loadConfig(f.configPath)
	if err != nil || cfg == nil {
		return 0, false
	}
	if !cfg.Clickhouse.Enabled || cfg.Clickhouse.SchemaManagement != config.SchemaManagementDumpOnly {
		return 0, false
	}
	opts := clickhouse.DumpOptions{RetentionDays: cfg.Clickhouse.RetentionDays}
	if opts.RetentionDays <= 0 {
		opts.RetentionDays = config.DefaultClickhouseConfig().RetentionDays
	}
	out, derr := clickhouse.DumpSchema(opts)
	if derr != nil {
		_, _ = fmt.Fprintf(deps.stderr, "schema_management=dump_only: %v\n", derr)
		return schemaCLIExitFail, true
	}
	if _, werr := io.WriteString(deps.stdout, out); werr != nil {
		_, _ = fmt.Fprintf(deps.stderr, "schema_management=dump_only: write stdout: %v\n", werr)
		return schemaCLIExitFail, true
	}
	return schemaCLIExitOK, true
}

// productionSchemaCLIDeps wires the real loader / driver. It exists in main
// (rather than as a value embedded in main()) so the test suite can construct
// its own deps without dragging in the clickhouse-go driver.
func productionSchemaCLIDeps() schemaCLIDeps {
	return schemaCLIDeps{
		stdout:     os.Stdout,
		stderr:     os.Stderr,
		loadConfig: config.ReadFromFile,
		openConn: func(ctx context.Context, cfg *config.ClickhouseConfig) (clickhouse.MigrationConn, func() error, error) {
			conn, err := clickhouse.NewClient(ctx, cfg)
			if err != nil {
				return nil, nil, err
			}
			if err := clickhouse.Ping(ctx, conn); err != nil {
				_ = conn.Close()
				return nil, nil, err
			}
			return conn, conn.Close, nil
		},
		applyMigrate: clickhouse.ApplyMigrations,
		verifySchema: clickhouse.VerifySchema,
	}
}
