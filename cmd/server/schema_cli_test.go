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

package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/open-gpdb/yagpcc/internal/config"
	"github.com/open-gpdb/yagpcc/internal/sink/clickhouse"
)

// fakeMigrationConn satisfies clickhouse.MigrationConn for testing — it never
// touches a real ClickHouse server. The test substitutes its own applyMigrate
// / verifySchema functions so this fake does not need to track any state.
type fakeMigrationConn struct{}

func (fakeMigrationConn) Exec(_ context.Context, _ string, _ ...any) error { return nil }
func (fakeMigrationConn) Select(_ context.Context, _ any, _ string, _ ...any) error {
	return nil
}

func enabledConfig() *config.Config {
	cfg := &config.Config{Clickhouse: config.DefaultClickhouseConfig()}
	cfg.Clickhouse.Enabled = true
	cfg.Clickhouse.Addrs = []string{"127.0.0.1:9000"}
	cfg.Clickhouse.Password = "x"
	return cfg
}

func newCapturingDeps(cfg *config.Config) (schemaCLIDeps, *bytes.Buffer, *bytes.Buffer) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	deps := schemaCLIDeps{
		stdout: stdout,
		stderr: stderr,
		loadConfig: func(_ string) (*config.Config, error) {
			if cfg == nil {
				return nil, errors.New("no config")
			}
			out := *cfg
			ch := cfg.Clickhouse
			out.Clickhouse = ch
			return &out, nil
		},
		openConn: func(_ context.Context, _ *config.ClickhouseConfig) (clickhouse.MigrationConn, func() error, error) {
			return fakeMigrationConn{}, func() error { return nil }, nil
		},
		applyMigrate: func(_ context.Context, _ clickhouse.MigrationConn, _ clickhouse.MigrateOptions) error {
			return nil
		},
		verifySchema: func(_ context.Context, _ clickhouse.MigrationConn) error {
			return nil
		},
	}
	return deps, stdout, stderr
}

func TestSchemaCLI_NoCommand_NoOp(t *testing.T) {
	deps, stdout, stderr := newCapturingDeps(nil)
	rc := runSchemaCLI(context.Background(), schemaFlags{}, deps)
	if rc != schemaCLIExitOK {
		t.Fatalf("rc=%d", rc)
	}
	if stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("expected silent no-op, stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestSchemaCLI_DumpSchema_NoConfig(t *testing.T) {
	deps, stdout, stderr := newCapturingDeps(nil)
	rc := runSchemaCLI(context.Background(), schemaFlags{dumpSchema: true}, deps)
	if rc != schemaCLIExitOK {
		t.Fatalf("rc=%d stderr=%q", rc, stderr.String())
	}
	if !strings.Contains(stdout.String(), "CREATE TABLE") {
		t.Fatalf("expected DDL in stdout, got %q", stdout.String())
	}
}

func TestSchemaCLI_DumpSchema_LoadsConfig(t *testing.T) {
	cfg := enabledConfig()
	cfg.Clickhouse.RetentionDays = 7
	deps, stdout, _ := newCapturingDeps(cfg)
	rc := runSchemaCLI(context.Background(), schemaFlags{dumpSchema: true, configPath: "/tmp/cfg.yaml"}, deps)
	if rc != schemaCLIExitOK {
		t.Fatalf("rc=%d", rc)
	}
	// The production schema carries fixed per-table TTLs, so the dump renders
	// them regardless of the config's retention_days value.
	if !strings.Contains(stdout.String(), "toIntervalDay(60)") {
		t.Fatalf("expected sessions TTL in DDL, got %q", stdout.String())
	}
}

func TestSchemaCLI_DumpSchema_ReplicatedFlag(t *testing.T) {
	deps, stdout, stderr := newCapturingDeps(nil)
	rc := runSchemaCLI(context.Background(), schemaFlags{dumpSchema: true, replicated: true}, deps)
	if rc != schemaCLIExitOK {
		t.Fatalf("rc=%d stderr=%q", rc, stderr.String())
	}
	out := stdout.String()
	for _, want := range []string{"ReplicatedReplacingMergeTree", "ON CLUSTER '{cluster}'", "Distributed("} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected %q in replicated dump, got %q", want, out)
		}
	}
}

func TestSchemaCLI_DumpSchema_StandaloneByDefault(t *testing.T) {
	deps, stdout, _ := newCapturingDeps(nil)
	rc := runSchemaCLI(context.Background(), schemaFlags{dumpSchema: true}, deps)
	if rc != schemaCLIExitOK {
		t.Fatalf("rc=%d", rc)
	}
	if strings.Contains(stdout.String(), "ReplicatedReplacingMergeTree") {
		t.Fatalf("standalone dump must not contain clustered engine, got %q", stdout.String())
	}
}

func TestSchemaCLI_DumpMigration_RequiresFromTo(t *testing.T) {
	deps, _, stderr := newCapturingDeps(nil)
	// pflag default for --from / --to is -1; simulate "user did not pass them".
	rc := runSchemaCLI(context.Background(), schemaFlags{dumpMigration: true, from: -1, to: -1}, deps)
	if rc != schemaCLIExitFail {
		t.Fatalf("expected fail, got rc=%d", rc)
	}
	if !strings.Contains(stderr.String(), "--from") {
		t.Fatalf("expected stderr to mention --from, got %q", stderr.String())
	}
}

func TestSchemaCLI_DumpMigration_OK(t *testing.T) {
	deps, stdout, _ := newCapturingDeps(nil)
	rc := runSchemaCLI(context.Background(), schemaFlags{dumpMigration: true, from: 0, to: 1}, deps)
	if rc != schemaCLIExitOK {
		t.Fatalf("rc=%d", rc)
	}
	if !strings.Contains(stdout.String(), "CREATE TABLE") {
		t.Fatalf("expected DDL, got %q", stdout.String())
	}
}

func TestSchemaCLI_MigrateOnly_OK(t *testing.T) {
	deps, stdout, stderr := newCapturingDeps(enabledConfig())
	rc := runSchemaCLI(context.Background(), schemaFlags{migrateOnly: true, configPath: "/tmp/cfg.yaml"}, deps)
	if rc != schemaCLIExitOK {
		t.Fatalf("rc=%d stderr=%q", rc, stderr.String())
	}
	if !strings.Contains(stdout.String(), "ok") {
		t.Fatalf("expected success message, got %q", stdout.String())
	}
}

func TestSchemaCLI_MigrateOnly_NoConfig(t *testing.T) {
	deps, _, stderr := newCapturingDeps(nil)
	rc := runSchemaCLI(context.Background(), schemaFlags{migrateOnly: true}, deps)
	if rc != schemaCLIExitFail {
		t.Fatalf("expected fail, got rc=%d", rc)
	}
	if !strings.Contains(stderr.String(), "config-path") {
		t.Fatalf("expected stderr to mention config-path, got %q", stderr.String())
	}
}

func TestSchemaCLI_MigrateOnly_DisabledClickhouseStillRuns(t *testing.T) {
	// Operators frequently bootstrap CH before flipping enabled=true; the CLI
	// commands must not require enabled=true. Connectivity fields are still
	// validated.
	cfg := enabledConfig()
	cfg.Clickhouse.Enabled = false
	deps, stdout, stderr := newCapturingDeps(cfg)
	rc := runSchemaCLI(context.Background(), schemaFlags{migrateOnly: true, configPath: "/tmp/cfg.yaml"}, deps)
	if rc != schemaCLIExitOK {
		t.Fatalf("expected ok, got rc=%d stderr=%q", rc, stderr.String())
	}
	if !strings.Contains(stdout.String(), "ok") {
		t.Fatalf("expected ok message, got %q", stdout.String())
	}
}

func TestSchemaCLI_MigrateOnly_MissingPasswordFails(t *testing.T) {
	cfg := enabledConfig()
	cfg.Clickhouse.Password = ""
	deps, _, stderr := newCapturingDeps(cfg)
	rc := runSchemaCLI(context.Background(), schemaFlags{migrateOnly: true, configPath: "/tmp/cfg.yaml"}, deps)
	if rc != schemaCLIExitFail {
		t.Fatalf("expected fail when password missing, got rc=%d", rc)
	}
	if !strings.Contains(stderr.String(), "password") {
		t.Fatalf("expected stderr to mention password, got %q", stderr.String())
	}
}

func TestSchemaCLI_MigrateOnly_OpenConnError(t *testing.T) {
	deps, _, stderr := newCapturingDeps(enabledConfig())
	deps.openConn = func(_ context.Context, _ *config.ClickhouseConfig) (clickhouse.MigrationConn, func() error, error) {
		return nil, nil, errors.New("dial timeout")
	}
	rc := runSchemaCLI(context.Background(), schemaFlags{migrateOnly: true, configPath: "/tmp/cfg.yaml"}, deps)
	if rc != schemaCLIExitFail {
		t.Fatalf("rc=%d", rc)
	}
	if !strings.Contains(stderr.String(), "dial timeout") {
		t.Fatalf("expected stderr to surface dial error, got %q", stderr.String())
	}
}

func TestSchemaCLI_MigrateOnly_ApplyError(t *testing.T) {
	deps, _, stderr := newCapturingDeps(enabledConfig())
	deps.applyMigrate = func(_ context.Context, _ clickhouse.MigrationConn, _ clickhouse.MigrateOptions) error {
		return errors.New("ddl failed")
	}
	rc := runSchemaCLI(context.Background(), schemaFlags{migrateOnly: true, configPath: "/tmp/cfg.yaml"}, deps)
	if rc != schemaCLIExitFail {
		t.Fatalf("rc=%d", rc)
	}
	if !strings.Contains(stderr.String(), "ddl failed") {
		t.Fatalf("expected stderr to surface apply error, got %q", stderr.String())
	}
}

func TestSchemaCLI_MigrateOnly_ClosesConn(t *testing.T) {
	closed := false
	deps, _, _ := newCapturingDeps(enabledConfig())
	deps.openConn = func(_ context.Context, _ *config.ClickhouseConfig) (clickhouse.MigrationConn, func() error, error) {
		return fakeMigrationConn{}, func() error { closed = true; return nil }, nil
	}
	rc := runSchemaCLI(context.Background(), schemaFlags{migrateOnly: true, configPath: "/tmp/cfg.yaml"}, deps)
	if rc != schemaCLIExitOK {
		t.Fatalf("rc=%d", rc)
	}
	if !closed {
		t.Fatal("expected close fn to be invoked")
	}
}

func TestSchemaCLI_VerifySchema_OK(t *testing.T) {
	deps, stdout, _ := newCapturingDeps(enabledConfig())
	rc := runSchemaCLI(context.Background(), schemaFlags{verifySchema: true, configPath: "/tmp/cfg.yaml"}, deps)
	if rc != schemaCLIExitOK {
		t.Fatalf("rc=%d", rc)
	}
	if !strings.Contains(stdout.String(), "ok") {
		t.Fatalf("expected ok, got %q", stdout.String())
	}
}

func TestSchemaCLI_VerifySchema_UpgradeRequired(t *testing.T) {
	deps, _, stderr := newCapturingDeps(enabledConfig())
	deps.verifySchema = func(_ context.Context, _ clickhouse.MigrationConn) error {
		return clickhouse.ErrSchemaUpgradeRequired
	}
	rc := runSchemaCLI(context.Background(), schemaFlags{verifySchema: true, configPath: "/tmp/cfg.yaml"}, deps)
	if rc != schemaCLIExitFail {
		t.Fatalf("rc=%d", rc)
	}
	if !strings.Contains(stderr.String(), "upgrade") {
		t.Fatalf("expected upgrade hint, got %q", stderr.String())
	}
}

func TestSchemaCLI_MutuallyExclusive(t *testing.T) {
	deps, _, stderr := newCapturingDeps(nil)
	rc := runSchemaCLI(context.Background(),
		schemaFlags{dumpSchema: true, migrateOnly: true},
		deps,
	)
	if rc != schemaCLIExitFail {
		t.Fatalf("rc=%d", rc)
	}
	if !strings.Contains(stderr.String(), "only one of") {
		t.Fatalf("expected exclusivity error, got %q", stderr.String())
	}
}

func TestSchemaCommandRequested(t *testing.T) {
	cases := []struct {
		f    schemaFlags
		want bool
	}{
		{schemaFlags{}, false},
		{schemaFlags{dumpSchema: true}, true},
		{schemaFlags{dumpMigration: true}, true},
		{schemaFlags{migrateOnly: true}, true},
		{schemaFlags{verifySchema: true}, true},
	}
	for i, c := range cases {
		if got := c.f.schemaCommandRequested(); got != c.want {
			t.Errorf("case %d: got %v want %v", i, got, c.want)
		}
	}
}

func TestMaybeRunDumpOnlyFromConfig_NoConfigPath(t *testing.T) {
	deps, _, _ := newCapturingDeps(enabledConfig())
	_, ok := maybeRunDumpOnlyFromConfig(schemaFlags{}, deps)
	if ok {
		t.Fatal("expected ok=false when configPath empty")
	}
}

func TestMaybeRunDumpOnlyFromConfig_DisabledClickhouse(t *testing.T) {
	cfg := enabledConfig()
	cfg.Clickhouse.Enabled = false
	deps, _, _ := newCapturingDeps(cfg)
	_, ok := maybeRunDumpOnlyFromConfig(schemaFlags{configPath: "/tmp/cfg.yaml"}, deps)
	if ok {
		t.Fatal("expected ok=false when clickhouse disabled")
	}
}

func TestMaybeRunDumpOnlyFromConfig_NotDumpOnlyMode(t *testing.T) {
	cfg := enabledConfig()
	cfg.Clickhouse.SchemaManagement = config.SchemaManagementAuto
	deps, _, _ := newCapturingDeps(cfg)
	_, ok := maybeRunDumpOnlyFromConfig(schemaFlags{configPath: "/tmp/cfg.yaml"}, deps)
	if ok {
		t.Fatal("expected ok=false when schema_management != dump_only")
	}
}

func TestMaybeRunDumpOnlyFromConfig_PrintsAndExits(t *testing.T) {
	cfg := enabledConfig()
	cfg.Clickhouse.SchemaManagement = config.SchemaManagementDumpOnly
	cfg.Clickhouse.RetentionDays = 14
	deps, stdout, _ := newCapturingDeps(cfg)
	rc, ok := maybeRunDumpOnlyFromConfig(schemaFlags{configPath: "/tmp/cfg.yaml"}, deps)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if rc != schemaCLIExitOK {
		t.Fatalf("rc=%d", rc)
	}
	if !strings.Contains(stdout.String(), "toIntervalDay(60)") {
		t.Fatalf("expected sessions TTL in DDL, got %q", stdout.String())
	}
}

func TestMaybeRunDumpOnlyFromConfig_LoadFailureFalseSilent(t *testing.T) {
	deps := schemaCLIDeps{
		stdout: &bytes.Buffer{},
		stderr: &bytes.Buffer{},
		loadConfig: func(_ string) (*config.Config, error) {
			return nil, errors.New("yaml: bad syntax")
		},
	}
	_, ok := maybeRunDumpOnlyFromConfig(schemaFlags{configPath: "/tmp/cfg.yaml"}, deps)
	if ok {
		t.Fatal("expected ok=false on load failure (proceed with normal startup)")
	}
}
