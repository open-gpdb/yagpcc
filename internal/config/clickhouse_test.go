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

package config

import (
	"strings"
	"testing"
	"time"
)

func TestDefaultClickhouseConfig(t *testing.T) {
	c := DefaultClickhouseConfig()

	if c.Enabled {
		t.Errorf("expected Enabled=false by default, got true")
	}
	if c.Database != "yagpcc" {
		t.Errorf("expected Database=yagpcc, got %q", c.Database)
	}
	if c.User != "yagpcc_writer" {
		t.Errorf("expected User=yagpcc_writer, got %q", c.User)
	}
	if c.SchemaManagement != SchemaManagementAuto {
		t.Errorf("expected SchemaManagement=auto, got %q", c.SchemaManagement)
	}
	if c.RetentionDays != 30 {
		t.Errorf("expected RetentionDays=30, got %d", c.RetentionDays)
	}
	if c.BatchSize != 10000 {
		t.Errorf("expected BatchSize=10000, got %d", c.BatchSize)
	}
	if c.FlushInterval != 10*time.Second {
		t.Errorf("expected FlushInterval=10s, got %s", c.FlushInterval)
	}
	if c.BufferMaxRows != 100000 {
		t.Errorf("expected BufferMaxRows=100000, got %d", c.BufferMaxRows)
	}
	if c.OnBufferOverflow != OnBufferOverflowDropOldest {
		t.Errorf("expected OnBufferOverflow=drop_oldest, got %q", c.OnBufferOverflow)
	}
	if !c.AsyncInsert {
		t.Errorf("expected AsyncInsert=true")
	}
	if c.MinDurationMs != 100 {
		t.Errorf("expected MinDurationMs=100, got %d", c.MinDurationMs)
	}
	if c.SessionSnapshotIntervalSec != 10 {
		t.Errorf("expected SessionSnapshotIntervalSec=10, got %d", c.SessionSnapshotIntervalSec)
	}
	if c.DialTimeout != 5*time.Second {
		t.Errorf("expected DialTimeout=5s, got %s", c.DialTimeout)
	}
	if c.ReadTimeout != 30*time.Second {
		t.Errorf("expected ReadTimeout=30s, got %s", c.ReadTimeout)
	}
	if !c.Sinks.QueryEvents || !c.Sinks.AggregatedMetrics || !c.Sinks.SessionSnapshots {
		t.Errorf("expected QueryEvents/AggregatedMetrics/SessionSnapshots sinks enabled by default, got %+v", c.Sinks)
	}
	if c.Sinks.PlanNodes {
		t.Errorf("expected PlanNodes sink disabled by default")
	}
}

func validClickhouseConfig() ClickhouseConfig {
	c := DefaultClickhouseConfig()
	c.Enabled = true
	c.Addrs = []string{"clickhouse-mon:9000"}
	c.Password = "secret"
	return c
}

func TestClickhouseConfigValidate_DisabledOK(t *testing.T) {
	c := DefaultClickhouseConfig()
	if err := c.Validate(); err != nil {
		t.Errorf("expected nil err when disabled, got %v", err)
	}
}

func TestClickhouseConfigValidate_HappyPath(t *testing.T) {
	c := validClickhouseConfig()
	if err := c.Validate(); err != nil {
		t.Errorf("expected nil err for valid config, got %v", err)
	}
}

func TestClickhouseConfigValidate_UnsupportedDatabase(t *testing.T) {
	c := validClickhouseConfig()
	c.Database = "otherdb"
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "database") {
		t.Errorf("expected database error, got %v", err)
	}
	c.Database = SupportedClickhouseDatabase
	if err := c.Validate(); err != nil {
		t.Errorf("expected nil err for supported database, got %v", err)
	}
}

func TestClickhouseConfigValidate_MissingAddrs(t *testing.T) {
	c := validClickhouseConfig()
	c.Addrs = nil
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "addrs") {
		t.Errorf("expected addrs error, got %v", err)
	}
}

func TestClickhouseConfigValidate_EmptyAddr(t *testing.T) {
	c := validClickhouseConfig()
	c.Addrs = []string{"clickhouse-mon:9000", ""}
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "addrs[1]") {
		t.Errorf("expected empty addr error, got %v", err)
	}
}

func TestClickhouseConfigValidate_MissingPassword(t *testing.T) {
	c := validClickhouseConfig()
	c.Password = ""
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "password") {
		t.Errorf("expected password error, got %v", err)
	}
}

func TestClickhouseConfigValidate_BadSchemaManagement(t *testing.T) {
	c := validClickhouseConfig()
	c.SchemaManagement = "magic"
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "schema_management") {
		t.Errorf("expected schema_management error, got %v", err)
	}

	for _, ok := range []string{SchemaManagementAuto, SchemaManagementVerifyOnly, SchemaManagementDumpOnly} {
		c.SchemaManagement = ok
		if err := c.Validate(); err != nil {
			t.Errorf("expected ok for schema_management=%q, got %v", ok, err)
		}
	}
}

func TestClickhouseConfigValidate_BadOnBufferOverflow(t *testing.T) {
	c := validClickhouseConfig()
	c.OnBufferOverflow = "panic"
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "on_buffer_overflow") {
		t.Errorf("expected on_buffer_overflow error, got %v", err)
	}

	for _, ok := range []string{OnBufferOverflowDropOldest, OnBufferOverflowBlock} {
		c.OnBufferOverflow = ok
		if err := c.Validate(); err != nil {
			t.Errorf("expected ok for on_buffer_overflow=%q, got %v", ok, err)
		}
	}
}

func TestClickhouseConfigValidate_RetentionDays(t *testing.T) {
	c := validClickhouseConfig()
	c.RetentionDays = 0
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "retention_days") {
		t.Errorf("expected retention_days error, got %v", err)
	}
	c.RetentionDays = -5
	err = c.Validate()
	if err == nil || !strings.Contains(err.Error(), "retention_days") {
		t.Errorf("expected retention_days error, got %v", err)
	}
}

func TestClickhouseConfigValidate_MinDurationNegative(t *testing.T) {
	c := validClickhouseConfig()
	c.MinDurationMs = -1
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "min_duration_ms") {
		t.Errorf("expected min_duration_ms error, got %v", err)
	}

	c.MinDurationMs = 0
	if err := c.Validate(); err != nil {
		t.Errorf("expected min_duration_ms=0 to be ok, got %v", err)
	}
}

func TestClickhouseConfigValidate_BatchAndBuffer(t *testing.T) {
	c := validClickhouseConfig()
	c.BatchSize = 0
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "batch_size") {
		t.Errorf("expected batch_size error, got %v", err)
	}

	c = validClickhouseConfig()
	c.BufferMaxRows = 0
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "buffer_max_rows") {
		t.Errorf("expected buffer_max_rows error, got %v", err)
	}

	c = validClickhouseConfig()
	c.FlushInterval = 0
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "flush_interval") {
		t.Errorf("expected flush_interval error, got %v", err)
	}
}

func TestClickhouseConfigValidate_SessionSnapshotInterval(t *testing.T) {
	c := validClickhouseConfig()
	c.SessionSnapshotIntervalSec = 0
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "session_snapshot_interval_sec") {
		t.Errorf("expected session_snapshot_interval_sec error when sink enabled, got %v", err)
	}

	c.Sinks.SessionSnapshots = false
	if err := c.Validate(); err != nil {
		t.Errorf("expected ok when session_snapshots sink disabled, got %v", err)
	}
}

func TestClickhouseConfigValidate_Timeouts(t *testing.T) {
	c := validClickhouseConfig()
	c.DialTimeout = 0
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "dial_timeout") {
		t.Errorf("expected dial_timeout error, got %v", err)
	}

	c = validClickhouseConfig()
	c.ReadTimeout = 0
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "read_timeout") {
		t.Errorf("expected read_timeout error, got %v", err)
	}
}

func TestClickhouseConfigValidate_TLSCAFileRequired(t *testing.T) {
	c := validClickhouseConfig()
	c.TLS.Enabled = true
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "ca_file") {
		t.Errorf("expected tls.ca_file error, got %v", err)
	}

	c.TLS.CAFile = "/etc/ssl/ca.crt"
	if err := c.Validate(); err != nil {
		t.Errorf("expected ok with ca_file, got %v", err)
	}

	c.TLS.CAFile = ""
	c.TLS.InsecureSkipVerify = true
	if err := c.Validate(); err != nil {
		t.Errorf("expected ok with insecure_skip_verify, got %v", err)
	}
}

func TestClickhouseConfigApplyEnvOverrides(t *testing.T) {
	t.Setenv(ClickhousePasswordEnv, "from-env")
	c := DefaultClickhouseConfig()
	c.Password = "from-yaml"
	c.ApplyEnvOverrides()
	if c.Password != "from-env" {
		t.Errorf("expected env override to win, got %q", c.Password)
	}
}

func TestClickhouseConfigApplyEnvOverrides_EmptyEnvKeepsYaml(t *testing.T) {
	t.Setenv(ClickhousePasswordEnv, "")
	c := DefaultClickhouseConfig()
	c.Password = "from-yaml"
	c.ApplyEnvOverrides()
	if c.Password != "from-yaml" {
		t.Errorf("expected yaml password to remain, got %q", c.Password)
	}
}

func TestConfigValidate_NonMasterSkipsClickhouse(t *testing.T) {
	cfg, err := DefaultConfig()
	if err != nil {
		t.Fatalf("DefaultConfig: %v", err)
	}
	cfg.Role = "segment"
	cfg.Clickhouse.Enabled = true
	cfg.Clickhouse.Addrs = nil
	cfg.Clickhouse.Password = ""
	if err := cfg.Validate(); err != nil {
		t.Errorf("non-master role should skip clickhouse validation, got %v", err)
	}
}

func TestConfigValidate_MasterRunsClickhouseValidate(t *testing.T) {
	cfg, err := DefaultConfig()
	if err != nil {
		t.Fatalf("DefaultConfig: %v", err)
	}
	cfg.Role = "master"
	cfg.Clickhouse.Enabled = true
	cfg.Clickhouse.Addrs = nil
	cfg.Clickhouse.Password = ""
	if err := cfg.Validate(); err == nil {
		t.Errorf("master role with bad clickhouse cfg should fail validation, got nil")
	}
}
