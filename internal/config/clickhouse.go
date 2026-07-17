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
	"fmt"
	"os"
	"time"
)

const (
	SchemaManagementAuto       = "auto"
	SchemaManagementVerifyOnly = "verify_only"
	SchemaManagementDumpOnly   = "dump_only"

	OnBufferOverflowDropOldest = "drop_oldest"
	OnBufferOverflowBlock      = "block"

	ClickhousePasswordEnv = "YAGPCC_CH_PASSWORD"

	// SupportedClickhouseDatabase is the only ClickHouse database the embedded
	// DDL/migrations create tables in. The runtime writer qualifies its INSERTs
	// with the configured database, so a value other than this would insert into
	// a database whose tables were never created — every batch would fail and be
	// dropped silently. Reject such configs up front instead.
	SupportedClickhouseDatabase = "yagpcc"
)

type ClickhouseTLSConfig struct {
	Enabled            bool   `config:"enabled" yaml:"enabled"`
	CAFile             string `config:"ca_file" yaml:"ca_file"`
	InsecureSkipVerify bool   `config:"insecure_skip_verify" yaml:"insecure_skip_verify"`
}

type ClickhouseSinks struct {
	QueryEvents       bool `config:"query_events" yaml:"query_events"`
	AggregatedMetrics bool `config:"aggregated_metrics" yaml:"aggregated_metrics"`
	SessionSnapshots  bool `config:"session_snapshots" yaml:"session_snapshots"`
	PlanNodes         bool `config:"plan_nodes" yaml:"plan_nodes"`
}

type ClickhouseConfig struct {
	Enabled  bool     `config:"enabled" yaml:"enabled"`
	Addrs    []string `config:"addrs" yaml:"addrs"`
	Database string   `config:"database" yaml:"database"`
	User     string   `config:"user" yaml:"user"`
	Password string   `config:"password" yaml:"password"`

	SchemaManagement string `config:"schema_management" yaml:"schema_management"`
	RetentionDays    int    `config:"retention_days" yaml:"retention_days"`

	BatchSize        int           `config:"batch_size" yaml:"batch_size"`
	FlushInterval    time.Duration `config:"flush_interval" yaml:"flush_interval"`
	BufferMaxRows    int           `config:"buffer_max_rows" yaml:"buffer_max_rows"`
	OnBufferOverflow string        `config:"on_buffer_overflow" yaml:"on_buffer_overflow"`
	AsyncInsert      bool          `config:"async_insert" yaml:"async_insert"`

	MinDurationMs              int `config:"min_duration_ms" yaml:"min_duration_ms"`
	SessionSnapshotIntervalSec int `config:"session_snapshot_interval_sec" yaml:"session_snapshot_interval_sec"`

	DialTimeout time.Duration `config:"dial_timeout" yaml:"dial_timeout"`
	ReadTimeout time.Duration `config:"read_timeout" yaml:"read_timeout"`

	TLS   ClickhouseTLSConfig `yaml:"tls"`
	Sinks ClickhouseSinks     `yaml:"sinks"`
}

func DefaultClickhouseConfig() ClickhouseConfig {
	return ClickhouseConfig{
		Enabled:                    false,
		Database:                   "yagpcc",
		User:                       "yagpcc_writer",
		SchemaManagement:           SchemaManagementAuto,
		RetentionDays:              30,
		BatchSize:                  10000,
		FlushInterval:              10 * time.Second,
		BufferMaxRows:              100000,
		OnBufferOverflow:           OnBufferOverflowDropOldest,
		AsyncInsert:                true,
		MinDurationMs:              100,
		SessionSnapshotIntervalSec: 10,
		DialTimeout:                5 * time.Second,
		ReadTimeout:                30 * time.Second,
		Sinks: ClickhouseSinks{
			QueryEvents:       true,
			AggregatedMetrics: true,
			SessionSnapshots:  true,
			PlanNodes:         false,
		},
	}
}

// ApplyEnvOverrides reads YAGPCC_CH_PASSWORD and overrides Password if set.
// Called after the yaml file is loaded so deployments can keep secrets out of
// the config file.
func (c *ClickhouseConfig) ApplyEnvOverrides() {
	if v := os.Getenv(ClickhousePasswordEnv); v != "" {
		c.Password = v
	}
}

func (c *ClickhouseConfig) Validate() error {
	if !c.Enabled {
		return nil
	}

	if len(c.Addrs) == 0 {
		return fmt.Errorf("clickhouse: addrs must not be empty when enabled")
	}
	if c.Database != "" && c.Database != SupportedClickhouseDatabase {
		return fmt.Errorf("clickhouse: database must be %q, got %q (the embedded schema only creates tables in that database)", SupportedClickhouseDatabase, c.Database)
	}
	for i, a := range c.Addrs {
		if a == "" {
			return fmt.Errorf("clickhouse: addrs[%d] is empty", i)
		}
	}
	if c.Password == "" {
		return fmt.Errorf("clickhouse: password is required when enabled (set %s)", ClickhousePasswordEnv)
	}

	switch c.SchemaManagement {
	case SchemaManagementAuto, SchemaManagementVerifyOnly, SchemaManagementDumpOnly:
	default:
		return fmt.Errorf("clickhouse: schema_management must be one of auto|verify_only|dump_only, got %q", c.SchemaManagement)
	}

	switch c.OnBufferOverflow {
	case OnBufferOverflowDropOldest, OnBufferOverflowBlock:
	default:
		return fmt.Errorf("clickhouse: on_buffer_overflow must be one of drop_oldest|block, got %q", c.OnBufferOverflow)
	}

	if c.RetentionDays <= 0 {
		return fmt.Errorf("clickhouse: retention_days must be > 0, got %d", c.RetentionDays)
	}
	if c.MinDurationMs < 0 {
		return fmt.Errorf("clickhouse: min_duration_ms must be >= 0, got %d", c.MinDurationMs)
	}
	if c.BatchSize <= 0 {
		return fmt.Errorf("clickhouse: batch_size must be > 0, got %d", c.BatchSize)
	}
	if c.BufferMaxRows <= 0 {
		return fmt.Errorf("clickhouse: buffer_max_rows must be > 0, got %d", c.BufferMaxRows)
	}
	if c.FlushInterval <= 0 {
		return fmt.Errorf("clickhouse: flush_interval must be > 0, got %s", c.FlushInterval)
	}
	if c.SessionSnapshotIntervalSec <= 0 && c.Sinks.SessionSnapshots {
		return fmt.Errorf("clickhouse: session_snapshot_interval_sec must be > 0 when session_snapshots sink is enabled")
	}
	if c.DialTimeout <= 0 {
		return fmt.Errorf("clickhouse: dial_timeout must be > 0")
	}
	if c.ReadTimeout <= 0 {
		return fmt.Errorf("clickhouse: read_timeout must be > 0")
	}

	if c.TLS.Enabled && c.TLS.CAFile == "" && !c.TLS.InsecureSkipVerify {
		return fmt.Errorf("clickhouse: tls.ca_file is required when tls.enabled and not insecure_skip_verify")
	}

	return nil
}
