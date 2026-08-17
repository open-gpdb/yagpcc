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
	"context"
	"fmt"
	"time"

	"github.com/heetch/confita"
	"github.com/heetch/confita/backend/file"
)

type SegmentDescription struct {
	DBID     int    `config:"dbid" yaml:"dbid"`
	Content  int    `config:"content" yaml:"content"`
	Hostname string `config:"hostname" yaml:"hostname"`
	PortN    int    `config:"portn" yaml:"portn"`
}

type SegmentList []SegmentDescription

// ArchiverConfigType holds the legacy archiver configuration.
// Kept for backward compatibility.
type ArchiverConfigType struct {
	ArciverProcesses   uint32 `config:"archiver_processes" yaml:"archiver_processes"`
	ArchiverQueueSize  uint32 `config:"archiver_queue_size" yaml:"archiver_queue_size"`
	SessionsFile       string `config:"sessions_file" yaml:"sessions_file"`
	SessionsQueueSize  uint32 `config:"sessions_queue_size" yaml:"sessions_queue_size"`
	SegmentsFile       string `config:"segments_file" yaml:"segments_file"`
	SegmentsQueueSize  uint32 `config:"segments_queue_size" yaml:"segments_queue_size"`
	QueriesFile        string `config:"queries_file" yaml:"queries_file"`
	QueriesQueueSize   uint32 `config:"queries_queue_size" yaml:"queries_queue_size"`
	PlanDetailFile     string `config:"plan_detail_file" yaml:"plan_detail_file"`
	PlanDetaiQueueSize uint32 `config:"plan_detail_queue_size" yaml:"plan_detail_queue_size"`
	MaxFileSize        int64  `config:"max_file_size" yaml:"max_file_size"`
}

// BatchProcessorConfig holds configuration for archive writer batching.
type BatchProcessorConfig struct {
	// BatchInterval is the interval at which batches are collected.
	BatchInterval time.Duration `config:"batch_interval" yaml:"batch_interval"`

	// WriteTimeout is the maximum time allowed to write a batch.
	WriteTimeout time.Duration `config:"write_timeout" yaml:"write_timeout"`

	// BatchQueueSize is the capacity of the pipe channel (number of batches).
	BatchQueueSize int `config:"batch_queue_size" yaml:"batch_queue_size"`
}

// WriterConfig holds configuration for the writer pipeline.
type WriterConfig struct {
	BatchProcessorConfig

	// Targets is the list of writer targets.
	Targets []WriterTarget `config:"targets" yaml:"targets"`
}

// WriterTarget describes a single writer target.
type WriterTarget struct {
	// Type is the writer type (e.g., "file", "clickhouse", "greenplum").
	Type string `config:"type" yaml:"type"`

	// Enabled indicates whether this target is enabled.
	Enabled bool `config:"enabled" yaml:"enabled"`

	// File-based writer settings
	SessionsFile string `config:"sessions_file" yaml:"sessions_file"`
	QueriesFile  string `config:"queries_file" yaml:"queries_file"`
	SegmentsFile string `config:"segments_file" yaml:"segments_file"`
	MaxFileSize  int64  `config:"max_file_size" yaml:"max_file_size"`

	// ClickHouse-based writer settings (Type == "clickhouse"). Password may be
	// left empty here and supplied through the YAGPCC_CH_PASSWORD env var.
	Addrs    []string            `config:"addrs" yaml:"addrs"`
	Database string              `config:"database" yaml:"database"`
	User     string              `config:"user" yaml:"user"`
	Password string              `config:"password" yaml:"password"`
	TLS      ClickhouseTLSConfig `config:"tls" yaml:"tls"`
}

// ClickhouseConfig builds a ClickhouseConfig for a Type=="clickhouse" target,
// filling connection defaults and applying the password env override. Direct
// batch inserts run synchronously, so async_insert is disabled.
func (t *WriterTarget) ClickhouseConfig() ClickhouseConfig {
	cfg := DefaultClickhouseConfig()
	cfg.Enabled = true
	cfg.Addrs = t.Addrs
	if t.Database != "" {
		cfg.Database = t.Database
	}
	if t.User != "" {
		cfg.User = t.User
	}
	if t.Password != "" {
		cfg.Password = t.Password
	}
	cfg.TLS = t.TLS
	cfg.AsyncInsert = false
	cfg.ApplyEnvOverrides()
	return cfg
}

// Config contains all yagpcc configuration
type Config struct {
	App                        BaseConfig         `json:"app" yaml:"app"`
	SocketFile                 string             `config:"socket_file" yaml:"socket_file"`
	UDSFile                    string             `config:"uds_file" yaml:"uds_file"`
	UDSBuffer                  uint32             `config:"uds_buffer" yaml:"uds_buffer"`
	ListenPort                 uint32             `config:"listen_port" yaml:"listen_port"`
	PingPort                   uint32             `config:"ping_port" yaml:"ping_port"`
	CSVPort                    uint32             `config:"csv_port" yaml:"csv_port"`
	UIPort                     uint32             `config:"ui_port" yaml:"ui_port"`
	DebugPort                  uint32             `config:"debug_port" yaml:"debug_port"`
	DebugMinutes               int                `config:"debug_minutes" yaml:"debug_minutes"`
	Lockfile                   string             `config:"lockfile"`
	Role                       string             `config:"role" yaml:"role"`
	ClearDeletedSessions       bool               `config:"clear_deleted_sessions" yaml:"clear_deleted_sessions"`
	MasterConnection           PGConfig           `config:"master_connection" yaml:"master_connection"`
	MasterConnectionTries      uint32             `config:"master_connection_tries" yaml:"master_connection_tries"`
	MasterConnectionFirstTries uint32             `config:"master_connection_first_tries" yaml:"master_connection_first_tries"`
	IgnoreDatabaseError        bool               `config:"ignore_database_error" yaml:"ignore_database_error"`
	MinimumQueryDurationSec    uint32             `config:"minimum_query_duration_sec" yaml:"minimum_query_duration_sec"`
	MaxShortQueriesPerUser     uint32             `config:"max_short_queris_per_user" yaml:"max_short_queris_per_user"`
	ShortAggInterval           time.Duration      `config:"short_agg_interval" yaml:"short_agg_interval"`
	SessionRefreshInterval     time.Duration      `config:"session_refresh_interval" yaml:"session_refresh_interval"`
	QueriesRefreshInterval     time.Duration      `config:"session_refresh_interval" yaml:"queries_refresh_interval"`
	ProcfsEnabled              bool               `config:"procfs_enabled" yaml:"procfs_enabled"`
	ProcfsRefreshInterval      time.Duration      `config:"procfs_refresh_interval" yaml:"procfs_refresh_interval"`
	ExtendedProcfsStat         bool               `config:"extended_procfs_stat" yaml:"extended_procfs_stat"`
	SessionSendMetricInterval  time.Duration      `config:"session_send_metric_interval" yaml:"session_send_metric_interval"`
	MinFreePercent             uint32             `config:"min_free_percent" yaml:"min_free_percent"`
	CustomSegmentList          *SegmentList       `config:"custom_segment_list" yaml:"custom_segment_list"`
	SegmentPullRateSec         float64            `config:"segment_pull_rate_sec" yaml:"segment_pull_rate_sec"`
	SegmentPullThreads         uint32             `config:"segment_pull_threads" yaml:"segment_pull_threads"`
	SegmentConnectTimeoutSec   float64            `config:"segment_connect_timeout_sec" yaml:"segment_connect_timeout_sec"`
	SegmentGetTimeoutSec       float64            `config:"segment_get_timeout_sec" yaml:"segment_get_timeout_sec"`
	SegmentLogWorkAmount       bool               `config:"segment_log_work_amount" yaml:"segment_log_work_amount"`
	ConfigCacheDurabilitySec   float64            `config:"config_cache_durability_sec" yaml:"config_cache_durability_sec"`
	StatActivityDurabilitySec  float64            `config:"stat_activity_durability_sec" yaml:"stat_activity_durability_sec"`
	ExtensionsCacheTTL         float64            `config:"extensions_cache_ttl_sec" yaml:"extensions_cache_ttl_sec"`
	ArchiverConfig             ArchiverConfigType `json:"arch_config" yaml:"arch_config"`
	Writers                    *WriterConfig      `json:"writers" yaml:"writers"`
	ClusterID                  string             `config:"cluster_id" yaml:"cluster_id"`
	ConnectorEnabled           bool               `config:"connector_enabled" yaml:"connector_enabled"`
	MaxMessageSize             int64              `config:"max_message_size" yaml:"max_message_size"`
	MaxOuterMessageSize        int64              `config:"max_outer_message_size" yaml:"max_outer_message_size"`
	MaximumStoredQueries       uint32             `config:"maximum_stored_queries" yaml:"maximum_stored_queries"`
	Clickhouse                 ClickhouseConfig   `json:"clickhouse" yaml:"clickhouse"`
}

var _ AppConfig = &Config{}

func DefaultArchiverConfig() ArchiverConfigType {
	return ArchiverConfigType{
		ArciverProcesses:   4,
		ArchiverQueueSize:  1000,
		SessionsFile:       "sessions.json",
		SessionsQueueSize:  1000,
		SegmentsFile:       "segments.json",
		SegmentsQueueSize:  4000,
		QueriesFile:        "queries.json",
		QueriesQueueSize:   1000,
		PlanDetailFile:     "plan_details.json",
		PlanDetaiQueueSize: 4000,
		MaxFileSize:        400 * 1024 * 1024,
	}
}

func DefaultBatchProcessorConfig() BatchProcessorConfig {
	return BatchProcessorConfig{
		BatchInterval:  time.Second,
		WriteTimeout:   time.Second,
		BatchQueueSize: 60,
	}
}

func DefaultWriterConfig() *WriterConfig {
	return &WriterConfig{
		BatchProcessorConfig: DefaultBatchProcessorConfig(),
		Targets: []WriterTarget{
			{
				Type:    "file",
				Enabled: true,
			},
		},
	}
}

func (cfg *Config) normalizeWriters() {
	if cfg.Writers == nil {
		cfg.Writers = DefaultWriterConfig()
	}
	if len(cfg.Writers.Targets) == 0 {
		cfg.Writers.Targets = []WriterTarget{{Type: "file", Enabled: true}}
	}

	fileTargetIndex := -1
	for i := range cfg.Writers.Targets {
		target := &cfg.Writers.Targets[i]
		if target.Type == "" {
			target.Type = "file"
		}
		if target.Type != "file" {
			continue
		}
		if !target.Enabled {
			target.Enabled = true
		}
		if target.SessionsFile == "" {
			target.SessionsFile = cfg.ArchiverConfig.SessionsFile
		}
		if target.QueriesFile == "" {
			target.QueriesFile = cfg.ArchiverConfig.QueriesFile
		}
		if target.SegmentsFile == "" {
			target.SegmentsFile = cfg.ArchiverConfig.SegmentsFile
		}
		if target.MaxFileSize == 0 {
			target.MaxFileSize = cfg.ArchiverConfig.MaxFileSize
		}
		if fileTargetIndex == -1 {
			fileTargetIndex = i
		}
	}

	if fileTargetIndex == -1 {
		cfg.Writers.Targets = append([]WriterTarget{{
			Type:         "file",
			Enabled:      true,
			SessionsFile: cfg.ArchiverConfig.SessionsFile,
			QueriesFile:  cfg.ArchiverConfig.QueriesFile,
			SegmentsFile: cfg.ArchiverConfig.SegmentsFile,
			MaxFileSize:  cfg.ArchiverConfig.MaxFileSize,
		}}, cfg.Writers.Targets...)
		return
	}

	cfg.Writers.Targets[0], cfg.Writers.Targets[fileTargetIndex] = cfg.Writers.Targets[fileTargetIndex], cfg.Writers.Targets[0]
}

// DefaultConfig returns default configuration for Agent
func DefaultConfig() (*Config, error) {
	masterConnection := PGConfig{
		Addrs: []string{"localhost:5432"},
		DB:    "postgres",
		User:  "gpadmin",
	}
	archiverConfig := DefaultArchiverConfig()
	config := Config{
		App:                        DefaultBaseConfig(),
		SocketFile:                 "/tmp/yagpcc_agent.sock",
		UDSFile:                    "/tmp/yagpcc_agent_uds.sock",
		UDSBuffer:                  4 * 1024,
		ListenPort:                 1432,
		PingPort:                   1435,
		CSVPort:                    1440,
		UIPort:                     0, // disabled by default; set to 1441 to enable web UI
		Lockfile:                   "/var/run/yagpcc/yagpcc.lock",
		Role:                       "segment",
		ClearDeletedSessions:       true,
		MasterConnection:           masterConnection,
		MasterConnectionTries:      3,
		MasterConnectionFirstTries: 86400,
		IgnoreDatabaseError:        false,
		MinimumQueryDurationSec:    10 * 60,
		MaxShortQueriesPerUser:     2000,
		ShortAggInterval:           time.Minute * 10,
		SessionRefreshInterval:     time.Second * 30,
		QueriesRefreshInterval:     time.Second * 1,
		ProcfsEnabled:              true,
		ProcfsRefreshInterval:      time.Second * 60,
		ExtendedProcfsStat:         false,
		SessionSendMetricInterval:  time.Second * 60,
		MinFreePercent:             10,
		CustomSegmentList:          nil,
		SegmentPullRateSec:         2,
		SegmentPullThreads:         15,
		SegmentConnectTimeoutSec:   5.0,
		SegmentGetTimeoutSec:       10.0,
		ConfigCacheDurabilitySec:   60,
		StatActivityDurabilitySec:  1,
		ExtensionsCacheTTL:         900, // 15 minutes
		ArchiverConfig:             archiverConfig,
		Writers:                    DefaultWriterConfig(),
		ClusterID:                  "unknown",
		ConnectorEnabled:           false,
		MaxMessageSize:             12 * 1024 * 1024,
		MaxOuterMessageSize:        4 * 1024 * 1024,
		MaximumStoredQueries:       50 * 1000,
		Clickhouse:                 DefaultClickhouseConfig(),
	}
	config.normalizeWriters()
	return &config, nil
}

func (cfg *Config) AppConfig() *BaseConfig {
	return &cfg.App
}

// ReadFromFile reads config from file, performing all necessary checks
func ReadFromFile(configFile string) (*Config, error) {
	config, err := DefaultConfig()
	if err != nil {
		return nil, err
	}
	// DefaultConfig returns a fully normalized config for in-memory callers.
	// For file loading, keep writer targets unexpanded until after YAML values
	// are loaded so legacy arch_config file paths can populate the default file
	// writer target instead of being shadowed by sessions.json/queries.json/etc.
	config.Writers = DefaultWriterConfig()
	loader := confita.NewLoader(file.NewBackend(configFile))
	if err = loader.Load(context.Background(), config); err != nil {
		err = fmt.Errorf("failed to load config from %s: %s", configFile, err.Error())
		return nil, err
	}
	config.normalizeWriters()
	// Apply the password env override before validating so a deployment that
	// keeps the ClickHouse secret out of the YAML (YAGPCC_CH_PASSWORD) does not
	// trip the "password is required when enabled" check at startup.
	config.Clickhouse.ApplyEnvOverrides()
	err = config.Validate()
	if err != nil {
		return nil, err
	}
	return config, nil
}

func (cfg *Config) Validate() error {
	if cfg.SessionRefreshInterval <= 0 {
		return fmt.Errorf("session_refresh_interval must be > 0, got %v", cfg.SessionRefreshInterval)
	}
	if cfg.QueriesRefreshInterval <= 0 {
		return fmt.Errorf("queries_refresh_interval must be > 0, got %v", cfg.QueriesRefreshInterval)
	}
	if cfg.ProcfsEnabled && cfg.ProcfsRefreshInterval <= 0 {
		return fmt.Errorf("procfs_refresh_interval must be > 0, got %v", cfg.ProcfsRefreshInterval)
	}
	if cfg.SessionSendMetricInterval <= 0 {
		return fmt.Errorf("session_send_metric_interval must be > 0, got %v", cfg.SessionSendMetricInterval)
	}
	if cfg.Writers == nil {
		return fmt.Errorf("writers config must be initialized")
	}
	if cfg.Writers.BatchInterval <= 0 {
		return fmt.Errorf("writers.batch_interval must be > 0, got %v", cfg.Writers.BatchInterval)
	}
	if cfg.Writers.WriteTimeout <= 0 {
		return fmt.Errorf("writers.write_timeout must be > 0, got %v", cfg.Writers.WriteTimeout)
	}
	if cfg.Writers.BatchQueueSize <= 0 {
		return fmt.Errorf("writers.batch_queue_size must be > 0, got %v", cfg.Writers.BatchQueueSize)
	}
	if len(cfg.Writers.Targets) == 0 {
		return fmt.Errorf("writers.targets must contain at least one target")
	}
	fileTarget := cfg.Writers.Targets[0]
	if fileTarget.Type != "file" || !fileTarget.Enabled {
		return fmt.Errorf("writers.targets must start with an enabled file target")
	}
	if fileTarget.SessionsFile == "" {
		return fmt.Errorf("writers.targets[0].sessions_file must not be empty")
	}
	if fileTarget.QueriesFile == "" {
		return fmt.Errorf("writers.targets[0].queries_file must not be empty")
	}
	if fileTarget.SegmentsFile == "" {
		return fmt.Errorf("writers.targets[0].segments_file must not be empty")
	}
	if fileTarget.MaxFileSize <= 0 {
		return fmt.Errorf("writers.targets[0].max_file_size must be > 0, got %v", fileTarget.MaxFileSize)
	}
	for i := range cfg.Writers.Targets {
		target := &cfg.Writers.Targets[i]
		switch target.Type {
		case "file":
		case "clickhouse":
			if target.Enabled && len(target.Addrs) == 0 {
				return fmt.Errorf("writers.targets[%d]: clickhouse target requires addrs when enabled", i)
			}
			if target.Database != "" && target.Database != SupportedClickhouseDatabase {
				return fmt.Errorf("writers.targets[%d]: clickhouse target database must be %q, got %q (the embedded schema only creates tables in that database)", i, SupportedClickhouseDatabase, target.Database)
			}
		default:
			return fmt.Errorf("writers.targets[%d]: unknown target type %q", i, target.Type)
		}
	}
	if cfg.Role == "master" {
		if err := cfg.Clickhouse.Validate(); err != nil {
			return err
		}
	}
	return nil
}
