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

// DefaultConfig returns default configuration for Agent
func DefaultConfig() (*Config, error) {
	masterConnection := PGConfig{
		Addrs: []string{"localhost:5432"},
		DB:    "postgres",
		User:  "gpadmin",
	}
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
		ArchiverConfig:             DefaultArchiverConfig(),
		ClusterID:                  "unknown",
		ConnectorEnabled:           false,
		MaxMessageSize:             12 * 1024 * 1024,
		MaxOuterMessageSize:        4 * 1024 * 1024,
		MaximumStoredQueries:       50 * 1000,
		Clickhouse:                 DefaultClickhouseConfig(),
	}
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
	loader := confita.NewLoader(file.NewBackend(configFile))
	if err = loader.Load(context.Background(), config); err != nil {
		err = fmt.Errorf("failed to load config from %s: %s", configFile, err.Error())
		return nil, err
	}
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
	if cfg.Role == "master" {
		if err := cfg.Clickhouse.Validate(); err != nil {
			return err
		}
	}
	return nil
}
