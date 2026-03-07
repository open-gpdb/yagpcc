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
	ArchiverConfig             ArchiverConfigType `json:"arch_config" yaml:"arch_config"`
	ClusterID                  string             `config:"cluster_id" yaml:"cluster_id"`
	ConnectorEnabled           bool               `config:"connector_enabled" yaml:"connector_enabled"`
	MaxMessageSize             int64              `config:"max_message_size" yaml:"max_message_size"`
	MaxOuterMessageSize        int64              `config:"max_outer_message_size" yaml:"max_outer_message_size"`
	MaximumStoredQueries       uint32             `config:"maximum_stored_queries" yaml:"maximum_stored_queries"`
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
		Lockfile:                   "/var/run/yagpcc/yagpcc.lock",
		Role:                       "segment",
		ClearDeletedSessions:       true,
		MasterConnection:           masterConnection,
		MasterConnectionTries:      3,
		MasterConnectionFirstTries: 86400,
		IgnoreDatabaseError:        false,
		MinimumQueryDurationSec:    10 * 60,
		MaxShortQueriesPerUser:     500,
		ShortAggInterval:           time.Duration(time.Minute * 10),
		SessionRefreshInterval:     time.Duration(time.Second * 30),
		QueriesRefreshInterval:     time.Duration(time.Second * 1),
		SessionSendMetricInterval:  time.Duration(time.Second * 60),
		MinFreePercent:             10,
		CustomSegmentList:          nil,
		SegmentPullRateSec:         2,
		SegmentPullThreads:         15,
		SegmentConnectTimeoutSec:   5.0,
		SegmentGetTimeoutSec:       10.0,
		ConfigCacheDurabilitySec:   60,
		StatActivityDurabilitySec:  1,
		ArchiverConfig:             DefaultArchiverConfig(),
		ClusterID:                  "unknown",
		ConnectorEnabled:           false,
		MaxMessageSize:             12 * 1024 * 1024,
		MaxOuterMessageSize:        4 * 1024 * 1024,
		MaximumStoredQueries:       50 * 1000,
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
	if err = loader.Load(context.Background(), &config); err != nil {
		err = fmt.Errorf("failed to load config from %s: %s", configFile, err.Error())
		return nil, err
	}
	err = config.Validate()
	if err != nil {
		return nil, err
	}
	return config, nil
}

func (cfg *Config) Validate() error {
	return nil
}
