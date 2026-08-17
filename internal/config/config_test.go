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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func defaultValidConfig() *Config {
	cfg, _ := DefaultConfig()
	return cfg
}

func TestValidate_ValidConfig(t *testing.T) {
	cfg := defaultValidConfig()
	require.NoError(t, cfg.Validate())
}

func TestValidate_ZeroSessionRefreshInterval(t *testing.T) {
	cfg := defaultValidConfig()
	cfg.SessionRefreshInterval = 0
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "session_refresh_interval")
}

func TestValidate_NegativeSessionRefreshInterval(t *testing.T) {
	cfg := defaultValidConfig()
	cfg.SessionRefreshInterval = -time.Second
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "session_refresh_interval")
}

func TestValidate_ZeroQueriesRefreshInterval(t *testing.T) {
	cfg := defaultValidConfig()
	cfg.QueriesRefreshInterval = 0
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "queries_refresh_interval")
}

func TestValidate_ZeroProcfsRefreshInterval(t *testing.T) {
	cfg := defaultValidConfig()
	cfg.ProcfsRefreshInterval = 0
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "procfs_refresh_interval")
}

func TestValidate_NegativeProcfsRefreshInterval(t *testing.T) {
	cfg := defaultValidConfig()
	cfg.ProcfsRefreshInterval = -time.Second
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "procfs_refresh_interval")
}

func TestValidate_WriterConfigDefaults(t *testing.T) {
	cfg := defaultValidConfig()
	require.NotNil(t, cfg.Writers)
	assert.Equal(t, time.Second, cfg.Writers.BatchInterval)
	assert.Equal(t, time.Second, cfg.Writers.WriteTimeout)
	assert.Equal(t, 60, cfg.Writers.BatchQueueSize)
	require.NoError(t, cfg.Validate())
}

func TestValidate_WriterConfigWithValues(t *testing.T) {
	cfg := defaultValidConfig()
	cfg.Writers = &WriterConfig{
		BatchProcessorConfig: BatchProcessorConfig{
			BatchInterval:  time.Second,
			WriteTimeout:   time.Second,
			BatchQueueSize: 60,
		},
		Targets: []WriterTarget{
			{
				Type:         "file",
				Enabled:      true,
				SessionsFile: "sessions.json",
				QueriesFile:  "queries.json",
				SegmentsFile: "segments.json",
				MaxFileSize:  419430400,
			},
		},
	}
	require.NoError(t, cfg.Validate())
}

func TestValidate_ClickhouseTarget_Enabled(t *testing.T) {
	cfg := defaultValidConfig()
	cfg.Writers.Targets = append(cfg.Writers.Targets, WriterTarget{
		Type:    "clickhouse",
		Enabled: true,
		Addrs:   []string{"ch-1:9000"},
	})
	require.NoError(t, cfg.Validate())
}

func TestValidate_ClickhouseTarget_MissingAddrs(t *testing.T) {
	cfg := defaultValidConfig()
	cfg.Writers.Targets = append(cfg.Writers.Targets, WriterTarget{
		Type:    "clickhouse",
		Enabled: true,
	})
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "clickhouse target requires addrs")
}

func TestValidate_ClickhouseTarget_DisabledNoAddrsOK(t *testing.T) {
	cfg := defaultValidConfig()
	cfg.Writers.Targets = append(cfg.Writers.Targets, WriterTarget{
		Type:    "clickhouse",
		Enabled: false,
	})
	require.NoError(t, cfg.Validate())
}

func TestValidate_ClickhouseTarget_UnsupportedDatabase(t *testing.T) {
	cfg := defaultValidConfig()
	cfg.Writers.Targets = append(cfg.Writers.Targets, WriterTarget{
		Type:     "clickhouse",
		Enabled:  true,
		Addrs:    []string{"ch-1:9000"},
		Database: "otherdb",
	})
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "database must be")
}

func TestValidate_UnknownTargetType(t *testing.T) {
	cfg := defaultValidConfig()
	cfg.Writers.Targets = append(cfg.Writers.Targets, WriterTarget{
		Type:    "kafka",
		Enabled: true,
	})
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown target type")
}

func TestClickhouseConfig_FromTarget(t *testing.T) {
	target := WriterTarget{
		Type:     "clickhouse",
		Enabled:  true,
		Addrs:    []string{"ch-1:9000", "ch-2:9000"},
		Database: "customdb",
		User:     "writer",
		Password: "secret",
	}
	chCfg := target.ClickhouseConfig()
	assert.True(t, chCfg.Enabled)
	assert.Equal(t, []string{"ch-1:9000", "ch-2:9000"}, chCfg.Addrs)
	assert.Equal(t, "customdb", chCfg.Database)
	assert.Equal(t, "writer", chCfg.User)
	assert.Equal(t, "secret", chCfg.Password)
	assert.False(t, chCfg.AsyncInsert)
}

func TestClickhouseConfig_FromTarget_Defaults(t *testing.T) {
	target := WriterTarget{Type: "clickhouse", Enabled: true, Addrs: []string{"ch:9000"}}
	chCfg := target.ClickhouseConfig()
	// Falls back to the ClickHouse defaults when the target omits them.
	assert.Equal(t, DefaultClickhouseConfig().Database, chCfg.Database)
	assert.Equal(t, DefaultClickhouseConfig().User, chCfg.User)
}

func TestClickhouseConfig_FromTarget_PasswordEnvOverride(t *testing.T) {
	t.Setenv(ClickhousePasswordEnv, "from-env")
	target := WriterTarget{Type: "clickhouse", Enabled: true, Addrs: []string{"ch:9000"}}
	chCfg := target.ClickhouseConfig()
	assert.Equal(t, "from-env", chCfg.Password)
}

func TestValidate_MultipleEnabledTargets(t *testing.T) {
	cfg := defaultValidConfig()
	cfg.Writers.Targets = append(cfg.Writers.Targets, WriterTarget{
		Type:    "clickhouse",
		Enabled: true,
		Addrs:   []string{"ch:9000"},
	})
	// file (targets[0]) + clickhouse both enabled is a valid configuration.
	require.NoError(t, cfg.Validate())
}

func TestValidate_ProcfsDisabled_ZeroIntervalOK(t *testing.T) {
	cfg := defaultValidConfig()
	cfg.ProcfsEnabled = false
	cfg.ProcfsRefreshInterval = 0
	require.NoError(t, cfg.Validate())
}

func TestValidate_ProcfsEnabledByDefault(t *testing.T) {
	cfg := defaultValidConfig()
	assert.True(t, cfg.ProcfsEnabled)
}

func TestValidate_ExtendedProcfsStatDisabledByDefault(t *testing.T) {
	cfg := defaultValidConfig()
	assert.False(t, cfg.ExtendedProcfsStat)
}

func TestReadFromFile_ExtendedProcfsStat(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "yagpcc.yaml")
	content := []byte(`extended_procfs_stat: true
`)
	require.NoError(t, os.WriteFile(configPath, content, 0o600))

	cfg, err := ReadFromFile(configPath)
	require.NoError(t, err)
	assert.True(t, cfg.ExtendedProcfsStat)
}

func TestReadFromFile_LegacyArchiverFilePathsPopulateDefaultWriter(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "yagpcc.yaml")
	content := []byte(`arch_config:
  sessions_file: /var/lib/greenplum/yandex/yagpcc/sessions.json
  queries_file: /var/lib/greenplum/yandex/yagpcc/queries.json
  segments_file: /var/lib/greenplum/yandex/yagpcc/segments.json
`)
	require.NoError(t, os.WriteFile(configPath, content, 0o600))

	cfg, err := ReadFromFile(configPath)
	require.NoError(t, err)
	require.NotNil(t, cfg.Writers)
	require.NotEmpty(t, cfg.Writers.Targets)
	fileTarget := cfg.Writers.Targets[0]
	assert.Equal(t, "/var/lib/greenplum/yandex/yagpcc/sessions.json", fileTarget.SessionsFile)
	assert.Equal(t, "/var/lib/greenplum/yandex/yagpcc/queries.json", fileTarget.QueriesFile)
	assert.Equal(t, "/var/lib/greenplum/yandex/yagpcc/segments.json", fileTarget.SegmentsFile)
}

func TestValidate_ZeroSessionSendMetricInterval(t *testing.T) {
	cfg := defaultValidConfig()
	cfg.SessionSendMetricInterval = 0
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "session_send_metric_interval")
}
