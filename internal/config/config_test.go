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

func TestValidate_ZeroSessionSendMetricInterval(t *testing.T) {
	cfg := defaultValidConfig()
	cfg.SessionSendMetricInterval = 0
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "session_send_metric_interval")
}
