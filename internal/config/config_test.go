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
