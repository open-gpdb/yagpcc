package config

import (
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type PrometheusConfig struct {
	URL       string `json:"url" yaml:"url"`
	Namespace string `json:"namespace" yaml:"namespace"`
}

// Config is basic application config

type InstrumentationConfig struct {
	Addr            string        `yaml:"addr,omitempty"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout,omitempty"`
}

func DefaultInstrumentationConfig() InstrumentationConfig {
	return InstrumentationConfig{
		Addr:            ":http",
		ShutdownTimeout: 10 * time.Second,
	}
}

type BaseConfig struct {
	Logging         LoggingConfig         `json:"logging" yaml:"logging"`
	Instrumentation InstrumentationConfig `json:"instrumentation,omitempty" yaml:"instrumentation,omitempty"`
	Prometheus      PrometheusConfig      `json:"prometheus,omitempty" yaml:"prometheus,omitempty"`
	AppName         string                `json:"app_name,omitempty" yaml:"app_name,omitempty"`
}

var _ AppConfig = &BaseConfig{}

func (c *BaseConfig) AppConfig() *BaseConfig {
	return c
}

type ConfigOption = func(config *BaseConfig)

// DefaultConfig returns default base config
func DefaultBaseConfig(opts ...ConfigOption) BaseConfig {
	cfg := BaseConfig{
		Logging:         DefaultLoggingConfig(),
		Instrumentation: DefaultInstrumentationConfig(),
	}
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

// WithAppName assigns a .AppName and .Tracing.ServiceName to the config
func WithAppName(appName string) ConfigOption {
	return func(config *BaseConfig) {
		config.AppName = appName
	}
}

type LoggingConfig struct {
	Level zapcore.Level `json:"level" yaml:"level"`
	File  string        `json:"file" yaml:"file"`
}

func DefaultLoggingConfig() LoggingConfig {
	return LoggingConfig{
		Level: zap.DebugLevel,
	}
}

// AppConfig defines interface for a basic application config
type AppConfig interface {
	// AppConfig returns basic application config which must be of reference type - application might
	// change its values during setup
	AppConfig() *BaseConfig
}
