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

// Package clickhouse implements a sink that streams query/session telemetry
// from yagpcc master into a ClickHouse cluster. This file holds the connection
// wrapper around clickhouse-go/v2 (NewClient, Ping, TLS config).
package clickhouse

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/open-gpdb/yagpcc/internal/config"
)

// asyncInsertBusyTimeoutMs and asyncInsertMaxDataSize tune ClickHouse-side
// async inserts: the server flushes its own buffer either when this many bytes
// of data have accumulated or when the timeout expires.
const (
	asyncInsertBusyTimeoutMs = 10000
	asyncInsertMaxDataSize   = 1048576
)

// NewClient opens a clickhouse-go/v2 connection using cfg. The returned
// driver.Conn is ready for Exec/Select but is not yet pinged; callers that
// need a health check should call Ping after construction.
//
// Settings applied:
//   - async_insert=1 / wait_for_async_insert=0 when cfg.AsyncInsert is true
//     (otherwise omitted so server defaults apply);
//   - LZ4 compression on the wire;
//   - DialTimeout / ReadTimeout copied from cfg.
//
// TLS is configured from cfg.TLS via buildTLSConfig.
func NewClient(_ context.Context, cfg *config.ClickhouseConfig) (driver.Conn, error) {
	if cfg == nil {
		return nil, errors.New("clickhouse: cfg is nil")
	}
	if len(cfg.Addrs) == 0 {
		return nil, errors.New("clickhouse: addrs is empty")
	}

	tlsCfg, err := buildTLSConfig(cfg.TLS)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: build tls config: %w", err)
	}

	settings := clickhouse.Settings{}
	if cfg.AsyncInsert {
		settings["async_insert"] = 1
		settings["wait_for_async_insert"] = 0
		settings["async_insert_busy_timeout_ms"] = asyncInsertBusyTimeoutMs
		settings["async_insert_max_data_size"] = asyncInsertMaxDataSize
	}

	opts := &clickhouse.Options{
		Addr: cfg.Addrs,
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.User,
			Password: cfg.Password,
		},
		Settings:    settings,
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
		DialTimeout: cfg.DialTimeout,
		ReadTimeout: cfg.ReadTimeout,
		TLS:         tlsCfg,
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: open: %w", err)
	}
	return conn, nil
}

// Ping verifies that the ClickHouse server is reachable. It is a thin wrapper
// over driver.Conn.Ping so callers can use it without importing the driver
// package.
func Ping(ctx context.Context, conn driver.Conn) error {
	if conn == nil {
		return errors.New("clickhouse: conn is nil")
	}
	if err := conn.Ping(ctx); err != nil {
		return fmt.Errorf("clickhouse: ping: %w", err)
	}
	return nil
}

// buildTLSConfig converts ClickhouseTLSConfig into a *tls.Config. Returns nil
// (and no error) when TLS is not enabled, signalling "plain TCP" to the
// driver. When InsecureSkipVerify is set, CAFile is optional.
func buildTLSConfig(cfg config.ClickhouseTLSConfig) (*tls.Config, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	out := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: cfg.InsecureSkipVerify,
	}
	if cfg.CAFile == "" {
		if cfg.InsecureSkipVerify {
			return out, nil
		}
		return nil, errors.New("ca_file is required when tls.enabled and not insecure_skip_verify")
	}
	pem, err := os.ReadFile(cfg.CAFile)
	if err != nil {
		return nil, fmt.Errorf("read ca_file %q: %w", cfg.CAFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("ca_file %q contains no PEM certificates", cfg.CAFile)
	}
	out.RootCAs = pool
	return out, nil
}
