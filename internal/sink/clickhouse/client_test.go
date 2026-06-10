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

package clickhouse

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/open-gpdb/yagpcc/internal/config"
)

func TestBuildTLSConfig_Disabled(t *testing.T) {
	got, err := buildTLSConfig(config.ClickhouseTLSConfig{Enabled: false, CAFile: "/does/not/matter"})
	if err != nil {
		t.Fatalf("disabled TLS should not error: %v", err)
	}
	if got != nil {
		t.Fatalf("disabled TLS should return nil *tls.Config, got %#v", got)
	}
}

func TestBuildTLSConfig_InsecureSkipVerifyNoCA(t *testing.T) {
	got, err := buildTLSConfig(config.ClickhouseTLSConfig{
		Enabled:            true,
		InsecureSkipVerify: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil *tls.Config")
	}
	if !got.InsecureSkipVerify {
		t.Error("InsecureSkipVerify should be true")
	}
	if got.RootCAs != nil {
		t.Error("RootCAs should be nil when no CA file is given")
	}
}

func TestBuildTLSConfig_MissingCAFile(t *testing.T) {
	_, err := buildTLSConfig(config.ClickhouseTLSConfig{
		Enabled: true,
	})
	if err == nil {
		t.Fatal("expected error when ca_file is empty and not insecure")
	}
}

func TestBuildTLSConfig_CAFileNotFound(t *testing.T) {
	_, err := buildTLSConfig(config.ClickhouseTLSConfig{
		Enabled: true,
		CAFile:  filepath.Join(t.TempDir(), "missing.pem"),
	})
	if err == nil {
		t.Fatal("expected error for missing ca_file")
	}
}

func TestBuildTLSConfig_CAFileInvalidPEM(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.pem")
	if err := os.WriteFile(bad, []byte("not a cert"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	_, err := buildTLSConfig(config.ClickhouseTLSConfig{Enabled: true, CAFile: bad})
	if err == nil {
		t.Fatal("expected error for ca_file without PEM data")
	}
}

func TestBuildTLSConfig_LoadsCAFile(t *testing.T) {
	caPath := writeSelfSignedCA(t)
	got, err := buildTLSConfig(config.ClickhouseTLSConfig{Enabled: true, CAFile: caPath})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil *tls.Config")
	}
	if got.RootCAs == nil {
		t.Fatal("RootCAs should be populated from CA file")
	}
	if got.MinVersion != 0x0303 { // tls.VersionTLS12
		t.Errorf("MinVersion = %#x, want TLS 1.2 (0x0303)", got.MinVersion)
	}
}

func TestNewClient_NilConfig(t *testing.T) {
	if _, err := NewClient(context.Background(), nil); err == nil {
		t.Fatal("expected error for nil cfg")
	}
}

func TestNewClient_EmptyAddrs(t *testing.T) {
	cfg := config.DefaultClickhouseConfig()
	if _, err := NewClient(context.Background(), &cfg); err == nil {
		t.Fatal("expected error for empty addrs")
	}
}

func TestNewClient_AsyncInsertSettings(t *testing.T) {
	// Use a non-routable address so Open does not actually dial; clickhouse-go
	// only validates options synchronously and connects lazily.
	cfg := config.DefaultClickhouseConfig()
	cfg.Addrs = []string{"127.0.0.1:1"}
	cfg.Password = "test"

	conn, err := NewClient(context.Background(), &cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if conn == nil {
		t.Fatal("expected non-nil conn")
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestNewClient_TLSValidationError(t *testing.T) {
	cfg := config.DefaultClickhouseConfig()
	cfg.Addrs = []string{"127.0.0.1:1"}
	cfg.Password = "test"
	cfg.TLS.Enabled = true
	// Missing CAFile and not insecure-skip → buildTLSConfig should error.
	if _, err := NewClient(context.Background(), &cfg); err == nil {
		t.Fatal("expected TLS validation error")
	}
}

func TestPing_NilConn(t *testing.T) {
	if err := Ping(context.Background(), nil); err == nil {
		t.Fatal("expected error for nil conn")
	}
}

func TestPing_PropagatesDriverError(t *testing.T) {
	// Build a real conn pointing at an unused TCP port so Ping fails fast.
	cfg := config.DefaultClickhouseConfig()
	cfg.Addrs = []string{"127.0.0.1:1"}
	cfg.Password = "test"
	cfg.DialTimeout = 200 * time.Millisecond
	cfg.ReadTimeout = 200 * time.Millisecond

	conn, err := NewClient(context.Background(), &cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := Ping(ctx, conn); err == nil {
		t.Fatal("expected ping error against unreachable address")
	}
}

// writeSelfSignedCA creates a throwaway PEM-encoded self-signed certificate on
// disk and returns its path. The cert is only used to verify CA-pool loading;
// nothing is signed against it.
func writeSelfSignedCA(t *testing.T) string {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test-ca"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IsCA:         true,
		KeyUsage:     x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "ca.pem")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}
	defer f.Close()
	if err := pem.Encode(f, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
		t.Fatalf("encode pem: %v", err)
	}
	return path
}
