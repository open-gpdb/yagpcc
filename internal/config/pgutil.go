package config

import (
	"fmt"
	"net"
	"strings"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"
)

func RegisterConfigForConnString(connString string, cfg PGConfig) (string, error) {
	pgxCfg, err := pgx.ParseConfig(connString)
	if err != nil {
		return "", fmt.Errorf("failed to parse pgx config: %w", err)
	}

	pgxCfg.PreferSimpleProtocol = true
	return stdlib.RegisterConnConfig(pgxCfg), nil
}

// Config represents PostgreSQL configuration (one or more nodes)
type PGConfig struct {
	Addrs            []string `json:"addrs" yaml:"addrs"`
	DB               string   `json:"db" yaml:"db"`
	User             string   `json:"user" yaml:"user"`
	Password         string   `json:"password" yaml:"password"`
	SSLMode          string   `json:"sslmode" yaml:"sslmode"`
	SSLRootCert      string   `json:"sslrootcert" yaml:"sslrootcert"`
	MaxOpenConn      int      `json:"max_open_conn" yaml:"max_open_conn"`
	MaxIdleConn      int      `json:"max_idle_conn" yaml:"max_idle_conn"`
	StatementTimeout int      `json:"statement_timeout" yaml:"statement_timeout"`
}

const (
	defaultSSLMode = "require"
)

type defaultConfigOptions struct {
	user string
	db   string
}

type DefaultConfigOption func(*defaultConfigOptions)

func WithUser(user string) DefaultConfigOption {
	return func(options *defaultConfigOptions) {
		options.user = user
	}
}

func WithDB(db string) DefaultConfigOption {
	return func(options *defaultConfigOptions) {
		options.db = db
	}
}

func DefaultPGConfig(options ...DefaultConfigOption) PGConfig {
	opts := &defaultConfigOptions{}
	for _, o := range options {
		o(opts)
	}
	return PGConfig{
		MaxOpenConn:      32,
		MaxIdleConn:      32,
		DB:               opts.db,
		User:             opts.user,
		StatementTimeout: 0,
	}
}

// String implements Stringer
func (c PGConfig) String() string {
	return fmt.Sprintf("Addrs: '%s' DB: '%s' User: '%s' SSLRootCert: '%s'", c.Addrs, c.DB, c.User, c.SSLRootCert)
}

func (c PGConfig) Validate() error {
	if len(c.Addrs) == 0 {
		return fmt.Errorf("no addresses provided")
	}

	if c.StatementTimeout < 0 {
		return fmt.Errorf("statement_timeout can't be negative")
	}

	return nil
}

func (c PGConfig) DSN() string {
	return c.DSNWithAddr("")
}

func (c PGConfig) DSNWithAddr(addr string) string {
	if addr == "" && len(c.Addrs) > 0 {
		addr = c.Addrs[0]
	}
	return ConnString(
		addr,
		c.DB,
		c.User,
		c.Password,
		c.SSLMode,
		c.SSLRootCert,
		c.StatementTimeout,
	)
}

// ConnString constructs PostgreSQL connection string
func ConnString(addr, dbname, user, password, sslMode, sslRootCert string, statementTimeout int) string {
	var connParams []string

	host, port, err := net.SplitHostPort(addr)
	if err == nil {
		connParams = append(connParams, "host="+host)
		connParams = append(connParams, "port="+port)
	} else {
		connParams = append(connParams, "host="+addr)
	}

	if dbname != "" {
		connParams = append(connParams, "dbname="+dbname)
	}

	if user != "" {
		connParams = append(connParams, "user="+user)
	}

	if password != "" {
		connParams = append(connParams, "password="+password)
	}

	if sslRootCert != "" {
		connParams = append(connParams, "sslrootcert="+sslRootCert)
	}

	if sslMode != "" {
		connParams = append(connParams, "sslmode="+sslMode)
	} else {
		connParams = append(connParams, "sslmode="+defaultSSLMode)
	}

	if statementTimeout != 0 {
		connParams = append(connParams, fmt.Sprintf("statement_timeout=%d", statementTimeout))
	}

	return strings.Join(connParams, " ")
}
