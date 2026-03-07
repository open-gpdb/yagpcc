package gp

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/open-gpdb/yagpcc/internal/config"
	"go.uber.org/zap"
)

type Connection struct {
	log    *zap.SugaredLogger
	config *config.PGConfig
	db     *sqlx.DB
}

func NewConnection(log *zap.SugaredLogger, config *config.PGConfig, db *sqlx.DB) *Connection {
	return &Connection{log: log, config: config, db: db}
}

var (
	connectionMap = make(map[string]*sqlx.DB)
	configKeyMap  = make(map[string]string)
)

func GetAliveConnection(ctx context.Context, log *zap.SugaredLogger, pgconfig *config.PGConfig) (*Connection, error) {
	const connectTimeout = time.Second * 1

	// iterate over items in address
	if len(pgconfig.Addrs) == 0 {
		return NewConnection(log, pgconfig, nil), fmt.Errorf("empty length of addrs")
	}

	addrs := make([]string, len(pgconfig.Addrs))
	copy(addrs, pgconfig.Addrs)
	rand.Shuffle(len(addrs), func(i, j int) {
		addrs[i], addrs[j] = addrs[j], addrs[i]
	})

	var err error

	for _, addr := range addrs {
		dbMutex.Lock()
		cachedDB, ok := connectionMap[addr]
		dbMutex.Unlock()
		if ok {
			// check if connection is still alive
			ctxTimeoutOps, ctxTimeoutCancel := context.WithTimeout(ctx, connectTimeout)
			defer ctxTimeoutCancel()

			err = cachedDB.PingContext(ctxTimeoutOps)
			if err != nil {
				errClose := cachedDB.Close()
				if errClose != nil {
					log.Debugf("cannot close connection with error %v", errClose)
				}
				dbMutex.Lock()
				delete(connectionMap, addr)
				dbMutex.Unlock()
				continue
			}

			return NewConnection(log, pgconfig, cachedDB), nil
		}
		// create new conection
		ctxTimeoutOps, ctxTimeoutCancel := context.WithTimeout(ctx, connectTimeout)
		defer ctxTimeoutCancel()
		connString := config.ConnString(addr, pgconfig.DB, pgconfig.User, pgconfig.Password, pgconfig.SSLMode, pgconfig.SSLRootCert, pgconfig.StatementTimeout)
		dbMutex.Lock()
		// check if we already register config
		configKey, ok := configKeyMap[connString]
		if !ok {
			configKey, err = config.RegisterConfigForConnString(connString, *pgconfig)
			if err != nil {
				log.Errorf("cannot get config %v", err)
				dbMutex.Unlock()
				return nil, err
			}
			configKeyMap[connString] = configKey
		}
		dbMutex.Unlock()

		var newDB *sqlx.DB
		newDB, err = sqlx.ConnectContext(ctxTimeoutOps, "pgx", configKey)
		if err != nil {
			log.Warnf("cannot get connect %v got error %v", configKey, err)
			continue
		}

		newDB.SetMaxIdleConns(pgconfig.MaxIdleConn)
		newDB.SetMaxOpenConns(pgconfig.MaxOpenConn)

		_, err = newDB.ExecContext(ctxTimeoutOps, "set session gp_resource_group_bypass = on")
		if err != nil {
			log.Warnf("error setting up bypass option for new connection: %v", err)
			continue
		}

		dbMutex.Lock()
		connectionMap[addr] = newDB
		dbMutex.Unlock()
		log.Debugf("Successfully connected to %v", addr)

		return NewConnection(log, pgconfig, newDB), nil
	}

	return NewConnection(log, pgconfig, nil), fmt.Errorf("cannot get alive connection: %w", err)
}
