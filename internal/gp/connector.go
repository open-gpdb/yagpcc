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

			err = cachedDB.PingContext(ctxTimeoutOps)
			if err != nil {
				ctxTimeoutCancel()
				errClose := cachedDB.Close()
				if errClose != nil {
					log.Debugf("cannot close connection with error %v", errClose)
				}
				dbMutex.Lock()
				delete(connectionMap, addr)
				dbMutex.Unlock()
				continue
			}

			ctxTimeoutCancel()
			return NewConnection(log, pgconfig, cachedDB), nil
		}
		// create new connection
		ctxTimeoutOps, ctxTimeoutCancel := context.WithTimeout(ctx, connectTimeout)
		connString := config.ConnString(addr, pgconfig.DB, pgconfig.User, pgconfig.Password, pgconfig.SSLMode, pgconfig.SSLRootCert, pgconfig.StatementTimeout)
		dbMutex.Lock()
		// check if we already register config
		configKey, ok := configKeyMap[connString]
		if !ok {
			configKey, err = config.RegisterConfigForConnString(connString, *pgconfig)
			if err != nil {
				log.Errorf("cannot get config %v", err)
				dbMutex.Unlock()
				ctxTimeoutCancel()
				return nil, err
			}
			configKeyMap[connString] = configKey
		}
		dbMutex.Unlock()

		var newDB *sqlx.DB
		newDB, err = sqlx.ConnectContext(ctxTimeoutOps, "pgx", configKey)
		if err != nil {
			ctxTimeoutCancel()
			log.Warnf("cannot get connect %v got error %v", configKey, err)
			continue
		}

		newDB.SetMaxIdleConns(pgconfig.MaxIdleConn)
		newDB.SetMaxOpenConns(pgconfig.MaxOpenConn)

		_, err = newDB.ExecContext(ctxTimeoutOps, "set session gp_resource_group_bypass = on")
		if err != nil {
			ctxTimeoutCancel()
			errClose := newDB.Close()
			if errClose != nil {
				log.Debugf("cannot close connection with error %v", errClose)
			}
			log.Warnf("error setting up bypass option for new connection: %v", err)
			continue
		}

		ctxTimeoutCancel()
		dbMutex.Lock()
		connectionMap[addr] = newDB
		dbMutex.Unlock()
		log.Debugf("Successfully connected to %v", addr)

		return NewConnection(log, pgconfig, newDB), nil
	}

	return NewConnection(log, pgconfig, nil), fmt.Errorf("cannot get alive connection: %w", err)
}
