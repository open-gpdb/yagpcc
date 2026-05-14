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
	"time"

	"github.com/jmoiron/sqlx"
)

const retryDelay = time.Millisecond * 200

func (c *Connection) ExecQueryNoRetry(ctx context.Context, query string, dest interface{}) error {
	conn, err := GetAliveConnection(ctx, c.log, c.config)
	if err != nil {
		c.log.Warnf("cannot get alive connection %v", err)
		return err
	}
	c.log.Debugf("will execute query %v", query)
	err = conn.db.SelectContext(ctx, dest, query)
	if err != nil {
		c.log.Warnf("got error %v while select %v", err, query)
		return err
	}
	return nil
}

func (c *Connection) ExecQuery(ctx context.Context, query string, dest interface{}) error {
	t := time.NewTicker(retryDelay) // Prevent spam
	defer t.Stop()

	if c == nil {
		return fmt.Errorf("not initialized connection")
	}
	err := c.ExecQueryNoRetry(ctx, query, dest)
	if err == nil {
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return err
		case <-t.C:
			err = c.ExecQueryNoRetry(ctx, query, dest)
			if err != nil {
				continue
			}
			return nil
		}
	}
}

func (c *Connection) namedExecQueryNoRetry(ctx context.Context, query string, args []interface{}) (*sqlx.Rows, error) {
	conn, err := GetAliveConnection(ctx, c.log, c.config)
	if err != nil {
		c.log.Errorf("cannot get alive connection %v", err)
		return nil, err
	}
	c.log.Debugf("will execute query %v", query)
	rows, err := conn.db.NamedQueryContext(ctx, query, args)
	if err != nil {
		c.log.Errorf("Got error %v while select %v", err, query)
		return nil, err
	}
	return rows, nil
}

func (c *Connection) NamedExecQuery(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	t := time.NewTicker(retryDelay) // Prevent spam
	defer t.Stop()

	if c == nil {
		return nil, fmt.Errorf("not initialized connection")
	}

	// check once immediately
	rows, err := c.namedExecQueryNoRetry(ctx, query, args)
	if err == nil {
		return rows, nil
	}

	for {
		select {
		case <-ctx.Done():
			return nil, err
		case <-t.C:
			rows, err = c.namedExecQueryNoRetry(ctx, query, args)
			if err != nil {
				continue
			}
			return rows, nil
		}
	}
}
