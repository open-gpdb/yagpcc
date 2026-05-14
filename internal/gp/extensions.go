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
	"github.com/open-gpdb/yagpcc/internal/config"
	"go.uber.org/zap"
)

// PgExtension represents a single row from pg_extension
type PgExtension struct {
	ExtName        string `db:"extname"`
	ExtOwner       string `db:"extowner"`     // resolved to role name via JOIN
	ExtNamespace   string `db:"extnamespace"` // resolved to schema name via JOIN
	ExtRelocatable bool   `db:"extrelocatable"`
	ExtVersion     string `db:"extversion"`
}

// DatabaseExtensions holds extensions for a single database
type DatabaseExtensions struct {
	Extensions []PgExtension
	Error      string // non-empty if query failed for this DB
}

// AllDatabaseExtensions is the top-level cached result
type AllDatabaseExtensions map[string]DatabaseExtensions

const (
	// DatabaseListQ queries the list of user databases
	DatabaseListQ = "SELECT datname FROM pg_database WHERE datistemplate = false AND datallowconn = true ORDER BY datname"

	// ExtensionsQ queries extensions in a specific database with JOINs to resolve names
	ExtensionsQ = `
SELECT e.extname,
       r.rolname AS extowner,
       n.nspname AS extnamespace,
       e.extrelocatable,
       e.extversion
FROM pg_extension e
JOIN pg_roles r ON r.oid = e.extowner
JOIN pg_namespace n ON n.oid = e.extnamespace
ORDER BY e.extname`
)

// connectToDatabase creates a temporary connection to a specific database
func connectToDatabase(ctx context.Context, log *zap.SugaredLogger, pgconfig *config.PGConfig, dbName string) (*Connection, error) {
	// Create a copy of the config with the target database name
	dbConfig := *pgconfig
	dbConfig.DB = dbName

	if len(dbConfig.Addrs) == 0 {
		return nil, fmt.Errorf("failed to connect to database %s: no addresses configured", dbName)
	}

	// Build connection string with the target database
	connString := config.ConnString(
		dbConfig.Addrs[0], // Use the first address
		dbConfig.DB,
		dbConfig.User,
		dbConfig.Password,
		dbConfig.SSLMode,
		dbConfig.SSLRootCert,
		dbConfig.StatementTimeout,
	)

	configKey, err := getConfigKey(connString, dbConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to register config for database %s: %w", dbName, err)
	}

	// Create a new database connection directly
	db, err := sqlx.ConnectContext(ctx, "pgx", configKey)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database %s: %w", dbName, err)
	}

	// Set connection limits
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// Set the bypass option
	_, err = db.ExecContext(ctx, "set session gp_resource_group_bypass = on")
	if err != nil {
		// Close the connection before returning error
		_ = db.Close()
		return nil, fmt.Errorf("error setting up bypass option for database %s: %w", dbName, err)
	}

	// Create and return a Connection wrapper
	return NewConnection(log, &dbConfig, db), nil
}

func getConfigKey(connString string, dbConfig config.PGConfig) (string, error) {
	dbMutex.Lock()
	defer dbMutex.Unlock()

	configKey, ok := configKeyMap[connString]
	if ok {
		return configKey, nil
	}

	configKey, err := config.RegisterConfigForConnString(connString, dbConfig)
	if err != nil {
		return "", err
	}
	configKeyMap[connString] = configKey
	return configKey, nil
}

func execQueryOnCurrentDB(ctx context.Context, conn *Connection, query string, dest interface{}) error {
	if conn == nil || conn.db == nil {
		return fmt.Errorf("not initialized connection")
	}

	conn.log.Debugf("will execute query %v", query)
	err := conn.db.SelectContext(ctx, dest, query)
	if err != nil {
		conn.log.Warnf("got error %v while select %v", err, query)
		return err
	}
	return nil
}

// GetAllExtensions retrieves extension information from all user databases
func GetAllExtensions(ctx context.Context, durability time.Duration) (AllDatabaseExtensions, error) {
	// Check cache first
	cachedItem, ok := CachedItems[ExtensionsConfig]
	if ok && checkCacheItem(cachedItem, durability) {
		return cachedItem.ItemValue.(AllDatabaseExtensions), nil
	}

	dbMutex.Lock()
	currentDB := db
	dbMutex.Unlock()
	if currentDB == nil {
		return nil, fmt.Errorf("internal - DB not initialized")
	}

	// Get list of databases
	databaseNames := make([]string, 0)
	err := currentDB.ExecQuery(ctx, DatabaseListQ, &databaseNames)
	if err != nil {
		return nil, fmt.Errorf("failed to query database list: %w", err)
	}

	// Query extensions for each database
	allExtensions := make(AllDatabaseExtensions) // Initialize the map

	for _, dbName := range databaseNames {
		dbExtensions := DatabaseExtensions{
			Extensions: make([]PgExtension, 0),
			Error:      "", // Initialize as empty
		}

		// Create a temporary connection to this database
		dbConn, err := connectToDatabase(ctx, currentDB.log, currentDB.config, dbName)
		if err != nil {
			// Log the error but continue with other databases
			dbExtensions.Error = err.Error()
			allExtensions[dbName] = dbExtensions
			continue
		}

		// Query extensions in this database
		extensions := make([]PgExtension, 0)
		err = execQueryOnCurrentDB(ctx, dbConn, ExtensionsQ, &extensions)
		if err != nil {
			// Log the error but continue with other databases
			dbExtensions.Error = err.Error()
		} else {
			dbExtensions.Extensions = extensions
		}

		// Close the temporary connection
		if dbConn.db != nil {
			_ = dbConn.db.Close()
		}

		allExtensions[dbName] = dbExtensions
	}

	// Cache the result
	CachedItems[ExtensionsConfig] = &CacheItem{
		ItemValue:   allExtensions,
		Status:      CacheOk,
		RefreshDate: time.Now(),
	}

	return allExtensions, nil
}

// GetDatabaseExtensions retrieves extension information for a specific database
func GetDatabaseExtensions(ctx context.Context, durability time.Duration, databaseName string) (DatabaseExtensions, error) {
	// Get all extensions first (this will use caching)
	allExtensions, err := GetAllExtensions(ctx, durability)
	if err != nil {
		return DatabaseExtensions{}, err
	}

	// Return the extensions for the specific database
	if dbExtensions, exists := allExtensions[databaseName]; exists {
		return dbExtensions, nil
	}

	// Return empty result if database not found
	return DatabaseExtensions{
		Extensions: make([]PgExtension, 0),
		Error:      fmt.Sprintf("database %s not found", databaseName),
	}, nil
}
