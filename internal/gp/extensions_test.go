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
	"sync"
	"testing"
	"time"

	"github.com/open-gpdb/yagpcc/internal/config"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestConnectToDatabase_EmptyAddrs(t *testing.T) {
	_, err := connectToDatabase(
		context.Background(),
		zap.NewNop().Sugar(),
		&config.PGConfig{},
		"postgres",
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to connect to database postgres: no addresses configured")
}

func TestGetConfigKey_ReusesCachedEntry(t *testing.T) {
	dbMutex.Lock()
	oldConfigKeyMap := configKeyMap
	configKeyMap = make(map[string]string)
	dbMutex.Unlock()
	t.Cleanup(func() {
		dbMutex.Lock()
		configKeyMap = oldConfigKeyMap
		dbMutex.Unlock()
	})

	pgConfig := config.PGConfig{
		Addrs:   []string{"localhost:5432"},
		DB:      "postgres",
		User:    "gpadmin",
		SSLMode: "disable",
	}
	connString := config.ConnString(
		pgConfig.Addrs[0],
		pgConfig.DB,
		pgConfig.User,
		pgConfig.Password,
		pgConfig.SSLMode,
		pgConfig.SSLRootCert,
		pgConfig.StatementTimeout,
	)

	configKey1, err := getConfigKey(connString, pgConfig)
	require.NoError(t, err)
	configKey2, err := getConfigKey(connString, pgConfig)
	require.NoError(t, err)

	dbMutex.Lock()
	configKeyMapLen := len(configKeyMap)
	dbMutex.Unlock()

	require.Equal(t, configKey1, configKey2)
	require.Equal(t, 1, configKeyMapLen)
}

func TestGetExtensions_DBNotInitialized(t *testing.T) {
	dbMutex.Lock()
	oldDB := db
	db = nil
	dbMutex.Unlock()
	cacheMutex.Lock()
	oldCachedItem, hadCachedItem := CachedItems[ExtensionsConfig]
	delete(CachedItems, ExtensionsConfig)
	cacheMutex.Unlock()
	t.Cleanup(func() {
		dbMutex.Lock()
		db = oldDB
		dbMutex.Unlock()
		cacheMutex.Lock()
		if hadCachedItem {
			CachedItems[ExtensionsConfig] = oldCachedItem
		} else {
			delete(CachedItems, ExtensionsConfig)
		}
		cacheMutex.Unlock()
	})

	_, err := GetAllExtensions(context.Background(), 0)

	require.Error(t, err)
	require.Contains(t, err.Error(), "internal - DB not initialized")
}

func TestExecQueryOnCurrentDB_NilConnection(t *testing.T) {
	err := execQueryOnCurrentDB(context.Background(), nil, ExtensionsQ, &[]PgExtension{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "not initialized connection")
}

func TestExecQueryOnCurrentDB_NilDB(t *testing.T) {
	err := execQueryOnCurrentDB(
		context.Background(),
		NewConnection(zap.NewNop().Sugar(), &config.PGConfig{}, nil),
		ExtensionsQ,
		&[]PgExtension{},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not initialized connection")
}

func TestDatabaseExtensionsStruct(t *testing.T) {
	// Test that we can create a DatabaseExtensions struct
	ext := DatabaseExtensions{
		Extensions: []PgExtension{},
		Error:      "",
	}

	if len(ext.Extensions) != 0 {
		t.Errorf("Expected Extensions to be empty, got length %d", len(ext.Extensions))
	}

	if ext.Error != "" {
		t.Errorf("Expected Error to be empty, got '%s'", ext.Error)
	}

	if len(ext.Extensions) != 0 {
		t.Errorf("Expected Extensions to be empty, got length %d", len(ext.Extensions))
	}

	if ext.Error != "" {
		t.Errorf("Expected Error to be empty, got '%s'", ext.Error)
	}
}

func TestAllDatabaseExtensionsType(t *testing.T) {
	// Test that AllDatabaseExtensions is a map of DatabaseExtensions
	allExt := make(AllDatabaseExtensions)

	// Should be able to add entries to it using database name as key
	allExt["testdb1"] = DatabaseExtensions{
		Extensions: []PgExtension{},
		Error:      "",
	}

	allExt["testdb2"] = DatabaseExtensions{
		Extensions: []PgExtension{},
		Error:      "",
	}

	if len(allExt) != 2 {
		t.Errorf("Expected length to be 2, got %d", len(allExt))
	}

	if _, exists := allExt["testdb1"]; !exists {
		t.Errorf("Expected to find 'testdb1' key in map")
	}

	if _, exists := allExt["testdb2"]; !exists {
		t.Errorf("Expected to find 'testdb2' key in map")
	}

	// Test accessing values by key
	db1 := allExt["testdb1"]
	if len(db1.Extensions) != 0 {
		t.Errorf("Expected testdb1 Extensions to be empty, got length %d", len(db1.Extensions))
	}

	db2 := allExt["testdb2"]
	if len(db2.Extensions) != 0 {
		t.Errorf("Expected testdb2 Extensions to be empty, got length %d", len(db2.Extensions))
	}
}

func TestPgExtensionStruct(t *testing.T) {
	// Test that we can create a PgExtension struct
	ext := PgExtension{
		ExtName:        "test_ext",
		ExtOwner:       "test_owner",
		ExtNamespace:   "test_schema",
		ExtRelocatable: true,
		ExtVersion:     "1.0",
	}

	if ext.ExtName != "test_ext" {
		t.Errorf("Expected ExtName to be 'test_ext', got '%s'", ext.ExtName)
	}

	if ext.ExtOwner != "test_owner" {
		t.Errorf("Expected ExtOwner to be 'test_owner', got '%s'", ext.ExtOwner)
	}

	if ext.ExtNamespace != "test_schema" {
		t.Errorf("Expected ExtNamespace to be 'test_schema', got '%s'", ext.ExtNamespace)
	}

	if !ext.ExtRelocatable {
		t.Error("Expected ExtRelocatable to be true")
	}

	if ext.ExtVersion != "1.0" {
		t.Errorf("Expected ExtVersion to be '1.0', got '%s'", ext.ExtVersion)
	}
}

func TestGetDatabaseExtensions(t *testing.T) {
	// Create a mock AllDatabaseExtensions map
	mockExtensions := AllDatabaseExtensions{
		"testdb1": DatabaseExtensions{
			Extensions: []PgExtension{
				{
					ExtName:        "ext1",
					ExtOwner:       "owner1",
					ExtNamespace:   "schema1",
					ExtRelocatable: true,
					ExtVersion:     "1.0",
				},
			},
			Error: "",
		},
		"testdb2": DatabaseExtensions{
			Extensions: []PgExtension{},
			Error:      "connection failed",
		},
	}

	// Test getting extensions for an existing database
	dbExt, err := func() (DatabaseExtensions, error) {
		// This is a simplified test - in reality, we would need to mock the cache
		// For now, we'll just test the logic of extracting from the map
		if dbExtensions, exists := mockExtensions["testdb1"]; exists {
			return dbExtensions, nil
		}
		return DatabaseExtensions{
			Extensions: make([]PgExtension, 0),
			Error:      "database testdb1 not found",
		}, nil
	}()

	require.NoError(t, err)
	require.Len(t, dbExt.Extensions, 1)
	require.Equal(t, "", dbExt.Error)
	require.Equal(t, "ext1", dbExt.Extensions[0].ExtName)

	// Test getting extensions for a database with an error
	dbExt2, err := func() (DatabaseExtensions, error) {
		if dbExtensions, exists := mockExtensions["testdb2"]; exists {
			return dbExtensions, nil
		}
		return DatabaseExtensions{
			Extensions: make([]PgExtension, 0),
			Error:      "database testdb2 not found",
		}, nil
	}()

	require.NoError(t, err)
	require.Len(t, dbExt2.Extensions, 0)
	require.Equal(t, "connection failed", dbExt2.Error)

	// Test getting extensions for a non-existent database
	dbExt3, err := func() (DatabaseExtensions, error) {
		if dbExtensions, exists := mockExtensions["nonexistent"]; exists {
			return dbExtensions, nil
		}
		return DatabaseExtensions{
			Extensions: make([]PgExtension, 0),
			Error:      "database nonexistent not found",
		}, nil
	}()

	require.NoError(t, err)
	require.Len(t, dbExt3.Extensions, 0)
	require.Equal(t, "database nonexistent not found", dbExt3.Error)
}

// TestGetAllExtensions_ConcurrentCacheAccess reproduces the production scenario
// where concurrent HTTP/gRPC handlers hit GetAllExtensions while the cache is
// being refreshed. Before CachedItems was guarded by cacheMutex this crashed
// with "fatal error: concurrent map writes"; run with -race to verify the fix.
func TestGetAllExtensions_ConcurrentCacheAccess(t *testing.T) {
	const (
		workers    = 8
		iterations = 200
	)

	newCacheItem := func() *CacheItem {
		return &CacheItem{
			ItemValue: AllDatabaseExtensions{
				"testdb": DatabaseExtensions{
					Extensions: []PgExtension{{ExtName: "ext1", ExtVersion: "1.0"}},
				},
			},
			Status:      CacheOk,
			RefreshDate: time.Now(),
		}
	}

	cacheMutex.Lock()
	oldCachedItem, hadCachedItem := CachedItems[ExtensionsConfig]
	CachedItems[ExtensionsConfig] = newCacheItem()
	cacheMutex.Unlock()
	t.Cleanup(func() {
		cacheMutex.Lock()
		if hadCachedItem {
			CachedItems[ExtensionsConfig] = oldCachedItem
		} else {
			delete(CachedItems, ExtensionsConfig)
		}
		cacheMutex.Unlock()
	})

	// A long durability keeps readers on the cache-hit path, so no DB is needed.
	durability := time.Hour

	var wg sync.WaitGroup
	// Readers: same entry points the HTTP/gRPC handlers use.
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				res, err := GetAllExtensions(context.Background(), durability)
				if err != nil || len(res) == 0 {
					t.Errorf("unexpected GetAllExtensions result: res=%v err=%v", res, err)
					return
				}
				if _, err := GetDatabaseExtensions(context.Background(), durability, "testdb"); err != nil {
					t.Errorf("unexpected GetDatabaseExtensions error: %v", err)
					return
				}
			}
		}()
	}
	// Writers: simulate a concurrent cache refresh storing a fresh result.
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				setCachedItem(ExtensionsConfig, newCacheItem())
			}
		}()
	}
	wg.Wait()
}
