package gp

import (
	"context"
	"testing"

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
	oldCachedItem, hadCachedItem := CachedItems[ExtensionsConfig]
	delete(CachedItems, ExtensionsConfig)
	dbMutex.Unlock()
	t.Cleanup(func() {
		dbMutex.Lock()
		db = oldDB
		if hadCachedItem {
			CachedItems[ExtensionsConfig] = oldCachedItem
		} else {
			delete(CachedItems, ExtensionsConfig)
		}
		dbMutex.Unlock()
	})

	_, err := GetExtensions(context.Background(), 0)

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
		DatabaseName: "testdb",
		Extensions:   []PgExtension{},
		Error:        "",
	}

	if ext.DatabaseName != "testdb" {
		t.Errorf("Expected DatabaseName to be 'testdb', got '%s'", ext.DatabaseName)
	}

	if len(ext.Extensions) != 0 {
		t.Errorf("Expected Extensions to be empty, got length %d", len(ext.Extensions))
	}

	if ext.Error != "" {
		t.Errorf("Expected Error to be empty, got '%s'", ext.Error)
	}
}

func TestAllDatabaseExtensionsType(t *testing.T) {
	// Test that AllDatabaseExtensions is a slice of DatabaseExtensions
	var allExt AllDatabaseExtensions

	// Should be able to append to it
	allExt = append(allExt, DatabaseExtensions{
		DatabaseName: "testdb1",
		Extensions:   []PgExtension{},
		Error:        "",
	})

	allExt = append(allExt, DatabaseExtensions{
		DatabaseName: "testdb2",
		Extensions:   []PgExtension{},
		Error:        "",
	})

	if len(allExt) != 2 {
		t.Errorf("Expected length to be 2, got %d", len(allExt))
	}

	if allExt[0].DatabaseName != "testdb1" {
		t.Errorf("Expected first database name to be 'testdb1', got '%s'", allExt[0].DatabaseName)
	}

	if allExt[1].DatabaseName != "testdb2" {
		t.Errorf("Expected second database name to be 'testdb2', got '%s'", allExt[1].DatabaseName)
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
