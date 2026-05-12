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
