package gp

import (
	"testing"
)

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
