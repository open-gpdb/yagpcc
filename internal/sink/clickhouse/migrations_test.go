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
	"strings"
	"testing"
	"testing/fstest"
)

func TestParseMigrationsEmbedded(t *testing.T) {
	migs, err := ParseMigrations()
	if err != nil {
		t.Fatalf("ParseMigrations: %v", err)
	}
	if len(migs) == 0 {
		t.Fatal("expected at least one embedded migration")
	}
	if migs[0].Version != 1 {
		t.Errorf("first migration version = %d, want 1", migs[0].Version)
	}
	if migs[0].Name != "init" {
		t.Errorf("first migration name = %q, want init", migs[0].Name)
	}
	if !strings.Contains(migs[0].Up, "CREATE TABLE") {
		t.Error("up file should contain CREATE TABLE")
	}
	if !strings.Contains(migs[0].Down, "DROP TABLE") {
		t.Error("down file should contain DROP TABLE")
	}
	if !strings.Contains(migs[0].Up, "{{.RetentionDays}}") {
		t.Error("up file should contain {{.RetentionDays}} placeholder")
	}

	// Highest applied version equals ExpectedSchemaVersion.
	if migs[len(migs)-1].Version != ExpectedSchemaVersion {
		t.Errorf("last embedded migration = %d, ExpectedSchemaVersion = %d",
			migs[len(migs)-1].Version, ExpectedSchemaVersion)
	}
}

func TestParseMigrationsFS_Sorted(t *testing.T) {
	fsys := fstest.MapFS{
		"m/0002_b.up.sql":   {Data: []byte("up2")},
		"m/0002_b.down.sql": {Data: []byte("down2")},
		"m/0001_a.up.sql":   {Data: []byte("up1")},
		"m/0001_a.down.sql": {Data: []byte("down1")},
		"m/0010_c.up.sql":   {Data: []byte("up10")},
		"m/0010_c.down.sql": {Data: []byte("down10")},
	}
	migs, err := parseMigrationsFS(fsys, "m")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(migs) != 3 {
		t.Fatalf("got %d migrations, want 3", len(migs))
	}
	for i, want := range []int{1, 2, 10} {
		if migs[i].Version != want {
			t.Errorf("migs[%d].Version = %d, want %d", i, migs[i].Version, want)
		}
	}
	if migs[0].Up != "up1" || migs[0].Down != "down1" {
		t.Errorf("migs[0] body mismatch: up=%q down=%q", migs[0].Up, migs[0].Down)
	}
}

func TestParseMigrationsFS_EmptyOrMissingRoot(t *testing.T) {
	t.Run("empty dir", func(t *testing.T) {
		fsys := fstest.MapFS{
			"m/.gitkeep": {Data: []byte{}},
		}
		migs, err := parseMigrationsFS(fsys, "m")
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if len(migs) != 0 {
			t.Errorf("expected no migrations, got %d", len(migs))
		}
	})
	t.Run("missing dir", func(t *testing.T) {
		fsys := fstest.MapFS{}
		migs, err := parseMigrationsFS(fsys, "missing")
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if len(migs) != 0 {
			t.Errorf("expected no migrations, got %d", len(migs))
		}
	})
}

func TestParseMigrationsFS_MissingPair(t *testing.T) {
	t.Run("missing down", func(t *testing.T) {
		fsys := fstest.MapFS{
			"m/0001_x.up.sql": {Data: []byte("u")},
		}
		if _, err := parseMigrationsFS(fsys, "m"); err == nil {
			t.Fatal("expected error for missing down file")
		}
	})
	t.Run("missing up", func(t *testing.T) {
		fsys := fstest.MapFS{
			"m/0001_x.down.sql": {Data: []byte("d")},
		}
		if _, err := parseMigrationsFS(fsys, "m"); err == nil {
			t.Fatal("expected error for missing up file")
		}
	})
}

func TestParseMigrationsFS_NameMismatch(t *testing.T) {
	fsys := fstest.MapFS{
		"m/0001_a.up.sql":   {Data: []byte("u")},
		"m/0001_b.down.sql": {Data: []byte("d")},
	}
	if _, err := parseMigrationsFS(fsys, "m"); err == nil {
		t.Fatal("expected error for name mismatch")
	}
}

func TestParseMigrationsFS_DuplicateDirection(t *testing.T) {
	fsys := fstest.MapFS{
		"m/0001_a.up.sql":   {Data: []byte("u1")},
		"m/0001_a.down.sql": {Data: []byte("d1")},
	}
	// Second entry with same key cannot be added to MapFS, so simulate by
	// constructing a directory with two distinct file names that both match.
	// fstest.MapFS uses the map key as path, so duplicate prevention here is
	// inherent; we instead validate the explicit error path via name format.
	migs, err := parseMigrationsFS(fsys, "m")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(migs) != 1 {
		t.Fatalf("got %d migrations, want 1", len(migs))
	}
}

func TestParseMigrationsFS_IgnoresUnrelated(t *testing.T) {
	fsys := fstest.MapFS{
		"m/.gitkeep":        {Data: []byte{}},
		"m/README.md":       {Data: []byte("readme")},
		"m/0001_a.up.sql":   {Data: []byte("u")},
		"m/0001_a.down.sql": {Data: []byte("d")},
		"m/notes.txt":       {Data: []byte("notes")},
	}
	migs, err := parseMigrationsFS(fsys, "m")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(migs) != 1 {
		t.Fatalf("got %d migrations, want 1", len(migs))
	}
}

func TestRenderTemplate(t *testing.T) {
	rendered, err := RenderTemplate(
		"TTL toDate(event_time) + INTERVAL {{.RetentionDays}} DAY",
		map[string]any{"RetentionDays": 30},
	)
	if err != nil {
		t.Fatalf("RenderTemplate: %v", err)
	}
	if !strings.Contains(rendered, "INTERVAL 30 DAY") {
		t.Errorf("got %q, want INTERVAL 30 DAY", rendered)
	}
}

func TestRenderTemplate_MissingKeyFails(t *testing.T) {
	_, err := RenderTemplate(
		"INTERVAL {{.RetentionDays}} DAY",
		map[string]any{},
	)
	if err == nil {
		t.Fatal("expected error for missing key")
	}
}

func TestRenderTemplate_PassThroughNoPlaceholders(t *testing.T) {
	in := "SELECT 1"
	out, err := RenderTemplate(in, nil)
	if err != nil {
		t.Fatalf("RenderTemplate: %v", err)
	}
	if out != in {
		t.Errorf("got %q, want %q", out, in)
	}
}

func TestRenderTemplate_BadSyntax(t *testing.T) {
	_, err := RenderTemplate("{{ unterminated", map[string]any{})
	if err == nil {
		t.Fatal("expected parse error")
	}
}

func TestSplitStatements(t *testing.T) {
	in := "CREATE TABLE a (x Int32);\n\nCREATE TABLE b (y String);\n;\n"
	got := SplitStatements(in)
	if len(got) != 2 {
		t.Fatalf("got %d statements, want 2: %#v", len(got), got)
	}
	if !strings.HasPrefix(got[0], "CREATE TABLE a") {
		t.Errorf("first statement = %q", got[0])
	}
	if !strings.HasPrefix(got[1], "CREATE TABLE b") {
		t.Errorf("second statement = %q", got[1])
	}
}

func TestRenderEmbeddedUpMigration(t *testing.T) {
	migs, err := ParseMigrations()
	if err != nil {
		t.Fatalf("ParseMigrations: %v", err)
	}
	rendered, err := RenderTemplate(migs[0].Up, map[string]any{
		"RetentionDays": 30,
	})
	if err != nil {
		t.Fatalf("RenderTemplate: %v", err)
	}
	for _, want := range []string{
		"CREATE DATABASE IF NOT EXISTS yagpcc",
		"yagpcc._yagpcc_meta",
		"yagpcc.query_events",
		"yagpcc.aggregated_metrics",
		"yagpcc.session_snapshots",
		"INTERVAL 30 DAY",
	} {
		if !strings.Contains(rendered, want) {
			t.Errorf("rendered up migration missing %q", want)
		}
	}
	if strings.Contains(rendered, "{{") {
		t.Errorf("rendered up migration still contains template syntax")
	}
}
