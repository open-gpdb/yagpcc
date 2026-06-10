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

// File: migrations.go applies embedded SQL migrations from migrations/*.sql,
// tracks the applied schema version through yagpcc._yagpcc_meta and exposes
// helpers for parsing and templating the migration files.
package clickhouse

import (
	"bytes"
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"text/template"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// ExpectedSchemaVersion is the schema version this binary expects after all
// embedded migrations have been applied. Bump when adding a new migration.
const ExpectedSchemaVersion = 1

// Migration is one parsed up/down pair from the embedded migrations FS.
type Migration struct {
	Version int
	Name    string
	Up      string
	Down    string
}

// migrationFilePattern matches files like "0001_init.up.sql" / "0001_init.down.sql".
var migrationFilePattern = regexp.MustCompile(`^(\d+)_([^.]+)\.(up|down)\.sql$`)

// ParseMigrations walks the embedded migrations FS and returns migrations
// sorted by version. Each returned entry must have both an up and a down file.
func ParseMigrations() ([]Migration, error) {
	return parseMigrationsFS(migrationsFS, "migrations")
}

func parseMigrationsFS(fsys fs.FS, root string) ([]Migration, error) {
	entries, err := fs.ReadDir(fsys, root)
	if err != nil {
		// Empty / missing root is treated as no migrations.
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read migrations dir: %w", err)
	}

	type pair struct {
		name string
		up   string
		down string
	}
	byVersion := map[int]*pair{}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		match := migrationFilePattern.FindStringSubmatch(e.Name())
		if match == nil {
			// Ignore unrelated files (e.g. .gitkeep).
			continue
		}
		version, err := strconv.Atoi(match[1])
		if err != nil {
			return nil, fmt.Errorf("parse version from %q: %w", e.Name(), err)
		}
		name := match[2]
		direction := match[3]

		body, err := fs.ReadFile(fsys, root+"/"+e.Name())
		if err != nil {
			return nil, fmt.Errorf("read %q: %w", e.Name(), err)
		}

		p, ok := byVersion[version]
		if !ok {
			p = &pair{name: name}
			byVersion[version] = p
		}
		if p.name != name {
			return nil, fmt.Errorf(
				"migration %d has inconsistent names: %q vs %q",
				version, p.name, name,
			)
		}
		switch direction {
		case "up":
			if p.up != "" {
				return nil, fmt.Errorf("duplicate up file for version %d", version)
			}
			p.up = string(body)
		case "down":
			if p.down != "" {
				return nil, fmt.Errorf("duplicate down file for version %d", version)
			}
			p.down = string(body)
		}
	}

	versions := make([]int, 0, len(byVersion))
	for v := range byVersion {
		versions = append(versions, v)
	}
	sort.Ints(versions)

	out := make([]Migration, 0, len(versions))
	for _, v := range versions {
		p := byVersion[v]
		if p.up == "" {
			return nil, fmt.Errorf("migration %d is missing the up file", v)
		}
		if p.down == "" {
			return nil, fmt.Errorf("migration %d is missing the down file", v)
		}
		out = append(out, Migration{
			Version: v,
			Name:    p.name,
			Up:      p.up,
			Down:    p.down,
		})
	}
	return out, nil
}

// RenderTemplate substitutes {{.Field}} placeholders in sql using params via
// text/template. Missing keys are rendered as <no value> by template defaults;
// callers should pass complete params.
func RenderTemplate(sql string, params map[string]any) (string, error) {
	tpl, err := template.New("migration").Option("missingkey=error").Parse(sql)
	if err != nil {
		return "", fmt.Errorf("parse template: %w", err)
	}
	var buf bytes.Buffer
	if err := tpl.Execute(&buf, params); err != nil {
		return "", fmt.Errorf("execute template: %w", err)
	}
	return buf.String(), nil
}

// SplitStatements splits a multi-statement SQL string on `;` boundaries,
// trimming whitespace and dropping empty pieces. ClickHouse Exec accepts only
// a single statement at a time, so callers iterate over the result.
func SplitStatements(sql string) []string {
	parts := strings.Split(sql, ";")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
}

// MigrationConn is the minimum subset of clickhouse driver.Conn needed for
// applying or inspecting schema migrations. The real *clickhouse.Conn from
// clickhouse-go/v2 satisfies this interface; tests use a fake.
type MigrationConn interface {
	Exec(ctx context.Context, query string, args ...any) error
	Select(ctx context.Context, dest any, query string, args ...any) error
}

// MigrateOptions parameterises ApplyMigrations. RetentionDays is rendered into
// `{{.RetentionDays}}` placeholders inside the up files; YagpccVersion is
// recorded alongside each meta row for forensic tracing.
type MigrateOptions struct {
	RetentionDays int
	YagpccVersion string
}

const (
	bootstrapDatabaseDDL = `CREATE DATABASE IF NOT EXISTS yagpcc`

	bootstrapMetaTableDDL = `CREATE TABLE IF NOT EXISTS yagpcc._yagpcc_meta (
    version        Int32,
    applied_at     DateTime64(3, 'UTC') DEFAULT now64(),
    yagpcc_version String,
    direction      LowCardinality(String)
) ENGINE = MergeTree
ORDER BY (version, applied_at)`

	insertMetaSQL = "INSERT INTO yagpcc._yagpcc_meta (version, yagpcc_version, direction) VALUES (?, ?, ?)"

	// selectMaxVersionSQL aliases the aggregate as `v` so the row can be scanned
	// into a struct field tagged `ch:"v"`. The clickhouse-go/v2 driver's Select
	// implementation requires struct destinations and matches columns by name —
	// see scan.go (scanSelect) and struct_map.go (structMap.Map).
	selectMaxVersionSQL = "SELECT max(version) AS v FROM yagpcc._yagpcc_meta WHERE direction = 'up'"

	selectTableExistsSQL = "SELECT count() AS c FROM system.tables WHERE database = ? AND name = ?"
)

// maxVersionRow is the Select destination for selectMaxVersionSQL. The `ch:"v"`
// tag pairs the struct field with the SQL alias so clickhouse-go's struct
// scanner can populate it (the driver rejects primitive-slice destinations).
type maxVersionRow struct {
	V int32 `ch:"v"`
}

// metaTableExistsRow is the Select destination for selectTableExistsSQL.
type metaTableExistsRow struct {
	C uint64 `ch:"c"`
}

// GetCurrentVersion returns the highest applied "up" schema version recorded
// in yagpcc._yagpcc_meta. If the meta table does not yet exist, the function
// returns 0 with no error.
func GetCurrentVersion(ctx context.Context, conn MigrationConn) (int, error) {
	exists, err := metaTableExists(ctx, conn)
	if err != nil {
		return 0, fmt.Errorf("check meta table exists: %w", err)
	}
	if !exists {
		return 0, nil
	}
	var rows []maxVersionRow
	if err := conn.Select(ctx, &rows, selectMaxVersionSQL); err != nil {
		return 0, fmt.Errorf("select max(version): %w", err)
	}
	if len(rows) == 0 {
		return 0, nil
	}
	v := rows[0].V
	if v < 0 {
		return 0, nil
	}
	return int(v), nil
}

func metaTableExists(ctx context.Context, conn MigrationConn) (bool, error) {
	var rows []metaTableExistsRow
	if err := conn.Select(ctx, &rows, selectTableExistsSQL, "yagpcc", "_yagpcc_meta"); err != nil {
		return false, err
	}
	if len(rows) == 0 {
		return false, nil
	}
	return rows[0].C > 0, nil
}

// ApplyMigrations brings the ClickHouse schema up to ExpectedSchemaVersion.
//
// The runner:
//  1. Creates the yagpcc database and _yagpcc_meta table if missing
//     (idempotent CREATE ... IF NOT EXISTS).
//  2. Reads the current applied version. If it is greater than the binary's
//     ExpectedSchemaVersion, returns a fail-fast error: downgrades require
//     manual intervention because dropping/altering tables can lose data.
//  3. Applies pending up migrations strictly in version order. Each rendered
//     up file is split on ';' and Exec'd statement-by-statement (ClickHouse
//     does not run multi-statement DDL atomically, so a partial failure
//     leaves the schema half-applied; the last successful version remains
//     recorded in _yagpcc_meta and operators must resolve manually).
//  4. After each successful up migration inserts a row into _yagpcc_meta.
func ApplyMigrations(ctx context.Context, conn MigrationConn, opts MigrateOptions) error {
	if opts.RetentionDays <= 0 {
		return fmt.Errorf("ApplyMigrations: RetentionDays must be > 0, got %d", opts.RetentionDays)
	}

	if err := conn.Exec(ctx, bootstrapDatabaseDDL); err != nil {
		return fmt.Errorf("bootstrap database: %w", err)
	}
	if err := conn.Exec(ctx, bootstrapMetaTableDDL); err != nil {
		return fmt.Errorf("bootstrap meta table: %w", err)
	}

	current, err := GetCurrentVersion(ctx, conn)
	if err != nil {
		return fmt.Errorf("read current version: %w", err)
	}
	if current > ExpectedSchemaVersion {
		return fmt.Errorf(
			"clickhouse schema is at version %d but yagpcc binary expects %d; "+
				"downgrade is not supported automatically",
			current, ExpectedSchemaVersion,
		)
	}
	if current == ExpectedSchemaVersion {
		return nil
	}

	migs, err := ParseMigrations()
	if err != nil {
		return fmt.Errorf("parse migrations: %w", err)
	}

	params := map[string]any{
		"RetentionDays": opts.RetentionDays,
	}
	yagpccVersion := opts.YagpccVersion
	if yagpccVersion == "" {
		yagpccVersion = "unknown"
	}

	for _, m := range migs {
		if m.Version <= current {
			continue
		}
		if m.Version > ExpectedSchemaVersion {
			break
		}
		rendered, err := RenderTemplate(m.Up, params)
		if err != nil {
			return fmt.Errorf("render migration %d: %w", m.Version, err)
		}
		for _, stmt := range SplitStatements(rendered) {
			if err := conn.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("apply migration %d: %w", m.Version, err)
			}
		}
		if err := conn.Exec(ctx, insertMetaSQL, int32(m.Version), yagpccVersion, "up"); err != nil {
			return fmt.Errorf("record migration %d in meta: %w", m.Version, err)
		}
	}
	return nil
}
