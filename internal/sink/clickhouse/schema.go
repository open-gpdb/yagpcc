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

// File: schema.go provides VerifySchema, DumpSchema and DumpMigration which
// inspect or render the embedded migrations without applying them.
//
// VerifySchema is the read-only counterpart to ApplyMigrations: it compares
// the version recorded in yagpcc._yagpcc_meta with ExpectedSchemaVersion and
// reports actionable errors for the upgrade-needed and downgrade-needed cases.
//
// DumpSchema and DumpMigration render embedded migration files without
// connecting to ClickHouse so operators can inspect or feed the SQL to
// external tooling.
package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// ErrSchemaUpgradeRequired is returned by VerifySchema when the ClickHouse
// schema version is older than the version this binary expects. Operators
// should rerun with schema_management=auto (or the --migrate-only CLI) to
// bring the schema forward.
var ErrSchemaUpgradeRequired = errors.New("clickhouse schema upgrade required")

// ErrSchemaDowngradeRequired is returned by VerifySchema when the ClickHouse
// schema is newer than this binary supports. The yagpcc binary is too old; a
// downgrade of the schema is not performed automatically.
var ErrSchemaDowngradeRequired = errors.New("clickhouse schema downgrade required")

// VerifySchema reads the current schema version from yagpcc._yagpcc_meta and
// compares it against ExpectedSchemaVersion. It never modifies the database.
//
// Returns:
//   - nil when actual == ExpectedSchemaVersion;
//   - ErrSchemaUpgradeRequired (wrapped with version detail) when actual <
//     ExpectedSchemaVersion, including the case where _yagpcc_meta does not
//     yet exist (current = 0);
//   - ErrSchemaDowngradeRequired (wrapped) when actual > ExpectedSchemaVersion;
//   - any underlying error from the conn unwrapped through the error chain.
func VerifySchema(ctx context.Context, conn MigrationConn) error {
	current, err := GetCurrentVersion(ctx, conn)
	if err != nil {
		return fmt.Errorf("verify schema: %w", err)
	}
	switch {
	case current < ExpectedSchemaVersion:
		return fmt.Errorf(
			"%w: clickhouse schema is at version %d, yagpcc binary expects %d; "+
				"run with schema_management=auto (or --migrate-only) to apply pending migrations",
			ErrSchemaUpgradeRequired, current, ExpectedSchemaVersion,
		)
	case current > ExpectedSchemaVersion:
		return fmt.Errorf(
			"%w: clickhouse schema is at version %d, yagpcc binary only supports %d; "+
				"upgrade the yagpcc binary",
			ErrSchemaDowngradeRequired, current, ExpectedSchemaVersion,
		)
	}
	return nil
}

// DumpOptions parameterises DumpSchema and DumpMigration. RetentionDays is
// rendered into `{{.RetentionDays}}` placeholders inside the embedded files.
type DumpOptions struct {
	RetentionDays int
}

// DumpSchema returns the cumulative DDL needed to bring an empty database to
// ExpectedSchemaVersion. It concatenates the rendered up files of all embedded
// migrations in version order, separated by blank lines and a header comment
// per migration. It does not connect to ClickHouse.
func DumpSchema(opts DumpOptions) (string, error) {
	return DumpMigration(0, ExpectedSchemaVersion, opts)
}

// DumpMigration returns the SQL needed to move the schema from version `from`
// to version `to`.
//
//   - When from < to, the up files of migrations (from, to] are concatenated
//     in ascending version order.
//   - When from > to, the down files of migrations [to+1, from] are
//     concatenated in descending version order.
//   - When from == to, the result is an empty string.
//
// It does not connect to ClickHouse.
func DumpMigration(from, to int, opts DumpOptions) (string, error) {
	if from < 0 || to < 0 {
		return "", fmt.Errorf("DumpMigration: from/to must be >= 0, got from=%d to=%d", from, to)
	}
	if opts.RetentionDays <= 0 {
		return "", fmt.Errorf("DumpMigration: RetentionDays must be > 0, got %d", opts.RetentionDays)
	}
	if from == to {
		return "", nil
	}

	migs, err := ParseMigrations()
	if err != nil {
		return "", fmt.Errorf("parse migrations: %w", err)
	}
	byVersion := make(map[int]Migration, len(migs))
	maxVersion := 0
	for _, m := range migs {
		byVersion[m.Version] = m
		if m.Version > maxVersion {
			maxVersion = m.Version
		}
	}

	upgrade := from < to
	bound := to
	if !upgrade {
		bound = from
	}
	if bound > maxVersion {
		return "", fmt.Errorf(
			"DumpMigration: requested version %d but highest embedded migration is %d",
			bound, maxVersion,
		)
	}

	params := map[string]any{"RetentionDays": opts.RetentionDays}
	var out strings.Builder

	render := func(version int, direction string) error {
		m, ok := byVersion[version]
		if !ok {
			return fmt.Errorf("DumpMigration: missing migration version %d", version)
		}
		body := m.Up
		if direction == "down" {
			body = m.Down
		}
		rendered, err := RenderTemplate(body, params)
		if err != nil {
			return fmt.Errorf("render migration %d %s: %w", version, direction, err)
		}
		if out.Len() > 0 {
			out.WriteString("\n")
		}
		fmt.Fprintf(&out, "-- migration %d (%s) %s\n", version, m.Name, direction)
		out.WriteString(strings.TrimRight(rendered, "\n"))
		out.WriteString("\n")
		return nil
	}

	if upgrade {
		for v := from + 1; v <= to; v++ {
			if err := render(v, "up"); err != nil {
				return "", err
			}
		}
	} else {
		for v := from; v > to; v-- {
			if err := render(v, "down"); err != nil {
				return "", err
			}
		}
	}
	return out.String(), nil
}
