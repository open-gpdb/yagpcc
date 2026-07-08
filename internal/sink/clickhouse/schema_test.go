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
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestVerifySchema_Match(t *testing.T) {
	conn := &fakeConn{
		selectResponses: []selectResponse{
			metaExistsResp(true),
			maxVersionResp(int32(ExpectedSchemaVersion)),
		},
	}
	if err := VerifySchema(context.Background(), conn); err != nil {
		t.Fatalf("VerifySchema: %v", err)
	}
}

func TestVerifySchema_UpgradeRequired_OldVersion(t *testing.T) {
	if ExpectedSchemaVersion < 1 {
		t.Skip("ExpectedSchemaVersion must be >= 1 for this test")
	}
	conn := &fakeConn{
		selectResponses: []selectResponse{
			metaExistsResp(true),
			maxVersionResp(int32(ExpectedSchemaVersion - 1)),
		},
	}
	err := VerifySchema(context.Background(), conn)
	if err == nil {
		t.Fatal("expected upgrade-required error")
	}
	if !errors.Is(err, ErrSchemaUpgradeRequired) {
		t.Errorf("error %v should wrap ErrSchemaUpgradeRequired", err)
	}
	if !strings.Contains(err.Error(), "schema_management=auto") {
		t.Errorf("error %q should hint at schema_management=auto", err.Error())
	}
}

func TestVerifySchema_UpgradeRequired_MetaMissing(t *testing.T) {
	conn := &fakeConn{
		selectResponses: []selectResponse{metaExistsResp(false)},
	}
	err := VerifySchema(context.Background(), conn)
	if err == nil {
		t.Fatal("expected upgrade-required error when meta table missing")
	}
	if !errors.Is(err, ErrSchemaUpgradeRequired) {
		t.Errorf("error %v should wrap ErrSchemaUpgradeRequired", err)
	}
	if !strings.Contains(err.Error(), "version 0") {
		t.Errorf("error %q should report current version 0", err.Error())
	}
}

func TestVerifySchema_DowngradeRequired(t *testing.T) {
	conn := &fakeConn{
		selectResponses: []selectResponse{
			metaExistsResp(true),
			maxVersionResp(int32(ExpectedSchemaVersion + 3)),
		},
	}
	err := VerifySchema(context.Background(), conn)
	if err == nil {
		t.Fatal("expected downgrade-required error")
	}
	if !errors.Is(err, ErrSchemaDowngradeRequired) {
		t.Errorf("error %v should wrap ErrSchemaDowngradeRequired", err)
	}
	if !strings.Contains(err.Error(), "upgrade the yagpcc binary") {
		t.Errorf("error %q should hint at binary upgrade", err.Error())
	}
}

func TestVerifySchema_PropagatesConnError(t *testing.T) {
	wantErr := errors.New("ch unreachable")
	conn := &fakeConn{
		selectResponses: []selectResponse{{err: wantErr}},
	}
	err := VerifySchema(context.Background(), conn)
	if err == nil {
		t.Fatal("expected error propagation")
	}
	if !errors.Is(err, wantErr) {
		t.Errorf("expected wrapped %v, got %v", wantErr, err)
	}
}

func TestDumpSchema_RendersAllUpFiles(t *testing.T) {
	out, err := DumpSchema(DumpOptions{RetentionDays: 30})
	if err != nil {
		t.Fatalf("DumpSchema: %v", err)
	}
	for _, want := range []string{
		"CREATE DATABASE IF NOT EXISTS yagpcc",
		"yagpcc._yagpcc_meta",
		"yagpcc.sessions_part",
		"yagpcc.statements_part",
		"yagpcc.segments_part",
		"ENGINE = ReplacingMergeTree",
		"toIntervalDay(60)",
		"-- migration 1 (init) up",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("DumpSchema output missing %q", want)
		}
	}
	if strings.Contains(out, "{{") {
		t.Error("DumpSchema output still contains template syntax")
	}
}

func TestDumpSchema_Replicated(t *testing.T) {
	std, err := DumpSchema(DumpOptions{RetentionDays: 30})
	if err != nil {
		t.Fatalf("DumpSchema standalone: %v", err)
	}
	repl, err := DumpSchema(DumpOptions{RetentionDays: 30, Replicated: true})
	if err != nil {
		t.Fatalf("DumpSchema replicated: %v", err)
	}

	// Standalone must not carry any clustered constructs.
	for _, absent := range []string{"ReplicatedReplacingMergeTree", "ON CLUSTER", "Distributed("} {
		if strings.Contains(std, absent) {
			t.Errorf("standalone dump should not contain %q", absent)
		}
	}
	// Replicated must carry all of them, including the Distributed fan-out tables.
	for _, want := range []string{
		"ReplicatedReplacingMergeTree",
		"ON CLUSTER '{cluster}'",
		"Distributed('{cluster}', yagpcc, sessions_part",
		"Distributed('{cluster}', yagpcc, statements_part",
		"Distributed('{cluster}', yagpcc, segments_part",
	} {
		if !strings.Contains(repl, want) {
			t.Errorf("replicated dump missing %q", want)
		}
	}
	if strings.Contains(repl, "{{") {
		t.Error("replicated dump still contains template syntax")
	}

	// Both variants must declare the same set of columns for every table.
	for _, table := range []string{"sessions_part", "statements_part", "segments_part"} {
		stdCols := extractColumns(t, std, table)
		replCols := extractColumns(t, repl, table)
		if len(stdCols) == 0 {
			t.Fatalf("no columns parsed for %s", table)
		}
		if !reflect.DeepEqual(stdCols, replCols) {
			t.Errorf("column set differs for %s:\n standalone=%v\n replicated=%v",
				table, stdCols, replCols)
		}
	}
}

// extractColumns pulls the "`name` Type" column declarations out of the
// CREATE TABLE ... for yagpcc.<table>_part block, returning "name Type" pairs
// in declaration order. INDEX lines are ignored so only real columns compare.
func extractColumns(t *testing.T, ddl, table string) []string {
	t.Helper()
	marker := "yagpcc." + table
	start := strings.Index(ddl, marker)
	if start < 0 {
		t.Fatalf("table %s not found in DDL", table)
	}
	open := strings.Index(ddl[start:], "(")
	if open < 0 {
		t.Fatalf("no column block for %s", table)
	}
	rest := ddl[start+open+1:]
	end := strings.Index(rest, "\n)")
	if end < 0 {
		t.Fatalf("no closing paren for %s", table)
	}
	body := rest[:end]

	var cols []string
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(line), ","))
		if !strings.HasPrefix(line, "`") {
			continue // skip INDEX lines and blanks
		}
		closeIdx := strings.Index(line[1:], "`")
		if closeIdx < 0 {
			continue
		}
		name := line[1 : closeIdx+1]
		typ := strings.TrimSpace(line[closeIdx+2:])
		cols = append(cols, name+" "+typ)
	}
	return cols
}

func TestDumpSchema_InvalidRetention(t *testing.T) {
	if _, err := DumpSchema(DumpOptions{RetentionDays: 0}); err == nil {
		t.Fatal("expected error for RetentionDays=0")
	}
	if _, err := DumpSchema(DumpOptions{RetentionDays: -1}); err == nil {
		t.Fatal("expected error for negative RetentionDays")
	}
}

func TestDumpMigration_Up(t *testing.T) {
	out, err := DumpMigration(0, ExpectedSchemaVersion, DumpOptions{RetentionDays: 7})
	if err != nil {
		t.Fatalf("DumpMigration: %v", err)
	}
	if !strings.Contains(out, "CREATE TABLE IF NOT EXISTS yagpcc.sessions_part") {
		t.Error("expected up SQL for sessions_part")
	}
	if !strings.Contains(out, "toIntervalDay(60)") {
		t.Errorf("sessions_part TTL should render; got %s", out)
	}
	if strings.Contains(out, "DROP TABLE") {
		t.Error("up dump should not contain DROP TABLE")
	}
}

func TestDumpMigration_Down(t *testing.T) {
	out, err := DumpMigration(ExpectedSchemaVersion, 0, DumpOptions{RetentionDays: 30})
	if err != nil {
		t.Fatalf("DumpMigration: %v", err)
	}
	for _, want := range []string{
		"DROP TABLE IF EXISTS yagpcc.segments_part",
		"DROP TABLE IF EXISTS yagpcc.statements_part",
		"DROP TABLE IF EXISTS yagpcc.sessions_part",
		"DROP TABLE IF EXISTS yagpcc._yagpcc_meta",
		"-- migration 1 (init) down",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("down dump missing %q", want)
		}
	}
	if strings.Contains(out, "CREATE TABLE") {
		t.Error("down dump should not contain CREATE TABLE")
	}
}

func TestDumpMigration_Equal(t *testing.T) {
	out, err := DumpMigration(1, 1, DumpOptions{RetentionDays: 30})
	if err != nil {
		t.Fatalf("DumpMigration: %v", err)
	}
	if out != "" {
		t.Errorf("expected empty output for from==to, got %q", out)
	}
}

func TestDumpMigration_TargetTooHigh(t *testing.T) {
	_, err := DumpMigration(0, ExpectedSchemaVersion+5, DumpOptions{RetentionDays: 30})
	if err == nil {
		t.Fatal("expected error for target version above embedded migrations")
	}
	if !strings.Contains(err.Error(), "highest embedded migration") {
		t.Errorf("error should mention embedded migrations bound, got %v", err)
	}
}

func TestDumpMigration_SourceTooHigh(t *testing.T) {
	_, err := DumpMigration(ExpectedSchemaVersion+5, 0, DumpOptions{RetentionDays: 30})
	if err == nil {
		t.Fatal("expected error for source version above embedded migrations")
	}
}

func TestDumpMigration_NegativeArgs(t *testing.T) {
	if _, err := DumpMigration(-1, 1, DumpOptions{RetentionDays: 30}); err == nil {
		t.Fatal("expected error for negative from")
	}
	if _, err := DumpMigration(1, -1, DumpOptions{RetentionDays: 30}); err == nil {
		t.Fatal("expected error for negative to")
	}
}

func TestDumpMigration_InvalidRetention(t *testing.T) {
	if _, err := DumpMigration(0, 1, DumpOptions{RetentionDays: 0}); err == nil {
		t.Fatal("expected error for RetentionDays=0")
	}
}

func TestDumpMigration_NoConnRequired(t *testing.T) {
	// Sanity check: ensure dump APIs don't take a MigrationConn parameter and
	// can run in isolation. If the signature changes to require a connection
	// this test will fail to compile — that is the point.
	if _, err := DumpSchema(DumpOptions{RetentionDays: 1}); err != nil {
		t.Fatalf("DumpSchema with no connection failed: %v", err)
	}
	if _, err := DumpMigration(0, 1, DumpOptions{RetentionDays: 1}); err != nil {
		t.Fatalf("DumpMigration with no connection failed: %v", err)
	}
}
