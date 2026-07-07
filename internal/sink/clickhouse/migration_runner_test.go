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

// fakeConn implements MigrationConn for tests. It records every Exec call and
// answers Select calls from a programmable response queue.
type fakeConn struct {
	execLog []execCall
	// execErrAt[index] returns this error when the index-th Exec is called.
	execErrAt map[int]error

	// selectResponses pops one entry per Select call. The matched entry's
	// values are copied into dest via reflection.
	selectResponses []selectResponse
	selectIdx       int
}

type execCall struct {
	query string
	args  []any
}

type selectResponse struct {
	rows []any
	err  error
}

func (f *fakeConn) Exec(_ context.Context, query string, args ...any) error {
	idx := len(f.execLog)
	f.execLog = append(f.execLog, execCall{query: query, args: append([]any(nil), args...)})
	if err, ok := f.execErrAt[idx]; ok {
		return err
	}
	return nil
}

func (f *fakeConn) Select(_ context.Context, dest any, _ string, _ ...any) error {
	if f.selectIdx >= len(f.selectResponses) {
		return errors.New("fakeConn: no select response queued")
	}
	resp := f.selectResponses[f.selectIdx]
	f.selectIdx++
	if resp.err != nil {
		return resp.err
	}

	dv := reflect.ValueOf(dest)
	if dv.Kind() != reflect.Pointer || dv.Elem().Kind() != reflect.Slice {
		return errors.New("fakeConn: dest must be pointer to slice")
	}
	slice := dv.Elem()
	elemType := slice.Type().Elem()
	out := reflect.MakeSlice(slice.Type(), 0, len(resp.rows))
	for _, r := range resp.rows {
		rv := reflect.ValueOf(r)
		if !rv.Type().ConvertibleTo(elemType) {
			return errors.New("fakeConn: row type does not match dest slice element")
		}
		out = reflect.Append(out, rv.Convert(elemType))
	}
	slice.Set(out)
	return nil
}

func metaExistsResp(exists bool) selectResponse {
	var c uint64
	if exists {
		c = 1
	}
	return selectResponse{rows: []any{metaTableExistsRow{C: c}}}
}

func maxVersionResp(v int32) selectResponse {
	return selectResponse{rows: []any{maxVersionRow{V: v}}}
}

func emptyMaxVersionResp() selectResponse {
	return selectResponse{rows: nil}
}

func findExec(log []execCall, contains string) int {
	for i, c := range log {
		if strings.Contains(c.query, contains) {
			return i
		}
	}
	return -1
}

func countExec(log []execCall, contains string) int {
	n := 0
	for _, c := range log {
		if strings.Contains(c.query, contains) {
			n++
		}
	}
	return n
}

func TestGetCurrentVersion_TableMissing(t *testing.T) {
	conn := &fakeConn{
		selectResponses: []selectResponse{metaExistsResp(false)},
	}
	v, err := GetCurrentVersion(context.Background(), conn)
	if err != nil {
		t.Fatalf("GetCurrentVersion: %v", err)
	}
	if v != 0 {
		t.Errorf("got version %d, want 0", v)
	}
	if conn.selectIdx != 1 {
		t.Errorf("expected one Select call, got %d", conn.selectIdx)
	}
}

func TestGetCurrentVersion_TableExistsEmpty(t *testing.T) {
	conn := &fakeConn{
		selectResponses: []selectResponse{
			metaExistsResp(true),
			emptyMaxVersionResp(),
		},
	}
	v, err := GetCurrentVersion(context.Background(), conn)
	if err != nil {
		t.Fatalf("GetCurrentVersion: %v", err)
	}
	if v != 0 {
		t.Errorf("got version %d, want 0", v)
	}
}

func TestGetCurrentVersion_TableHasRows(t *testing.T) {
	conn := &fakeConn{
		selectResponses: []selectResponse{
			metaExistsResp(true),
			maxVersionResp(3),
		},
	}
	v, err := GetCurrentVersion(context.Background(), conn)
	if err != nil {
		t.Fatalf("GetCurrentVersion: %v", err)
	}
	if v != 3 {
		t.Errorf("got version %d, want 3", v)
	}
}

func TestGetCurrentVersion_ExistsCheckError(t *testing.T) {
	wantErr := errors.New("boom")
	conn := &fakeConn{
		selectResponses: []selectResponse{{err: wantErr}},
	}
	_, err := GetCurrentVersion(context.Background(), conn)
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped %v, got %v", wantErr, err)
	}
}

func TestGetCurrentVersion_SelectMaxError(t *testing.T) {
	wantErr := errors.New("kaboom")
	conn := &fakeConn{
		selectResponses: []selectResponse{
			metaExistsResp(true),
			{err: wantErr},
		},
	}
	_, err := GetCurrentVersion(context.Background(), conn)
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped %v, got %v", wantErr, err)
	}
}

func TestApplyMigrations_FreshDatabase(t *testing.T) {
	conn := &fakeConn{
		selectResponses: []selectResponse{
			// GetCurrentVersion: meta table does not exist yet (the bootstrap
			// CREATE happens via Exec, but our existence check still queries
			// system.tables — the fake reports false here).
			metaExistsResp(false),
		},
	}
	err := ApplyMigrations(context.Background(), conn, MigrateOptions{
		RetentionDays: 30,
		YagpccVersion: "1.2.3",
	})
	if err != nil {
		t.Fatalf("ApplyMigrations: %v", err)
	}

	// Bootstrap DDL must run.
	if findExec(conn.execLog, "CREATE DATABASE IF NOT EXISTS yagpcc") < 0 {
		t.Error("missing CREATE DATABASE bootstrap")
	}
	if findExec(conn.execLog, "CREATE TABLE IF NOT EXISTS yagpcc._yagpcc_meta") < 0 {
		t.Error("missing meta table bootstrap")
	}

	// Each of the four core tables must be created.
	for _, table := range []string{
		"yagpcc.query_events",
		"yagpcc.aggregated_metrics",
		"yagpcc.session_snapshots",
	} {
		if countExec(conn.execLog, "CREATE TABLE IF NOT EXISTS "+table) == 0 {
			t.Errorf("missing CREATE TABLE for %s", table)
		}
	}

	// The retention placeholder must be rendered.
	if findExec(conn.execLog, "INTERVAL 30 DAY") < 0 {
		t.Error("retention_days placeholder was not rendered")
	}
	if findExec(conn.execLog, "{{") >= 0 {
		t.Error("rendered DDL still contains template syntax")
	}

	// Final INSERT into _yagpcc_meta records the applied version.
	insertIdx := findExec(conn.execLog, "INSERT INTO yagpcc._yagpcc_meta")
	if insertIdx < 0 {
		t.Fatal("missing INSERT INTO yagpcc._yagpcc_meta")
	}
	args := conn.execLog[insertIdx].args
	if len(args) != 3 {
		t.Fatalf("insert args = %v, want 3", args)
	}
	if v, ok := args[0].(int32); !ok || v != int32(ExpectedSchemaVersion) {
		t.Errorf("insert version arg = %v, want %d", args[0], ExpectedSchemaVersion)
	}
	if v, ok := args[1].(string); !ok || v != "1.2.3" {
		t.Errorf("insert yagpcc_version arg = %v, want 1.2.3", args[1])
	}
	if v, ok := args[2].(string); !ok || v != "up" {
		t.Errorf("insert direction arg = %v, want up", args[2])
	}
}

func TestApplyMigrations_AlreadyAtExpectedVersion(t *testing.T) {
	conn := &fakeConn{
		selectResponses: []selectResponse{
			metaExistsResp(true),
			maxVersionResp(int32(ExpectedSchemaVersion)),
		},
	}
	err := ApplyMigrations(context.Background(), conn, MigrateOptions{RetentionDays: 30})
	if err != nil {
		t.Fatalf("ApplyMigrations: %v", err)
	}

	// No migration up DDL nor INSERT into meta should run.
	if findExec(conn.execLog, "INSERT INTO yagpcc._yagpcc_meta") >= 0 {
		t.Error("unexpected INSERT into _yagpcc_meta when already current")
	}
	if findExec(conn.execLog, "yagpcc.query_events") >= 0 {
		t.Error("unexpected CREATE TABLE for query_events when already current")
	}
}

func TestApplyMigrations_DowngradeForbidden(t *testing.T) {
	conn := &fakeConn{
		selectResponses: []selectResponse{
			metaExistsResp(true),
			maxVersionResp(int32(ExpectedSchemaVersion + 5)),
		},
	}
	err := ApplyMigrations(context.Background(), conn, MigrateOptions{RetentionDays: 30})
	if err == nil {
		t.Fatal("expected error for downgrade scenario")
	}
	if !strings.Contains(err.Error(), "downgrade") {
		t.Errorf("error %q should mention downgrade", err.Error())
	}
}

func TestApplyMigrations_RetentionDaysRequired(t *testing.T) {
	conn := &fakeConn{}
	err := ApplyMigrations(context.Background(), conn, MigrateOptions{RetentionDays: 0})
	if err == nil {
		t.Fatal("expected error for missing retention_days")
	}
	if len(conn.execLog) != 0 {
		t.Errorf("no Exec should run when validation fails, got %d", len(conn.execLog))
	}
}

func TestApplyMigrations_MidMigrationFailure(t *testing.T) {
	// Inject error on the third Exec — that lands inside the rendered up DDL
	// (after CREATE DATABASE and meta bootstrap). The runner must propagate
	// the error wrapped with the migration version.
	wantErr := errors.New("ch: server panic")
	conn := &fakeConn{
		selectResponses: []selectResponse{metaExistsResp(false)},
		execErrAt:       map[int]error{2: wantErr},
	}
	err := ApplyMigrations(context.Background(), conn, MigrateOptions{RetentionDays: 30})
	if err == nil {
		t.Fatal("expected error from mid-migration failure")
	}
	if !errors.Is(err, wantErr) {
		t.Errorf("expected wrapped %v, got %v", wantErr, err)
	}
	if !strings.Contains(err.Error(), "migration 1") {
		t.Errorf("error %q should reference migration 1", err.Error())
	}
	// The post-migration INSERT must NOT run when DDL failed.
	if findExec(conn.execLog, "INSERT INTO yagpcc._yagpcc_meta") >= 0 {
		t.Error("INSERT into meta must not run after mid-migration failure")
	}
}

func TestApplyMigrations_DefaultsYagpccVersion(t *testing.T) {
	conn := &fakeConn{
		selectResponses: []selectResponse{metaExistsResp(false)},
	}
	err := ApplyMigrations(context.Background(), conn, MigrateOptions{
		RetentionDays: 14,
		// YagpccVersion intentionally empty.
	})
	if err != nil {
		t.Fatalf("ApplyMigrations: %v", err)
	}
	idx := findExec(conn.execLog, "INSERT INTO yagpcc._yagpcc_meta")
	if idx < 0 {
		t.Fatal("missing INSERT into meta")
	}
	if v, ok := conn.execLog[idx].args[1].(string); !ok || v != "unknown" {
		t.Errorf("yagpcc_version arg = %v, want unknown", conn.execLog[idx].args[1])
	}
	if !strings.Contains(strings.Join(execQueries(conn.execLog), "\n"), "INTERVAL 14 DAY") {
		t.Error("retention_days=14 not rendered into DDL")
	}
}

func execQueries(log []execCall) []string {
	out := make([]string, len(log))
	for i, c := range log {
		out[i] = c.query
	}
	return out
}
