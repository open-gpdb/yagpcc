package gp

import (
	"context"
	"fmt"
)

const (
	cancelQuery = `
	SELECT
		pg_cancel_backend(pid, 'cancelled by admin from console') as "Result"
	FROM
    	pg_stat_activity
	WHERE
		sess_id = :ssid
	`

	killQuery = `
	SELECT
    	pg_terminate_backend(pid, 'cancelled by admin from console') as "Result"
	FROM
    	pg_stat_activity
	WHERE
		sess_id = :ssid
	`

	moveToResourceGroupQuery = `
	SELECT
    	gp_toolkit.pg_resgroup_move_query(pid, :resgroup) as Result, pid as Pid
	FROM
    	pg_stat_activity
	WHERE
		sess_id = :ssid
	`
)

func CancelQuery(ctx context.Context, ssid int, kill bool) error {
	type cancelResult struct {
		Result bool `db:"Result"`
	}
	if db == nil {
		return fmt.Errorf("fail to connecto to database - not initialized")
	}
	params := map[string]interface{}{
		"ssid": ssid,
	}
	q := cancelQuery
	if kill {
		q = killQuery
	}
	rows, err := db.NamedExecQuery(ctx, q, params)
	if err != nil {
		return err
	}
	for rows.Next() {
		p := &cancelResult{}
		err = rows.StructScan(p)
		if err != nil {
			return err
		}
		if !p.Result {
			if kill {
				return fmt.Errorf("fail to terminate session %d after waiting 30 seconds", ssid)
			} else {
				return fmt.Errorf("try to cancel queries for session %d but failed", ssid)
			}
		}
	}
	err = rows.Close()
	if err != nil {
		return err
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	return nil
}

func MoveQueryToResourceGroup(ctx context.Context, ssid int, resgroup string) error {
	type pid struct {
		Result bool `db:"result"`
		Pid    int  `db:"pid"`
	}
	if db == nil {
		return fmt.Errorf("fail to connecto to database - not initialized")
	}
	params := map[string]interface{}{
		"ssid":     ssid,
		"resgroup": resgroup,
	}
	rows, err := db.NamedExecQuery(ctx, moveToResourceGroupQuery, params)
	if err != nil {
		return err
	}
	failedPids := make([]int, 0)
	for rows.Next() {
		p2 := &pid{}
		err = rows.StructScan(p2)
		if err != nil {
			return err
		}
		if !p2.Result {
			failedPids = append(failedPids, p2.Pid)
		}
	}
	err = rows.Close()
	if err != nil {
		return err
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	if len(failedPids) > 0 {
		return fmt.Errorf("try to move queries for session %d to resource group %q but failed on pids: %d", ssid, resgroup, failedPids)
	}
	return nil
}
