package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"
)

// All test writes stay in two newly created private schemas. The real source
// schema is never a test destination, including for the expected mismatch case.
func runSelfTest() error {
	base := jobDB
	sourceDB = base + "_test_src"
	jobDB = base + "_test_work"
	for _, name := range []string{sourceDB, jobDB} {
		var n int
		if e := db.QueryRowContext(root, "SELECT COUNT(*) FROM information_schema.schemata WHERE SCHEMA_NAME=?", name).Scan(&n); e != nil {
			return e
		}
		if n != 0 {
			return fmt.Errorf("self-test schema %s already exists; refusing to overwrite", name)
		}
	}
	for _, name := range []string{sourceDB, jobDB} {
		if _, e := db.ExecContext(root, "CREATE DATABASE "+qi(name)+" CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"); e != nil {
			return e
		}
	}
	if _, e := db.ExecContext(root, "CREATE TABLE "+qt(sourceDB, "meta")+" ("+qi("key")+" varchar(40) PRIMARY KEY,val varchar(10))"); e != nil {
		return e
	}
	for _, key := range []string{"tracking_date", "flushed_date", "counted_date", "counted_week"} {
		if _, e := db.ExecContext(root, "INSERT INTO "+qt(sourceDB, "meta")+" VALUES (?,?)", key, time.Now().UTC().AddDate(0, 0, -2).Format("060102")); e != nil {
			return e
		}
	}
	ctl = &controller{}
	ctl.sample()
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-root.Done():
				return
			case <-t.C:
				ctl.sample()
			}
		}
	}()
	if e := ctl.gate(); e != nil {
		return e
	}
	sum = summary{Status: "self_test", StartedUTC: stamp(), SourceDatabase: sourceDB, JobDatabase: jobDB, Planned: 2}
	seed := func(table string) error {
		_, e := db.ExecContext(root, "CREATE TABLE "+qt(sourceDB, table)+" (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,v varchar(200) DEFAULT NULL,amount varchar(80) DEFAULT NULL, KEY idx_v (v(20) DESC), KEY idx_amount ((CAST(amount AS UNSIGNED)))) ENGINE=InnoDB ROW_FORMAT=DYNAMIC")
		if e != nil {
			return e
		}
		vals := []string{}
		args := []any{}
		for i := 1; i <= 1000; i++ {
			vals = append(vals, "(?,?,?)")
			var v any = strings.Repeat("sample,'),", 18)
			if i%3 == 0 {
				v = nil
			}
			args = append(args, i, v, fmt.Sprint(123456789+i))
		}
		_, e = db.ExecContext(root, "INSERT INTO "+qt(sourceDB, table)+" (id,v,amount) VALUES "+strings.Join(vals, ","), args...)
		return e
	}
	first := "transactions_260602"
	if e := seed(first); e != nil {
		return e
	}
	r, e := load(first)
	if e != nil {
		return e
	}
	if e = migrate(r); e != nil {
		return fmt.Errorf("full migration self-test: %w", e)
	}
	if r.Status != "complete" || !same(r.Source, r.Promoted) {
		return fmt.Errorf("self-test promotion incomplete")
	}
	// Simulate a crash after DROP succeeded but before the completed checkpoint.
	r.Status = "running"
	r.Phase = "verify_promoted"
	if e = migrate(r); e != nil || r.Status != "complete" {
		return fmt.Errorf("post-drop recovery self-test: %v", e)
	}
	second := "transactions_260603"
	if e = seed(second); e != nil {
		return e
	}
	r, e = load(second)
	if e != nil {
		return e
	}
	r.BeforeBytes, _, r.AutoIncrement, e = size(sourceDB, second)
	if e != nil {
		return e
	}
	r.OriginalDDL, e = ddl(root, db, sourceDB, second)
	if e != nil {
		return e
	}
	r.SchemaSHA, e = schema(sourceDB, second)
	if e != nil {
		return e
	}
	r.Source, e = hashTable(db, sourceDB, second)
	if e != nil {
		return e
	}
	if _, e = db.ExecContext(root, "CREATE TABLE "+qt(jobDB, second)+" LIKE "+qt(sourceDB, second)); e != nil {
		return e
	}
	if _, e = db.ExecContext(root, "INSERT INTO "+qt(jobDB, second)+" SELECT * FROM "+qt(sourceDB, second)); e != nil {
		return e
	}
	if e = runDDL("ALTER TABLE " + qt(jobDB, second) + " ROW_FORMAT=COMPRESSED,KEY_BLOCK_SIZE=8"); e != nil {
		return e
	}
	if _, e = db.ExecContext(root, "UPDATE "+qt(sourceDB, second)+" SET v='changed after copy' WHERE id=1"); e != nil {
		return e
	}
	r.Phase = "ready_for_cutover"
	e = migrate(r)
	if e == nil || !strings.Contains(e.Error(), "source changed during migration") {
		return fmt.Errorf("source-write rejection self-test: %v", e)
	}
	var value string
	if e = db.QueryRowContext(root, "SELECT v FROM "+qt(sourceDB, second)+" WHERE id=1").Scan(&value); e != nil || value != "changed after copy" {
		return fmt.Errorf("changed source was not preserved: %v", e)
	}
	old, e := exists(jobDB, r.Backup)
	if e != nil || old {
		return fmt.Errorf("mismatch unexpectedly promoted a table: %v", e)
	}
	for _, name := range []string{sourceDB, jobDB} {
		if _, e = db.ExecContext(context.Background(), "DROP DATABASE "+qi(name)); e != nil {
			return e
		}
	}
	sum.Status = "self_test_complete"
	sum.Error = ""
	sum.Phase = "copy, schema, locked rename, recovery and mismatch preservation passed"
	if e = saveSummary(); e != nil {
		return e
	}
	log.Print("SELF_TEST_PASSED")
	return nil
}
