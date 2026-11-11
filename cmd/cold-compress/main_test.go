package main

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSecondaryRoundTrip(t *testing.T) {
	ddl := "CREATE TABLE `t` (\n `id` bigint unsigned NOT NULL,\n `x` varchar(80) DEFAULT 'a,b)\\'c',\n PRIMARY KEY (`id`),\n UNIQUE KEY `u``x` (`x`(20) DESC),\n KEY `expr` ((cast(`x` as unsigned))),\n KEY `multi` (`id`,`x`) COMMENT 'q,(z)'\n) ENGINE=InnoDB"
	names, defs, e := secondary(ddl)
	if e != nil {
		t.Fatal(e)
	}
	if len(defs) != 3 || names[0] != "u`x" || names[1] != "expr" || names[2] != "multi" {
		t.Fatalf("%q %q", names, defs)
	}
	if defs[0] != "UNIQUE KEY `u``x` (`x`(20) DESC)" || defs[1] != "KEY `expr` ((cast(`x` as unsigned)))" || defs[2] != "KEY `multi` (`id`,`x`) COMMENT 'q,(z)'" {
		t.Fatal(defs)
	}
	for _, bad := range []string{"CREATE TABLE x", "CREATE TABLE x (`id` int, KEY `x (`id`))", "CREATE TABLE x (`id` int, FULLTEXT KEY `k` (`id`))"} {
		if _, _, e = secondary(bad); e == nil {
			t.Fatalf("accepted %s", bad)
		}
	}
}
func TestColdDateBoundary(t *testing.T) {
	cutoff := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name string
		ok   bool
	}{{"transactions_261011", true}, {"transactions_261012", false}, {"from_stats_261011", true}, {"to_stats_261011", true}, {"user_token_stats_261011", true}, {"transactions_261332", false}, {"transactions_261011_extra", false}, {"chargers_261011", false}, {"transactions_26101", false}} {
		if validTable(tc.name, cutoff) != tc.ok {
			t.Fatal(tc)
		}
	}
	for _, h := range []int{0, 1, 2, 23} {
		if !window(time.Date(2026, 11, 11, h, 0, 0, 0, time.UTC)) {
			t.Fatal(h)
		}
	}
	for _, h := range []int{3, 8, 12, 22} {
		if window(time.Date(2026, 11, 11, h, 0, 0, 0, time.UTC)) {
			t.Fatal(h)
		}
	}
}
func TestFingerprintBoundariesAndNull(t *testing.T) {
	digest := func(v ...[]byte) string {
		h := sha256.New()
		for _, x := range v {
			put(h, x)
		}
		return hex.EncodeToString(h.Sum(nil))
	}
	if digest(nil) == digest([]byte{}) {
		t.Fatal("NULL confused with empty")
	}
	if digest([]byte("ab"), []byte("c")) == digest([]byte("a"), []byte("bc")) {
		t.Fatal("field boundaries lost")
	}
	if same(fingerprint{}, fingerprint{}) {
		t.Fatal("missing digest accepted")
	}
}
func TestReportIdentityAndAtomicCheckpoint(t *testing.T) {
	oldWork, oldDB := workDir, jobDB
	defer func() { workDir = oldWork; jobDB = oldDB }()
	workDir = t.TempDir()
	jobDB = "own_job"
	if e := os.Mkdir(filepath.Join(workDir, "tables"), 0700); e != nil {
		t.Fatal(e)
	}
	r, e := load("transactions_260601")
	if e != nil {
		t.Fatal(e)
	}
	r.Source = fingerprint{Rows: 4, SHA256: "checked"}
	path := filepath.Join(workDir, "tables", r.Table+".json")
	if e = writeJSON(path, r); e != nil {
		t.Fatal(e)
	}
	got, e := load(r.Table)
	if e != nil || got.Source.Rows != 4 {
		t.Fatalf("%v %v", got, e)
	}
	r.ShadowDB = "unrelated_production"
	if e = writeJSON(path, r); e != nil {
		t.Fatal(e)
	}
	if _, e = load(r.Table); e == nil {
		t.Fatal("accepted foreign shadow database")
	}
}

func TestAutoIncrementUsesCreateOption(t *testing.T) {
	s := "CREATE TABLE `t` (\n `id` bigint NOT NULL AUTO_INCREMENT,\n `v` varchar(10) COMMENT 'AUTO_INCREMENT=9'\n) ENGINE=InnoDB AUTO_INCREMENT=12345 DEFAULT CHARSET=utf8mb4"
	v, e := nextAutoIncrement(s)
	if e != nil || v != 12345 {
		t.Fatalf("%d %v", v, e)
	}
	v, e = nextAutoIncrement("CREATE TABLE `t` (\n `id` int\n) ENGINE=InnoDB")
	if e != nil || v != 0 {
		t.Fatalf("%d %v", v, e)
	}
}
