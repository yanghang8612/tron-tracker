//go:build integration

// Integration test for the one invariant unit tests can't cover: gorm Save
// back-filling the auto-increment ID into the passed *models.Charger, and the
// update path producing no duplicate rows. Run with:
//
//	go test ./database/ -tags=integration -run TestChargerStoreFlushIDBackfill -v
//
// Requires a local MySQL at 127.0.0.1:3306 (root, empty password). Uses a
// throwaway database `charger_itest` that is dropped on cleanup; never touches
// the production-sized local DB.
package database

import (
	"testing"

	"go.uber.org/zap"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"tron-tracker/database/models"
)

func TestChargerStoreFlushIDBackfill(t *testing.T) {
	const base = "root:@tcp(127.0.0.1:3306)/"
	root, err := gorm.Open(mysql.Open(base+"?charset=utf8mb4&parseTime=True&loc=Local"), &gorm.Config{Logger: logger.Discard})
	if err != nil {
		t.Skipf("no local mysql available: %v", err)
	}
	if err := root.Exec("CREATE DATABASE IF NOT EXISTS charger_itest").Error; err != nil {
		t.Skipf("cannot create test database: %v", err)
	}
	t.Cleanup(func() { root.Exec("DROP DATABASE IF EXISTS charger_itest") })

	db, err := gorm.Open(mysql.Open(base+"charger_itest?charset=utf8mb4&parseTime=True&loc=Local"), &gorm.Config{Logger: logger.Discard})
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	if err := db.AutoMigrate(&models.Charger{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	s := NewChargerStore(db, zap.NewNop().Sugar())
	c := addrN(1)
	ex := addrN(100)
	key, _ := decodeAddr(c)

	count := func() int64 {
		var n int64
		db.Model(&models.Charger{}).Count(&n)
		return n
	}

	// 1. new charger -> flush -> exactly 1 row, id back-filled into compactCharger
	s.ApplyCharge(c, ex, true, "Binance")
	s.FlushDirty()
	if n := count(); n != 1 {
		t.Fatalf("after first flush: expected 1 row, got %d", n)
	}
	id := s.chargers[key].id
	if id == 0 {
		t.Fatalf("auto-increment id was not back-filled into compactCharger")
	}
	var row models.Charger
	db.First(&row)
	if uint32(row.ID) != id {
		t.Fatalf("DB id %d != back-filled id %d", row.ID, id)
	}

	// 2. mutate the same charger (first unknown address -> markDirty) -> flush ->
	//    still 1 row (UPDATE path, no duplicate insert), id unchanged
	s.ApplyCharge(c, addrN(2), false, "")
	if s.DirtyLen() == 0 {
		t.Fatalf("expected the mutation to mark the charger dirty")
	}
	s.FlushDirty()
	if n := count(); n != 1 {
		t.Fatalf("after update flush: expected still 1 row, got %d (duplicate insert!)", n)
	}
	if s.chargers[key].id != id {
		t.Fatalf("id changed on update: %d -> %d", id, s.chargers[key].id)
	}
}
