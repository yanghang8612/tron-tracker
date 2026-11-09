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
	"encoding/binary"
	"errors"
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

	// A failed second batch leaves just its rows pending, and a retry does
	// not re-insert the successful first batch.
	for i := range chargerWriteBatch + 7 {
		var payload [20]byte
		binary.BigEndian.PutUint32(payload[:], uint32(i))
		s.ApplyCharge(encodeAddr(payload), ex, true, "Binance")
	}
	injected := errors.New("simulated charger write failure")
	calls := 0
	const hook = "test:charger-write"
	callback := db.Callback().Create()
	if err := callback.Before("gorm:create").Register(hook, func(tx *gorm.DB) {
		if tx.Statement.Table == "chargers" {
			calls++
			if calls == 2 {
				tx.AddError(injected)
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	err = s.doFlushLocked()
	callback.Remove(hook)
	if !errors.Is(err, injected) || count() != chargerWriteBatch+1 || s.DirtyLen() != 7 {
		t.Fatalf("failed batch: error=%v rows=%d pending=%d", err, count(), s.DirtyLen())
	}
	s.FlushDirty()
	if count() != chargerWriteBatch+8 || s.DirtyLen() != 0 {
		t.Fatalf("retry: rows=%d pending=%d", count(), s.DirtyLen())
	}

	// The streaming loader reads nullable legacy fields as zero values and
	// keeps the last ID for duplicate addresses, as the old loader did.
	if err := db.Exec("INSERT INTO chargers(address, exchange_name, backup_address, is_fake) VALUES (?, 'Legacy', NULL, NULL), (NULL, NULL, NULL, NULL)", c).Error; err != nil {
		t.Fatal(err)
	}
	reloaded := NewChargerStore(db, zap.NewNop().Sugar())
	if err := reloaded.streamChargersLocked(); err != nil {
		t.Fatal(err)
	}
	if reloaded.Len() != chargerWriteBatch+8 || reloaded.skipped != 1 {
		t.Fatalf("streamed chargers=%d skipped=%d", reloaded.Len(), reloaded.skipped)
	}
	if view, ok := reloaded.Get(c); !ok || view.ExchangeName != "Legacy" || view.BackupAddress != "" || view.IsFake {
		t.Fatalf("duplicate row did not replace earlier state: %+v, %t", view, ok)
	}
}
