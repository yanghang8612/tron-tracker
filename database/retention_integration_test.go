//go:build integration

package database

import (
	"errors"
	"strings"
	"testing"
	"time"

	"tron-tracker/database/models"

	"gorm.io/gorm"
)

func retentionTestDB(t *testing.T) *RawDB {
	t.Helper()
	db := newFlushTestDB(t)
	db.trackingDate = "261028"
	db.flushedDate = "261027"
	db.countedDate = "261027"
	db.countedWeek = "261026"
	if err := db.db.AutoMigrate(&models.Meta{}); err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]string{
		models.TrackingDateKey: db.trackingDate, models.FlushedDateKey: db.flushedDate,
		models.CountedDateKey: db.countedDate, models.CountedWeekKey: db.countedWeek,
	} {
		if err := db.db.Create(&models.Meta{Key: key, Val: value}).Error; err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func createRetentionTestTable(t *testing.T, db *RawDB, name string) {
	t.Helper()
	if err := db.db.Exec("CREATE TABLE `" + name + "` (id BIGINT PRIMARY KEY)").Error; err != nil {
		t.Fatal(err)
	}
	db.isTableMigrated[name] = true
}

func TestDropStaleDailyTablesKeepsBoundaryAndUnrelatedTables(t *testing.T) {
	db := retentionTestDB(t)
	var expired, kept []string
	for _, prefix := range []string{"transactions_", "from_stats_", "to_stats_", "user_token_stats_"} {
		expired = append(expired, prefix+"250101")
		kept = append(kept, prefix+"250102", prefix+"250103")
	}
	kept = append(kept, "transactions_250230", "transactions_250101_backup",
		"transactions_backup_250101", "transactionsX250101", "token_stats_250101")
	for _, name := range append(append([]string{}, expired...), kept...) {
		createRetentionTestTable(t, db, name)
	}
	cutoff := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
	if err := db.DropStaleStatsTables(cutoff); err != nil {
		t.Fatal(err)
	}
	for _, name := range expired {
		if db.db.Migrator().HasTable(name) || db.isTableMigrated[name] {
			t.Errorf("expired table or migration cache entry retained: %s", name)
		}
	}
	for _, name := range kept {
		if !db.db.Migrator().HasTable(name) || !db.isTableMigrated[name] {
			t.Errorf("protected table or migration cache entry lost: %s", name)
		}
	}
	if err := db.DropStaleStatsTables(cutoff); err != nil {
		t.Fatalf("repeated cleanup: %v", err)
	}
}

func TestDropStaleDailyTablesProtectsCatchupAndRejectsFutureCutoff(t *testing.T) {
	db := retentionTestDB(t)
	db.flushedDate = "250101"
	if err := db.db.Model(&models.Meta{}).Where("`key` = ?", models.FlushedDateKey).Update("val", db.flushedDate).Error; err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"transactions_250101", "transactions_250102", "transactions_250103"} {
		createRetentionTestTable(t, db, name)
	}
	if err := db.DropStaleStatsTables(time.Now().AddDate(0, 0, 1)); err == nil {
		t.Fatal("future cutoff must fail")
	}
	if !db.db.Migrator().HasTable("transactions_250101") {
		t.Fatal("future cutoff deleted a table")
	}
	if err := db.DropStaleStatsTables(time.Date(2025, 1, 4, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	if db.db.Migrator().HasTable("transactions_250101") {
		t.Fatal("already processed expired day was retained")
	}
	for _, name := range []string{"transactions_250102", "transactions_250103"} {
		if !db.db.Migrator().HasTable(name) {
			t.Fatalf("deleted an unprocessed source table: %s", name)
		}
	}
}

func TestDropStaleDailyTablesReportsDropFailure(t *testing.T) {
	db := retentionTestDB(t)
	name := "transactions_250101"
	createRetentionTestTable(t, db, name)
	dropErr := errors.New("injected DROP failure")
	if err := db.db.Callback().Raw().Before("gorm:raw").Register("retention_test_drop_failure", func(tx *gorm.DB) {
		if strings.HasPrefix(tx.Statement.SQL.String(), "DROP TABLE") {
			tx.AddError(dropErr)
		}
	}); err != nil {
		t.Fatal(err)
	}
	err := db.DropStaleStatsTables(time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC))
	if !errors.Is(err, dropErr) {
		t.Fatalf("cleanup error = %v, want DROP failure", err)
	}
	if !db.db.Migrator().HasTable(name) || !db.isTableMigrated[name] {
		t.Fatal("failed DROP lost table or migration cache entry")
	}
}
