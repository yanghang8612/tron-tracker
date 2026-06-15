//go:build integration

package database

import (
	"testing"
	"time"

	"tron-tracker/database/models"
)

func seedUserStats(t *testing.T, db *RawDB, prefix, date string, rows []*models.UserStatistic) {
	table := prefix + date
	if err := db.db.Table(table).AutoMigrate(&models.UserStatistic{}); err != nil {
		t.Fatalf("migrate %s: %v", table, err)
	}
	if err := db.db.Table(table).Create(rows).Error; err != nil {
		t.Fatalf("seed %s: %v", table, err)
	}
}

// GetTransferStatisticByDateDays must read the from_stats_/to_stats_ table chosen
// by direction, merge an address across the whole window, and drop the "total"
// summary row (which is the network aggregate, not a real address).
func TestGetTransferStatisticByDateDays_DirectionAndMerge(t *testing.T) {
	db := newFlushTestDB(t)
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.Local)

	// from side: A spans both days (must merge to 17); a "total" row must be skipped.
	seedUserStats(t, db, "from_stats_", "250101", []*models.UserStatistic{
		{Address: "A", TRXTotal: 10},
		{Address: "B", TRXTotal: 5},
		{Address: "total", TRXTotal: 999},
	})
	seedUserStats(t, db, "from_stats_", "250102", []*models.UserStatistic{
		{Address: "A", TRXTotal: 7},
	})
	// to side: a distinct address, to prove direction selects the right table.
	seedUserStats(t, db, "to_stats_", "250101", []*models.UserStatistic{
		{Address: "X", TRXTotal: 100},
		{Address: "total", TRXTotal: 999},
	})

	from := db.GetTransferStatisticByDateDays(start, 2, "from")
	if from["total"] != nil {
		t.Fatal("from: total summary row must be excluded")
	}
	if from["A"] == nil || from["A"].TRXTotal != 17 {
		t.Fatalf("from: A trx_total = %v, want 17 (merged across 2 days)", from["A"])
	}
	if from["B"] == nil || from["B"].TRXTotal != 5 {
		t.Fatalf("from: B trx_total wrong: %v", from["B"])
	}
	if from["X"] != nil {
		t.Fatal("from: must not read to_stats rows")
	}

	to := db.GetTransferStatisticByDateDays(start, 2, "to")
	if to["X"] == nil || to["X"].TRXTotal != 100 {
		t.Fatalf("to: X trx_total = %v, want 100", to["X"])
	}
	if to["A"] != nil {
		t.Fatal("to: must not read from_stats rows")
	}
	if to["total"] != nil {
		t.Fatal("to: total summary row must be excluded")
	}
}
