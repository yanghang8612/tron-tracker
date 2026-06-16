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

// GetTopTransferStatsByDateDays must take each day's top-n by the metric (DB-side
// ORDER BY LIMIT, not a full scan), merge an address across days, exclude the
// "total" row, and select the from_/to_ table by direction. An address that
// never makes a daily top-n is intentionally absent (documented approximation).
func TestGetTopTransferStatsByDateDays(t *testing.T) {
	db := newFlushTestDB(t)
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.Local)

	seedUserStats(t, db, "from_stats_", "250101", []*models.UserStatistic{
		{Address: "A", TRXTotal: 50},
		{Address: "B", TRXTotal: 30},
		{Address: "C", TRXTotal: 10}, // never in a daily top-2
		{Address: "total", TRXTotal: 90},
	})
	seedUserStats(t, db, "from_stats_", "250102", []*models.UserStatistic{
		{Address: "A", TRXTotal: 5}, // merges with day 1
		{Address: "D", TRXTotal: 40},
	})
	// to side seeded differently so we can prove direction selects to_stats_.
	seedUserStats(t, db, "to_stats_", "250101", []*models.UserStatistic{
		{Address: "X", TRXTotal: 99},
	})

	got := db.GetTopTransferStatsByDateDays(start, 2, "from", "trx_total", 2)
	m := map[string]int64{}
	for _, s := range got {
		m[s.Address] = s.TRXTotal
	}
	if _, ok := m["total"]; ok {
		t.Fatal("total summary row must be excluded")
	}
	if m["A"] != 55 {
		t.Fatalf("A = %d, want 55 (50 day1 + 5 day2, merged)", m["A"])
	}
	if m["D"] != 40 {
		t.Fatalf("D = %d, want 40", m["D"])
	}
	if _, ok := m["C"]; ok {
		t.Fatal("C must be absent (never in a daily top-2) — the top-n approximation")
	}
	if _, ok := m["X"]; ok {
		t.Fatal("direction=from must not read to_stats rows")
	}

	// direction=to reads the other table.
	toGot := db.GetTopTransferStatsByDateDays(start, 2, "to", "trx_total", 2)
	if len(toGot) != 1 || toGot[0].Address != "X" {
		t.Fatalf("direction=to = %+v, want only X", toGot)
	}
}

// GetTransferTotalByDateDays must give the exact network denominator: from reads
// the 'total' row directly; to has no total row, so it SUMs the address rows.
// Both sum across the day window.
func TestGetTransferTotalByDateDays(t *testing.T) {
	db := newFlushTestDB(t)
	start := time.Date(2025, 2, 1, 0, 0, 0, 0, time.Local)

	seedUserStats(t, db, "from_stats_", "250201", []*models.UserStatistic{
		{Address: "A", TRXTotal: 10},
		{Address: "total", TRXTotal: 1000},
	})
	seedUserStats(t, db, "from_stats_", "250202", []*models.UserStatistic{
		{Address: "total", TRXTotal: 500},
	})
	seedUserStats(t, db, "to_stats_", "250201", []*models.UserStatistic{
		{Address: "X", TRXTotal: 7},
		{Address: "Y", TRXTotal: 3},
	})

	if got := db.GetTransferTotalByDateDays(start, 2, "from", "trx_total"); got != 1500 {
		t.Fatalf("from total = %d, want 1500 (sum of 'total' rows over 2 days)", got)
	}
	// to: no total row, SUM the address rows; day 2 table absent contributes 0.
	if got := db.GetTransferTotalByDateDays(start, 2, "to", "trx_total"); got != 10 {
		t.Fatalf("to total = %d, want 10 (SUM of address rows)", got)
	}
}
