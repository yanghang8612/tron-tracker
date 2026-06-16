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

// GetTopTransferStatsByDateDays takes each day's top candidates by the metric
// (DB-side ORDER BY LIMIT, over-fetched to n*10 / min 1000 so the cross-day merge
// converges to the true top-n — verified against /q exact top-50), merges an
// address across days, excludes the "total" row, and selects from_/to_ by
// direction. The over-fetched pool covers this small table entirely, so the merge
// here is exact: C is kept even though it never makes a daily top-2.
func TestGetTopTransferStatsByDateDays(t *testing.T) {
	db := newFlushTestDB(t)
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.Local)

	seedUserStats(t, db, "from_stats_", "250101", []*models.UserStatistic{
		{Address: "A", TRXTotal: 50},
		{Address: "B", TRXTotal: 30},
		{Address: "C", TRXTotal: 10}, // outside a tight daily top-2, but pool covers it
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
	if m["B"] != 30 {
		t.Fatalf("B = %d, want 30", m["B"])
	}
	if m["D"] != 40 {
		t.Fatalf("D = %d, want 40", m["D"])
	}
	if m["C"] != 10 {
		t.Fatalf("C = %d, want 10 (kept by the over-fetched candidate pool)", m["C"])
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
