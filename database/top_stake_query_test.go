//go:build integration

package database

import (
	"testing"
	"time"

	"tron-tracker/database/models"
)

func itestStakeTx(owner string, typ uint8, amount int64) *models.Transaction {
	tx := &models.Transaction{OwnerAddr: owner, Type: typ}
	tx.SetAmount(amount)
	return tx
}

// GetStakeRelatedTxsByDateDays must gather freeze/unfreeze rows across the whole
// [date, date+days) window and keep ONLY the stake/unstake/cancel contract types
// — crucially including CancelAllUnfreezeV2 (59) and legacy UnfreezeBalance (12),
// and excluding delegate (57/58) and transfers (255). This is what distinguishes
// it from GetTopResourceRelatedTxsByDate (which drops 59/12 and keeps 57/58).
func TestGetStakeRelatedTxsByDateDays(t *testing.T) {
	db := newFlushTestDB(t)

	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.Local)
	seedTxs(t, db, "250101", []*models.Transaction{
		itestStakeTx("A", 54, 100), // stake2          -> keep
		itestStakeTx("A", 59, 10),  // cancel          -> keep (old helper drops)
		itestStakeTx("B", 255, 5),  // transfer        -> drop
		itestStakeTx("B", 57, 7),   // delegate        -> drop (old helper keeps)
	})
	seedTxs(t, db, "250102", []*models.Transaction{
		itestStakeTx("A", 55, 200), // unstake2        -> keep
		itestStakeTx("C", 12, 30),  // v1 unstake      -> keep (old helper drops)
		itestStakeTx("C", 154, 50), // energy stake2   -> keep
	})

	got := db.GetStakeRelatedTxsByDateDays(start, 2)

	if len(got) != 5 {
		t.Fatalf("got %d stake txs across 2 days, want 5", len(got))
	}
	seen := map[uint8]bool{}
	for _, tx := range got {
		seen[tx.Type] = true
		if tx.Type == 255 || tx.Type%100 == 57 || tx.Type%100 == 58 {
			t.Fatalf("type %d must be excluded from stake txs", tx.Type)
		}
	}
	for _, want := range []uint8{54, 59, 55, 12, 154} {
		if !seen[want] {
			t.Fatalf("type %d missing from result (seen=%v)", want, seen)
		}
	}
}

// A gap in the window (a day whose transactions_<date> table was never created)
// must not fail the whole query — that day simply contributes nothing.
func TestGetStakeRelatedTxsByDateDays_MissingDayTableIsSkipped(t *testing.T) {
	db := newFlushTestDB(t)

	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.Local)
	seedTxs(t, db, "250101", []*models.Transaction{
		itestStakeTx("A", 54, 100),
		itestStakeTx("A", 55, 60),
	})
	// 250102 and 250103 tables intentionally absent.

	got := db.GetStakeRelatedTxsByDateDays(start, 3)

	if len(got) != 2 {
		t.Fatalf("got %d txs over a window with 2 missing day tables, want 2 (only day 1)", len(got))
	}
}
