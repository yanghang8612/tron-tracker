//go:build integration

package database

import (
	"math/big"
	"testing"
	"time"

	"tron-tracker/database/models"
	"tron-tracker/database/models/types"
)

func indexExists(t *testing.T, db *RawDB, table, index string) bool {
	t.Helper()
	var n int64
	err := db.db.Raw(
		"SELECT COUNT(*) FROM information_schema.statistics "+
			"WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?",
		table, index,
	).Scan(&n).Error
	if err != nil {
		t.Fatalf("index check: %v", err)
	}
	return n > 0
}

// The rewritten query must still return the global top-n delegate txs ranked by
// NUMERIC amount, merged across bandwidth (57) and energy (157), excluding
// undelegate (58/158). Locks correctness across the UNION-ALL rewrite.
func TestGetTopDelegateRelatedTxsByDateAndN_RanksNumericAcrossTypes(t *testing.T) {
	db := newFlushTestDB(t)
	start := time.Date(2025, 2, 1, 0, 0, 0, 0, time.Local)
	seedTxs(t, db, "250201", []*models.Transaction{
		itestStakeTx("a", 157, 5_000_000_000),  // energy
		itestStakeTx("b", 57, 30_000_000_000),  // bandwidth, largest
		itestStakeTx("c", 157, 100),            // small, must drop at n=3
		itestStakeTx("d", 57, 999),             // 3rd largest
		itestStakeTx("e", 158, 99_000_000_000), // undelegate, must NOT appear
	})

	got := db.GetTopDelegateRelatedTxsByDateAndN(start, 3, false)

	if len(got) != 3 {
		t.Fatalf("got %d txs, want 3", len(got))
	}
	wantOrder := []string{"30000000000", "5000000000", "999"} // numeric desc, NOT lexical
	for i, w := range wantOrder {
		if got[i].Amount.String() != w {
			t.Fatalf("rank %d = %s, want %s (full order %s,%s,%s)",
				i, got[i].Amount.String(), w, got[0].Amount, got[1].Amount, got[2].Amount)
		}
	}
	for _, tx := range got {
		if tx.Type%100 == 58 {
			t.Fatalf("undelegate type %d leaked into delegate query", tx.Type)
		}
	}
}

// Undelegate mode must rank types 58/158 only.
func TestGetTopDelegateRelatedTxsByDateAndN_UndelegateMode(t *testing.T) {
	db := newFlushTestDB(t)
	start := time.Date(2025, 2, 2, 0, 0, 0, 0, time.Local)
	seedTxs(t, db, "250202", []*models.Transaction{
		itestStakeTx("a", 158, 7_000_000_000),
		itestStakeTx("b", 58, 8_000_000_000),
		itestStakeTx("c", 157, 99_000_000_000), // delegate, must NOT appear
	})

	got := db.GetTopDelegateRelatedTxsByDateAndN(start, 5, true)

	if len(got) != 2 {
		t.Fatalf("got %d txs, want 2", len(got))
	}
	if got[0].Amount.String() != "8000000000" || got[1].Amount.String() != "7000000000" {
		t.Fatalf("undelegate order = %s,%s, want 8e9,7e9", got[0].Amount, got[1].Amount)
	}
}

// SaveTransactions must create the idx_type_amount_num functional index on the
// day's table so top_delegate's per-type ORDER BY is index-served.
func TestSaveTransactionsCreatesDelegateAmountIndex(t *testing.T) {
	db := newFlushTestDB(t)
	db.trackingDate = "250203"

	if err := db.SaveTransactions([]*models.Transaction{itestStakeTx("a", 157, 100)}); err != nil {
		t.Fatalf("SaveTransactions: %v", err)
	}

	if !indexExists(t, db, "transactions_250203", "idx_type_amount_num") {
		t.Fatalf("idx_type_amount_num was not created by SaveTransactions")
	}
}

// The functional index must build over REAL tx data, which contains "<nil>"
// (unset BigInt amount) and >uint64 token amounts. Without the guard expression
// CREATE INDEX fails with "Data truncated for functional index" (err 3751), which
// is exactly what happened in production. Delegate ranking must still be correct.
func TestDelegateAmountIndexBuildsWithDirtyAmounts(t *testing.T) {
	db := newFlushTestDB(t)
	start := time.Date(2025, 3, 1, 0, 0, 0, 0, time.Local)

	huge, _ := new(big.Int).SetString("123456789012345678901234567890", 10) // 30 digits, > uint64
	dirty := &models.Transaction{Type: 31}                                  // unset amount -> stored as "<nil>"
	overflow := &models.Transaction{Type: 31, Amount: types.NewBigInt(huge)}
	seedTxs(t, db, "250301", []*models.Transaction{
		dirty, overflow,
		itestStakeTx("a", 157, 5_000_000_000),
		itestStakeTx("b", 57, 9_000_000_000),
	})

	db.EnsureDelegateAmountIndex("250301")

	if !indexExists(t, db, "transactions_250301", "idx_type_amount_num") {
		t.Fatalf("index failed to build over dirty amounts (<nil>/overflow) — guard expression broken")
	}

	got := db.GetTopDelegateRelatedTxsByDateAndN(start, 5, false)
	if len(got) != 2 || got[0].Amount.String() != "9000000000" || got[1].Amount.String() != "5000000000" {
		t.Fatalf("delegate ranking wrong with dirty rows present: %v", got)
	}
}

// BackfillDelegateAmountIndexes must add the index to recent existing day tables
// and skip days with no table (no error).
func TestBackfillDelegateAmountIndexes(t *testing.T) {
	db := newFlushTestDB(t)
	today := time.Now().Format("060102")
	yesterday := time.Now().AddDate(0, 0, -1).Format("060102")

	// Only create today's table; yesterday's is absent and must be skipped.
	seedTxs(t, db, today, []*models.Transaction{itestStakeTx("a", 157, 100)})

	db.BackfillDelegateAmountIndexes(2)

	if !indexExists(t, db, "transactions_"+today, "idx_type_amount_num") {
		t.Fatalf("backfill did not index today's table")
	}
	if db.db.Migrator().HasTable("transactions_" + yesterday) {
		t.Fatalf("precondition: yesterday's table should not exist")
	}
}
