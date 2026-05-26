package database

import (
	"testing"

	"tron-tracker/database/models"
)

func mkTx(typ uint8, owner, from, to, name string, amount, fee int64) *models.Transaction {
	tx := &models.Transaction{Type: typ, OwnerAddr: owner, FromAddr: from, ToAddr: to, Name: name, Fee: fee, Result: 1}
	tx.SetAmount(amount)
	return tx
}

func TestAccumulateAttribution(t *testing.T) {
	db := &RawDB{}
	cache := newCache("250101")
	cache.date = "250101"

	// TRX transfer: Owner=A, From=A, To=B
	db.accumulate(cache, mkTx(1, "A", "A", "B", "TRX", 2_000_000, 100))

	if cache.fromStats["A"] == nil {
		t.Fatal("fromStats must be keyed by OwnerAddr")
	}
	if cache.fromStats["total"] == nil {
		t.Fatal("fromStats must include total")
	}
	if cache.toStats["B"] == nil {
		t.Fatal("toStats must be keyed by ToAddr")
	}
	if got := cache.fromStats["A"].Fee; got != 100 {
		t.Fatalf("from A fee = %d, want 100", got)
	}
	// fee>0 path: for a type-1 tx, FeeStatistic.Add computes specialFee = Fee = 100
	// and books it into NewAcc. Assert the value (not just non-nil, which newCache
	// always satisfies) so the fee branch can't silently regress.
	if cache.feeStats.NewAcc != 100 {
		t.Fatalf("feeStats.NewAcc = %d, want 100", cache.feeStats.NewAcc)
	}
	if cache.tokenStats["TRX"] == nil {
		t.Fatal("tokenStats[TRX] missing")
	}

	// 不填 FromAddr 的类型（如 type=12 unfreeze）：from 仍记到 OwnerAddr
	db.accumulate(cache, mkTx(12, "C", "", "", "", 500, 50))
	if cache.fromStats["C"] == nil {
		t.Fatal("fromStats must use OwnerAddr even when FromAddr empty")
	}
	// 无 ToAddr -> 不进 toStats
	if cache.toStats["C"] != nil {
		t.Fatal("toStats must not be created when ToAddr empty")
	}
}
