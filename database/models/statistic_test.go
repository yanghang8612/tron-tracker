package models

import (
	"strings"
	"testing"
)

func TestUSDTStorageStatistic_Diff_ZeroCounts(t *testing.T) {
	// Regression: zero Set/Reset tx counts caused an integer divide-by-zero panic
	// (e.g. SetEnergyFee/uint64(SetTxCount)). Diff must return safely instead.
	cur := &USDTStorageStatistic{}
	last := &USDTStorageStatistic{}

	got := cur.Diff(last) // must not panic
	if !strings.Contains(got, "insufficient") {
		t.Fatalf("Diff(zero counts) = %q, want insufficient-data message", got)
	}

	// Non-zero path still produces a real report.
	cur2 := &USDTStorageStatistic{SetTxCount: 10, ResetTxCount: 5, SetEnergyFee: 1e7, ResetEnergyFee: 5e6, SetEnergyTotal: 100, ResetEnergyTotal: 100}
	last2 := &USDTStorageStatistic{SetTxCount: 8, ResetTxCount: 4, SetEnergyFee: 8e6, ResetEnergyFee: 4e6, SetEnergyTotal: 80, ResetEnergyTotal: 80}
	if out := cur2.Diff(last2); !strings.Contains(out, "SetStorage") {
		t.Fatalf("Diff(data) = %q, want a SetStorage report", out)
	}
}
