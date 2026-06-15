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

func TestUserStatisticMetric(t *testing.T) {
	s := &UserStatistic{Fee: 1, EnergyTotal: 2, TxTotal: 3, TRXTotal: 4, SmallTRXTotal: 5, TRC10Total: 6, USDTTotal: 7}

	want := map[string]int64{
		"fee": 1, "energy_total": 2, "tx_total": 3,
		"trx_total": 4, "small_trx_total": 5, "trc10_total": 6, "usdt_total": 7,
	}
	for name, exp := range want {
		got, ok := s.Metric(name)
		if !ok || got != exp {
			t.Fatalf("Metric(%q) = (%d, %v), want (%d, true)", name, got, ok, exp)
		}
	}

	// Unknown metric must report not-ok so the handler can reject it (400),
	// rather than silently ranking everything by a zero column.
	if got, ok := s.Metric("delegate_total_typo"); ok || got != 0 {
		t.Fatalf("Metric(unknown) = (%d, %v), want (0, false)", got, ok)
	}
}
