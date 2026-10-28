package database

import (
	"testing"
	"time"
)

func TestDailyTableRetentionCutoffUsesUTCDate(t *testing.T) {
	shanghai := time.FixedZone("Asia/Shanghai", 8*60*60)
	for _, tc := range []struct {
		name string
		now  time.Time
		want string
	}{
		{"anniversary", time.Date(2026, 10, 28, 9, 0, 0, 0, shanghai), "2025-10-28"},
		{"before UTC midnight", time.Date(2026, 10, 28, 7, 59, 0, 0, shanghai), "2025-10-27"},
		{"year boundary", time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC), "2026-01-01"},
		{"leap anniversary", time.Date(2025, 2, 28, 0, 0, 0, 0, time.UTC), "2024-02-28"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := dailyTableRetentionCutoff(tc.now)
			if got.Format("2006-01-02") != tc.want || got.Location() != time.UTC || got.Hour() != 0 {
				t.Fatalf("cutoff = %v, want UTC midnight %s", got, tc.want)
			}
		})
	}
}

func TestTransactionRetentionProtectsUnprocessedDays(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(map[string]string)
		want   string
	}{
		{"caught up", func(map[string]string) {}, "251028"},
		{"tracking", func(c map[string]string) { c["tracking_date"] = "250101" }, "250101"},
		{"daily flush", func(c map[string]string) { c["flushed_date"] = "250101" }, "250102"},
		{"daily counting", func(c map[string]string) { c["counted_date"] = "250101" }, "250102"},
		{"weekly counting", func(c map[string]string) { c["counted_week"] = "250101" }, "250101"},
		{"missing cursor", func(c map[string]string) { delete(c, "flushed_date") }, ""},
		{"invalid cursor", func(c map[string]string) { c["counted_week"] = "251332" }, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cursors := map[string]string{"tracking_date": "261028", "flushed_date": "261027", "counted_date": "261027", "counted_week": "261026"}
			tc.change(cursors)
			got, err := transactionRetentionCutoff("251028", cursors)
			if got != tc.want || (err != nil) != (tc.want == "") {
				t.Fatalf("cutoff = %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}
