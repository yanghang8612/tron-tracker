package main

import (
	"flag"
	"time"

	"tron-tracker/config"
	"tron-tracker/database"
	"tron-tracker/log"
	"tron-tracker/net"
)

// Usage:
//   go run ./cmd/backfill -date 260420
//   go run ./cmd/backfill -date 260420,260421
//
// Stamps the current on-chain frozen-TRX state of every known exchange
// address with the given date(s) and inserts them into
// exchange_resource_statistics. Useful for filling gaps caused by missed
// cron runs. Note: because /wallet/getaccount only returns current state,
// backfilled rows reflect "now", not the actual value at 00:00 of the date.
func main() {
	dateFlag := flag.String("date", time.Now().Format("060102"), "comma-separated YYMMDD dates to backfill")
	flag.Parse()

	cfg := config.LoadConfig()
	net.Init(&cfg.Net)
	log.Init(&cfg.Log)

	db := database.New(&cfg.DB)

	dates := splitCSV(*dateFlag)
	for _, date := range dates {
		if err := db.DoExchangeResourceStatistics(date); err != nil {
			panic(err)
		}
	}
}

func splitCSV(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			if i > start {
				out = append(out, s[start:i])
			}
			start = i + 1
		}
	}
	return out
}
