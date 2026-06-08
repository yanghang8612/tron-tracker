package main

import (
	"flag"

	"tron-tracker/config"
	"tron-tracker/database"
	"tron-tracker/log"
	"tron-tracker/net"
)

// Usage:
//
//	go run ./cmd/index-backfill -days 90
//	go run ./cmd/index-backfill -days 90 -config /data/tracker/config.toml
//
// Adds the idx_type_amount_num functional index (type, CAST(amount AS UNSIGNED))
// to the most recent N daily transactions_* tables. This makes /top_delegate's
// per-type "ORDER BY amount DESC LIMIT n" run as an index scan instead of a
// full-table scan + filesort. Idempotent and safe to re-run; brand-new daily
// tables get the index automatically at creation time, so this is only needed
// once to cover existing history.
func main() {
	daysFlag := flag.Int("days", 90, "number of most-recent days to backfill")
	configFlag := flag.String("config", "./config.toml", "path to config.toml")
	flag.Parse()

	cfg := config.LoadConfigFrom(*configFlag)
	net.Init(&cfg.Net)
	log.Init(&cfg.Log)

	db := database.New(&cfg.DB)
	db.BackfillDelegateAmountIndexes(*daysFlag)
}
