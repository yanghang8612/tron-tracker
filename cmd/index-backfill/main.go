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
//	go run ./cmd/index-backfill -days 30
//	go run ./cmd/index-backfill -days 30 -config /data/tracker/config.toml
//
// Ensures idx_type_amount_num on the most recent N daily transactions_* tables so
// /top_delegate's per-type "ORDER BY amount DESC LIMIT n" runs as an index scan
// instead of a full-table scan + filesort. Idempotent. Normally unnecessary: new
// tables get the index at creation time, and the daily flush (reconcileTxIndexes)
// keeps a rolling 30-day window and drops older ones. Use this only to apply the
// index immediately instead of waiting for the next flush.
func main() {
	daysFlag := flag.Int("days", 30, "number of most-recent days to backfill")
	configFlag := flag.String("config", "./config.toml", "path to config.toml")
	flag.Parse()

	cfg := config.LoadConfigFrom(*configFlag)
	net.Init(&cfg.Net)
	log.Init(&cfg.Log)

	db := database.New(&cfg.DB)
	db.BackfillDelegateAmountIndexes(*daysFlag)
}
