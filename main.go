package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/robfig/cron/v3"
	"tron-tracker/api"
	"tron-tracker/database"
	"tron-tracker/log"
)

func main() {
	cfg := loadConfig()

	log.Init(&cfg.Log)

	db := database.New(&cfg.DB)

	tracker := New(db)
	tracker.Start()

	apiSrv := api.New(db, &cfg.Server, &cfg.DeFi)
	apiSrv.Start()

	c := cron.New(cron.WithSeconds())
	_, _ = c.AddFunc("0 */5 0-12 * * 1", func() {
		db.DoTronLinkWeeklyStatistics(time.Now(), false)
	})
	_, _ = c.AddFunc("0 5 * * * *", func() {
		db.DoMarketPairStatistics()
	})
	_, _ = c.AddFunc("0 */10 * * * *", func() {
		tracker.Report()
		db.Report()
	})
	c.Start()

	watchOSSignal(tracker, apiSrv)
}

func watchOSSignal(tracker *Tracker, apiSrv *api.Server) {
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	<-c

	tracker.Stop()
	apiSrv.Stop()
}
