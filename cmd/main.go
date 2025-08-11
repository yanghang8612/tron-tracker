package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"tron-tracker/api"
	"tron-tracker/bot"
	"tron-tracker/config"
	"tron-tracker/database"
	"tron-tracker/google"
	"tron-tracker/log"
	"tron-tracker/net"
	"tron-tracker/tron"

	"github.com/robfig/cron/v3"
)

func main() {
	cfg := config.LoadConfig()

	net.Init(&cfg.Net)
	log.Init(&cfg.Log)

	db := database.New(&cfg.DB)

	updater := google.NewUpdater(db, &cfg.PPT)

	tracker := tron.NewTracker(db)
	tracker.Start()

	apiSrv := api.New(db, updater, &cfg.Server, &cfg.DeFi)
	apiSrv.Start()

	tgBot := bot.New(&cfg.Bot, db, updater)
	tgBot.Start()

	c := cron.New(cron.WithSeconds())
	_, _ = c.AddFunc("0 0 */1 * * *", func() {
		db.DoUSDTSupplyStatistics()
	})
	_, _ = c.AddFunc("0 */5 5-12 * * *", func() {
		db.DoTronLinkWeeklyStatistics(time.Now(), false)
	})
	_, _ = c.AddFunc("0 */10 * * * *", func() {
		tgBot.DoMarketPairStatistics()
	})
	_, _ = c.AddFunc("0 0 0 * * *", func() {
		tgBot.ReportMarketPairStatistics()
	})
	_, _ = c.AddFunc("30 1/30 * * * *", func() {
		tgBot.DoTokenListingStatistics()
	})
	_, _ = c.AddFunc("0 */10 * * * *", func() {
		tracker.Report()
		db.Report()
	})
	c.Start()

	watchOSSignal(tracker, apiSrv)
}

func watchOSSignal(tracker *tron.Tracker, apiSrv *api.Server) {
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	<-c

	tracker.Stop()
	apiSrv.Stop()
}
