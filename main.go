package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/robfig/cron/v3"
	"tron-tracker/api"
	"tron-tracker/bot"
	"tron-tracker/database"
	"tron-tracker/log"
	"tron-tracker/net"
)

func main() {
	cfg := loadConfig()

	net.Init(&cfg.Net)
	log.Init(&cfg.Log)

	db := database.New(&cfg.DB)

	tracker := New(db)
	tracker.Start()

	apiSrv := api.New(db, &cfg.Server, &cfg.DeFi)
	apiSrv.Start()

	tgBot := bot.New(cfg.BotToken, db)
	tgBot.Start()

	c := cron.New(cron.WithSeconds())
	_, _ = c.AddFunc("0 */5 2-12 * * *", func() {
		db.DoTronLinkWeeklyStatistics(time.Now(), false)
	})
	_, _ = c.AddFunc("0 */10 * * * *", func() {
		tgBot.DoMarketPairStatistics()
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

func watchOSSignal(tracker *Tracker, apiSrv *api.Server) {
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	<-c

	tracker.Stop()
	apiSrv.Stop()
}
