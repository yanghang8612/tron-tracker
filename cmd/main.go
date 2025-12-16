package main

import (
	"errors"
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
	"go.uber.org/zap"
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

	loggerForCron := cron.PrintfLogger(zap.NewStdLog(zap.L()))
	c := cron.New(cron.WithSeconds(), cron.WithLogger(loggerForCron), cron.WithChain(cron.Recover(loggerForCron)))

	wrapper := func(task func() error) func() {
		return func() {
			err := task()
			if err != nil {
				net.ReportWarningToSlack(err.Error())
			}
		}
	}

	// Schedule tasks for USDT Supply statistics
	_, _ = c.AddFunc("0 0 */1 * * *", wrapper(func() error {
		return db.DoUSDTSupplyStatistics()
	}))

	// TronLink data is no longer provided by us.
	// _, _ = c.AddFunc("0 */5 5-12 * * *", func() {
	// 	db.DoTronLinkWeeklyStatistics(time.Now(), false)
	// })

	_, _ = c.AddFunc("0 */10 * * * *",
		wrapper(func() error {
			return tgBot.DoMarketPairStatistics()
		}))

	_, _ = c.AddFunc("0 0 0 * * *",
		wrapper(func() error {
			return tgBot.DoHoldingsStatistics()
		}))

	_, _ = c.AddFunc("0 0 7 * * *",
		wrapper(func() error {
			if time.Now().Weekday() == time.Monday || time.Now().Weekday() == time.Friday {
				return tgBot.CheckMarketPairs(true)
			}
			return tgBot.CheckMarketPairs(false)
		}))

	_, _ = c.AddFunc("30 1/30 * * * *",
		wrapper(func() error {
			return tgBot.DoTokenListingStatistics()
		}))

	_, _ = c.AddFunc("0 */10 * * * *",
		wrapper(func() error {
			tracker.Report()
			db.Report()

			if !tracker.IsTracking() {
				return errors.New("not tracking, Please check app or fullnode")
			}

			return nil
		}))

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
