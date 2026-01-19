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

	apiSrv := api.New(db, updater, &cfg.Server, &cfg.DeFi)
	apiSrv.Start()

	alertBot := bot.NewAlertBot(&cfg.Bot, db)
	alertBot.Start()
	alertBot.RegisterFilters(tracker)

	trackerBot := bot.NewTrackerBot(&cfg.Bot, db, updater)
	trackerBot.Start()

	volumeBot := bot.NewVolumeBot(&cfg.Bot, db)
	volumeBot.Start()

	tracker.Start()

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

	_, _ = c.AddFunc("0 */1 * * * *", alertBot.GetFilterLogs)

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
			return volumeBot.DoMarketPairStatistics()
		}))

	_, _ = c.AddFunc("0 0 0 * * *",
		wrapper(func() error {
			return volumeBot.DoHoldingsStatistics()
		}))

	_, _ = c.AddFunc("0 0 7 * * *",
		func() {
			if time.Now().Weekday() == time.Monday || time.Now().Weekday() == time.Friday {
				volumeBot.CheckMarketPairs(true)
			} else {
				volumeBot.CheckMarketPairs(false)
			}
		})

	_, _ = c.AddFunc("30 1/30 * * * *",
		wrapper(func() error {
			return volumeBot.DoTokenListingStatistics()
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
