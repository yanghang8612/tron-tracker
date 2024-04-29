package main

import (
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"
	"time"

	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
	"tron-tracker/api"
	"tron-tracker/database"
	"tron-tracker/log"
)

func main() {
	cfg := loadConfig()

	log.Init(&cfg.Log)

	f, err := os.Create("cpuprofile")
	if err != nil {
		zap.S().Fatal(err)
	}
	defer f.Close()
	if err := pprof.StartCPUProfile(f); err != nil {
		zap.S().Fatal(err)
	}
	defer pprof.StopCPUProfile()

	db := database.New(&cfg.DB)

	c := cron.New(cron.WithSeconds())
	_, _ = c.AddFunc("0 */5 0-12 * * 1", func() {
		db.DoTronLinkWeeklyStatistics(time.Now(), false)
	})
	c.Start()

	tracker := New(db)
	tracker.Start()

	apiSrv := api.New(db, &cfg.Server, &cfg.DeFi)
	apiSrv.Start()

	watchOSSignal(tracker, apiSrv)
}

func watchOSSignal(tracker *Tracker, apiSrv *api.Server) {
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	<-c

	tracker.Stop()
	apiSrv.Stop()
}
