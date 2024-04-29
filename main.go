package main

import (
	"net/http"
	_ "net/http/pprof"
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
	go func() {
		http.ListenAndServe("0.0.0.0:8080", nil)
	}()

	cfg := loadConfig()

	log.Init(&cfg.Log)

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
