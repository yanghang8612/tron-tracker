package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/robfig/cron/v3"
	"tron-tracker/api"
	"tron-tracker/database"
	"tron-tracker/log"
)

func main() {
	// f, err := os.Create("cpuprofile")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer f.Close()
	// if err := pprof.StartCPUProfile(f); err != nil {
	// 	log.Fatal(err)
	// }
	// defer pprof.StopCPUProfile()
	cfg := loadConfig()

	log.Init(&cfg.Log)

	db := database.New(&cfg.DB)

	cron := cron.New()
	_, _ = cron.AddFunc("0 */10 0-12 * * 1", db.DoTronLinkWeeklyStatistics)
	cron.Start()

	tracker := New(db)
	tracker.Start()

	apiSrv := api.New(db, &cfg.DeFi)
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
