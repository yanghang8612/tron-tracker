package main

import (
	"os"
	"os/signal"
	"syscall"

	"tron-tracker/api"
	"tron-tracker/database"
	"tron-tracker/log"
	"tron-tracker/utils"
)

func main() {
	a := uint64(74388855307020)
	b := uint64(79693086114680)
	println(utils.FormatChangePercent(b, a))
	println(utils.FormatChangePercent(a, b))
}

func main1() {
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
