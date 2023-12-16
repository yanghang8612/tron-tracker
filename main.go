package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/gin-gonic/gin"
	"tron-tracker/database"
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

	db := database.New()
	tracker := New(db)
	tracker.Start()

	router := gin.Default()
	router.GET("/lastTrackedBlockNumber", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"block_number": db.GetLastTrackedBlockNum(),
			"exchanges":    tracker.el,
		})
	})

	srv := &http.Server{
		Addr:    ":8080",
		Handler: router,
	}

	go srv.ListenAndServe()

	watchOSSignal(tracker, srv)
}

func watchOSSignal(tracker *Tracker, httpSrv *http.Server) {
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	<-c

	tracker.Stop()
	httpSrv.Shutdown(context.Background())
}
