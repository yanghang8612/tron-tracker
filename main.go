package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jinzhu/now"
	"go.uber.org/zap"
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

	fmt.Println(now.BeginningOfWeek().Add(-1 * 24 * 6 * time.Hour).Format("060102"))

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

	router.GET("/totalFeeOfTronLinkUsers", func(c *gin.Context) {
		f, err := os.Open("week.txt")
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()

		scanner := bufio.NewScanner(f)

		zap.L().Info("Start count TronLink user fee")
		count := 0
		var totalFee uint64
		lastMonday := now.BeginningOfWeek().Add(-1 * 24 * 6 * time.Hour)
		for scanner.Scan() {
			user := scanner.Text()
			for i := 0; i < 7; i++ {
				date := lastMonday.Add(time.Duration(i) * 24 * time.Hour).Format("060102")
				totalFee += uint64(db.GetUserStatistic(date, user).EnergyFee)
			}
			count += 1
			if count%10000 == 0 {
				zap.S().Infof("Counted [%d] user fee, current total fee [%d]", count, totalFee)
			}
		}
		c.JSON(200, gin.H{
			"total_fee": totalFee,
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
