package api

import (
	"bufio"
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jinzhu/now"
	"go.uber.org/zap"
	"tron-tracker/database"
)

type Server struct {
	router *gin.Engine
	srv    *http.Server

	db *database.RawDB
}

func New(db *database.RawDB) *Server {
	router := gin.Default()
	srv := &http.Server{
		Addr:    ":8080",
		Handler: router,
	}

	return &Server{
		router: router,
		srv:    srv,
		db:     db,
	}
}

func (s *Server) Start() {
	s.router.GET("/lastTrackedBlockNumber", s.lastTrackedBlockNumber)
	s.router.GET("/totalFeeOfTronLinkUsers", s.totalFeeOfTronLinkUsers)
	s.router.GET("/exchangesStatistic", s.exchangesStatistic)

	go func() {
		if err := s.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			panic(err)
		}
	}()
}

func (s *Server) Stop() {
	if err := s.srv.Shutdown(context.Background()); err != nil {
		panic(err)
	}
}

func (s *Server) lastTrackedBlockNumber(c *gin.Context) {
	c.JSON(200, gin.H{
		"last_tracked_block_number": s.db.GetLastTrackedBlockNum(),
	})
}

func (s *Server) totalFeeOfTronLinkUsers(c *gin.Context) {
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
			totalFee += uint64(s.db.GetUserStatistic(date, user).EnergyFee)
		}
		count += 1
		if count%10000 == 0 {
			zap.S().Infof("Counted [%d] user fee, current total fee [%d]", count, totalFee)
		}
	}
	c.JSON(200, gin.H{
		"total_fee": totalFee,
	})
}

func (s *Server) exchangesStatistic(c *gin.Context) {
	date, ok := c.Get("date")
	if ok {
		c.JSON(200, gin.H{
			"exchanges_statistic": s.db.GetExchangeStatistic(date.(string)),
		})
	} else {
		c.JSON(200, gin.H{
			"code":  400,
			"error": "date must be present",
		})
	}
}
