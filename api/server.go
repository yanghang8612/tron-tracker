package api

import (
	"bufio"
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jinzhu/now"
	"go.uber.org/zap"
	"tron-tracker/database"
	"tron-tracker/database/models"
	"tron-tracker/utils"
)

type DeFiConfig struct {
	SunSwapV1  []string `toml:"sunswap_v1"`
	SunSwapV2  []string `toml:"sunswap_v2"`
	JustLend   []string `toml:"justlend"`
	BTTC       []string `toml:"bttc"`
	USDTCasino []string `toml:"usdtcasino"`
}

type Server struct {
	router *gin.Engine
	srv    *http.Server

	db     *database.RawDB
	config *DeFiConfig
}

func New(db *database.RawDB, config *DeFiConfig) *Server {
	router := gin.Default()
	srv := &http.Server{
		Addr:    ":8080",
		Handler: router,
	}

	return &Server{
		router: router,
		srv:    srv,

		db:     db,
		config: config,
	}
}

func (s *Server) Start() {
	s.router.GET("/last-tracked-block-num", s.lastTrackedBlockNumber)
	s.router.GET("/total-fee-of-tronlink-users", s.totalFeeOfTronLinkUsers)
	s.router.GET("/exchanges_daily_statistic", s.exchangesDailyStatistic)
	s.router.GET("/exchanges_weekly_statistic", s.exchangesWeeklyStatistic)
	s.router.GET("/special_statistic", s.specialStatistic)
	s.router.GET("/cached_charges", s.cachedCharges)
	s.router.GET("/total_statistics", s.totalStatistics)
	s.router.GET("/revenue_weekly_statistics", s.revenueWeeklyStatistics)

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
			totalFee += uint64(s.db.GetFromStatisticByDateAndUser(date, user).EnergyFee)
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

func (s *Server) exchangesDailyStatistic(c *gin.Context) {
	date, ok := c.GetQuery("date")
	if ok {
		resultMap := make(map[string]*models.ExchangeStatistic)
		totalFee, totalEnergyUsage := uint64(0), uint64(0)
		for _, es := range s.db.GetExchangeStatisticsByDate(date) {
			totalFee += es.ChargeFee + es.CollectFee + es.WithdrawFee
			totalEnergyUsage += es.ChargeEnergyUsage + es.CollectEnergyUsage + es.WithdrawEnergyUsage
			exchangeName := utils.TrimExchangeName(es.Name)
			if _, ok := resultMap[exchangeName]; !ok {
				resultMap[exchangeName] = &models.ExchangeStatistic{}
				resultMap[exchangeName].Name = exchangeName
			}
			resultMap[exchangeName].Merge(&es)
		}

		resultArray := make([]*models.ExchangeStatistic, 0)
		for _, es := range resultMap {
			es.TotalFee = es.ChargeFee + es.CollectFee + es.WithdrawFee
			resultArray = append(resultArray, es)
		}

		c.JSON(200, gin.H{
			"exchanges_statistic": resultArray,
			"total_fee":           totalFee,
			"total_energy_usage":  totalEnergyUsage,
		})
	} else {
		c.JSON(200, gin.H{
			"code":  400,
			"error": "date must be present",
		})
	}
}

func (s *Server) exchangesWeeklyStatistic(c *gin.Context) {
	startDate := prepareDateParam(c, "start_date")
	if startDate == nil {
		return
	}

	resultMap := make(map[string]*models.ExchangeStatistic)
	totalFee := uint64(0)
	totalEnergyUsage := uint64(0)
	for i := 0; i < 7; i++ {
		for _, es := range s.db.GetExchangeStatisticsByDate(startDate.AddDate(0, 0, i).Format("060102")) {
			totalFee += es.ChargeFee + es.CollectFee + es.WithdrawFee
			totalEnergyUsage += es.ChargeEnergyUsage + es.CollectEnergyUsage + es.WithdrawEnergyUsage
			exchangeName := utils.TrimExchangeName(es.Name)
			if _, ok := resultMap[exchangeName]; !ok {
				resultMap[exchangeName] = &models.ExchangeStatistic{}
				resultMap[exchangeName].Date = startDate.Format("060102") + "~" + startDate.AddDate(0, 0, 6).Format("060102")
				resultMap[exchangeName].Name = exchangeName
			}
			resultMap[exchangeName].Merge(&es)
		}
	}

	resultArray := make([]*models.ExchangeStatistic, 0)
	for _, es := range resultMap {
		es.TotalFee = es.ChargeFee + es.CollectFee + es.WithdrawFee
		resultArray = append(resultArray, es)
	}

	sort.Slice(resultArray, func(i, j int) bool {
		return resultArray[i].TotalFee > resultArray[j].TotalFee
	})

	c.JSON(200, gin.H{
		"total_fee":                 totalFee,
		"total_energy_usage":        totalEnergyUsage,
		"exchanges_total_statistic": resultArray,
	})
}

func (s *Server) specialStatistic(c *gin.Context) {
	date, ok := getStringParam(c, "date")
	if !ok {
		return
	}

	addr, ok := getStringParam(c, "addr")
	if !ok {
		return
	}

	if ok {
		chargeFee, withdrawFee, chargeCount, withdrawCount := s.db.GetSpecialStatisticByDateAndAddr(date, addr)
		c.JSON(200, gin.H{
			"charge_fee":     chargeFee,
			"withdraw_fee":   withdrawFee,
			"charge_count":   chargeCount,
			"withdraw_count": withdrawCount,
		})
	} else {
		c.JSON(200, gin.H{
			"code":  400,
			"error": "addr must be present",
		})
	}
}

func (s *Server) cachedCharges(c *gin.Context) {
	addr, ok := c.GetQuery("addr")
	if ok {
		c.JSON(200, gin.H{
			"cached_charges": s.db.GetCachedChargesByAddr(addr),
		})
	} else {
		c.JSON(200, gin.H{
			"code":  400,
			"error": "addr must be present",
		})
	}
}

func (s *Server) totalStatistics(c *gin.Context) {
	startDate := prepareDateParam(c, "start_date")
	if startDate == nil {
		return
	}

	days, ok := getIntParam(c, "days")
	if !ok {
		return
	}

	totalStatistic := &models.UserStatistic{}
	for i := 0; i < days; i++ {
		dayStatistic := s.db.GetTotalStatisticsByDate(startDate.AddDate(0, 0, i).Format("060102"))
		totalStatistic.Merge(&dayStatistic)
	}

	c.JSON(200, gin.H{
		"total_statistic": totalStatistic,
	})
}

func (s *Server) revenueWeeklyStatistics(c *gin.Context) {
	startDate := prepareDateParam(c, "start_date")
	if startDate == nil {
		return
	}

	var (
		totalFee      uint64
		exchangeFee   uint64
		sunswapV1Fee  uint64
		sunswapV2Fee  uint64
		justlendFee   uint64
		bttcFee       uint64
		usdtcasinoFee uint64
	)
	for i := 0; i < 7; i++ {
		date := startDate.AddDate(0, 0, i).Format("060102")

		totalFee += s.db.GetTotalStatisticsByDate(date).Fee

		for _, es := range s.db.GetExchangeStatisticsByDate(startDate.AddDate(0, 0, i).Format("060102")) {
			exchangeFee += es.ChargeFee + es.CollectFee + es.WithdrawFee
		}

		for _, addr := range s.config.SunSwapV1 {
			sunswapV1Fee += s.db.GetFromStatisticByDateAndUser(date, addr).Fee
		}

		for _, addr := range s.config.SunSwapV2 {
			sunswapV2Fee += s.db.GetFromStatisticByDateAndUser(date, addr).Fee
		}

		for _, addr := range s.config.JustLend {
			justlendFee += s.db.GetFromStatisticByDateAndUser(date, addr).Fee
		}

		for _, addr := range s.config.BTTC {
			bttcFee += s.db.GetFromStatisticByDateAndUser(date, addr).Fee
		}

		for _, addr := range s.config.BTTC {
			usdtcasinoFee += s.db.GetFromStatisticByDateAndUser(date, addr).Fee
		}
	}

	c.JSON(200, gin.H{
		"total_fee":      totalFee,
		"exchange_fee":   exchangeFee,
		"sunswap_v1_fee": sunswapV1Fee,
		"sunswap_v2_fee": sunswapV2Fee,
		"justlend_fee":   justlendFee,
		"bttc_fee":       bttcFee,
		"usdtcasino_fee": usdtcasinoFee,
	})
}

func prepareDateParam(c *gin.Context, name string) *time.Time {
	dateStr, ok := getStringParam(c, name)
	if !ok {
		return nil
	}

	date, err := time.Parse("060102", dateStr)
	if err != nil {
		c.JSON(200, gin.H{
			"code":  400,
			"error": "start_date cannot be parsed",
		})
		return nil
	}

	return &date
}

func getStringParam(c *gin.Context, name string) (string, bool) {
	param, ok := c.GetQuery(name)
	if !ok {
		c.JSON(200, gin.H{
			"code":  400,
			"error": name + " must be present",
		})
	}

	return param, ok
}

func getIntParam(c *gin.Context, name string) (int, bool) {
	paramStr, ok := c.GetQuery(name)
	if !ok {
		c.JSON(200, gin.H{
			"code":  400,
			"error": name + " must be present",
		})
	}

	param, err := strconv.Atoi(paramStr)
	if err != nil {
		c.JSON(200, gin.H{
			"code":  400,
			"error": name + " cannot cast into int",
		})
	}

	return param, ok && err == nil
}
