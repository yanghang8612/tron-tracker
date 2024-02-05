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
	"strings"
	"time"

	"github.com/dustin/go-humanize"
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
	s.router.GET("/exchanges_statistic", s.exchangesStatistic)
	s.router.GET("/special_statistic", s.specialStatistic)
	s.router.GET("/cached_charges", s.cachedCharges)
	s.router.GET("/total_statistics", s.totalStatistics)
	s.router.GET("/exchanges_weekly_statistic", s.exchangesWeeklyStatistic)
	s.router.GET("/tron_weekly_statistics", s.tronWeeklyStatistics)
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
		"last_tracked_block_time":   time.Unix(s.db.GetLastTrackedBlockTime(), 0).Format("2006-01-02 15:04:05"),
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
	var totalFee int64
	lastMonday := now.BeginningOfWeek().Add(-1 * 24 * 6 * time.Hour)
	for scanner.Scan() {
		user := scanner.Text()
		for i := 0; i < 7; i++ {
			date := lastMonday.Add(time.Duration(i) * 24 * time.Hour).Format("060102")
			fee := s.db.GetFromStatisticByDateAndUser(date, user).Fee
			if fee > 1_000_000_000_000 {
				zap.S().Infof("User [%s] fee on [%s] is [%d]", user, date, fee)
			}
			totalFee += fee
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
	startDate := prepareDateParam(c, "start_date")
	if startDate == nil {
		return
	}

	days, ok := getIntParam(c, "days")
	if !ok {
		return
	}

	resultMap := make(map[string]*models.ExchangeStatistic)
	totalFee, totalEnergyUsage := int64(0), int64(0)
	for i := 0; i < days; i++ {
		for _, es := range s.db.GetExchangeStatisticsByDate(startDate.AddDate(0, 0, i).Format("060102")) {
			totalFee += es.ChargeFee + es.CollectFee + es.WithdrawFee
			totalEnergyUsage += es.ChargeEnergyUsage + es.CollectEnergyUsage + es.WithdrawEnergyUsage
			exchangeName := utils.TrimExchangeName(es.Name)
			if _, ok := resultMap[exchangeName]; !ok {
				resultMap[exchangeName] = &models.ExchangeStatistic{}
				resultMap[exchangeName].Date = startDate.Format("060102") + "~" + startDate.AddDate(0, 0, days-1).Format("060102")
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

func (s *Server) exchangesWeeklyStatistic(c *gin.Context) {
	startDate := prepareDateParam(c, "start_date")
	if startDate == nil {
		return
	}

	curWeekStats := s.getOneWeekExchangeStatistics(*startDate)
	lastWeekStats := s.getOneWeekExchangeStatistics(startDate.AddDate(0, 0, -7))

	type JsonStat struct {
		Name               string
		FeePerDay          int64
		ChangeFromLastWeek string
	}

	result := make(map[string]*JsonStat)

	for name, fee := range curWeekStats {
		result[name] = &JsonStat{
			Name:               name,
			FeePerDay:          fee / 7_000_000,
			ChangeFromLastWeek: utils.FormatChangePercent(lastWeekStats[name], fee),
		}
	}

	resultArray := make([]*JsonStat, 0)
	for _, es := range result {
		resultArray = append(resultArray, es)
	}

	sort.Slice(result, func(i, j int) bool {
		return resultArray[i].FeePerDay > resultArray[j].FeePerDay
	})

	c.JSON(200, resultArray)
}

func (s *Server) getOneWeekExchangeStatistics(startDate time.Time) map[string]int64 {
	resultMap := make(map[string]int64)
	totalFee := int64(0)
	for i := 0; i < 7; i++ {
		for _, es := range s.db.GetExchangeStatisticsByDate(startDate.AddDate(0, 0, i).Format("060102")) {
			totalFee += es.ChargeFee + es.CollectFee + es.WithdrawFee
			exchangeName := utils.TrimExchangeName(es.Name)
			if _, ok := resultMap[exchangeName]; !ok {
				resultMap[exchangeName] = totalFee
			}
			resultMap[exchangeName] += totalFee
		}
	}
	resultMap["total_fee"] = totalFee

	return resultMap
}

func (s *Server) tronWeeklyStatistics(c *gin.Context) {
	startDate := prepareDateParam(c, "start_date")
	if startDate == nil {
		return
	}

	curWeekTotalStatistic := &models.UserStatistic{}
	for i := 0; i < 7; i++ {
		dayStatistic := s.db.GetTotalStatisticsByDate(startDate.AddDate(0, 0, i).Format("060102"))
		curWeekTotalStatistic.Merge(&dayStatistic)
	}

	curWeekUSDTStatistic := &models.UserStatistic{}
	for i := 0; i < 7; i++ {
		dayStatistic := s.db.GetFromStatisticByDateAndUser(startDate.AddDate(0, 0, i).Format("060102"), "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t")
		curWeekUSDTStatistic.Merge(&dayStatistic)
	}

	lastWeekTotalStatistic := &models.UserStatistic{}
	for i := 1; i <= 7; i++ {
		dayStatistic := s.db.GetTotalStatisticsByDate(startDate.AddDate(0, 0, -i).Format("060102"))
		lastWeekTotalStatistic.Merge(&dayStatistic)
	}

	lastWeekUSDTStatistic := &models.UserStatistic{}
	for i := 1; i <= 7; i++ {
		dayStatistic := s.db.GetFromStatisticByDateAndUser(startDate.AddDate(0, 0, -i).Format("060102"), "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t")
		lastWeekUSDTStatistic.Merge(&dayStatistic)
	}

	c.JSON(200, gin.H{
		"fee":                humanize.Comma(curWeekTotalStatistic.Fee / 7_000_000),
		"fee_change":         utils.FormatChangePercent(lastWeekTotalStatistic.Fee, curWeekTotalStatistic.Fee),
		"usdt_fee":           humanize.Comma(curWeekUSDTStatistic.Fee / 7_000_000),
		"usdt_fee_change":    utils.FormatChangePercent(lastWeekUSDTStatistic.Fee, curWeekUSDTStatistic.Fee),
		"tx_total":           humanize.Comma(curWeekTotalStatistic.TXTotal / 7),
		"tx_total_change":    utils.FormatChangePercent(lastWeekTotalStatistic.TXTotal, curWeekTotalStatistic.TXTotal),
		"trx_total":          humanize.Comma(curWeekTotalStatistic.TRXTotal / 7),
		"trx_total_change":   utils.FormatChangePercent(lastWeekTotalStatistic.TRXTotal, curWeekTotalStatistic.TRXTotal),
		"trc10_total":        humanize.Comma(curWeekTotalStatistic.TRC10Total / 7),
		"trc10_total_change": utils.FormatChangePercent(lastWeekTotalStatistic.TRC10Total, curWeekTotalStatistic.TRC10Total),
		"sc_total":           humanize.Comma(curWeekTotalStatistic.SCTotal / 7),
		"sc_total_change":    utils.FormatChangePercent(lastWeekTotalStatistic.SCTotal, curWeekTotalStatistic.SCTotal),
		"usdt_total":         humanize.Comma(curWeekTotalStatistic.USDTTotal / 7),
		"usdt_total_change":  utils.FormatChangePercent(lastWeekTotalStatistic.USDTTotal, curWeekTotalStatistic.USDTTotal),
		"other_total":        humanize.Comma((curWeekTotalStatistic.SCTotal - curWeekTotalStatistic.USDTTotal) / 7),
		"other_total_change": utils.FormatChangePercent(lastWeekTotalStatistic.SCTotal-lastWeekTotalStatistic.USDTTotal, curWeekTotalStatistic.SCTotal-curWeekTotalStatistic.USDTTotal),
	})
}

func (s *Server) revenueWeeklyStatistics(c *gin.Context) {
	startDate := prepareDateParam(c, "start_date")
	if startDate == nil {
		return
	}

	curWeekStats := s.getOneWeekRevenueStatistics(*startDate)
	totalWeekStats := &models.UserStatistic{}
	for i := 0; i < 7; i++ {
		dayStatistic := s.db.GetTotalStatisticsByDate(startDate.AddDate(0, 0, i).Format("060102"))
		totalWeekStats.Merge(&dayStatistic)
	}
	lastWeekStats := s.getOneWeekRevenueStatistics(startDate.AddDate(0, 0, -7))

	result := make(map[string]any)
	for k, v := range curWeekStats {
		result[k] = humanize.Comma(v)
		result[k+"_last_week"] = humanize.Comma(lastWeekStats[k])
		result[k+"_change"] = utils.FormatChangePercent(lastWeekStats[k], v)
		if strings.Contains(k, "fee") {
			result[k+"_of_total"] = utils.FormatOfPercent(totalWeekStats.Fee/7_000_000, v)
		}
	}

	c.JSON(200, result)
}

func (s *Server) getOneWeekRevenueStatistics(startDate time.Time) map[string]int64 {
	var (
		totalFee         int64
		totalEnergy      int64
		exchangeFee      int64
		exchangeEnergy   int64
		sunswapV1Fee     int64
		sunswapV1Energy  int64
		sunswapV2Fee     int64
		sunswapV2Energy  int64
		justlendFee      int64
		justlendEnergy   int64
		bttcFee          int64
		bttcEnergy       int64
		usdtcasinoFee    int64
		usdtcasinoEnergy int64
	)
	for i := 0; i < 7; i++ {
		date := startDate.AddDate(0, 0, i).Format("060102")

		totalFee += s.db.GetTotalStatisticsByDate(date).Fee
		totalEnergy += s.db.GetTotalStatisticsByDate(date).EnergyTotal

		for _, es := range s.db.GetExchangeStatisticsByDate(startDate.AddDate(0, 0, i).Format("060102")) {
			exchangeFee += es.ChargeFee + es.CollectFee + es.WithdrawFee
			exchangeEnergy += es.ChargeEnergyUsage + es.CollectEnergyUsage + es.WithdrawEnergyUsage
			exchangeEnergy += exchangeFee / 420
		}

		for _, addr := range s.config.SunSwapV1 {
			sunswapV1Fee += s.db.GetFromStatisticByDateAndUser(date, addr).Fee
			sunswapV1Energy += s.db.GetFromStatisticByDateAndUser(date, addr).EnergyTotal
		}

		for _, addr := range s.config.SunSwapV2 {
			sunswapV2Fee += s.db.GetFromStatisticByDateAndUser(date, addr).Fee
			sunswapV2Energy += s.db.GetFromStatisticByDateAndUser(date, addr).EnergyTotal
		}

		for _, addr := range s.config.JustLend {
			justlendFee += s.db.GetFromStatisticByDateAndUser(date, addr).Fee
			justlendEnergy += s.db.GetFromStatisticByDateAndUser(date, addr).EnergyTotal
		}

		for _, addr := range s.config.BTTC {
			bttcFee += s.db.GetFromStatisticByDateAndUser(date, addr).Fee
			bttcEnergy += s.db.GetFromStatisticByDateAndUser(date, addr).EnergyTotal
		}

		for _, addr := range s.config.BTTC {
			usdtcasinoFee += s.db.GetFromStatisticByDateAndUser(date, addr).Fee
			usdtcasinoEnergy += s.db.GetFromStatisticByDateAndUser(date, addr).EnergyTotal
		}
	}

	return map[string]int64{
		"total_fee":         totalFee / 7_000_000,
		"total_energy":      totalEnergy / 7,
		"exchange_fee":      exchangeFee / 7_000_000,
		"exchange_energy":   exchangeEnergy / 7,
		"sunswap_v1_fee":    sunswapV1Fee / 7_000_000,
		"sunswap_v1_energy": sunswapV1Energy / 7,
		"sunswap_v2_fee":    sunswapV2Fee / 7_000_000,
		"sunswap_v2_energy": sunswapV2Energy / 7,
		"justlend_fee":      justlendFee / 7_000_000,
		"justlend_energy":   justlendEnergy / 7,
		"bttc_fee":          bttcFee / 7_000_000,
		"bttc_energy":       bttcEnergy / 7,
		"usdtcasino_fee":    usdtcasinoFee / 7_000_000,
		"usdtcasino_energy": usdtcasinoEnergy / 7,
	}
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
