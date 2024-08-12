package api

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/go-resty/resty/v2"
	"github.com/jinzhu/now"
	"go.uber.org/zap"
	"tron-tracker/database"
	"tron-tracker/database/models"
	"tron-tracker/utils"
)

type ServerConfig struct {
	HttpPort  int    `toml:"http_port"`
	HttpsPort int    `toml:"https_port"`
	CertFile  string `toml:"cert_file"`
	KeyFile   string `toml:"key_file"`
}

type DeFiConfig struct {
	SunSwapV1  []string `toml:"sunswap_v1"`
	SunSwapV2  []string `toml:"sunswap_v2"`
	JustLend   []string `toml:"justlend"`
	BTTC       []string `toml:"bttc"`
	USDTCasino []string `toml:"usdtcasino"`
}

type Server struct {
	httpRouter *gin.Engine
	httpServer *http.Server

	httpsRouter *gin.Engine
	httpsServer *http.Server

	db           *database.RawDB
	serverConfig *ServerConfig
	defiConfig   *DeFiConfig

	logger *zap.SugaredLogger
}

func New(db *database.RawDB, serverConfig *ServerConfig, deficonfig *DeFiConfig) *Server {
	httpRouter := gin.Default()
	httpRouter.Use(cors.Default())
	httpsRouter := gin.Default()
	httpsRouter.Use(cors.Default())

	return &Server{
		httpRouter: httpRouter,
		httpServer: &http.Server{
			Addr:    ":" + strconv.Itoa(serverConfig.HttpPort),
			Handler: httpRouter,
		},

		httpsRouter: httpsRouter,
		httpsServer: &http.Server{
			Addr:    ":" + strconv.Itoa(serverConfig.HttpsPort),
			Handler: httpsRouter,
		},

		db:           db,
		serverConfig: serverConfig,
		defiConfig:   deficonfig,

		logger: zap.S().Named("[api]"),
	}
}

func (s *Server) Start() {
	s.httpRouter.GET("/last-tracked-block-num", s.lastTrackedBlockNumber)
	s.httpRouter.GET("/exchange_statistics", s.exchangesStatistic)
	s.httpRouter.GET("/exchange_token_statistics", s.exchangesTokenStatistic)
	s.httpRouter.GET("/exchange_token_daily_statistics", s.exchangesTokenDailyStatistic)
	s.httpRouter.GET("/exchange_token_weekly_statistics", s.exchangesTokenWeeklyStatistic)
	s.httpRouter.GET("/special_statistics", s.specialStatistic)
	s.httpRouter.GET("/total_statistics", s.totalStatistics)
	s.httpRouter.GET("/do-tronlink-users-weekly-statistics", s.doTronlinkUsersWeeklyStatistics)
	s.httpRouter.GET("/tronlink-users-weekly-statistics", s.tronlinkUsersWeeklyStatistics)
	s.httpRouter.GET("/exchange_weekly_statistics", s.exchangesWeeklyStatistic)
	s.httpRouter.GET("/tron_weekly_statistics", s.tronWeeklyStatistics)
	s.httpRouter.GET("/revenue_weekly_statistics", s.revenueWeeklyStatistics)
	s.httpRouter.GET("/trx_statistics", s.trxStatistics)
	s.httpRouter.GET("/usdt_statistics", s.usdtStatistics)
	s.httpRouter.GET("/user_statistics", s.userStatistics)
	s.httpRouter.GET("/top_users", s.topUsers)
	s.httpRouter.GET("/token_statistics", s.tokenStatistics)
	s.httpRouter.GET("/eth_statistics", s.ethStatistics)
	s.httpRouter.GET("/tron_statistics", s.forward)

	s.httpRouter.GET("/market_pair_statistics", s.marketPairStatistics)
	s.httpRouter.GET("/market_pair_weekly_volumes", s.marketPairWeeklyVolumes)
	s.httpRouter.GET("/market_pair_weekly_depths", s.marketPairWeeklyDepths)

	go func() {
		err := s.httpServer.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			panic(err)
		}
	}()

	go func() {
		err := s.httpsServer.ListenAndServeTLS(s.serverConfig.CertFile, s.serverConfig.KeyFile)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			panic(err)
		}
	}()

	s.logger.Infof("API server started at %d&%d", s.serverConfig.HttpPort, s.serverConfig.HttpsPort)
}

func (s *Server) Stop() {
	if err := s.httpServer.Shutdown(context.Background()); err != nil {
		panic(err)
	}

	if err := s.httpsServer.Shutdown(context.Background()); err != nil {
		panic(err)
	}
}

func (s *Server) lastTrackedBlockNumber(c *gin.Context) {
	c.JSON(200, gin.H{
		"last_tracked_block_number": s.db.GetLastTrackedBlockNum(),
		"last_tracked_block_time":   time.Unix(s.db.GetLastTrackedBlockTime(), 0).Format("2006-01-02 15:04:05"),
	})
}

func (s *Server) exchangesStatistic(c *gin.Context) {
	startDate, ok := prepareDateParam(c, "start_date")
	if !ok {
		return
	}

	days, ok := getIntParam(c, "days")
	if !ok {
		return
	}

	resultMap := make(map[string]*models.ExchangeStatistic)
	totalEnergy, totalFee, totalEnergyUsage := int64(0), int64(0), int64(0)
	dateRangeStr := startDate.Format("060102") + "~" + startDate.AddDate(0, 0, days-1).Format("060102")
	for i := 0; i < days; i++ {
		queryDate := startDate.AddDate(0, 0, i)
		for _, es := range s.db.GetExchangeTotalStatisticsByDate(queryDate) {
			totalEnergy += es.ChargeEnergyTotal + es.CollectEnergyTotal + es.WithdrawEnergyTotal
			totalFee += es.ChargeFee + es.CollectFee + es.WithdrawFee
			totalEnergyUsage += es.ChargeEnergyUsage + es.CollectEnergyUsage + es.WithdrawEnergyUsage
			if _, ok := resultMap[es.Name]; !ok {
				resultMap[es.Name] = &models.ExchangeStatistic{
					Date: dateRangeStr,
					Name: es.Name,
				}
			}
			resultMap[es.Name].Merge(&es)
		}
	}

	resultArray := make([]*models.ExchangeStatistic, 0)
	for _, es := range resultMap {
		resultArray = append(resultArray, es)
	}
	sort.Slice(resultArray, func(i, j int) bool {
		return resultArray[i].TotalFee > resultArray[j].TotalFee
	})

	c.JSON(200, gin.H{
		"total_energy":              totalEnergy,
		"total_fee":                 totalFee,
		"total_energy_usage":        totalEnergyUsage,
		"exchanges_total_statistic": resultArray,
	})
}

type ExchangeTokenStatisticInResult struct {
	Name           string `json:"name"`
	Fee            int64  `json:"fee"`
	FeePercent     string `json:"fee_percent"`
	TxCount        int64  `json:"tx_count"`
	TxCountPercent string `json:"tx_count_percent"`
}

func (s *Server) exchangesTokenStatistic(c *gin.Context) {
	startDate, ok := prepareDateParam(c, "start_date")
	if !ok {
		return
	}

	weeks, ok := getIntParam(c, "weeks")
	if !ok {
		return
	}

	token, ok := getStringParam(c, "token")
	if !ok {
		token = "Total"
	}

	concernedExchanges := []string{
		"Okex",
		"Binance",
		"bybit",
		"WhiteBIT",
		"MXC",
		"bitget",
		"Kraken",
		"Kucoin",
		"BtcTurk",
		"HTX",
		"Gate",
		"Bitfinex",
		"CEX.IO",
	}

	weeklyStats := make([]map[string]map[string]*models.ExchangeStatistic, 0)
	for i := 0; i < weeks; i++ {
		weekStartDate := startDate.AddDate(0, 0, i*7)
		weeklyStat := s.db.GetExchangeTokenStatisticsByDateAndDays(weekStartDate, 7)
		weeklyStats = append(weeklyStats, weeklyStat)
	}

	result := strings.Builder{}
	concernedExchangesStats := make([]models.ExchangeStatistic, weeks)

	for _, concernedExchange := range concernedExchanges {
		result.WriteString(fmt.Sprintf("%s,", concernedExchange))

		for i, weeklyStat := range weeklyStats {
			if exchangeStat, ok := weeklyStat[concernedExchange]; ok {
				if es, ok := exchangeStat[token]; ok {
					concernedExchangesStats[i].Merge(es)
					result.WriteString(fmt.Sprintf("%d,%d,%d,%d,%d,%d,", es.ChargeTxCount, es.ChargeFee, es.CollectTxCount, es.CollectFee, es.WithdrawTxCount, es.WithdrawFee))
				} else {
					result.WriteString(fmt.Sprintf("0,0,0,0,0,0,"))
				}
			} else {
				result.WriteString(fmt.Sprintf("0,0,0,0,0,0,"))
			}
		}
		result.WriteString("\n")
	}

	result.WriteString(fmt.Sprintf("Other,"))
	for i, es := range concernedExchangesStats {
		result.WriteString(fmt.Sprintf("%d,%d,%d,%d,%d,%d,",
			weeklyStats[i]["All"][token].ChargeTxCount-es.ChargeTxCount,
			weeklyStats[i]["All"][token].ChargeFee-es.ChargeFee,
			weeklyStats[i]["All"][token].CollectTxCount-es.CollectTxCount,
			weeklyStats[i]["All"][token].CollectFee-es.CollectFee,
			weeklyStats[i]["All"][token].WithdrawTxCount-es.WithdrawTxCount,
			weeklyStats[i]["All"][token].WithdrawFee-es.WithdrawFee))
	}
	result.WriteString("\n")

	c.String(200, result.String())
}

func (s *Server) exchangesTokenDailyStatistic(c *gin.Context) {
	startDate, ok := prepareDateParam(c, "date")
	if !ok {
		return
	}

	etsMap := s.db.GetExchangeTokenStatisticsByDateAndDays(startDate, 1)

	token, ok := getStringParam(c, "token")

	if !ok {
		c.JSON(200, etsMap)
		return
	}

	resultMap := make(map[string]*models.ExchangeStatistic)
	for exchange, ets := range etsMap {
		if es, ok := ets[token]; ok {
			resultMap[exchange] = es
		}
	}

	c.JSON(200, resultMap)
}

func (s *Server) exchangesTokenWeeklyStatistic(c *gin.Context) {
	startDate, ok := prepareDateParam(c, "start_date")
	if !ok {
		return
	}

	etsMap := s.db.GetExchangeTokenStatisticsByDateAndDays(startDate, 7)

	resultMap := make(map[string]map[string][]*ExchangeTokenStatisticInResult)
	for exchange, ets := range etsMap {
		resultMap[exchange] = analyzeExchangeTokenStatistics(ets)
	}

	c.JSON(200, resultMap)
}

func analyzeExchangeTokenStatistics(ets map[string]*models.ExchangeStatistic) map[string][]*ExchangeTokenStatisticInResult {
	chargeResults := make([]*ExchangeTokenStatisticInResult, 0)
	withdrawResults := make([]*ExchangeTokenStatisticInResult, 0)
	chargeCount, withdrawCount := int64(0), int64(0)
	chargeFee, withdrawFee := int64(0), int64(0)

	for token, es := range ets {
		if es.ChargeTxCount > 0 {
			chargeCount += es.ChargeTxCount
			chargeFee += es.ChargeFee
			chargeResults = append(chargeResults, &ExchangeTokenStatisticInResult{
				Name:           token,
				Fee:            es.ChargeFee,
				FeePercent:     "",
				TxCount:        es.ChargeTxCount,
				TxCountPercent: "",
			})
		}

		if es.WithdrawTxCount > 0 {
			withdrawCount += es.WithdrawTxCount
			withdrawFee += es.WithdrawFee
			withdrawResults = append(withdrawResults, &ExchangeTokenStatisticInResult{
				Name:           token,
				Fee:            es.WithdrawFee,
				FeePercent:     "",
				TxCount:        es.WithdrawTxCount,
				TxCountPercent: "",
			})
		}
	}

	for _, res := range chargeResults {
		res.FeePercent = utils.FormatOfPercent(chargeFee, res.Fee)
		res.TxCountPercent = utils.FormatOfPercent(chargeCount, res.TxCount)
	}

	for _, res := range withdrawResults {
		res.FeePercent = utils.FormatOfPercent(withdrawFee, res.Fee)
		res.TxCountPercent = utils.FormatOfPercent(withdrawCount, res.TxCount)
	}

	sort.Slice(chargeResults, func(i, j int) bool {
		return chargeResults[i].Fee > chargeResults[j].Fee
	})

	sort.Slice(withdrawResults, func(i, j int) bool {
		return withdrawResults[i].Fee > withdrawResults[j].Fee
	})

	return map[string][]*ExchangeTokenStatisticInResult{
		"charge":   chargeResults,
		"withdraw": withdrawResults,
	}
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

func (s *Server) totalStatistics(c *gin.Context) {
	startDate, ok := prepareDateParam(c, "start_date")
	if !ok {
		return
	}

	days, ok := getIntParam(c, "days")
	if !ok {
		return
	}

	currentTotalStatistic := &models.UserStatistic{}
	for i := 0; i < days; i++ {
		dayStatistic := s.db.GetTotalStatisticsByDate(startDate.AddDate(0, 0, i).Format("060102"))
		currentTotalStatistic.Merge(&dayStatistic)
	}

	currentPhishingStatistic := &models.PhishingStatistic{}
	for i := 0; i < days; i++ {
		dayPhishingStatistic := s.db.GetPhishingStatisticsByDate(startDate.AddDate(0, 0, i).Format("060102"))
		currentPhishingStatistic.Merge(&dayPhishingStatistic)
	}
	currentPhishingStatistic.Date = ""

	withComment := c.DefaultQuery("comment", "false")
	if withComment == "false" {
		c.JSON(200, gin.H{
			"total_statistic":    currentTotalStatistic,
			"phishing_statistic": currentPhishingStatistic,
		})
		return
	}

	startDate = startDate.AddDate(0, 0, -days)
	lastTotalStatistic := &models.UserStatistic{}
	for i := 0; i < days; i++ {
		dayStatistic := s.db.GetTotalStatisticsByDate(startDate.AddDate(0, 0, i).Format("060102"))
		lastTotalStatistic.Merge(&dayStatistic)
	}

	comment := strings.Builder{}
	comment.WriteString(fmt.Sprintf(
		"上周TRX转账: %s笔(%s), 相比上上周 %s笔\n上周低价值TRX转账: %s笔(%s), 相比上上周 %s笔\n\n",
		humanize.Comma(currentTotalStatistic.TRXTotal),
		utils.FormatChangePercent(lastTotalStatistic.TRXTotal, currentTotalStatistic.TRXTotal),
		humanize.Comma(currentTotalStatistic.TRXTotal-lastTotalStatistic.TRXTotal),
		humanize.Comma(currentTotalStatistic.SmallTRXTotal),
		utils.FormatChangePercent(lastTotalStatistic.SmallTRXTotal, currentTotalStatistic.SmallTRXTotal),
		humanize.Comma(currentTotalStatistic.SmallTRXTotal-lastTotalStatistic.SmallTRXTotal)))
	comment.WriteString(fmt.Sprintf(
		"上周USDT转账: %s笔(%s), 相比上上周 %s笔\n上周低价值USDT转账: %s笔(%s), 相比上上周 %s笔",
		humanize.Comma(currentTotalStatistic.USDTTotal),
		utils.FormatChangePercent(lastTotalStatistic.USDTTotal, currentTotalStatistic.USDTTotal),
		humanize.Comma(currentTotalStatistic.USDTTotal-lastTotalStatistic.USDTTotal),
		humanize.Comma(currentTotalStatistic.SmallUSDTTotal),
		utils.FormatChangePercent(lastTotalStatistic.SmallUSDTTotal, currentTotalStatistic.SmallUSDTTotal),
		humanize.Comma(currentTotalStatistic.SmallUSDTTotal-lastTotalStatistic.SmallUSDTTotal)))

	c.JSON(200, gin.H{
		"total_statistic":    currentTotalStatistic,
		"phishing_statistic": currentPhishingStatistic,
		"comment":            comment.String(),
	})
}

func (s *Server) doTronlinkUsersWeeklyStatistics(c *gin.Context) {
	if date, ok := prepareDateParam(c, "date"); ok {
		go func() {
			s.db.DoTronLinkWeeklyStatistics(date, true)
		}()
	}
}

func (s *Server) tronlinkUsersWeeklyStatistics(c *gin.Context) {
	date, ok := prepareDateParam(c, "date")
	if !ok {
		return
	}

	thisMonday := now.With(date).BeginningOfWeek().AddDate(0, 0, 1).Format("20060102")

	statsResultFile, err := os.Open(fmt.Sprintf("/data/tronlink/week%s_stats.txt", thisMonday))
	if err != nil {
		c.JSON(200, gin.H{
			"code":    500,
			"message": "week stats file not found",
		})
		return
	}
	defer statsResultFile.Close()

	scanner := bufio.NewScanner(statsResultFile)

	nums := make([]int, 0)
	for scanner.Scan() {
		num, err := strconv.Atoi(scanner.Text())
		if err != nil {
			c.JSON(200, gin.H{
				"code":    500,
				"message": "week stats file cannot be parsed",
			})
			return
		}

		nums = append(nums, num)
	}
	c.JSON(200, gin.H{
		"total_fee":    nums[0],
		"total_energy": nums[1],
	})
}

func (s *Server) exchangesWeeklyStatistic(c *gin.Context) {
	startDate, ok := prepareDateParam(c, "start_date")
	if !ok {
		return
	}

	curWeekStats := s.getOneWeekExchangeStatistics(startDate)
	lastWeekStats := s.getOneWeekExchangeStatistics(startDate.AddDate(0, 0, -7))

	type JsonStat struct {
		Name               string `json:"name"`
		FeePerDay          int64  `json:"fee_per_day"`
		ChangeFromLastWeek string `json:"change_from_last_week"`
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

	sort.Slice(resultArray, func(i, j int) bool {
		return resultArray[i].FeePerDay > resultArray[j].FeePerDay
	})

	c.JSON(200, resultArray)
}

func (s *Server) getOneWeekExchangeStatistics(startDate time.Time) map[string]int64 {
	resultMap := make(map[string]int64)
	for i := 0; i < 7; i++ {
		queryDate := startDate.AddDate(0, 0, i)
		for _, es := range s.db.GetExchangeTotalStatisticsByDate(queryDate) {
			resultMap[es.Name] += es.TotalFee
			resultMap["total_fee"] += es.TotalFee
		}
	}

	return resultMap
}

func (s *Server) tronWeeklyStatistics(c *gin.Context) {
	startDate, ok := prepareDateParam(c, "start_date")
	if !ok {
		return
	}

	curWeekTotalStatistic := &models.UserStatistic{}
	for i := 0; i < 7; i++ {
		dayStatistic := s.db.GetTotalStatisticsByDate(startDate.AddDate(0, 0, i).Format("060102"))
		curWeekTotalStatistic.Merge(&dayStatistic)
	}

	curWeekUSDTStatistic := &models.TokenStatistic{}
	for i := 0; i < 7; i++ {
		dayStatistic := s.db.GetTokenStatisticsByDate(startDate.AddDate(0, 0, i).Format("060102"), "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t")
		curWeekUSDTStatistic.Merge(&dayStatistic)
	}

	lastWeekTotalStatistic := &models.UserStatistic{}
	for i := 1; i <= 7; i++ {
		dayStatistic := s.db.GetTotalStatisticsByDate(startDate.AddDate(0, 0, -i).Format("060102"))
		lastWeekTotalStatistic.Merge(&dayStatistic)
	}

	lastWeekUSDTStatistic := &models.TokenStatistic{}
	for i := 1; i <= 7; i++ {
		dayStatistic := s.db.GetTokenStatisticsByDate(startDate.AddDate(0, 0, -i).Format("060102"), "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t")
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
	startDate, ok := prepareDateParam(c, "start_date")
	if !ok {
		return
	}

	curWeekStats := s.getOneWeekRevenueStatistics(startDate)
	totalWeekStats := &models.UserStatistic{}
	for i := 0; i < 7; i++ {
		dayStatistic := s.db.GetTotalStatisticsByDate(startDate.AddDate(0, 0, i).Format("060102"))
		totalWeekStats.Merge(&dayStatistic)
	}
	lastWeekStats := s.getOneWeekRevenueStatistics(startDate.AddDate(0, 0, -7))

	result := make(map[string]any)
	for k, v := range curWeekStats {
		if strings.Contains(k, "fee") {
			result[k] = fmt.Sprintf("%s TRX (%s)", humanize.Comma(v), utils.FormatChangePercent(lastWeekStats[k], v))
			result[k+"_of_total"] = utils.FormatOfPercent(totalWeekStats.Fee/7_000_000, v)
		} else {
			result[k] = fmt.Sprintf("%s (%s)", humanize.Comma(v), utils.FormatChangePercent(lastWeekStats[k], v))
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

		for _, es := range s.db.GetExchangeTotalStatisticsByDate(startDate.AddDate(0, 0, i)) {
			fee := es.ChargeFee + es.CollectFee + es.WithdrawFee
			exchangeFee += fee
			exchangeEnergy += es.ChargeEnergyUsage + es.CollectEnergyUsage + es.WithdrawEnergyUsage
			exchangeEnergy += fee / 420
		}

		for _, addr := range s.defiConfig.SunSwapV1 {
			tokenStats := s.db.GetTokenStatisticsByDate(date, addr)
			sunswapV1Fee += tokenStats.Fee
			sunswapV1Energy += tokenStats.EnergyTotal
		}

		for _, addr := range s.defiConfig.SunSwapV2 {
			tokenStats := s.db.GetTokenStatisticsByDate(date, addr)
			sunswapV2Fee += tokenStats.Fee
			sunswapV2Energy += tokenStats.EnergyTotal
		}

		for _, addr := range s.defiConfig.JustLend {
			tokenStats := s.db.GetTokenStatisticsByDate(date, addr)
			justlendFee += tokenStats.Fee
			justlendEnergy += tokenStats.EnergyTotal
		}

		for _, addr := range s.defiConfig.BTTC {
			tokenStats := s.db.GetTokenStatisticsByDate(date, addr)
			bttcFee += tokenStats.Fee
			bttcEnergy += tokenStats.EnergyTotal
		}

		for _, addr := range s.defiConfig.USDTCasino {
			tokenStats := s.db.GetTokenStatisticsByDate(date, addr)
			usdtcasinoFee += tokenStats.Fee
			usdtcasinoEnergy += tokenStats.EnergyTotal
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

func (s *Server) trxStatistics(c *gin.Context) {
	startDate, ok := prepareDateParam(c, "start_date")
	if !ok {
		return
	}

	days, ok := getIntParam(c, "days")
	if !ok {
		return
	}

	n, ok := getIntParam(c, "n")
	if !ok {
		return
	}

	curWeekStats := s.db.GetFromStatisticByDateAndDays(startDate, days)
	lastWeekStats := s.db.GetFromStatisticByDateAndDays(startDate.AddDate(0, 0, -7), days)

	changedStats := make([]*models.UserStatistic, 0)

	for _, stats := range curWeekStats {
		changedStats = append(changedStats, stats)
	}

	for user, stats := range lastWeekStats {
		if _, ok := curWeekStats[user]; !ok {
			stats.TRXTotal = -stats.TRXTotal
			stats.SmallTRXTotal = -stats.SmallTRXTotal
			changedStats = append(changedStats, stats)
		} else {
			curWeekStats[user].TRXTotal -= stats.TRXTotal
			curWeekStats[user].SmallTRXTotal -= stats.SmallTRXTotal
		}
	}

	type ResEntity struct {
		Address   string `json:"address"`
		TXChanged int    `json:"tx_changed"`
	}

	sort.Slice(changedStats, func(i, j int) bool {
		return changedStats[i].TRXTotal > changedStats[j].TRXTotal
	})

	resStatsSortedByTRX := pickTopNAndLastN(changedStats, n, func(t *models.UserStatistic) *ResEntity {
		return &ResEntity{
			Address:   t.Address,
			TXChanged: int(t.TRXTotal),
		}
	})

	sort.Slice(changedStats, func(i, j int) bool {
		return changedStats[i].SmallTRXTotal > changedStats[j].SmallTRXTotal
	})

	resStatsSortedBySmallTRX := pickTopNAndLastN(changedStats, n, func(t *models.UserStatistic) *ResEntity {
		return &ResEntity{
			Address:   t.Address,
			TXChanged: int(t.SmallTRXTotal),
		}
	})

	c.JSON(200, gin.H{
		"sorted_by_trx":       resStatsSortedByTRX,
		"sorted_by_small_trx": resStatsSortedBySmallTRX,
	})
}

func (s *Server) usdtStatistics(c *gin.Context) {
	startDate, ok := prepareDateParam(c, "start_date")
	if !ok {
		return
	}

	days, ok := getIntParam(c, "days")
	if !ok {
		return
	}

	n, ok := getIntParam(c, "n")
	if !ok {
		return
	}

	usdtStatsMap := s.db.GetUserTokenStatisticsByDateAndDaysAndToken(startDate, days, "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t")

	usdtStats := make([]*models.UserTokenStatistic, 0)
	for _, stats := range usdtStatsMap {
		usdtStats = append(usdtStats, stats)
	}

	sort.Slice(usdtStats, func(i, j int) bool {
		return usdtStats[i].FromTXCount > usdtStats[j].FromTXCount
	})

	fromLen := n
	if len(usdtStats) < n {
		fromLen = len(usdtStats)
	}
	topNFrom := make([]*models.UserTokenStatistic, fromLen)
	copy(topNFrom, usdtStats)

	sort.Slice(usdtStats, func(i, j int) bool {
		return usdtStats[i].ToTXCount > usdtStats[j].ToTXCount
	})

	var topNTo []*models.UserTokenStatistic
	if len(usdtStats) > n {
		topNTo = usdtStats[:n]
	} else {
		topNTo = usdtStats
	}

	c.JSON(200, gin.H{
		"top_from": topNFrom,
		"top_to":   topNTo,
	})
}

func pickTopNAndLastN[T any, S any](src []T, n int, convert func(T) S) []S {
	dsc := make([]S, 0)

	if len(src) <= 2*n {
		for _, s := range src {
			dsc = append(dsc, convert(s))
		}
	} else {
		for i := 0; i < n; i++ {
			dsc = append(dsc, convert(src[i]))
		}

		for i := n; i > 0; i-- {
			dsc = append(dsc, convert(src[len(src)-i]))
		}
	}

	return dsc
}

func (s *Server) userStatistics(c *gin.Context) {
	date, ok := prepareDateParam(c, "date")
	if !ok {
		return
	}

	user, ok := getStringParam(c, "user")
	if !ok {
		return
	}

	days, ok := getIntParam(c, "days")
	if !ok {
		return
	}

	c.JSON(200, s.db.GetFromStatisticByDateAndUserAndDays(date, user, days))
}

func (s *Server) topUsers(c *gin.Context) {
	startDate, ok := prepareDateParam(c, "start_date")
	if !ok {
		return
	}

	days, ok := getIntParam(c, "days")
	if !ok {
		return
	}

	n, ok := getIntParam(c, "n")
	if !ok {
		return
	}

	fromStatsMap := s.db.GetFromStatisticByDateAndDays(startDate, days)

	fromStats := make([]*models.UserStatistic, 0)

	for _, stats := range fromStatsMap {
		fromStats = append(fromStats, stats)
	}

	type ResEntity struct {
		Address  string `json:"address"`
		TotalFee int64  `json:"total_fee"`
	}

	sort.Slice(fromStats, func(i, j int) bool {
		return fromStats[i].Fee > fromStats[j].Fee
	})

	resStatsSortedByFee := pickTopNAndLastN(fromStats, n, func(t *models.UserStatistic) *ResEntity {
		return &ResEntity{
			Address:  t.Address,
			TotalFee: t.Fee,
		}
	})

	c.JSON(200, resStatsSortedByFee[:n])
}

func (s *Server) tokenStatistics(c *gin.Context) {
	startDate, ok := prepareDateParam(c, "start_date")
	if !ok {
		return
	}

	days, ok := getIntParam(c, "days")
	if !ok {
		return
	}

	n, ok := getIntParam(c, "n")
	if !ok {
		return
	}

	var (
		TRC10Stat   = &models.TokenStatistic{Address: "TRC10"}
		NonUSDTStat = &models.TokenStatistic{Address: "Other Contract"}
		resultArray = make([]*models.TokenStatistic, 0)
	)
	for _, ts := range s.db.GetTokenStatisticsByDateAndDays(startDate, days) {
		switch len(ts.Address) {
		case 7:
			TRC10Stat.Merge(ts)
		case 34:
			if ts.Address != "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t" {
				NonUSDTStat.Merge(ts)
			}
		}
		resultArray = append(resultArray, ts)
	}

	sort.Slice(resultArray, func(i, j int) bool {
		return resultArray[i].Fee > resultArray[j].Fee
	})

	if len(resultArray) > n {
		resultArray = resultArray[:n]
	}
	resultArray = append(resultArray, TRC10Stat, NonUSDTStat)

	c.JSON(200, resultArray)
}

func (s *Server) ethStatistics(c *gin.Context) {
	date, ok := prepareDateParam(c, "date")
	if !ok {
		return
	}

	todayStats := getEthereumDailyStats(date.Format("060102"))
	yesterdayStats := getEthereumDailyStats(date.AddDate(0, 0, -1).Format("060102"))

	c.JSON(200, gin.H{
		"eth_accounts":        humanize.Comma(int64(todayStats.totalAccounts)),
		"eth_accounts_change": humanize.Comma(int64(todayStats.totalAccounts - yesterdayStats.totalAccounts)),
		"account_db_size":     humanize.Bytes(todayStats.accountDBSize),
		"account_db_change":   humanize.Bytes(todayStats.accountDBSize - yesterdayStats.accountDBSize),
		"eth_txs":             humanize.Comma(int64(todayStats.totalTxs)),
		"eth_txs_change":      humanize.Comma(int64(todayStats.totalTxs - yesterdayStats.totalTxs)),
		"tx_db_size":          humanize.Bytes(todayStats.txDBSize),
		"tx_db_change":        humanize.Bytes(todayStats.txDBSize - yesterdayStats.txDBSize),
		"total_db_size":       humanize.Bytes(todayStats.totalDBSize),
		"total_db_change":     humanize.Bytes(todayStats.totalDBSize - yesterdayStats.totalDBSize),
	})
}

func (s *Server) forward(c *gin.Context) {
	client := resty.New()
	resp, err := client.R().Get("http://localhost:8088/wallet/getaddressandtx?" + c.Request.URL.RawQuery)
	if err != nil {
		c.JSON(200, gin.H{
			"code":  500,
			"error": err.Error(),
		})
		return
	}

	c.Data(200, "application/json", resp.Body())
}

func (s *Server) marketPairStatistics(c *gin.Context) {
	startDate, ok := prepareDateParam(c, "start_date")
	if !ok {
		return
	}

	days, ok := getIntParam(c, "days")
	if !ok {
		return
	}

	token, ok := getStringParam(c, "token")
	if !ok {
		return
	}

	marketPairStats := s.db.GetMarketPairStatisticsByDateAndDaysAndToken(startDate, days, token)

	result := make([]*models.MarketPairStatistic, 0)
	for _, mps := range marketPairStats {
		result = append(result, mps)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Percent > result[j].Percent
	})

	c.JSON(200, result)
}

func (s *Server) marketPairWeeklyVolumes(c *gin.Context) {
	startDate, ok := prepareDateParam(c, "start_date")
	if !ok {
		return
	}

	token, ok := getStringParam(c, "token")
	if !ok {
		return
	}

	marketPairDailyVolumes := s.db.GetMarketPairDailyVolumesByDateAndDaysAndToken(startDate, 7, token)

	result := make([]*models.MarketPairStatistic, 0)
	for _, mps := range marketPairDailyVolumes {
		result = append(result, mps)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Volume > result[j].Volume
	})

	c.JSON(200, result)
}

func (s *Server) marketPairWeeklyDepths(c *gin.Context) {
	startDate, ok := prepareDateParam(c, "start_date")
	if !ok {
		return
	}

	token, ok := getStringParam(c, "token")
	if !ok {
		return
	}

	marketPairAverageDepths := s.db.GetMarketPairAverageDepthsByDateAndDaysAndToken(startDate, 7, token)

	result := make([]*models.MarketPairStatistic, 0)
	for _, mps := range marketPairAverageDepths {
		result = append(result, mps)
	}

	c.JSON(200, result)
}

// Helper function to convert size string to bytes
func sizeToBytes(sizeStr string) uint64 {
	sizeStr = strings.TrimSpace(sizeStr)
	var multiplier uint64
	switch {
	case strings.HasSuffix(sizeStr, "KiB"):
		multiplier = 1024
	case strings.HasSuffix(sizeStr, "MiB"):
		multiplier = 1024 * 1024
	case strings.HasSuffix(sizeStr, "GiB"):
		multiplier = 1024 * 1024 * 1024
	case strings.HasSuffix(sizeStr, "TiB"):
		multiplier = 1024 * 1024 * 1024 * 1024
	default:
		multiplier = 1
	}
	valueStr := strings.Fields(sizeStr)[0]
	value, _ := strconv.ParseFloat(valueStr, 64)
	return uint64(value * float64(multiplier))
}

type ethStatistics struct {
	totalAccounts uint64
	accountDBSize uint64
	totalTxs      uint64
	txDBSize      uint64
	totalDBSize   uint64
}

func getEthereumDailyStats(day string) ethStatistics {
	file, err := os.Open("/data/ethereum/execution/" + day + ".log")

	if err != nil {
		fmt.Println(err)
		return ethStatistics{}
	}

	var (
		result  = ethStatistics{}
		scanner = bufio.NewScanner(file)
	)

	for scanner.Scan() {
		line := scanner.Text()

		if strings.Contains(line, "store") {
			items := strings.Split(line, "|")
			database := items[1]
			category := items[2]
			size := sizeToBytes(items[3])
			result.totalDBSize += size

			if strings.Contains(database, "Key-Value") && strings.Contains(category, "Account snapshot") {
				totalAccounts, _ := strconv.ParseUint(strings.TrimSpace(items[4]), 10, 64)
				result.totalAccounts = totalAccounts
			}

			if strings.Contains(database, "Key-Value") && strings.Contains(category, "Path trie account nodes") {
				result.accountDBSize = size
			}

			if strings.Contains(database, "Key-Value") && strings.Contains(category, "Transaction index") {
				totalTxs, _ := strconv.ParseUint(strings.TrimSpace(items[4]), 10, 64)
				result.totalTxs = totalTxs
			}

			if strings.Contains(database, "Ancient") && strings.Contains(category, "Bodies") {
				result.txDBSize = size
			}
		}
	}

	return result
}

func prepareDateParam(c *gin.Context, name string) (time.Time, bool) {
	dateStr, ok := getStringParam(c, name)
	if !ok {
		return time.Time{}, false
	}

	date, err := time.Parse("060102", dateStr)
	if err != nil {
		c.JSON(200, gin.H{
			"code":  400,
			"error": name + " cannot be parsed",
		})
		return time.Time{}, false
	}

	return date, true
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

		return 0, false
	}

	param, err := strconv.Atoi(paramStr)
	if err != nil {
		c.JSON(200, gin.H{
			"code":  400,
			"error": name + " cannot cast into int",
		})

		return 0, false
	}

	return param, true
}
