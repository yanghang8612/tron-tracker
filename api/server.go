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
	HttpPort int `toml:"http_port"`
}

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
		router: httpRouter,
		srv: &http.Server{
			Addr:    ":" + strconv.Itoa(serverConfig.HttpPort),
			Handler: httpRouter,
		},

		db:           db,
		serverConfig: serverConfig,
		defiConfig:   deficonfig,

		logger: zap.S().Named("[api]"),
	}
}

func (s *Server) Start() {
	s.router.GET("/last-tracked-block-num", s.lastTrackedBlockNumber)
	s.router.GET("/exchange_statistics", s.exchangesStatistic)
	s.router.GET("/exchange_token_statistics", s.exchangesTokenStatistic)
	s.router.GET("/exchange_token_daily_statistics", s.exchangesTokenDailyStatistic)
	s.router.GET("/exchange_token_weekly_statistics", s.exchangesTokenWeeklyStatistic)
	s.router.GET("/special_statistics", s.specialStatistic)
	s.router.GET("/total_statistics", s.totalStatistics)
	s.router.GET("/do-tronlink-users-weekly-statistics", s.doTronlinkUsersWeeklyStatistics)
	s.router.GET("/tronlink-users-weekly-statistics", s.tronlinkUsersWeeklyStatistics)
	s.router.GET("/exchange_weekly_statistics", s.exchangesWeeklyStatistic)
	s.router.GET("/tron_weekly_statistics", s.tronWeeklyStatistics)
	s.router.GET("/revenue_weekly_statistics", s.revenueWeeklyStatistics)
	s.router.GET("/trx_statistics", s.trxStatistics)
	s.router.GET("/usdt_statistics", s.usdtStatistics)
	s.router.GET("/usdt_storage_statistics", s.usdtStorageStatistics)
	s.router.GET("/user_statistics", s.userStatistics)
	s.router.GET("/top_users", s.topUsers)
	s.router.GET("/token_statistics", s.tokenStatistics)
	s.router.GET("/eth_statistics", s.ethStatistics)
	s.router.GET("/tron_statistics", s.forward)

	s.router.GET("/market_pair_statistics", s.marketPairStatistics)
	s.router.GET("/market_pair_volumes", s.marketPairVolumes)
	s.router.GET("/market_pair_weekly_volumes", s.marketPairWeeklyVolumes)
	s.router.GET("/market_pair_weekly_depths", s.marketPairWeeklyDepths)

	s.router.GET("/tx_analyse", s.txAnalyze)

	go func() {
		err := s.srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			panic(err)
		}
	}()

	s.logger.Infof("API server started at %d", s.serverConfig.HttpPort)
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

func (s *Server) exchangesStatistic(c *gin.Context) {
	startDate, days, ok := prepareStartDateAndDays(c)
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
	startDate, ok := getDateParam(c, "start_date")
	if !ok {
		return
	}

	weeks, ok := getIntParam(c, "weeks", 1)
	if !ok {
		return
	}

	token, ok := c.GetQuery("token")
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
	startDate, ok := getDateParam(c, "date")
	if !ok {
		return
	}

	etsMap := s.db.GetExchangeTokenStatisticsByDateAndDays(startDate, 1)

	token, ok := c.GetQuery("token")

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
	startDate, ok := getDateParam(c, "start_date")
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
	date, ok := getDateParam(c, "date")
	if !ok {
		return
	}

	addr, ok := c.GetQuery("addr")
	if !ok {
		return
	}

	chargeFee, withdrawFee, chargeCount, withdrawCount := s.db.GetSpecialStatisticByDateAndAddr(date.Format("060102"), addr)
	c.JSON(200, gin.H{
		"charge_fee":     chargeFee,
		"withdraw_fee":   withdrawFee,
		"charge_count":   chargeCount,
		"withdraw_count": withdrawCount,
	})
}

func (s *Server) totalStatistics(c *gin.Context) {
	startDate, days, ok := prepareStartDateAndDays(c)
	if !ok {
		return
	}

	currentTotalStatistic := models.NewUserStatistic("")
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
	lastTotalStatistic := models.NewUserStatistic("")
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
	if date, ok := getDateParam(c, "date"); ok {
		go func() {
			s.db.DoTronLinkWeeklyStatistics(date, true)
		}()
	}
}

func (s *Server) tronlinkUsersWeeklyStatistics(c *gin.Context) {
	date, ok := getDateParam(c, "date")
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
	startDate, ok := getDateParam(c, "start_date")
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
	startDate, ok := getDateParam(c, "start_date")
	if !ok {
		return
	}

	curWeekTotalStatistic := models.NewUserStatistic("")
	for i := 0; i < 7; i++ {
		dayStatistic := s.db.GetTotalStatisticsByDate(startDate.AddDate(0, 0, i).Format("060102"))
		curWeekTotalStatistic.Merge(&dayStatistic)
	}

	curWeekUSDTStatistic := &models.TokenStatistic{}
	for i := 0; i < 7; i++ {
		dayStatistic := s.db.GetTokenStatisticsByDateAndToken(startDate.AddDate(0, 0, i).Format("060102"), "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t")
		curWeekUSDTStatistic.Merge(&dayStatistic)
	}

	lastWeekTotalStatistic := models.NewUserStatistic("")
	for i := 1; i <= 7; i++ {
		dayStatistic := s.db.GetTotalStatisticsByDate(startDate.AddDate(0, 0, -i).Format("060102"))
		lastWeekTotalStatistic.Merge(&dayStatistic)
	}

	lastWeekUSDTStatistic := &models.TokenStatistic{}
	for i := 1; i <= 7; i++ {
		dayStatistic := s.db.GetTokenStatisticsByDateAndToken(startDate.AddDate(0, 0, -i).Format("060102"), "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t")
		lastWeekUSDTStatistic.Merge(&dayStatistic)
	}

	c.JSON(200, gin.H{
		"fee":                humanize.Comma(curWeekTotalStatistic.Fee / 7_000_000),
		"fee_change":         utils.FormatChangePercent(lastWeekTotalStatistic.Fee, curWeekTotalStatistic.Fee),
		"usdt_fee":           humanize.Comma(curWeekUSDTStatistic.Fee / 7_000_000),
		"usdt_fee_change":    utils.FormatChangePercent(lastWeekUSDTStatistic.Fee, curWeekUSDTStatistic.Fee),
		"tx_total":           humanize.Comma(curWeekTotalStatistic.TxTotal / 7),
		"tx_total_change":    utils.FormatChangePercent(lastWeekTotalStatistic.TxTotal, curWeekTotalStatistic.TxTotal),
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
	startDate, ok := getDateParam(c, "start_date")
	if !ok {
		return
	}

	curWeekStats := s.getOneWeekRevenueStatistics(startDate)
	totalWeekStats := models.NewUserStatistic("")
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
			tokenStats := s.db.GetTokenStatisticsByDateAndToken(date, addr)
			sunswapV1Fee += tokenStats.Fee
			sunswapV1Energy += tokenStats.EnergyTotal
		}

		for _, addr := range s.defiConfig.SunSwapV2 {
			tokenStats := s.db.GetTokenStatisticsByDateAndToken(date, addr)
			sunswapV2Fee += tokenStats.Fee
			sunswapV2Energy += tokenStats.EnergyTotal
		}

		for _, addr := range s.defiConfig.JustLend {
			tokenStats := s.db.GetTokenStatisticsByDateAndToken(date, addr)
			justlendFee += tokenStats.Fee
			justlendEnergy += tokenStats.EnergyTotal
		}

		for _, addr := range s.defiConfig.BTTC {
			tokenStats := s.db.GetTokenStatisticsByDateAndToken(date, addr)
			bttcFee += tokenStats.Fee
			bttcEnergy += tokenStats.EnergyTotal
		}

		for _, addr := range s.defiConfig.USDTCasino {
			tokenStats := s.db.GetTokenStatisticsByDateAndToken(date, addr)
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
	startDate, days, ok := prepareStartDateAndDays(c)
	if !ok {
		return
	}

	n, ok := getIntParam(c, "n", 50)
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
	startDate, days, ok := prepareStartDateAndDays(c)
	if !ok {
		return
	}

	n, ok := getIntParam(c, "n", 50)
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

func (s *Server) usdtStorageStatistics(c *gin.Context) {
	startDate, days, ok := prepareStartDateAndDays(c)
	if !ok {
		return
	}

	curStats := s.db.GetUSDTStorageStatisticsByDateAndDays(startDate, days)

	withComment := c.DefaultQuery("comment", "false")
	if withComment == "false" {
		c.JSON(200, curStats)
		return
	}

	lastStats := s.db.GetUSDTStorageStatisticsByDateAndDays(startDate.AddDate(0, 0, -7), days)

	comment := strings.Builder{}
	comment.WriteString(fmt.Sprintf("SetStorage:\n"+
		"Average Fee Per Tx: %.2f TRX (%s)\n"+
		"Daily transactions: %s (%s)\n"+
		"Daily total energy: %s (%s)\n"+
		"Daily energy with staking: %s (%s)\n"+
		"Daily energy fee: %s TRX (%s)\n"+
		"Burn energy: %.2f%%\n"+
		"ResetStorage:\n"+
		"Average Fee Per Tx: %.2f TRX (%s)\n"+
		"Daily transactions: %s (%s)\n"+
		"Daily total energy: %s (%s)\n"+
		"Daily energy with staking: %s (%s)\n"+
		"Daily energy fee: %s TRX (%s)\n"+
		"Burn energy: %.2f%%",
		float64(curStats.SetEnergyFee)/float64(curStats.SetTxCount)/1e6,
		utils.FormatChangePercent(int64(lastStats.SetEnergyFee/uint64(lastStats.SetTxCount)), int64(curStats.SetEnergyFee/uint64(curStats.SetTxCount))),
		humanize.Comma(int64(curStats.SetTxCount/7)),
		utils.FormatChangePercent(int64(lastStats.SetTxCount), int64(curStats.SetTxCount)),
		humanize.Comma(int64(curStats.SetEnergyTotal/7)),
		utils.FormatChangePercent(int64(lastStats.SetEnergyTotal), int64(curStats.SetEnergyTotal)),
		humanize.Comma(int64(curStats.SetEnergyUsage+curStats.SetEnergyOriginUsage)/7),
		utils.FormatChangePercent(int64(lastStats.SetEnergyUsage+lastStats.SetEnergyOriginUsage), int64(curStats.SetEnergyUsage+curStats.SetEnergyOriginUsage)),
		humanize.Comma(int64(curStats.SetEnergyFee/7_000_000)),
		utils.FormatChangePercent(int64(lastStats.SetEnergyFee), int64(curStats.SetEnergyFee)),
		100.0-float64(curStats.SetEnergyUsage+curStats.SetEnergyOriginUsage)/float64(curStats.SetEnergyTotal)*100,
		float64(curStats.ResetEnergyFee)/float64(curStats.ResetTxCount)/1e6,
		utils.FormatChangePercent(int64(lastStats.ResetEnergyFee/uint64(lastStats.ResetTxCount)), int64(curStats.ResetEnergyFee/uint64(curStats.ResetTxCount))),
		humanize.Comma(int64(curStats.ResetTxCount/7)),
		utils.FormatChangePercent(int64(lastStats.ResetTxCount), int64(curStats.ResetTxCount)),
		humanize.Comma(int64(curStats.ResetEnergyTotal/7)),
		utils.FormatChangePercent(int64(lastStats.ResetEnergyTotal), int64(curStats.ResetEnergyTotal)),
		humanize.Comma(int64(curStats.ResetEnergyUsage+curStats.ResetEnergyOriginUsage)/7),
		utils.FormatChangePercent(int64(lastStats.ResetEnergyUsage+lastStats.ResetEnergyOriginUsage), int64(curStats.ResetEnergyUsage+curStats.ResetEnergyOriginUsage)),
		humanize.Comma(int64(curStats.ResetEnergyFee/7_000_000)),
		utils.FormatChangePercent(int64(lastStats.ResetEnergyFee), int64(curStats.ResetEnergyFee)),
		100.0-float64(curStats.ResetEnergyUsage+curStats.ResetEnergyOriginUsage)/float64(curStats.ResetEnergyTotal)*100))

	c.String(200, comment.String())
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
	date, ok := getDateParam(c, "date")
	if !ok {
		return
	}

	days, ok := getIntParam(c, "days", 1)
	if !ok {
		return
	}

	user, ok := c.GetQuery("user")
	if !ok {
		c.JSON(500, gin.H{
			"code":  500,
			"error": "user must be provided",
		})
		return
	}

	c.JSON(200, s.db.GetFromStatisticByDateAndUserAndDays(date, user, days))
}

func (s *Server) topUsers(c *gin.Context) {
	startDate, days, ok := prepareStartDateAndDays(c)
	if !ok {
		return
	}

	n, ok := getIntParam(c, "n", 50)
	if !ok {
		return
	}

	orderBy := c.DefaultQuery("order_by", "fee")

	fromStatsMap := s.db.GetTopNFromStatisticByDateAndDays(startDate, days, n, orderBy)

	fromStats := make([]*models.UserStatistic, 0)

	for _, stats := range fromStatsMap {
		fromStats = append(fromStats, stats)
	}

	if orderBy == "fee" {
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
	} else if orderBy == "delegate_total" {
		type ResEntity struct {
			Address       string `json:"address"`
			TotalDelegate int64  `json:"total_delegate"`
		}

		sort.Slice(fromStats, func(i, j int) bool {
			return fromStats[i].DelegateTotal > fromStats[j].DelegateTotal
		})

		resStatsSortedByDelegate := pickTopNAndLastN(fromStats, n, func(t *models.UserStatistic) *ResEntity {
			return &ResEntity{
				Address:       t.Address,
				TotalDelegate: t.DelegateTotal,
			}
		})

		c.JSON(200, resStatsSortedByDelegate[:n])
	} else {
		c.JSON(200, gin.H{
			"code":  500,
			"error": orderBy + " not supported",
		})
	}
}

func (s *Server) tokenStatistics(c *gin.Context) {
	startDate, days, ok := prepareStartDateAndDays(c)
	if !ok {
		return
	}

	token, ok := c.GetQuery("token")
	if ok {
		result := &models.TokenStatistic{}
		for i := 0; i < days; i++ {
			queryDate := startDate.AddDate(0, 0, i)
			dayStat := s.db.GetTokenStatisticsByDateAndToken(queryDate.Format("060102"), token)
			result.Merge(&dayStat)
		}

		c.JSON(200, result)
		return
	}

	n, ok := getIntParam(c, "n", 50)
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
	date, ok := getDateParam(c, "date")
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
	startDate, days, ok := prepareStartDateAndDays(c)
	if !ok {
		return
	}

	token := c.DefaultQuery("token", "tron")

	marketPairStats := s.db.GetMarketPairStatisticsByDateAndDaysAndToken(startDate, days, token)

	result := make([]*models.MarketPairStatistic, 0)

	if _, ok = c.GetQuery("group_by"); ok {
		exchangeStats := make(map[string]*models.MarketPairStatistic)
		for _, mps := range marketPairStats {
			if _, ok := exchangeStats[mps.ExchangeName]; !ok {
				exchangeStats[mps.ExchangeName] = mps
				exchangeStats[mps.ExchangeName].Reputation = 0
				exchangeStats[mps.ExchangeName].Pair = ""
			} else {
				exchangeStats[mps.ExchangeName].Volume += mps.Volume
				exchangeStats[mps.ExchangeName].Percent += mps.Percent
				exchangeStats[mps.ExchangeName].DepthUsdPositiveTwo += mps.DepthUsdPositiveTwo
				exchangeStats[mps.ExchangeName].DepthUsdNegativeTwo += mps.DepthUsdNegativeTwo
			}
		}

		for _, es := range exchangeStats {
			result = append(result, es)
		}
	} else {
		for _, mps := range marketPairStats {
			result = append(result, mps)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Percent > result[j].Percent
	})

	c.JSON(200, result)
}

func (s *Server) marketPairVolumes(c *gin.Context) {
	startDate, days, ok := prepareStartDateAndDays(c)
	if !ok {
		return
	}

	token := c.DefaultQuery("token", "tron")

	marketPairDailyVolumes := s.db.GetMarketPairDailyVolumesByDateAndDaysAndToken(startDate, days, token)

	result := make([]*models.MarketPairStatistic, 0)
	for _, mps := range marketPairDailyVolumes {
		result = append(result, mps)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Volume > result[j].Volume
	})

	c.JSON(200, result)
}

func (s *Server) marketPairWeeklyVolumes(c *gin.Context) {
	startDate, ok := getDateParam(c, "start_date")
	if !ok {
		return
	}

	token := c.DefaultQuery("token", "tron")

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
	startDate, ok := getDateParam(c, "start_date")
	if !ok {
		return
	}

	token := c.DefaultQuery("token", "tron")

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

func (s *Server) txAnalyze(c *gin.Context) {
	startDate, days, ok := prepareStartDateAndDays(c)
	if !ok {
		return
	}

	contract := c.DefaultQuery("contract", "")

	result, ok := getIntParam(c, "result", 11)
	if !ok {
		return
	}

	results := s.db.GetTxsByDateAndDaysAndContractAndResult(startDate, days, contract, result)

	type Transaction struct {
		Height   uint   `json:"height"`
		Index    uint16 `json:"index"`
		Contract string `json:"contract"`
		Fee      int64  `json:"fee"`
	}

	transactions := make([]*Transaction, 0)
	for _, tx := range results {
		transactions = append(transactions, &Transaction{
			Height:   tx.Height,
			Index:    tx.Index,
			Contract: tx.Name,
			Fee:      tx.Fee,
		})
	}

	c.JSON(200, transactions)
}

func prepareStartDateAndDays(c *gin.Context) (time.Time, int, bool) {
	startDate, ok := getDateParam(c, "start_date")
	if !ok {
		return time.Time{}, 0, false
	}

	days, ok := getIntParam(c, "days", 1)
	if !ok {
		return time.Time{}, 0, false
	}

	return startDate, days, true
}

func getDateParam(c *gin.Context, name string) (time.Time, bool) {
	dateStr, ok := c.GetQuery(name)
	if !ok {
		// If date is not present, use yesterday as default
		return time.Now().AddDate(0, 0, -1), true
	}

	date, err := time.Parse("060102", dateStr)
	if err != nil {
		c.JSON(200, gin.H{
			"code":  400,
			"error": name + " cannot be parsed, regular example: 241111",
		})
		return time.Time{}, false
	}

	return date, true
}

func getIntParam(c *gin.Context, name string, defaultValue int) (int, bool) {
	paramStr, ok := c.GetQuery(name)
	if !ok {
		return defaultValue, true
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
