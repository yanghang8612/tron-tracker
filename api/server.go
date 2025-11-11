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

	"tron-tracker/common"
	"tron-tracker/config"
	"tron-tracker/database"
	"tron-tracker/database/models"
	"tron-tracker/google"
	"tron-tracker/net"

	"github.com/dustin/go-humanize"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/go-resty/resty/v2"
	"go.uber.org/zap"
)

type Server struct {
	router *gin.Engine
	srv    *http.Server

	db        *database.RawDB
	updater   *google.Updater
	serverCfg *config.ServerConfig
	defiCfg   *config.DeFiConfig

	isRepairing bool
	isUpdating  bool

	logger *zap.SugaredLogger
}

func New(db *database.RawDB, updater *google.Updater, serverCfg *config.ServerConfig, defiCfg *config.DeFiConfig) *Server {
	httpRouter := gin.Default()
	httpRouter.Use(cors.Default())
	httpsRouter := gin.Default()
	httpsRouter.Use(cors.Default())

	return &Server{
		router: httpRouter,
		srv: &http.Server{
			Addr:    ":" + strconv.Itoa(serverCfg.HttpPort),
			Handler: httpRouter,
		},

		db:        db,
		updater:   updater,
		serverCfg: serverCfg,
		defiCfg:   defiCfg,

		logger: zap.S().Named("[api]"),
	}
}

func (s *Server) Start() {
	s.router.GET("/repair_exchange_statistics", s.repairExchangesStatistic)
	s.router.GET("/exchange_statistics", s.exchangesStatistic)
	s.router.GET("/exchange_statistics_now", s.exchangesStatisticNow)
	s.router.GET("/exchange_token_statistics", s.exchangesTokenStatistic)
	s.router.GET("/exchange_token_daily_statistics", s.exchangesTokenDailyStatistic)
	s.router.GET("/exchange_token_weekly_statistics", s.exchangesTokenWeeklyStatistic)
	s.router.GET("/total_statistics", s.totalStatistics)
	s.router.GET("/do-tronlink-users-weekly-statistics", s.doTronlinkUsersWeeklyStatistics)
	s.router.GET("/tronlink-users-weekly-statistics", s.tronlinkUsersWeeklyStatistics)
	s.router.GET("/exchange_weekly_statistics", s.exchangesWeeklyStatistic)
	s.router.GET("/tron_weekly_statistics", s.tronWeeklyStatistics)
	s.router.GET("/revenue_weekly_statistics", s.revenueWeeklyStatistics)
	s.router.GET("/revenue_ppt_data", s.revenuePPTData)
	s.router.GET("/trx_statistics", s.trxStatistics)
	s.router.GET("/usdt_statistics", s.usdtStatistics)
	s.router.GET("/usdt_storage_statistics", s.usdtStorageStatistics)
	s.router.GET("/user_statistics", s.userStatistics)
	s.router.GET("/user_token_statistics", s.userTokenStatistics)
	s.router.GET("/top_users", s.topUsers)
	s.router.GET("/top_tokens", s.topTokens)
	s.router.GET("/top_user_token_change", s.topUserTokenChange)
	s.router.GET("/token_statistics", s.tokenStatistics)
	s.router.GET("/eth_statistics", s.ethStatistics)
	s.router.GET("/tron_statistics", s.forward)

	s.router.GET("/market_pair_statistics", s.marketPairStatistics)
	s.router.GET("/market_pair_volumes", s.marketPairVolumes)
	s.router.GET("/market_pair_weekly_volumes", s.marketPairWeeklyVolumes)
	s.router.GET("/market_pair_weekly_depths", s.marketPairWeeklyDepths)
	s.router.GET("/token_listing_statistic", s.tokenListingStatistic)
	s.router.GET("/volume_ppt_data", s.volumePPTData)
	s.router.GET("/traverse_ppt_objects", s.traversePPTObjects)
	s.router.GET("/update_ppt_data", s.updatePPTData)

	s.router.GET("/top_delegate", s.topDelegate)
	s.router.GET("/tx_analyse", s.txAnalyze)
	s.router.GET("/count_for_date", s.countForDate)

	s.router.GET("/", s.lastTrackedBlockNumber)
	s.router.GET("/last-tracked-block-num", s.lastTrackedBlockNumber)
	s.router.GET("/system/exchanges", s.getExchange)
	s.router.GET("/system/add_exchange", s.addExchange)

	s.router.Static("/usdt_transfer_statistics", "/data/usdt")

	go func() {
		err := s.srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			panic(err)
		}
	}()

	s.logger.Infof("API server started at %d", s.serverCfg.HttpPort)
}

func (s *Server) Stop() {
	if err := s.srv.Shutdown(context.Background()); err != nil {
		panic(err)
	}
}

func (s *Server) repairExchangesStatistic(c *gin.Context) {
	startDate, days, ok := prepareStartDateAndDays(c, lastWeek(), 7)
	if !ok {
		return
	}

	if s.isRepairing {
		c.JSON(200, "already repairing")
		return
	}

	s.isRepairing = true
	go func() {
		for i := 0; i < days; i++ {
			queryDate := startDate.AddDate(0, 0, i)
			s.db.DoExchangeStatistics(queryDate.Format("060102"))
		}

		s.isRepairing = false
	}()

	c.JSON(200, "repairing started")
}

func (s *Server) exchangesStatistic(c *gin.Context) {
	startDate, days, ok := prepareStartDateAndDays(c, lastWeek(), 7)
	if !ok {
		return
	}

	esMap := s.db.GetExchangeStatisticsByDateDays(startDate, days)
	totalEnergy, totalFee, totalEnergyUsage := int64(0), int64(0), int64(0)
	dateRangeStr := startDate.Format("060102") + "~" + startDate.AddDate(0, 0, days-1).Format("060102")
	resultArray := make([]*models.ExchangeStatistic, 0)
	for _, es := range esMap {
		totalEnergy += es.ChargeEnergyTotal + es.CollectEnergyTotal + es.WithdrawEnergyTotal
		totalFee += es.ChargeFee + es.CollectFee + es.WithdrawFee
		totalEnergyUsage += es.ChargeEnergyUsage + es.CollectEnergyUsage

		es.Date = dateRangeStr
		es.ClearAmountFields()
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

func (s *Server) exchangesStatisticNow(c *gin.Context) {
	date, ok := getDateParam(c, "date", lastWeek())
	if !ok {
		return
	}

	batchSize, ok := getIntParam(c, "batch_size", 100)
	if !ok {
		return
	}

	exchangeStats := make(map[string]*models.ExchangeStatistic)
	s.db.TraverseTransactions(date.Format("060102"), batchSize, func(tx *models.Transaction) {
		if len(s.db.GetTokenName(tx.Name)) == 0 ||
			len(tx.FromAddr) == 0 || len(tx.ToAddr) == 0 ||
			tx.Amount.Length() < 6 {
			return
		}

		from := tx.FromAddr
		to := tx.ToAddr

		if s.db.IsExchange(from) {
			exchange := s.db.GetExchange(from).Name

			if _, ok := exchangeStats[exchange]; !ok {
				exchangeStats[exchange] =
					models.NewExchangeStatistic("", exchange, "")
			}
			exchangeStats[exchange].TotalFee += tx.Fee
			exchangeStats[exchange].WithdrawFee += tx.Fee
			exchangeStats[exchange].WithdrawTxCount += 1

		} else if _, fromIsCharger := s.db.GetChargers()[from]; fromIsCharger && s.db.IsExchange(to) {
			exchange := s.db.GetExchange(to).Name

			if _, ok := exchangeStats[exchange]; !ok {
				exchangeStats[exchange] =
					models.NewExchangeStatistic("", exchange, "")
			}
			exchangeStats[exchange].TotalFee += tx.Fee
			exchangeStats[exchange].CollectFee += tx.Fee
			exchangeStats[exchange].CollectTxCount += 1

		} else if charger, toIsCharger := s.db.GetChargers()[to]; toIsCharger {
			exchange := charger.ExchangeName

			if _, ok := exchangeStats[exchange]; !ok {
				exchangeStats[exchange] =
					models.NewExchangeStatistic("", exchange, "")
			}
			exchangeStats[exchange].TotalFee += tx.Fee
			exchangeStats[exchange].ChargeTxCount += 1
			exchangeStats[exchange].ChargeFee += tx.Fee
		}
	})

	resultArray := make([]*models.ExchangeStatistic, 0)
	for _, es := range exchangeStats {
		resultArray = append(resultArray, es)
	}

	sort.Slice(resultArray, func(i, j int) bool {
		return resultArray[i].TotalFee > resultArray[j].TotalFee
	})

	c.JSON(200, resultArray)
}

type ExchangeTokenStatisticInResult struct {
	Name           string `json:"name"`
	Fee            int64  `json:"fee"`
	FeePercent     string `json:"fee_percent"`
	TxCount        int64  `json:"tx_count"`
	TxCountPercent string `json:"tx_count_percent"`
}

func (s *Server) exchangesTokenStatistic(c *gin.Context) {
	startDate, ok := getDateParam(c, "start_date", lastWeek())
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
		weeklyStat := s.db.GetExchangeTokenStatisticsByDateDays(weekStartDate, 7)
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
	startDate, ok := getDateParam(c, "date", yesterday())
	if !ok {
		return
	}

	etsMap := s.db.GetExchangeTokenStatisticsByDateDays(startDate, 1)

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
	startDate, ok := getDateParam(c, "start_date", lastWeek())
	if !ok {
		return
	}

	etsMap := s.db.GetExchangeTokenStatisticsByDateDays(startDate, 7)

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
		res.FeePercent = common.FormatOfPercent(chargeFee, res.Fee)
		res.TxCountPercent = common.FormatOfPercent(chargeCount, res.TxCount)
	}

	for _, res := range withdrawResults {
		res.FeePercent = common.FormatOfPercent(withdrawFee, res.Fee)
		res.TxCountPercent = common.FormatOfPercent(withdrawCount, res.TxCount)
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

func (s *Server) totalStatistics(c *gin.Context) {
	startDate, days, ok := prepareStartDateAndDays(c, lastWeek(), 7)
	if !ok {
		return
	}

	curTotalStatistic := s.db.GetTotalStatisticsByDateDays(startDate, days)

	currentPhishingStatistic := &models.PhishingStatistic{}
	for i := 0; i < days; i++ {
		dayPhishingStatistic := s.db.GetPhishingStatisticsByDate(startDate.AddDate(0, 0, i).Format("060102"))
		currentPhishingStatistic.Merge(dayPhishingStatistic)
	}
	currentPhishingStatistic.Date = ""

	withComment, ok := getBoolParam(c, "with_comment", false)
	if !ok {
		return
	}

	if !withComment {
		c.JSON(200, gin.H{
			"total_statistic": curTotalStatistic,
			// "phishing_statistic": currentPhishingStatistic,
		})
		return
	}

	lastStartDate := startDate.AddDate(0, 0, -days)
	lastTotalStatistic := s.db.GetTotalStatisticsByDateDays(lastStartDate, days)

	comment := strings.Builder{}
	comment.WriteString(fmt.Sprintf(
		"上周TRX转账: %s笔(%s), 相比上上周 %s笔\n上周低价值TRX转账: %s笔(%s), 相比上上周 %s笔\n\n",
		humanize.Comma(curTotalStatistic.TRXTotal),
		common.FormatChangePercent(lastTotalStatistic.TRXTotal, curTotalStatistic.TRXTotal),
		humanize.Comma(curTotalStatistic.TRXTotal-lastTotalStatistic.TRXTotal),
		humanize.Comma(curTotalStatistic.SmallTRXTotal),
		common.FormatChangePercent(lastTotalStatistic.SmallTRXTotal, curTotalStatistic.SmallTRXTotal),
		humanize.Comma(curTotalStatistic.SmallTRXTotal-lastTotalStatistic.SmallTRXTotal)))
	comment.WriteString(fmt.Sprintf(
		"上周USDT转账: %s笔(%s), 相比上上周 %s笔\n上周低价值USDT转账: %s笔(%s), 相比上上周 %s笔",
		humanize.Comma(curTotalStatistic.USDTTotal),
		common.FormatChangePercent(lastTotalStatistic.USDTTotal, curTotalStatistic.USDTTotal),
		humanize.Comma(curTotalStatistic.USDTTotal-lastTotalStatistic.USDTTotal),
		humanize.Comma(curTotalStatistic.SmallUSDTTotal),
		common.FormatChangePercent(lastTotalStatistic.SmallUSDTTotal, curTotalStatistic.SmallUSDTTotal),
		humanize.Comma(curTotalStatistic.SmallUSDTTotal-lastTotalStatistic.SmallUSDTTotal)))

	c.JSON(200, gin.H{
		"total_statistic": curTotalStatistic,
		// "phishing_statistic": currentPhishingStatistic,
		"comment": comment.String(),
	})
}

func (s *Server) doTronlinkUsersWeeklyStatistics(c *gin.Context) {
	if date, ok := getDateParam(c, "date", lastWeek()); ok {
		go func() {
			s.db.DoTronLinkWeeklyStatistics(date, true)
		}()

		c.String(200, "TronLink users weekly statistics is being updated, please check back later.")
	}
}

func (s *Server) tronlinkUsersWeeklyStatistics(c *gin.Context) {
	date, ok := getDateParam(c, "date", lastWeek())
	if !ok {
		return
	}

	statsResultFile, err := os.Open(fmt.Sprintf("/data/tronlink/week%s_stats.txt", date.Format("20060102")))
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
		"total_fee":       nums[0],
		"total_energy":    nums[1],
		"withdraw_fee":    nums[2],
		"withdraw_energy": nums[3],
		"collect_fee":     nums[4],
		"collect_energy":  nums[5],
		"charge_fee":      nums[6],
		"charge_energy":   nums[7],
	})
}

func (s *Server) exchangesWeeklyStatistic(c *gin.Context) {
	startDate, ok := getDateParam(c, "start_date", lastWeek())
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
			ChangeFromLastWeek: common.FormatChangePercent(lastWeekStats[name], fee),
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
	esMap := s.db.GetExchangeStatisticsByDateDays(startDate, 7)
	for _, es := range esMap {
		resultMap[es.Name] += es.TotalFee
		resultMap["total_fee"] += es.TotalFee
	}
	return resultMap
}

func (s *Server) tronWeeklyStatistics(c *gin.Context) {
	startDate, ok := getDateParam(c, "start_date", lastWeek())
	if !ok {
		return
	}

	curWeekTotalStatistic := s.db.GetTotalStatisticsByDateDays(startDate, 7)
	curWeekUSDTStatistic := s.db.GetTokenStatisticsByDateDaysToken(startDate, 7, "USDT")

	lastWeekStartDate := startDate.AddDate(0, 0, -7)
	lastWeekTotalStatistic := s.db.GetTotalStatisticsByDateDays(lastWeekStartDate, 7)
	lastWeekUSDTStatistic := s.db.GetTokenStatisticsByDateDaysToken(lastWeekStartDate, 7, "USDT")

	c.JSON(200, gin.H{
		"fee":                humanize.Comma(curWeekTotalStatistic.Fee / 7_000_000),
		"fee_change":         common.FormatChangePercent(lastWeekTotalStatistic.Fee, curWeekTotalStatistic.Fee),
		"usdt_fee":           humanize.Comma(curWeekUSDTStatistic.Fee / 7_000_000),
		"usdt_fee_change":    common.FormatChangePercent(lastWeekUSDTStatistic.Fee, curWeekUSDTStatistic.Fee),
		"tx_total":           humanize.Comma(curWeekTotalStatistic.TxTotal / 7),
		"tx_total_change":    common.FormatChangePercent(lastWeekTotalStatistic.TxTotal, curWeekTotalStatistic.TxTotal),
		"trx_total":          humanize.Comma(curWeekTotalStatistic.TRXTotal / 7),
		"trx_total_change":   common.FormatChangePercent(lastWeekTotalStatistic.TRXTotal, curWeekTotalStatistic.TRXTotal),
		"trc10_total":        humanize.Comma(curWeekTotalStatistic.TRC10Total / 7),
		"trc10_total_change": common.FormatChangePercent(lastWeekTotalStatistic.TRC10Total, curWeekTotalStatistic.TRC10Total),
		"sc_total":           humanize.Comma(curWeekTotalStatistic.SCTotal / 7),
		"sc_total_change":    common.FormatChangePercent(lastWeekTotalStatistic.SCTotal, curWeekTotalStatistic.SCTotal),
		"usdt_total":         humanize.Comma(curWeekTotalStatistic.USDTTotal / 7),
		"usdt_total_change":  common.FormatChangePercent(lastWeekTotalStatistic.USDTTotal, curWeekTotalStatistic.USDTTotal),
		"other_total":        humanize.Comma((curWeekTotalStatistic.SCTotal - curWeekTotalStatistic.USDTTotal) / 7),
		"other_total_change": common.FormatChangePercent(lastWeekTotalStatistic.SCTotal-lastWeekTotalStatistic.USDTTotal, curWeekTotalStatistic.SCTotal-curWeekTotalStatistic.USDTTotal),
	})
}

func (s *Server) revenueWeeklyStatistics(c *gin.Context) {
	startDate, ok := getDateParam(c, "start_date", lastWeek())
	if !ok {
		return
	}

	curWeekRevenueStats := s.getOneWeekRevenueStatistics(startDate)
	curWeekTotalStats := s.db.GetTotalStatisticsByDateDays(startDate, 7)
	lastWeekRevenueStats := s.getOneWeekRevenueStatistics(startDate.AddDate(0, 0, -7))

	result := make(map[string]any)
	for k, v := range curWeekRevenueStats {
		if strings.Contains(k, "fee") {
			result[k] = fmt.Sprintf("%s TRX (%s)", humanize.Comma(v), common.FormatChangePercent(lastWeekRevenueStats[k], v))
			result[k+"_of_total"] = common.FormatOfPercent(curWeekTotalStats.Fee/7_000_000, v)
		} else {
			result[k] = fmt.Sprintf("%s (%s)", humanize.Comma(v), common.FormatChangePercent(lastWeekRevenueStats[k], v))
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
		sunswapV3Fee     int64
		sunswapV3Energy  int64
		sunpumpFee       int64
		sunpumpEnergy    int64
		justlendFee      int64
		justlendEnergy   int64
		bttcFee          int64
		bttcEnergy       int64
		usdtcasinoFee    int64
		usdtcasinoEnergy int64
	)

	totalStats := s.db.GetTotalStatisticsByDateDays(startDate, 7)
	totalFee = totalStats.Fee
	totalEnergy = totalStats.EnergyTotal

	esMap := s.db.GetExchangeStatisticsByDateDays(startDate, 7)
	for _, es := range esMap {
		exchangeFee += es.TotalFee
		exchangeEnergy += es.ChargeEnergyTotal + es.CollectEnergyTotal + es.WithdrawEnergyTotal
	}

	for _, addr := range s.defiCfg.SunSwapV1 {
		tokenStats := s.db.GetTokenStatisticsByDateDaysToken(startDate, 7, addr)
		sunswapV1Fee += tokenStats.Fee
		sunswapV1Energy += tokenStats.EnergyTotal
	}

	for _, addr := range s.defiCfg.SunSwapV2 {
		tokenStats := s.db.GetTokenStatisticsByDateDaysToken(startDate, 7, addr)
		sunswapV2Fee += tokenStats.Fee
		sunswapV2Energy += tokenStats.EnergyTotal
	}

	for _, addr := range s.defiCfg.SunSwapV3 {
		tokenStats := s.db.GetTokenStatisticsByDateDaysToken(startDate, 7, addr)
		sunswapV3Fee += tokenStats.Fee
		sunswapV3Energy += tokenStats.EnergyTotal
	}

	for _, addr := range s.defiCfg.SunPump {
		tokenStats := s.db.GetTokenStatisticsByDateDaysToken(startDate, 7, addr)
		sunpumpFee += tokenStats.Fee
		sunpumpEnergy += tokenStats.EnergyTotal
	}

	for _, addr := range s.defiCfg.JustLend {
		tokenStats := s.db.GetTokenStatisticsByDateDaysToken(startDate, 7, addr)
		justlendFee += tokenStats.Fee
		justlendEnergy += tokenStats.EnergyTotal
	}

	for _, addr := range s.defiCfg.BTTC {
		tokenStats := s.db.GetTokenStatisticsByDateDaysToken(startDate, 7, addr)
		bttcFee += tokenStats.Fee
		bttcEnergy += tokenStats.EnergyTotal
	}

	for _, addr := range s.defiCfg.USDTCasino {
		tokenStats := s.db.GetTokenStatisticsByDateDaysToken(startDate, 7, addr)
		usdtcasinoFee += tokenStats.Fee
		usdtcasinoEnergy += tokenStats.EnergyTotal
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
		"sunswap_v3_fee":    sunswapV3Fee / 7_000_000,
		"sunswap_v3_energy": sunswapV3Energy / 7,
		"sunpump_fee":       sunpumpFee / 7_000_000,
		"sunpump_energy":    sunpumpEnergy / 7,
		"justlend_fee":      justlendFee / 7_000_000,
		"justlend_energy":   justlendEnergy / 7,
		"bttc_fee":          bttcFee / 7_000_000,
		"bttc_energy":       bttcEnergy / 7,
		"usdtcasino_fee":    usdtcasinoFee / 7_000_000,
		"usdtcasino_energy": usdtcasinoEnergy / 7,
	}
}

func (s *Server) revenuePPTData(c *gin.Context) {
	startDate, days, ok := prepareStartDateAndDays(c, lastMonth(), 30)
	if !ok {
		return
	}

	isJson, ok := getBoolParam(c, "json", false)
	if !ok {
		return
	}

	if isJson {
		type ResEntity struct {
			Date             string  `json:"date"`
			TrxPrice         float64 `json:"trx_price"`
			TotalEnergyFee   int64   `json:"total_energy_fee"`
			TotalNetFee      int64   `json:"total_net_fee"`
			TotalEnergyUsage int64   `json:"total_energy_usage"`
			TotalNetUsage    int64   `json:"total_net_usage"`
			UsdtEnergyFee    int64   `json:"usdt_energy_fee"`
			UsdtNetFee       int64   `json:"usdt_net_fee"`
			UsdtEnergyUsage  int64   `json:"usdt_energy_usage"`
			UsdtNetUsage     int64   `json:"usdt_net_usage"`
		}

		result := make([]ResEntity, 0)
		for i := 0; i < days; i++ {
			queryDate := startDate.AddDate(0, 0, i)
			trxPrice := s.db.GetClosePriceByTokenDate("TRX", queryDate)
			totalStats := s.db.GetTotalStatisticsByDateDays(queryDate, 1)
			usdtStats := s.db.GetTokenStatisticsByDateDaysToken(queryDate, 1, "USDT")

			result = append(result, ResEntity{
				Date:             queryDate.Format("2006-01-02"),
				TrxPrice:         trxPrice,
				TotalEnergyFee:   totalStats.EnergyFee,
				TotalNetFee:      totalStats.NetFee,
				TotalEnergyUsage: totalStats.EnergyUsage + totalStats.EnergyOriginUsage,
				TotalNetUsage:    totalStats.NetUsage,
				UsdtEnergyFee:    usdtStats.EnergyFee,
				UsdtNetFee:       usdtStats.NetFee,
				UsdtEnergyUsage:  usdtStats.EnergyUsage + usdtStats.EnergyOriginUsage,
				UsdtNetUsage:     usdtStats.NetUsage,
			})
		}

		c.JSON(200, result)
	} else {
		result := strings.Builder{}
		for i := 0; i < days; i++ {
			queryDate := startDate.AddDate(0, 0, i)
			trxPrice := s.db.GetClosePriceByTokenDate("TRX", queryDate)
			totalStats := s.db.GetTotalStatisticsByDateDays(queryDate, 1)
			usdtStats := s.db.GetTokenStatisticsByDateDaysToken(queryDate, 1, "USDT")

			result.WriteString(fmt.Sprintf("%s %f %d %d %d %d %d %d %d %d\n", queryDate.Format("2006-01-02"), trxPrice,
				totalStats.EnergyFee, totalStats.NetFee, totalStats.EnergyUsage+totalStats.EnergyOriginUsage, totalStats.NetUsage,
				usdtStats.EnergyFee, usdtStats.NetFee, usdtStats.EnergyUsage+usdtStats.EnergyOriginUsage, usdtStats.NetUsage))
		}

		c.String(200, result.String())
	}

}

func (s *Server) trxStatistics(c *gin.Context) {
	startDate, days, ok := prepareStartDateAndDays(c, lastWeek(), 7)
	if !ok {
		return
	}

	n, ok := getIntParam(c, "n", 50)
	if !ok {
		return
	}

	curWeekStats := s.db.GetFromStatisticByDateDays(startDate, days)
	lastWeekStats := s.db.GetFromStatisticByDateDays(startDate.AddDate(0, 0, -7), days)

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
	startDate, days, ok := prepareStartDateAndDays(c, lastWeek(), 7)
	if !ok {
		return
	}

	n, ok := getIntParam(c, "n", 50)
	if !ok {
		return
	}

	usdtStatsMap := s.db.GetUserTokenStatisticsByDateDaysToken(startDate, days, "USDT", "")

	filterExchange, ok := getBoolParam(c, "filter_exchange", false)
	if !ok {
		return
	}

	usdtStats := make([]*models.UserTokenStatistic, 0)
	for _, stats := range usdtStatsMap {
		if filterExchange && s.db.IsExchange(stats.User) {
			continue
		}
		usdtStats = append(usdtStats, stats)
	}

	orderBy := c.DefaultQuery("order_by", "fee")

	var topNFrom []*models.UserTokenStatistic
	switch orderBy {
	case "fee":
		topNFrom = common.TopN(usdtStats, n, func(a, b *models.UserTokenStatistic) bool {
			return a.FromFee > b.FromFee
		})
	case "tx":
		topNFrom = common.TopN(usdtStats, n, func(a, b *models.UserTokenStatistic) bool {
			return a.FromTXCount > b.FromTXCount
		})
	case "energy":
		topNFrom = common.TopN(usdtStats, n, func(a, b *models.UserTokenStatistic) bool {
			return a.FromEnergyTotal > b.FromEnergyTotal
		})
	default:
		c.JSON(200, gin.H{
			"code":  500,
			"error": orderBy + " not supported",
		})
		return
	}

	var topNTo []*models.UserTokenStatistic
	switch orderBy {
	case "fee":
		topNTo = common.TopN(usdtStats, n, func(a, b *models.UserTokenStatistic) bool {
			return a.ToFee > b.ToFee
		})
	case "tx":
		topNTo = common.TopN(usdtStats, n, func(a, b *models.UserTokenStatistic) bool {
			return a.ToTXCount > b.ToTXCount
		})
	default:
		topNTo = common.TopN(usdtStats, n, func(a, b *models.UserTokenStatistic) bool {
			return a.ToEnergyTotal > b.ToEnergyTotal
		})
	}

	c.JSON(200, gin.H{
		"top_from": topNFrom,
		"top_to":   topNTo,
	})
}

func (s *Server) usdtStorageStatistics(c *gin.Context) {
	startDate, days, ok := prepareStartDateAndDays(c, lastWeek(), 7)
	if !ok {
		return
	}

	curStats := s.db.GetUSDTStorageStatisticsByDateDays(startDate, days)

	inComment, ok := getBoolParam(c, "in_comment", false)
	if !ok {
		return
	}

	if !inComment {
		c.JSON(200, curStats)
		return
	}

	lastStats := s.db.GetUSDTStorageStatisticsByDateDays(startDate.AddDate(0, 0, -7), days)

	c.String(200, common.FormatStorageDiffReport(curStats, lastStats))
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
	startDate, days, ok := prepareStartDateAndDays(c, yesterday(), 1)
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

	c.JSON(200, s.db.GetFromStatisticByDateUserDays(startDate, user, days))
}

func (s *Server) userTokenStatistics(c *gin.Context) {
	startDate, days, ok := prepareStartDateAndDays(c, yesterday(), 1)
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

	token, ok := c.GetQuery("token")
	if !ok {
		c.JSON(500, gin.H{
			"code":  500,
			"error": "token must be provided",
		})
		return
	}

	c.JSON(200, s.db.GetUserTokenStatisticsByDateDaysUserToken(startDate, days, user, token))
}

func (s *Server) topUsers(c *gin.Context) {
	startDate, days, ok := prepareStartDateAndDays(c, lastWeek(), 7)
	if !ok {
		return
	}

	n, ok := getIntParam(c, "n", 50)
	if !ok {
		return
	}

	orderBy := c.DefaultQuery("order_by", "fee")

	fromStatsMap := s.db.GetTopNFromStatisticByDateDays(startDate, days, n, orderBy)

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

func (s *Server) topTokens(c *gin.Context) {
	startDate, days, ok := prepareStartDateAndDays(c, lastWeek(), 7)
	if !ok {
		return
	}

	n, ok := getIntParam(c, "n", 10)
	if !ok {
		return
	}

	orderBy := c.DefaultQuery("order_by", "fee")

	curTokenStats := s.db.GetTokenStatisticsByDateDays(startDate, days)
	preTokenStats := s.db.GetTokenStatisticsByDateDays(startDate.AddDate(0, 0, -days), days)

	var resultArray []*models.TokenStatistic
	for _, ts := range curTokenStats {
		resultArray = append(resultArray, ts)
	}

	type ResEntity struct {
		Address string `json:"address"`
		Change  int64  `json:"change"`
	}
	var results []*ResEntity
	if orderBy == "fee" {
		sort.Slice(resultArray, func(i, j int) bool {
			preFeeWithI := int64(0)
			if preTokenStats[resultArray[i].Address] != nil {
				preFeeWithI = preTokenStats[resultArray[i].Address].Fee
			}
			preFeeWithJ := int64(0)
			if preTokenStats[resultArray[j].Address] != nil {
				preFeeWithJ = preTokenStats[resultArray[j].Address].Fee
			}
			return (resultArray[i].Fee - preFeeWithI) > (resultArray[j].Fee - preFeeWithJ)
		})
		results = pickTopNAndLastN(resultArray, n, func(t *models.TokenStatistic) *ResEntity {
			preFee := int64(0)
			if preTokenStats[t.Address] != nil {
				preFee = preTokenStats[t.Address].Fee
			}
			return &ResEntity{
				Address: t.Address,
				Change:  t.Fee - preFee,
			}
		})
	} else if orderBy == "tx" {
		sort.Slice(resultArray, func(i, j int) bool {
			preTxWithI := int64(0)
			if preTokenStats[resultArray[i].Address] != nil {
				preTxWithI = preTokenStats[resultArray[i].Address].TxTotal
			}
			preTxWithJ := int64(0)
			if preTokenStats[resultArray[j].Address] != nil {
				preTxWithJ = preTokenStats[resultArray[j].Address].TxTotal
			}
			return (resultArray[i].TxTotal - preTxWithI) > (resultArray[j].TxTotal - preTxWithJ)
		})
		results = pickTopNAndLastN(resultArray, n, func(t *models.TokenStatistic) *ResEntity {
			preTx := int64(0)
			if preTokenStats[t.Address] != nil {
				preTx = preTokenStats[t.Address].TxTotal
			}
			return &ResEntity{
				Address: t.Address,
				Change:  t.TxTotal - preTx,
			}
		})
	} else if orderBy == "energy_total" {
		sort.Slice(resultArray, func(i, j int) bool {
			preEnergyWithI := int64(0)
			if preTokenStats[resultArray[i].Address] != nil {
				preEnergyWithI = preTokenStats[resultArray[i].Address].EnergyTotal
			}
			preEnergyWithJ := int64(0)
			if preTokenStats[resultArray[j].Address] != nil {
				preEnergyWithJ = preTokenStats[resultArray[j].Address].EnergyTotal
			}
			return (resultArray[i].EnergyTotal - preEnergyWithI) > (resultArray[j].EnergyTotal - preEnergyWithJ)
		})
		results = pickTopNAndLastN(resultArray, n, func(t *models.TokenStatistic) *ResEntity {
			preEnergy := int64(0)
			if preTokenStats[t.Address] != nil {
				preEnergy = preTokenStats[t.Address].EnergyTotal
			}
			return &ResEntity{
				Address: t.Address,
				Change:  t.EnergyTotal - preEnergy,
			}
		})
	} else {
		c.JSON(200, gin.H{
			"code":  500,
			"error": orderBy + " not supported",
		})
		return
	}

	c.JSON(200, results)
}

func (s *Server) topUserTokenChange(c *gin.Context) {
	startDate, days, ok := prepareStartDateAndDays(c, lastWeek(), 7)
	if !ok {
		return
	}

	n, ok := getIntParam(c, "n", 50)
	if !ok {
		return
	}

	token := c.DefaultQuery("token", "USDT")

	curUserTokenStatsMap := s.db.GetUserTokenStatisticsByDateDaysToken(startDate, days, token, "from_fee > 0")
	preUserTokenStatsMap := s.db.GetUserTokenStatisticsByDateDaysToken(startDate.AddDate(0, 0, -days), days, token, "from_fee > 0")

	var resultArray []*models.UserTokenStatistic
	for _, uts := range curUserTokenStatsMap {
		resultArray = append(resultArray, uts)
	}

	type ResEntity struct {
		Address   string `json:"address"`
		Fee       int64  `json:"fee"`
		FeeChange int64  `json:"change"`
		TxCount   int64  `json:"tx_count"`
		TxChange  int64  `json:"tx_change"`
	}

	for user, uts := range curUserTokenStatsMap {
		uts.ToFee = uts.FromFee

		if preUts, ok := preUserTokenStatsMap[user]; ok {
			uts.ToFee = uts.FromFee - preUts.FromFee
			uts.ToTXCount = uts.FromTXCount - preUts.FromTXCount
		}
	}

	for user, uts := range preUserTokenStatsMap {
		if _, ok := curUserTokenStatsMap[user]; !ok {
			uts.FromFee = 0
			uts.ToFee = -uts.FromFee
			uts.FromTXCount = 0
			uts.ToTXCount = -uts.FromTXCount
			resultArray = append(resultArray, uts)
		}
	}

	sort.Slice(resultArray, func(i, j int) bool {
		return resultArray[i].ToFee > resultArray[j].ToFee
	})

	increaseSum, decreaseSum := int64(0), int64(0)
	for _, uts := range resultArray {
		if uts.FromFee > 0 {
			increaseSum += uts.ToFee
		}

		if uts.FromFee < 0 {
			decreaseSum += uts.ToFee
		}
	}

	results := pickTopNAndLastN(resultArray, n, func(t *models.UserTokenStatistic) *ResEntity {
		return &ResEntity{
			Address:   t.User,
			Fee:       t.FromFee,
			FeeChange: t.ToFee,
			TxCount:   t.FromTXCount,
			TxChange:  t.ToTXCount,
		}
	})

	c.JSON(200, gin.H{
		"top_changes":  results,
		"increase_sum": increaseSum,
		"decrease_sum": decreaseSum,
	})
}

func (s *Server) tokenStatistics(c *gin.Context) {
	startDate, days, ok := prepareStartDateAndDays(c, lastWeek(), 7)
	if !ok {
		return
	}

	token, ok := c.GetQuery("token")
	if ok {
		c.JSON(200, s.db.GetTokenStatisticsByDateDaysToken(startDate, days, token))
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
	for _, ts := range s.db.GetTokenStatisticsByDateDays(startDate, days) {
		switch len(ts.Address) {
		case 7:
			TRC10Stat.Merge(ts)
		case 34:
			if ts.Address != s.db.GetTokenAddress("USDT") {
				NonUSDTStat.Merge(ts)
			}
		}
		resultArray = append(resultArray, ts)
	}

	orderBy := c.DefaultQuery("order_by", "fee")

	switch orderBy {
	case "fee":
		sort.Slice(resultArray, func(i, j int) bool {
			return resultArray[i].Fee > resultArray[j].Fee
		})
	case "tx":
		sort.Slice(resultArray, func(i, j int) bool {
			return resultArray[i].TxTotal > resultArray[j].TxTotal
		})
	case "energy":
		sort.Slice(resultArray, func(i, j int) bool {
			return resultArray[i].EnergyTotal > resultArray[j].EnergyTotal
		})
	default:
		c.JSON(200, gin.H{
			"code":  500,
			"error": orderBy + " not supported",
		})
		return
	}

	if len(resultArray) > n {
		resultArray = resultArray[:n]
	}
	resultArray = append(resultArray, TRC10Stat, NonUSDTStat)

	c.JSON(200, resultArray)
}

func (s *Server) ethStatistics(c *gin.Context) {
	date, ok := getDateParam(c, "date", lastWeek())
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
	startDate, days, ok := prepareStartDateAndDays(c, lastWeek(), 7)
	if !ok {
		return
	}

	token := c.DefaultQuery("token", "TRX")

	groupByExchange, ok := getBoolParam(c, "group_by_exchange", false)
	if !ok {
		return
	}

	curMarketPairStats := s.db.GetMergedMarketPairStatistics(startDate, days, token, true, groupByExchange)
	lastMarketPairStats := s.db.GetMergedMarketPairStatistics(startDate.AddDate(0, 0, -days), days, token, true, groupByExchange)

	type JsonStat struct {
		ID                        float64 `json:"-"`
		ExchangeName              string  `json:"exchange_name,omitempty"`
		Pair                      string  `json:"pair,omitempty"`
		Volume                    string  `json:"volume,omitempty"`
		VolumeChange              string  `json:"volume_change,omitempty"`
		Percent                   string  `json:"percent,omitempty"`
		PercentChange             string  `json:"percent_change,omitempty"`
		DepthUsdPositiveTwo       string  `json:"depth_usd_positive_two,omitempty"`
		DepthUsdPositiveTwoChange string  `json:"depth_usd_positive_two_change,omitempty"`
		DepthUsdNegativeTwo       string  `json:"depth_usd_negative_two,omitempty"`
		DepthUsdNegativeTwoChange string  `json:"depth_usd_negative_two_change,omitempty"`
	}

	statResults := make([]*JsonStat, 0)
	for key, curStat := range curMarketPairStats {
		jsonStat := &JsonStat{
			ID:                  curStat.Percent,
			ExchangeName:        curStat.ExchangeName,
			Volume:              humanize.SIWithDigits(curStat.Volume, 2, ""),
			Percent:             fmt.Sprintf("%.2f%%", curStat.Percent*100),
			DepthUsdPositiveTwo: humanize.SIWithDigits(curStat.DepthUsdPositiveTwo, 2, ""),
			DepthUsdNegativeTwo: humanize.SIWithDigits(curStat.DepthUsdNegativeTwo, 2, ""),
		}

		if !groupByExchange {
			jsonStat.Pair = curStat.Pair
		}

		if lastStat, ok := lastMarketPairStats[key]; ok {
			jsonStat.VolumeChange = common.FormatChangePercent(int64(lastStat.Volume), int64(curStat.Volume))
			jsonStat.PercentChange = common.FormatPercentWithSign((curStat.Percent - lastStat.Percent) * 100)
			jsonStat.DepthUsdPositiveTwoChange = common.FormatChangePercent(int64(lastStat.DepthUsdPositiveTwo), int64(curStat.DepthUsdPositiveTwo))
			jsonStat.DepthUsdNegativeTwoChange = common.FormatChangePercent(int64(lastStat.DepthUsdNegativeTwo), int64(curStat.DepthUsdNegativeTwo))
		} else {
			jsonStat.VolumeChange = "+∞%"
			jsonStat.PercentChange = common.FormatPercentWithSign(curStat.Percent * 100)
			jsonStat.DepthUsdPositiveTwoChange = "+∞%"
			jsonStat.DepthUsdNegativeTwoChange = "+∞%"
		}

		statResults = append(statResults, jsonStat)
	}

	for key, lastStat := range lastMarketPairStats {
		if _, ok := curMarketPairStats[key]; !ok {
			jsonStat := &JsonStat{
				ID:                  -lastStat.Percent,
				ExchangeName:        lastStat.ExchangeName,
				Volume:              "0",
				VolumeChange:        "-100%",
				Percent:             "0%",
				PercentChange:       common.FormatPercentWithSign(-lastStat.Percent * 100),
				DepthUsdPositiveTwo: "-100%",
				DepthUsdNegativeTwo: "-100%",
			}

			if !groupByExchange {
				jsonStat.Pair = lastStat.Pair
			}

			statResults = append(statResults, jsonStat)
		}
	}

	sort.Slice(statResults, func(i, j int) bool {
		return statResults[i].ID > statResults[j].ID
	})

	c.JSON(200, statResults)
}

func (s *Server) marketPairVolumes(c *gin.Context) {
	startDate, days, ok := prepareStartDateAndDays(c, lastWeek(), 7)
	if !ok {
		return
	}

	token := c.DefaultQuery("token", "TRX")

	marketPairDailyVolumes := s.db.GetMarketPairDailyVolumesByDateDaysToken(startDate, days, token)

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
	startDate, ok := getDateParam(c, "start_date", lastWeek())
	if !ok {
		return
	}

	token := c.DefaultQuery("token", "TRX")

	marketPairDailyVolumes := s.db.GetMarketPairDailyVolumesByDateDaysToken(startDate, 7, token)

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
	startDate, ok := getDateParam(c, "start_date", lastWeek())
	if !ok {
		return
	}

	token := c.DefaultQuery("token", "TRX")

	marketPairAverageDepths := s.db.GetMarketPairAverageDepthsByDateDaysToken(startDate, 7, token)

	result := make([]*models.MarketPairStatistic, 0)
	for _, mps := range marketPairAverageDepths {
		result = append(result, mps)
	}

	c.JSON(200, result)
}

func (s *Server) tokenListingStatistic(c *gin.Context) {
	date, ok := getDateParam(c, "date", today())
	if !ok {
		return
	}

	token := c.DefaultQuery("token", "TRX")

	curWeekStat := s.db.GetTokenListingStatistic(date, token)
	lastWeekStat := s.db.GetTokenListingStatistic(date.AddDate(0, 0, -7), token)

	type StatEntity struct {
		models.TokenListingStatistic
		PriceChange7Days     string `json:"price_change_7days"`
		MarketCapChange7Days string `json:"market_cap_change_7days"`
	}

	result := StatEntity{
		TokenListingStatistic: *curWeekStat,
		PriceChange7Days:      common.FormatFloatChangePercent(lastWeekStat.Price, curWeekStat.Price),
		MarketCapChange7Days:  common.FormatFloatChangePercent(lastWeekStat.MarketCap, curWeekStat.MarketCap),
	}

	c.JSON(200, result)
}

func (s *Server) volumePPTData(c *gin.Context) {
	startDate, days, ok := prepareStartDateAndDays(c, lastMonth(), 30)
	if !ok {
		return
	}

	token := c.DefaultQuery("token", "TRX")

	var exchanges []string
	exchangesParam := c.DefaultQuery("exchanges", "")
	if exchangesParam != "" {
		exchanges = strings.Split(exchangesParam, ",")
	}
	if !strings.Contains(exchangesParam, "Binance") {
		exchanges = append(exchanges, "Binance")
	}

	isJson, ok := getBoolParam(c, "json", false)
	if !ok {
		return
	}

	if isJson {
		type ResEntity struct {
			Date    string             `json:"date"`
			Volumes map[string]float64 `json:"volumes"`
		}

		result := make([]ResEntity, 0)
		for i := 0; i < days; i++ {
			curDate := startDate.AddDate(0, 0, i)
			queryDate := curDate.AddDate(0, 0, 1)
			marketPairStats := s.db.GetMergedMarketPairStatistics(queryDate, 1, token, false, true)

			entity := ResEntity{
				Date:    curDate.Format("2006-01-02"),
				Volumes: make(map[string]float64),
			}
			for _, exchange := range exchanges {
				if _, inStats := marketPairStats[exchange]; inStats {
					entity.Volumes[exchange] = marketPairStats[exchange].Volume
				} else {
					entity.Volumes[exchange] = 0
				}
			}

			if _, inStats := marketPairStats["Total"]; inStats {
				entity.Volumes["Total"] = marketPairStats["Total"].Volume
			} else {
				entity.Volumes["Total"] = 0
			}

			result = append(result, entity)
		}

		c.JSON(200, result)
	} else {
		result := strings.Builder{}
		for i := 0; i < days; i++ {
			curDate := startDate.AddDate(0, 0, i)
			queryDate := curDate.AddDate(0, 0, 1)
			marketPairStats := s.db.GetMergedMarketPairStatistics(queryDate, 1, token, false, true)

			result.WriteString(curDate.Format("2006-01-02") + " ")
			for _, exchange := range exchanges {
				if _, inStats := marketPairStats[exchange]; inStats {
					result.WriteString(fmt.Sprintf("%.2f ", marketPairStats[exchange].Volume))
				} else {
					result.WriteString("0 ")
				}
			}

			if _, inStats := marketPairStats["Total"]; inStats {
				result.WriteString(fmt.Sprintf("%.2f\n", marketPairStats["Total"].Volume))
			} else {
				result.WriteString("0\n")
			}
		}

		c.String(200, result.String())
	}
}

func (s *Server) traversePPTObjects(c *gin.Context) {
	c.String(200, s.updater.TraverseAllObjectsInPPT())
}

func (s *Server) updatePPTData(c *gin.Context) {
	date, ok := getDateParam(c, "date", today())
	if !ok {
		return
	}

	if s.isUpdating {
		c.JSON(500, gin.H{
			"code":  500,
			"error": "updating in progress",
		})
		return
	}

	s.isUpdating = true
	go func() {
		s.updater.Update(date)
		s.isUpdating = false
	}()

	c.String(200, "Updating started for date: "+date.Format("2006-01-02"))
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

func (s *Server) topDelegate(c *gin.Context) {
	date, ok := getDateParam(c, "date", yesterday())
	if !ok {
		return
	}

	n, ok := getIntParam(c, "n", 20)
	if !ok {
		return
	}

	isUnDelegate, ok := getBoolParam(c, "is_undelegate", false)
	if !ok {
		return
	}

	isExchange, ok := getBoolParam(c, "is_exchange", false)
	if !ok {
		return
	}

	type ResEntity struct {
		Height   uint   `json:"height"`
		Index    uint16 `json:"index,omitempty"`
		ID       string `json:"id,omitempty"`
		From     string `json:"from"`
		To       string `json:"to"`
		Amount   string `json:"amount"`
		Resource string `json:"resource"`
	}

	results := make([]*ResEntity, 0)

	if isExchange {
		exchangeTxs := make([]*models.Transaction, 0)

		for _, tx := range s.db.GetTopResourceRelatedTxsByDate(date) {
			if s.db.IsExchange(tx.OwnerAddr) || s.db.IsExchange(tx.ToAddr) {
				exchangeTxs = append(exchangeTxs, tx)
			}
		}

		sort.Slice(exchangeTxs, func(i, j int) bool {
			return exchangeTxs[i].Amount.Cmp(exchangeTxs[j].Amount) > 0
		})

		for i := 0; i < n && i < len(exchangeTxs); i++ {
			tx := exchangeTxs[i]

			resEntity := &ResEntity{
				Height: tx.Height,
				Index:  tx.Index,
				From:   tx.OwnerAddr,
				To:     tx.ToAddr,
				Amount: tx.Amount.String(),
			}

			if s.db.IsExchange(tx.OwnerAddr) {
				resEntity.From = s.db.GetExchange(tx.OwnerAddr).Name + ": " + tx.OwnerAddr
			}

			if s.db.IsExchange(tx.ToAddr) {
				resEntity.To = s.db.GetExchange(tx.ToAddr).Name + ": " + tx.ToAddr
			}

			txType := tx.Type % 100
			if txType == 55 || txType == 58 {
				resEntity.Amount = "-" + resEntity.Amount
			}

			if tx.Type < 100 {
				resEntity.Resource = "Bandwidth"
			} else {
				resEntity.Resource = "Energy"
			}

			results = append(results, resEntity)
		}

	} else {
		txs := s.db.GetTopDelegateRelatedTxsByDateAndN(date, n, isUnDelegate)

		for _, tx := range txs {
			resEntity := &ResEntity{
				Height: tx.Height,
				Index:  tx.Index,
				From:   tx.OwnerAddr,
				To:     tx.ToAddr,
				Amount: tx.Amount.String(),
			}

			if tx.Type < 100 {
				resEntity.Resource = "Bandwidth"
			} else {
				resEntity.Resource = "Energy"
			}

			results = append(results, resEntity)
		}
	}

	for _, resEntity := range results {
		txID, err := net.GetTransactionIdByBlockNumAndIndex(resEntity.Height, resEntity.Index)
		if err == nil {
			resEntity.Index = 0
			resEntity.ID = txID
		}
	}

	c.JSON(200, results)
}

func (s *Server) txAnalyze(c *gin.Context) {
	startDate, days, ok := prepareStartDateAndDays(c, lastWeek(), 7)
	if !ok {
		return
	}

	contract := c.DefaultQuery("contract", "")

	result, ok := getIntParam(c, "result", 11)
	if !ok {
		return
	}

	results := s.db.GetTxsByDateDaysContractResult(startDate, days, contract, result)

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

func (s *Server) countForDate(c *gin.Context) {
	date, ok := getDateParam(c, "date", yesterday())
	if !ok {
		return
	}

	go func() {
		s.db.ManualCountForDate(date.Format("060102"))
	}()

	c.String(200, "Counting started for date: "+date.Format("2006-01-02"))
}

func (s *Server) lastTrackedBlockNumber(c *gin.Context) {
	c.JSON(200, gin.H{
		"last_tracked_block_number": s.db.GetLastTrackedBlockNum(),
		"last_tracked_block_time":   time.Unix(s.db.GetLastTrackedBlockTime(), 0).Format("2006-01-02 15:04:05"),
	})
}

func (s *Server) getExchange(c *gin.Context) {
	exchanges := s.db.GetExchanges()

	c.JSON(200, exchanges)
}

func (s *Server) addExchange(c *gin.Context) {
	addr, ok := mustGetParam(c, "addr")
	if !ok {
		return
	}

	name, ok := mustGetParam(c, "name")
	if !ok {
		return
	}

	s.db.AddOrOverrideExchange(addr, name)
	c.JSON(200, gin.H{
		"code":  200,
		"error": "success",
	})
}

func today() time.Time {
	return time.Now()
}

func yesterday() time.Time {
	return time.Now().AddDate(0, 0, -1)
}

func lastWeek() time.Time {
	return time.Now().AddDate(0, 0, -7)
}

func lastMonth() time.Time {
	return time.Now().AddDate(0, 0, -30)
}

func prepareStartDateAndDays(c *gin.Context, defaultStartDate time.Time, defaultDays int) (time.Time, int, bool) {
	startDate, ok := getDateParam(c, "start_date", defaultStartDate)
	if !ok {
		return time.Time{}, 0, false
	}

	days, ok := getIntParam(c, "days", defaultDays)
	if !ok {
		return time.Time{}, 0, false
	}

	return startDate, days, true
}

func getDateParam(c *gin.Context, name string, defaultDate time.Time) (time.Time, bool) {
	dateStr, ok := c.GetQuery(name)
	if !ok {
		return defaultDate.Truncate(24 * time.Hour), true
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

func getBoolParam(c *gin.Context, name string, defaultValue bool) (bool, bool) {
	paramStr, ok := c.GetQuery(name)
	if !ok {
		return defaultValue, true
	}

	param, err := strconv.ParseBool(paramStr)
	if err != nil {
		c.JSON(200, gin.H{
			"code":  400,
			"error": name + " cannot cast into bool",
		})
		return false, false
	}

	return param, true
}

func mustGetParam(c *gin.Context, name string) (string, bool) {
	param, ok := c.GetQuery(name)
	if !ok {
		c.JSON(200, gin.H{
			"code":  400,
			"error": name + " must be provided",
		})
		return "", false
	}

	return param, true
}
