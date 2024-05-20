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
	"github.com/gin-gonic/gin"
	"github.com/jinzhu/now"
	"go.uber.org/zap"
	"tron-tracker/database"
	"tron-tracker/database/models"
	"tron-tracker/utils"
)

type ServerConfig struct {
	Port int `toml:"port"`
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
	router := gin.Default()
	srv := &http.Server{
		Addr:    ":" + strconv.Itoa(serverConfig.Port),
		Handler: router,
	}

	return &Server{
		router: router,
		srv:    srv,

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
	s.router.GET("/exchange_token_weekly_statistics", s.exchangesTokenWeeklyStatistic)
	s.router.GET("/special_statistics", s.specialStatistic)
	s.router.GET("/total_statistics", s.totalStatistics)
	s.router.GET("/do-tronlink-users-weekly-statistics", s.doTronlinkUsersWeeklyStatistics)
	s.router.GET("/tronlink-users-weekly-statistics", s.tronlinkUsersWeeklyStatistics)
	s.router.GET("/exchange_weekly_statistics", s.exchangesWeeklyStatistic)
	s.router.GET("/tron_weekly_statistics", s.tronWeeklyStatistics)
	s.router.GET("/revenue_weekly_statistics", s.revenueWeeklyStatistics)
	s.router.GET("/trx_statistics", s.trxStatistics)
	s.router.GET("/user_statistics", s.userStatistics)
	s.router.GET("/token_statistics", s.tokenStatistics)

	go func() {
		if err := s.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			panic(err)
		}
	}()

	s.logger.Infof("API server started at %d", s.serverConfig.Port)
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

	days, ok := getIntParam(c, "days")
	if !ok {
		return
	}

	resultMap := make(map[string]map[string]*models.ExchangeStatistic)
	for i := 0; i < days; i++ {
		queryDate := startDate.AddDate(0, 0, i)
		for _, es := range s.db.GetExchangeTokenStatisticsByDate(queryDate) {
			if _, ok := resultMap[es.Name]; !ok {
				resultMap[es.Name] = make(map[string]*models.ExchangeStatistic)
			}

			// Skip total statistics
			if es.Token == "_" {
				continue
			}

			tokenName := s.db.GetTokenName(es.Token)

			if _, ok := resultMap[es.Name][tokenName]; !ok {
				resultMap[es.Name][tokenName] = &models.ExchangeStatistic{}
			}
			resultMap[es.Name][tokenName].Merge(&es)
		}
	}

	token, ok := getStringParam(c, "token")

	if !ok {
		c.JSON(200, resultMap)
		return
	}

	chargeResults := make([]*ExchangeTokenStatisticInResult, 0)
	withdrawResults := make([]*ExchangeTokenStatisticInResult, 0)

	for exchange, ets := range resultMap {
		if es, ok := ets[token]; ok {
			if es.ChargeTxCount > 0 {
				chargeResults = append(chargeResults, &ExchangeTokenStatisticInResult{
					Name:    exchange,
					Fee:     es.ChargeFee,
					TxCount: es.ChargeTxCount,
				})
			}

			if es.WithdrawTxCount > 0 {
				withdrawResults = append(withdrawResults, &ExchangeTokenStatisticInResult{
					Name:    exchange,
					Fee:     es.WithdrawFee,
					TxCount: es.WithdrawTxCount,
				})
			}
		}
	}

	result := strings.Builder{}
	result.WriteString("charge: \n")
	for _, res := range chargeResults {
		result.WriteString(fmt.Sprintf("%s,%d,%d\n", res.Name, res.Fee, res.TxCount))
	}
	result.WriteString("withdraw: \n")
	for _, res := range withdrawResults {
		result.WriteString(fmt.Sprintf("%s,%d,%d\n", res.Name, res.Fee, res.TxCount))
	}

	c.String(200, result.String())
}

func (s *Server) exchangesTokenWeeklyStatistic(c *gin.Context) {
	startDate, ok := prepareDateParam(c, "start_date")
	if !ok {
		return
	}

	etsMap := make(map[string]map[string]*models.ExchangeStatistic)
	for i := 0; i < 7; i++ {
		queryDate := startDate.AddDate(0, 0, i)
		for _, es := range s.db.GetExchangeTokenStatisticsByDate(queryDate) {
			if _, ok := etsMap[es.Name]; !ok {
				etsMap[es.Name] = make(map[string]*models.ExchangeStatistic)
			}

			// Skip total statistics
			if es.Token == "_" {
				continue
			}

			tokenName := s.db.GetTokenName(es.Token)

			if _, ok := etsMap[es.Name][tokenName]; !ok {
				etsMap[es.Name][tokenName] = &models.ExchangeStatistic{}
			}
			etsMap[es.Name][tokenName].Merge(&es)
		}
	}

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

	totalStatistic := &models.UserStatistic{}
	for i := 0; i < days; i++ {
		dayStatistic := s.db.GetTotalStatisticsByDate(startDate.AddDate(0, 0, i).Format("060102"))
		totalStatistic.Merge(&dayStatistic)
	}

	c.JSON(200, gin.H{
		"total_statistic": totalStatistic,
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

	statsResultFile, err := os.Open(fmt.Sprintf("tronlink/week%s_stats.txt", thisMonday))
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
	for _, ts := range s.db.GetTokenStatisticByDateAndTokenAndDays(startDate, days) {
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
