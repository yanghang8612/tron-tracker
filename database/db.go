package database

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"go.uber.org/zap"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"tron-tracker/database/models"
	"tron-tracker/net"
	"tron-tracker/types"
	"tron-tracker/utils"
)

type Config struct {
	Host        string     `toml:"host"`
	DB          string     `toml:"db"`
	User        string     `toml:"user"`
	Password    string     `toml:"password"`
	StartDate   string     `toml:"start_date"`
	StartNum    int        `toml:"start_num"`
	ValidTokens [][]string `toml:"valid_tokens"`
}

type dbCache struct {
	date           string
	fromStats      map[string]*models.UserStatistic
	toStats        map[string]*models.UserStatistic
	tokenStats     map[string]*models.TokenStatistic
	userTokenStats map[string]*models.UserTokenStatistic
}

func newCache() *dbCache {
	return &dbCache{
		fromStats:      make(map[string]*models.UserStatistic),
		toStats:        make(map[string]*models.UserStatistic),
		tokenStats:     make(map[string]*models.TokenStatistic),
		userTokenStats: make(map[string]*models.UserTokenStatistic),
	}
}

type RawDB struct {
	db                 *gorm.DB
	exchanges          map[string]*models.Exchange
	exchangesUpdatedAt time.Time
	validTokens        map[string]string

	lastTrackedBlockNum  uint
	lastTrackedBlockTime int64
	isTableMigrated      map[string]bool
	tableLock            sync.Mutex
	statsLock            sync.Mutex
	chargers             map[string]*models.Charger
	chargersLock         sync.RWMutex
	chargersToSave       map[string]*models.Charger

	trackingDate string
	flushCh      chan *dbCache
	cache        *dbCache
	countedDate  string
	countedWeek  string
	statsCh      chan string

	loopWG sync.WaitGroup
	quitCh chan struct{}

	logger *zap.SugaredLogger
}

func New(config *Config) *RawDB {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=True&loc=Local", config.User, config.Password, config.Host, config.DB)
	db, dbErr := gorm.Open(mysql.Open(dsn), &gorm.Config{
		SkipDefaultTransaction: true,
		Logger:                 logger.Discard,
	})
	if dbErr != nil {
		panic(dbErr)
	}

	dbErr = db.AutoMigrate(&models.ExchangeStatistic{})
	if dbErr != nil {
		panic(dbErr)
	}

	dbErr = db.AutoMigrate(&models.FungibleTokenStatistic{})
	if dbErr != nil {
		panic(dbErr)
	}

	dbErr = db.AutoMigrate(&models.Meta{})
	if dbErr != nil {
		panic(dbErr)
	}

	dbErr = db.AutoMigrate(&models.PhishingStatistic{})
	if dbErr != nil {
		panic(dbErr)
	}

	dbErr = db.AutoMigrate(&models.USDTStorageStatistic{})
	if dbErr != nil {
		panic(dbErr)
	}

	var trackingDateMeta models.Meta
	db.Where(models.Meta{Key: models.TrackingDateKey}).Attrs(models.Meta{Val: config.StartDate}).FirstOrCreate(&trackingDateMeta)

	var countedDateMeta models.Meta
	db.Where(models.Meta{Key: models.CountedDateKey}).Attrs(models.Meta{Val: config.StartDate}).FirstOrCreate(&countedDateMeta)

	var countedWeekMeta models.Meta
	db.Where(models.Meta{Key: models.CountedWeekKey}).Attrs(models.Meta{Val: config.StartDate}).FirstOrCreate(&countedWeekMeta)

	db.Migrator().DropTable("transactions_" + trackingDateMeta.Val)
	db.Migrator().DropTable("transfers_" + trackingDateMeta.Val)

	var trackingStartBlockNumMeta models.Meta
	db.Where(models.Meta{Key: models.TrackingStartBlockNumKey}).Attrs(models.Meta{Val: strconv.Itoa(config.StartNum)}).FirstOrCreate(&trackingStartBlockNumMeta)
	trackingStartBlockNum, _ := strconv.Atoi(trackingStartBlockNumMeta.Val)

	validTokens := make(map[string]string)
	for _, token := range config.ValidTokens {
		validTokens[token[0]] = token[1]
	}
	// TRX token symbol is "TRX"
	validTokens["TRX"] = "TRX"

	rawDB := &RawDB{
		db:          db,
		exchanges:   make(map[string]*models.Exchange),
		validTokens: validTokens,

		lastTrackedBlockNum: uint(trackingStartBlockNum - 1),
		isTableMigrated:     make(map[string]bool),
		chargers:            make(map[string]*models.Charger),
		chargersToSave:      make(map[string]*models.Charger),

		flushCh:     make(chan *dbCache),
		cache:       newCache(),
		countedDate: countedDateMeta.Val,
		countedWeek: countedWeekMeta.Val,
		statsCh:     make(chan string),

		quitCh: make(chan struct{}),
		logger: zap.S().Named("[db]"),
	}

	rawDB.loadExchanges()
	rawDB.loadChargers()
	rawDB.refreshChargers()

	return rawDB
}

func (db *RawDB) loadChargers() {
	if db.db.Migrator().HasTable(&models.Charger{}) {
		chargers := make([]*models.Charger, 0)

		db.logger.Info("Start loading chargers from db")
		result := db.db.Find(&chargers)
		db.logger.Infof("Loaded [%d] chargers from db", result.RowsAffected)

		db.logger.Info("Start building chargers map")
		for i, charger := range chargers {
			db.chargers[charger.Address] = charger
			if i%1_000_000 == 0 {
				db.logger.Infof("Built [%d] chargers map", i)
			}
		}
		db.logger.Info("Complete building chargers map")

		return
	}

	dbErr := db.db.AutoMigrate(&models.Charger{})
	if dbErr != nil {
		panic(dbErr)
	}

	f, err := os.Open("all")
	if err != nil {
		db.logger.Errorf("Load charger file error: [%s]", err.Error())
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	db.logger.Info("Start loading chargers")
	count := 0
	var chargeToSave []*models.Charger
	for scanner.Scan() {
		line := scanner.Text()
		cols := strings.Split(line, ",")
		chargeToSave = append(chargeToSave, &models.Charger{
			Address:      cols[0],
			ExchangeName: utils.TrimExchangeName(cols[1]),
		})
		if len(chargeToSave) == 1000 {
			db.db.Create(&chargeToSave)
			for _, charger := range chargeToSave {
				db.chargers[charger.Address] = charger
			}
			chargeToSave = make([]*models.Charger, 0)
		}
		count++
		if count%1_000_000 == 0 {
			db.logger.Infof("Loaded [%d] chargers", count)
		}
	}
	db.db.Create(&chargeToSave)
	for _, charger := range chargeToSave {
		db.chargers[charger.Address] = charger
	}
	db.logger.Info("Complete loading chargers")

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func (db *RawDB) refreshChargers() {
	db.chargersLock.Lock()
	defer db.chargersLock.Unlock()

	db.logger.Info("Start refreshing chargers")

	chargersToSave := make(map[string]*models.Charger)
	count := 0
	for _, charger := range db.chargers {
		if charger.ExchangeName != utils.TrimExchangeName(charger.ExchangeName) {
			charger.ExchangeName = utils.TrimExchangeName(charger.ExchangeName)

			if charger.IsFake && len(charger.BackupAddress) == 0 {
				charger.IsFake = false
			}

			chargersToSave[charger.Address] = charger
		}

		// Sometimes, backup address can become to an exchange address
		if len(charger.BackupAddress) == 34 && db.IsExchange(charger.BackupAddress) {
			if charger.ExchangeName != db.GetExchange(charger.BackupAddress).Name {
				if !charger.IsFake {
					charger.IsFake = true
					chargersToSave[charger.Address] = charger
				}
			} else {
				charger.BackupAddress = ""
				chargersToSave[charger.Address] = charger
			}
		}

		if len(chargersToSave) == 200 {
			chargers := make([]*models.Charger, 0)
			for _, chargerToSave := range chargersToSave {
				chargers = append(chargers, chargerToSave)
			}
			db.db.Save(chargers)
			chargersToSave = make(map[string]*models.Charger)
		}

		count++
		if count%1_000_000 == 0 {
			db.logger.Infof("Refreshed [%d] chargers", count)
		}
	}

	if len(chargersToSave) != 0 {
		chargers := make([]*models.Charger, 0)
		for _, chargerToSave := range chargersToSave {
			chargers = append(chargers, chargerToSave)
		}
		db.db.Save(chargers)
	}

	db.logger.Info("Complete refreshing chargers")
}

func (db *RawDB) loadExchanges() {
	if db.db.Migrator().HasTable(&models.Exchange{}) {
		dbErr := db.db.AutoMigrate(&models.Exchange{})
		if dbErr != nil {
			panic(dbErr)
		}

		exchanges := make([]*models.Exchange, 0)

		db.logger.Info("Start loading exchanges from db")
		result := db.db.Find(&exchanges)
		db.logger.Infof("Loaded [%d] exchanges from db", result.RowsAffected)

		db.logger.Info("Start building exchanges map")
		for _, exchange := range exchanges {
			db.exchanges[exchange.Address] = exchange
		}
		db.logger.Info("Complete building exchanges map")

		return
	}

	dbErr := db.db.AutoMigrate(&models.Exchange{})
	if dbErr != nil {
		panic(dbErr)
	}

	exchanges := net.GetExchanges()
	for _, exchange := range exchanges.Val {
		exchange.Name = utils.TrimExchangeName(exchange.OriginName)
		db.db.Create(&exchange)
		db.exchanges[exchange.Address] = exchange
	}
}

func (db *RawDB) updateExchanges() {
	if time.Now().Sub(db.exchangesUpdatedAt) < 1*time.Hour {
		return
	}

	exchanges := net.GetExchanges()
	for _, newExchange := range exchanges.Val {
		if oldExchange, ok := db.exchanges[newExchange.Address]; ok {
			if oldExchange.OriginName != newExchange.OriginName {
				if !utils.IsSameExchange(oldExchange.OriginName, newExchange.OriginName) {
					db.logger.Warnf("Exchange origin name mismatch: [%s] - [%s & %s]",
						oldExchange.Address, oldExchange.OriginName, newExchange.OriginName)
					continue
				}
				db.logger.Infof("Exchange origin name updated: [%s] - [%s -> %s]",
					oldExchange.Address, oldExchange.OriginName, newExchange.OriginName)
				db.db.Model(&oldExchange).Update("origin_name", newExchange.OriginName)
			}
		} else {
			newExchange.Name = utils.TrimExchangeName(newExchange.OriginName)
			db.db.Create(&newExchange)
			db.exchanges[newExchange.Address] = newExchange
			db.logger.Infof("New exchange added: [%s - %s] - [%s]",
				newExchange.Address, newExchange.OriginName, newExchange.Name)
		}
	}

	for addr, oldExchange := range db.exchanges {
		found := false
		for _, newExchange := range exchanges.Val {
			if addr == newExchange.Address {
				found = true
				break
			}
		}

		if !found && !oldExchange.FromAsuka {
			db.logger.Warnf("Exchange removed: [%s - %s]", oldExchange.Address, oldExchange.OriginName)
		}
	}

	db.exchangesUpdatedAt = time.Now()
}

func (db *RawDB) Start() {
	db.loopWG.Add(3)
	go db.marketPairDepthStatisticsLoop()
	go db.cacheFlushLoop()
	go db.countLoop()
}

func (db *RawDB) Close() {
	close(db.quitCh)
	db.loopWG.Wait()

	db.flushChargerToDB()

	underDB, _ := db.db.DB()
	_ = underDB.Close()
}

func (db *RawDB) Report() {
	db.logger.Infof("Status report, chargers size [%d]", len(db.chargers))
}

func (db *RawDB) IsExchange(addr string) bool {
	_, ok := db.exchanges[addr]
	return ok
}

func (db *RawDB) GetExchange(addr string) *models.Exchange {
	return db.exchanges[addr]
}

func (db *RawDB) GetExchanges() map[string]*models.Exchange {
	return db.exchanges
}

func (db *RawDB) AddOrOverrideExchange(addr, name string) {
	if exchange, ok := db.exchanges[addr]; ok {
		db.logger.Infof("Updated exchange name: [%s] - [%s -> %s]", addr, exchange.Name, name)

		exchange.Name = name
		db.db.Save(exchange)
	} else {
		db.logger.Infof("Added exchange from asuka: [%s - %s]", addr, name)

		exchangeToSave := &models.Exchange{
			Address:    addr,
			Name:       name,
			OriginName: name,
			FromAsuka:  true,
		}
		db.db.Create(exchangeToSave)
		db.exchanges[addr] = exchangeToSave
	}
}

func (db *RawDB) GetLastTrackedBlockNum() uint {
	return db.lastTrackedBlockNum
}

func (db *RawDB) GetLastTrackedBlockTime() int64 {
	return db.lastTrackedBlockTime
}

func (db *RawDB) GetTokenName(addr string) string {
	return db.validTokens[addr]
}

func (db *RawDB) GetChargers() map[string]*models.Charger {
	return db.chargers
}

func (db *RawDB) TraverseTransactions(date string, batchSize int, handler func(*models.Transaction)) {
	db.logger.Infof("Start traversing transactions for date [%s], batch size [%d]", date, batchSize)

	var (
		start   = time.Now()
		results = make([]*models.Transaction, 0)
		count   = 0
	)

	result := db.db.Table("transactions_"+date).FindInBatches(&results, batchSize, func(_ *gorm.DB, _ int) error {
		for _, tx := range results {
			handler(tx)

			count++
			if count%1_000_000 == 0 {
				db.logger.Infof("Traversed [%s] transactions", humanize.Comma(int64(count)))
			}
		}
		return nil
	})

	db.logger.Infof("Traversed [%d] results, cost: [%s], error: [%v]", result.RowsAffected, time.Since(start), result.Error)
}

func (db *RawDB) GetTxsByDateAndDaysAndContractAndResult(date time.Time, days int, contract string, result int) []*models.Transaction {
	var results []*models.Transaction

	for i := 0; i < days; i++ {
		var txs []*models.Transaction

		queryDate := date.AddDate(0, 0, i).Format("060102")
		if len(contract) > 0 {
			db.db.Table("transactions_"+queryDate).Where("type = 31 and name = ? and result = ?", contract, result).Find(&txs)
		} else {
			db.db.Table("transactions_"+queryDate).Where("type = 31 and result = ?", result).Find(&txs)
		}

		if len(txs) != 0 {
			results = append(results, txs...)
		}
	}

	return results
}

func (db *RawDB) GetFromStatisticByDateAndDays(date time.Time, days int) map[string]*models.UserStatistic {
	resultMap := make(map[string]*models.UserStatistic)

	for i := 0; i < days; i++ {
		queryDate := date.AddDate(0, 0, i).Format("060102")

		db.logger.Infof("Start querying from_stats for date [%s]", queryDate)

		dayStats := make([]*models.UserStatistic, 0)
		result := db.db.Table("from_stats_"+queryDate).FindInBatches(&dayStats, 100, func(_ *gorm.DB, _ int) error {
			for _, dayStat := range dayStats {
				user := dayStat.Address

				if _, ok := resultMap[user]; !ok {
					resultMap[user] = dayStat
				} else {
					resultMap[user].Merge(dayStat)
				}
			}

			return nil
		})

		db.logger.Infof("Finish querying from_stats for date [%s], query rows: %d, error: %v", queryDate, result.RowsAffected, result.Error)
	}

	return resultMap
}

func (db *RawDB) GetFromStatisticByDateAndUserAndDays(date time.Time, user string, days int) *models.UserStatistic {
	result := models.NewUserStatistic(user)

	for i := 0; i < days; i++ {
		queryDate := date.AddDate(0, 0, i).Format("060102")

		dayStat := models.NewUserStatistic("")
		db.db.Table("from_stats_"+queryDate).Where("address = ?", user).Limit(1).Find(dayStat)

		result.Merge(dayStat)
	}

	return result
}

func (db *RawDB) GetTopNFromStatisticByDateAndDays(date time.Time, days, n int, orderByField string) map[string]*models.UserStatistic {
	resultMap := make(map[string]*models.UserStatistic)

	for i := 0; i < days; i++ {
		queryDate := date.AddDate(0, 0, i).Format("060102")

		dayStats := make([]*models.UserStatistic, 0)
		db.db.Table("from_stats_" + queryDate).Order(orderByField + " desc").Limit(n).Find(&dayStats)

		for _, dayStat := range dayStats {
			user := dayStat.Address

			if _, ok := resultMap[user]; !ok {
				resultMap[user] = dayStat
			} else {
				resultMap[user].Merge(dayStat)
			}
		}
	}

	return resultMap
}

func (db *RawDB) GetFeeAndEnergyByDateAndUsers(date string, users []string) (int64, int64) {
	type result struct {
		F int64
		E int64
	}
	var res result
	db.db.Table("from_stats_"+date).Select("sum(fee) as f, sum(energy_total) as e").Where("address IN ?", users).Limit(1).Find(&res)
	return res.F, res.E
}

func (db *RawDB) GetTotalStatisticsByDate(date string) *models.UserStatistic {
	totalStat := models.NewUserStatistic("")
	db.db.Table("from_stats_"+date).Where("address = ?", "total").Limit(1).Find(totalStat)
	return totalStat
}

func (db *RawDB) GetTokenStatisticsByDateAndToken(date, token string) *models.TokenStatistic {
	var tokenStat models.TokenStatistic
	db.db.Table("token_stats_"+date).Where("address = ?", token).Limit(1).Find(&tokenStat)
	return &tokenStat
}

func (db *RawDB) GetTokenStatisticsByDateAndDays(date time.Time, days int) map[string]*models.TokenStatistic {
	resultMap := make(map[string]*models.TokenStatistic)

	for i := 0; i < days; i++ {
		queryDate := date.AddDate(0, 0, i).Format("060102")

		var dayStats []*models.TokenStatistic
		db.db.Table("token_stats_" + queryDate).Find(&dayStats)

		for _, dayStat := range dayStats {
			if _, ok := resultMap[dayStat.Address]; !ok {
				resultMap[dayStat.Address] = dayStat
			} else {
				resultMap[dayStat.Address].Merge(dayStat)
			}
		}
	}

	return resultMap
}

func (db *RawDB) GetUserTokenStatisticsByDateAndDaysAndToken(date time.Time, days int, token string) map[string]*models.UserTokenStatistic {
	resultMap := make(map[string]*models.UserTokenStatistic)

	for i := 0; i < days; i++ {
		queryDate := date.AddDate(0, 0, i).Format("060102")

		var dayStats []*models.UserTokenStatistic
		db.db.Table("user_token_stats_"+queryDate).Where("token = ?", token).Find(&dayStats)

		for _, dayStat := range dayStats {
			if _, ok := resultMap[dayStat.User]; !ok {
				dayStat.Token = ""
				resultMap[dayStat.User] = dayStat
			} else {
				resultMap[dayStat.User].Merge(dayStat)
			}
		}
	}

	return resultMap
}

func (db *RawDB) GetUserTokenStatisticsByDateAndDaysAndUserAndToken(date time.Time, days int, user, token string) map[string]*models.UserTokenStatistic {
	resultMap := make(map[string]*models.UserTokenStatistic)

	for i := 0; i < days; i++ {
		queryDate := date.AddDate(0, 0, i).Format("060102")

		var dayStats []*models.UserTokenStatistic
		db.db.Table("user_token_stats_"+queryDate).Where("user = ? and token = ?", user, token).Find(&dayStats)

		for _, dayStat := range dayStats {
			if _, ok := resultMap[dayStat.User]; !ok {
				dayStat.Token = ""
				resultMap[dayStat.User] = dayStat
			} else {
				resultMap[dayStat.User].Merge(dayStat)
			}
		}
	}

	return resultMap
}

func (db *RawDB) GetExchangeTotalStatisticsByDate(date time.Time) []*models.ExchangeStatistic {
	return db.GetExchangeStatisticsByDateAndToken(date.Format("060102"), "_")
}

func (db *RawDB) GetExchangeStatisticsByDateAndToken(date, token string) []*models.ExchangeStatistic {
	var results []*models.ExchangeStatistic
	db.db.Where("date = ? and token = ?", date, token).Find(&results)
	return results
}

func (db *RawDB) GetExchangeTokenStatisticsByDateAndDays(date time.Time, days int) map[string]map[string]*models.ExchangeStatistic {
	resultMap := make(map[string]map[string]*models.ExchangeStatistic)
	resultMap["All"] = make(map[string]*models.ExchangeStatistic)

	for i := 0; i < days; i++ {
		queryDate := date.AddDate(0, 0, i)

		var dayStats []models.ExchangeStatistic
		db.db.Where("date = ? and token <> ?", queryDate.Format("060102"), "Special").Find(&dayStats)

		for _, es := range dayStats {
			if _, ok := resultMap[es.Name]; !ok {
				resultMap[es.Name] = make(map[string]*models.ExchangeStatistic)
			}

			// Skip total statistics
			var tokenName string
			if es.Token == "_" {
				tokenName = "TOTAL"
			} else {
				tokenName = db.GetTokenName(es.Token)
			}

			if _, ok := resultMap[es.Name][tokenName]; !ok {
				resultMap[es.Name][tokenName] = models.NewExchangeStatistic("", "", "")
			}
			resultMap[es.Name][tokenName].Merge(&es)

			if _, ok := resultMap["All"][tokenName]; !ok {
				resultMap["All"][tokenName] = models.NewExchangeStatistic("", "", "")
			}
			resultMap["All"][tokenName].Merge(&es)
		}
	}

	return resultMap
}

func (db *RawDB) GetSpecialStatisticByDateAndAddr(date, addr string) (uint, uint, uint, uint) {
	USDT := "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
	var chargeFee uint
	db.db.Table("transactions_"+date).
		Select("SUM(fee)").
		Where("hash IN (?)",
			db.db.Table("transfers_"+date).
				Distinct("hash").
				Where("to_addr = ? and token = ?", addr, USDT)).
		Find(&chargeFee)

	var withdrawFee uint
	db.db.Table("transactions_"+date).
		Select("SUM(fee)").
		Where("hash IN (?)",
			db.db.Table("transfers_"+date).
				Distinct("hash").
				Where("from_addr = ? and token = ?", addr, USDT)).
		Find(&withdrawFee)

	var chargeCnt int64
	db.db.Table("transfers_"+date).Where("to_addr = ? and token = ?", addr, USDT).Count(&chargeCnt)

	var withdrawCnt int64
	db.db.Table("transfers_"+date).Where("from_addr = ? and token = ?", addr, USDT).Count(&withdrawCnt)

	return chargeFee, withdrawFee, uint(chargeCnt), uint(withdrawCnt)
}

func (db *RawDB) GetTRXPriceByDate(date time.Time) float64 {
	queryDateDBName := "market_pair_statistics_" + date.Format("0601")

	// Query the earliest Binance-TRX/USDT price of the day
	var earliestPrice float64
	db.db.Table(queryDateDBName).
		Select("price").
		Where("datetime like ? and exchange_name = ? and pair = ?",
			date.Format("02")+"%", "Binance", "TRX/USDT").
		Order("datetime").Limit(1).
		Find(&earliestPrice)

	return earliestPrice
}

func (db *RawDB) GetMarketPairStatisticsByDateAndDaysAndToken(date time.Time, days int, token string, queryDepth bool) map[string]*models.MarketPairStatistic {
	resultMap := make(map[string]*models.MarketPairStatistic)

	var (
		totalVolume float64
		emptyDays   int
	)
	for i := 0; i < days; i++ {
		today := date.AddDate(0, 0, i)
		todayDBName := "market_pair_statistics_" + today.Format("0601")

		// First query the earliest datetime of the day
		var earliestDatetime string
		db.db.Table(todayDBName).
			Select("datetime").
			Where("datetime like ? and token = ? and percent > 0", today.Format("02")+"%", token).
			Order("datetime").Limit(1).
			Find(&earliestDatetime)

		// Then query the volume statistics
		var volumeStats []*models.MarketPairStatistic
		db.db.Table(todayDBName).
			Select("exchange_name", "pair", "volume").
			Where("datetime = ? and token = ? and percent > 0 and volume > 0", earliestDatetime, token).
			Find(&volumeStats)

		if len(volumeStats) == 0 {
			emptyDays += 1
			continue
		}

		for _, dayStat := range volumeStats {
			key := dayStat.ExchangeName + "_" + dayStat.Pair
			if _, ok := resultMap[key]; !ok {
				resultMap[key] = dayStat
			} else {
				resultMap[key].Volume += dayStat.Volume
			}

			totalVolume += dayStat.Volume
		}

		if queryDepth {
			lastDay := today.AddDate(0, 0, -1)
			lastDayDBName := "market_pair_statistics_" + lastDay.Format("0601")

			var depthStats []*models.MarketPairStatistic
			db.db.Table(lastDayDBName).
				Where("datetime = ? and token = ?", lastDay.Format("060102"), token).
				Find(&depthStats)

			for _, depthStat := range depthStats {
				key := depthStat.ExchangeName + "_" + depthStat.Pair
				if _, ok := resultMap[key]; !ok {
					resultMap[key] = depthStat
				} else {
					resultMap[key].DepthUsdPositiveTwo += depthStat.DepthUsdPositiveTwo
					resultMap[key].DepthUsdNegativeTwo += depthStat.DepthUsdNegativeTwo
				}
			}
		}
	}

	days -= emptyDays
	if days != 0 {
		totalVolume /= float64(days)
		for _, stat := range resultMap {
			stat.Datetime = ""
			stat.Token = ""
			stat.Volume /= float64(days)
			stat.Percent = stat.Volume / totalVolume
			stat.DepthUsdPositiveTwo /= float64(days)
			stat.DepthUsdNegativeTwo /= float64(days)
		}
	}

	resultMap["Total"] = &models.MarketPairStatistic{
		ExchangeName: "Total",
		Volume:       totalVolume,
	}

	return resultMap
}

func (db *RawDB) GetMarketPairDailyVolumesByDateAndDaysAndToken(date time.Time, days int, token string) map[string]*models.MarketPairStatistic {
	resultMap := make(map[string]*models.MarketPairStatistic)

	actualDays := days
	for i := 1; i <= days; i++ {
		queryDate := date.AddDate(0, 0, i)
		todayDBName := "market_pair_statistics_" + queryDate.Format("0601")

		var todayStats []*models.MarketPairStatistic
		result := db.db.Table(todayDBName).Select("exchange_name", "pair", "reputation", "volume", "percent").
			Where("datetime = ? and token = ?", queryDate.Format("02")+"0000", token).Find(&todayStats)

		if result.Error != nil {
			actualDays -= 1
			db.logger.Errorf("Failed to query market pair statistics for date [%s], error: %v", queryDate.Format("060102"), result.Error)
			continue
		}

		for _, dayStat := range todayStats {
			if dayStat.Percent == 0 {
				continue
			}

			key := dayStat.ExchangeName + "_" + dayStat.Pair
			if _, ok := resultMap[key]; !ok {
				resultMap[key] = dayStat
			} else {
				resultMap[key].Reputation += dayStat.Reputation
				resultMap[key].Volume += dayStat.Volume
			}
		}
	}

	if actualDays == 0 {
		return resultMap
	}

	for key, stat := range resultMap {
		if stat.Reputation/float64(actualDays) < 0.76 {
			delete(resultMap, key)
			continue
		}

		stat.Percent = 0
		stat.Reputation = 0
		stat.Volume /= float64(actualDays)
	}

	return resultMap
}

func (db *RawDB) GetMarketPairAverageDepthsByDateAndDaysAndToken(date time.Time, days int, token string) map[string]*models.MarketPairStatistic {
	mpsMap := make(map[string]*models.MarketPairStatistic)
	dateMap := make(map[string]bool)

	actualDays := days
	for i := 0; i < days; i++ {
		queryDate := date.AddDate(0, 0, i)
		dbNameSuffix := queryDate.Format("0601")
		todayDBName := "market_pair_statistics_" + dbNameSuffix

		var todayStats []*models.MarketPairStatistic
		result := db.db.Table(todayDBName).Where("datetime like ?", queryDate.Format("02")+"%").Find(&todayStats)

		if result.Error != nil {
			actualDays -= 1
			db.logger.Errorf("Failed to query market pair statistics for date [%s], error: %v", queryDate.Format("060102"), result.Error)
			continue
		}

		for _, dayStat := range todayStats {
			if dayStat.Percent == 0 || dayStat.Token != token {
				continue
			}

			if _, ok := dateMap[dbNameSuffix+dayStat.Datetime]; !ok {
				dateMap[dbNameSuffix+dayStat.Datetime] = true
			}

			key := dayStat.ExchangeName + "_" + dayStat.Pair
			if _, ok := mpsMap[key]; !ok {
				mpsMap[key] = dayStat
			} else {
				mpsMap[key].Reputation += dayStat.Reputation
				mpsMap[key].DepthUsdPositiveTwo += dayStat.DepthUsdPositiveTwo
				mpsMap[key].DepthUsdNegativeTwo += dayStat.DepthUsdNegativeTwo
			}
		}
	}

	resultMap := make(map[string]*models.MarketPairStatistic)

	if actualDays == 0 {
		return resultMap
	}

	for key, stat := range mpsMap {
		if stat.Reputation/float64(actualDays) < 0.76 {
			continue
		}

		resultMap[key] = &models.MarketPairStatistic{
			ExchangeName:        stat.ExchangeName,
			Pair:                stat.Pair,
			DepthUsdPositiveTwo: stat.DepthUsdPositiveTwo / float64(len(dateMap)),
			DepthUsdNegativeTwo: stat.DepthUsdNegativeTwo / float64(len(dateMap)),
		}
	}

	return resultMap
}

func (db *RawDB) GetTokenListingStatistic(date time.Time, token string) *models.TokenListingStatistic {
	queryDateDBName := "token_listing_statistics_" + date.Format("0601")

	var todayStat models.TokenListingStatistic
	db.db.Table(queryDateDBName).
		Where("datetime like ? and token = ?", date.Format("02")+"%", strings.ToUpper(token)).
		Order("datetime").Limit(1).
		Find(&todayStat)

	todayStat.Datetime = date.Format("2006-01-02")
	return &todayStat
}

func (db *RawDB) GetPhishingStatisticsByDate(date string) models.PhishingStatistic {
	var phishingStatistic models.PhishingStatistic
	db.db.Where("date = ?", date).Limit(1).Find(&phishingStatistic)
	return phishingStatistic
}

func (db *RawDB) GetUSDTStorageStatisticsByDateAndDays(date time.Time, days int) models.USDTStorageStatistic {
	result := models.USDTStorageStatistic{}

	for i := 0; i < days; i++ {
		queryDate := date.AddDate(0, 0, i)

		var dayStat models.USDTStorageStatistic
		db.db.Where("date = ?", queryDate.Format("060102")).Limit(1).Find(&dayStat)

		result.Merge(&dayStat)
	}

	return result
}

func (db *RawDB) SetLastTrackedBlock(block *types.Block) {
	db.updateExchanges()

	trackingDate := generateDate(block.BlockHeader.RawData.Timestamp)
	if db.trackingDate == "" {
		db.trackingDate = trackingDate
	} else if db.trackingDate != trackingDate {
		db.flushCh <- db.cache
		db.statsCh <- db.cache.date

		db.trackingDate = trackingDate
		db.cache = newCache()

		db.db.Model(&models.Meta{}).Where(models.Meta{Key: models.TrackingDateKey}).Update("val", trackingDate)
		db.db.Model(&models.Meta{}).Where(models.Meta{Key: models.TrackingStartBlockNumKey}).Update("val", strconv.Itoa(int(block.BlockHeader.RawData.Number)))
	}

	db.lastTrackedBlockNum = block.BlockHeader.RawData.Number
	db.lastTrackedBlockTime = block.BlockHeader.RawData.Timestamp
}

func (db *RawDB) SaveTransactions(transactions []*models.Transaction) {
	if transactions == nil || len(transactions) == 0 {
		return
	}

	dbName := "transactions_" + db.trackingDate
	db.createTableIfNotExist(dbName, models.Transaction{})
	db.db.Table(dbName).Create(transactions)
}

func (db *RawDB) UpdateStatistics(ts int64, tx *models.Transaction) {
	db.updateUserStatistic(tx.OwnerAddr, ts, tx, db.cache.fromStats)
	db.updateUserStatistic("total", ts, tx, db.cache.fromStats)

	if len(tx.ToAddr) > 0 {
		db.updateUserStatistic(tx.ToAddr, ts, tx, db.cache.toStats)
	}

	if len(tx.Name) > 0 {
		db.updateTokenStatistic(tx.Name, tx, db.cache.tokenStats)
		db.updateTokenStatistic("total", tx, db.cache.tokenStats)
		if len(tx.FromAddr) > 0 && len(tx.ToAddr) > 0 {
			db.updateUserTokenStatistic(tx, db.cache.userTokenStats)
		}
	}
}

func (db *RawDB) updateUserStatistic(user string, ts int64, tx *models.Transaction, stats map[string]*models.UserStatistic) {
	db.cache.date = generateDate(ts)
	if _, ok := stats[user]; !ok {
		stats[user] = models.NewUserStatistic(user)
	}
	stats[user].Add(tx)
}

func (db *RawDB) updateTokenStatistic(name string, tx *models.Transaction, stats map[string]*models.TokenStatistic) {
	if _, ok := stats[name]; !ok {
		stats[name] = &models.TokenStatistic{
			Address: name,
		}
	}
	stats[name].Add(tx)
}

func (db *RawDB) updateUserTokenStatistic(tx *models.Transaction, stats map[string]*models.UserTokenStatistic) {
	if _, ok := stats[tx.FromAddr+tx.Name]; !ok {
		stats[tx.FromAddr+tx.Name] = models.NewUserTokenStatistic(tx.FromAddr, tx.Name)
	}
	stats[tx.FromAddr+tx.Name].AddFrom(tx)

	if _, ok := stats[tx.ToAddr+tx.Name]; !ok {
		stats[tx.ToAddr+tx.Name] = models.NewUserTokenStatistic(tx.ToAddr, tx.Name)
	}
	stats[tx.ToAddr+tx.Name].AddTo(tx)
}

func (db *RawDB) SaveCharger(from, to, token string) {
	// Filter invalid token charger
	if _, ok := db.validTokens[token]; !ok {
		return
	}

	// Filter exchange address
	if db.IsExchange(from) {
		return
	}

	if charger, ok := db.chargers[from]; !ok && db.IsExchange(to) {
		db.chargersLock.Lock()
		db.chargers[from] = &models.Charger{
			Address:      from,
			ExchangeName: db.GetExchange(to).Name,
		}
		db.chargersLock.Unlock()

		db.chargersToSave[from] = db.chargers[from]
	} else if ok && !charger.IsFake {
		if db.IsExchange(to) {
			// Charger interact with other exchange address, so it is a fake charger
			if db.GetExchange(to).Name != charger.ExchangeName {
				// Here we save the other exchange address into backup address
				charger.BackupAddress = db.GetExchange(to).Name
				charger.IsFake = true
				db.chargersToSave[from] = charger
			}
		} else {
			// Charger can interact with at most one unknown other address. Otherwise, it is a fake charger
			if len(charger.BackupAddress) == 0 {
				charger.BackupAddress = to
				db.chargersToSave[from] = charger
			} else if to != charger.BackupAddress {
				charger.IsFake = true
				db.chargersToSave[from] = charger
			}
		}
	}

	if len(db.chargersToSave) == 200 {
		db.flushChargerToDB()
	}
}

func (db *RawDB) DoTronLinkWeeklyStatistics(date time.Time, override bool) {
	db.statsLock.Lock()
	defer db.statsLock.Unlock()

	endDay := date.Format("20060102")

	weeklyUsersFile, err := os.Open(fmt.Sprintf("/data/tronlink/week%s.txt", endDay))
	if err != nil {
		db.logger.Info("Weekly file has`t been created yet")
		return
	}
	defer weeklyUsersFile.Close()

	statsResultFilePath := fmt.Sprintf("/data/tronlink/week%s_stats.txt", endDay)
	if _, err := os.Stat(statsResultFilePath); err == nil && !override {
		return
	}

	statsResultFile, err := os.Create(statsResultFilePath)
	if err != nil {
		db.logger.Errorf("Create stats file error: [%s]", err.Error())
		return
	}
	defer statsResultFile.Close()

	scanner := bufio.NewScanner(weeklyUsersFile)

	db.logger.Info("Start count TronLink user fee for week " + endDay)

	startDay := date.AddDate(0, 0, -7)
	db.logger.Infof("Start Day: %s", startDay.Format("2006-01-02"))

	lineCount := 0
	users := make(map[string]bool)
	for scanner.Scan() {
		users[scanner.Text()] = true
		lineCount += 1
	}
	db.logger.Infof("Total users: %d, read [%d] lines from file", len(users), lineCount)

	var (
		totalFee       int64
		totalEnergy    int64
		withdrawFee    int64
		withdrawEnergy int64
		collectFee     int64
		collectEnergy  int64
		chargeFee      int64
		chargeEnergy   int64
	)
	for i := 0; i < 7; i++ {
		queryDate := startDay.AddDate(0, 0, i).Format("060102")
		db.logger.Infof("Start querying transactions for date: %s", queryDate)

		txCount := 0
		results := make([]*models.Transaction, 0)
		result := db.db.Table("transactions_"+queryDate).FindInBatches(&results, 100, func(_ *gorm.DB, _ int) error {
			for _, result := range results {
				user := result.OwnerAddr
				if _, ok := users[user]; !ok {
					continue
				}

				if db.IsExchange(user) {
					withdrawFee += result.Fee
					withdrawEnergy += result.EnergyTotal
				} else if _, ok := db.isCharger(user); ok {
					collectFee += result.Fee
					collectEnergy += result.EnergyTotal
				} else if _, ok := db.isCharger(result.ToAddr); ok {
					chargeFee += result.Fee
					chargeEnergy += result.EnergyTotal
				} else {
					totalFee += result.Fee
					totalEnergy += result.EnergyTotal
				}
			}
			txCount += 100

			if txCount%1_000_000 == 0 {
				db.logger.Infof("Queried rows: %d", txCount)
			}

			return nil
		})
		db.logger.Infof("Finish querying transactions for date: %s, query rows: %d, error: %v", queryDate, result.RowsAffected, result.Error)
		db.logger.Infof("Total fee: %d, total energy: %d", totalFee, totalEnergy)
		db.logger.Infof("Withdraw fee: %d, withdraw energy: %d", withdrawFee, withdrawEnergy)
		db.logger.Infof("Collect fee: %d, collect energy: %d", collectFee, collectEnergy)
		db.logger.Infof("Charge fee: %d, charge energy: %d", chargeFee, chargeEnergy)
	}

	statsResultFile.WriteString(strconv.FormatInt(totalFee, 10) + "\n")
	statsResultFile.WriteString(strconv.FormatInt(totalEnergy, 10) + "\n")
	statsResultFile.WriteString(strconv.FormatInt(withdrawFee, 10) + "\n")
	statsResultFile.WriteString(strconv.FormatInt(withdrawEnergy, 10) + "\n")
	statsResultFile.WriteString(strconv.FormatInt(collectFee, 10) + "\n")
	statsResultFile.WriteString(strconv.FormatInt(collectEnergy, 10) + "\n")
	statsResultFile.WriteString(strconv.FormatInt(chargeFee, 10) + "\n")
	statsResultFile.WriteString(strconv.FormatInt(chargeEnergy, 10) + "\n")

	slackMessage := "TronLink Weekly Fee Statistics\n"
	slackMessage += fmt.Sprintf("> Total Fee: `%s`, total energy: `%s`\n", humanize.Comma(totalFee), humanize.Comma(totalEnergy))
	slackMessage += fmt.Sprintf("> Withdraw Fee: `%s`, withdraw energy: `%s`\n", humanize.Comma(withdrawFee), humanize.Comma(withdrawEnergy))
	slackMessage += fmt.Sprintf("> Collect Energy: `%s`, collect energy: `%s`\n", humanize.Comma(collectFee), humanize.Comma(collectEnergy))
	slackMessage += fmt.Sprintf("> Charge Fee: `%s`, charge energy: `%s`\n", humanize.Comma(chargeFee), humanize.Comma(chargeEnergy))
	net.ReportToSlack(slackMessage)
}

func (db *RawDB) DoMarketPairStatistics() {
	db.logger.Infof("Start doing market pair statistics")

	statsDBName := "market_pair_statistics_" + time.Now().Format("0601")
	db.createTableIfNotExist(statsDBName, models.MarketPairStatistic{})

	tokens := []string{"tron", "steem", "wink", "just", "bittorrent-new", "apenft", "sun-token"}

	for _, token := range tokens {
		db.doMarketPairStatistics(statsDBName, token)
		time.Sleep(time.Second * 1)
	}

	db.logger.Infof("Finish doing market pair statistics")
}

func (db *RawDB) doMarketPairStatistics(dbName, token string) {
	originData, marketPairs, err := net.GetMarketPairs(token)
	if err != nil {
		db.logger.Errorf("Get %s market pairs error: [%s]", token, err.Error())
		return
	}

	db.saveMarketPairOriginData(token, originData)
	res := db.db.Table(dbName).Create(marketPairs)
	if res.Error != nil {
		db.logger.Warnf("Save %s market pairs error: [%s]", token, res.Error.Error())
	} else {
		db.logger.Infof("Save %s market pairs success, affected rows: %d", token, res.RowsAffected)
	}
}

func (db *RawDB) DoTokenListingStatistics() {
	db.logger.Infof("Start doing token listing statistics")

	statsDBName := "token_listing_statistics_" + time.Now().Format("0601")
	db.createTableIfNotExist(statsDBName, models.TokenListingStatistic{})

	originData, tokenListings, err := net.GetTokenListings()
	if err != nil {
		db.logger.Errorf("Get token listing error: [%s]", err.Error())
		return
	}

	db.saveTokenListingOriginData(originData)
	res := db.db.Table(statsDBName).Create(tokenListings)
	if res.Error != nil {
		db.logger.Warnf("Save token listings error: [%s]", res.Error)
	} else {
		db.logger.Infof("Save token listings success, affected rows: %d", res.RowsAffected)
	}

	db.logger.Infof("Finish doing token listing statistics")
}

func (db *RawDB) DoExchangeStatistics(date string) {
	db.logger.Infof("Start doing exchange statistics for date [%s]", date)

	exchangeStats := make(map[string]map[string]*models.ExchangeStatistic)
	db.TraverseTransactions(date, 500, func(tx *models.Transaction) {
		if _, ok := db.validTokens[tx.Name]; !ok ||
			len(tx.FromAddr) == 0 || len(tx.ToAddr) == 0 ||
			tx.Amount.Length() < 6 {
			return
		}

		from := tx.FromAddr
		to := tx.ToAddr
		token := db.validTokens[tx.Name]

		if db.IsExchange(from) {
			exchange := db.GetExchange(from).Name
			setExchangeStatsMap(exchangeStats, date, exchange, token)
			exchangeStats[exchange]["_"].AddWithdraw(tx)
			exchangeStats[exchange][token].AddWithdraw(tx)

		} else if _, fromIsCharger := db.isCharger(from); fromIsCharger && db.IsExchange(to) {
			exchange := db.GetExchange(to).Name
			setExchangeStatsMap(exchangeStats, date, exchange, token)
			exchangeStats[exchange]["_"].AddCollect(tx)
			exchangeStats[exchange][token].AddCollect(tx)

		} else if charger, toIsCharger := db.isCharger(to); toIsCharger {
			setExchangeStatsMap(exchangeStats, date, charger.ExchangeName, token)
			exchangeStats[charger.ExchangeName]["_"].AddCharge(tx)
			exchangeStats[charger.ExchangeName][token].AddCharge(tx)
		}
	})

	db.db.Delete(&models.ExchangeStatistic{}, "date = ?", date)
	for _, stats := range exchangeStats {
		for _, stat := range stats {
			db.db.Create(stat)
		}
	}
}

func setExchangeStatsMap(stats map[string]map[string]*models.ExchangeStatistic, date, exchange, token string) {
	if _, ok := stats[exchange]; !ok {
		stats[exchange] = make(map[string]*models.ExchangeStatistic)
		stats[exchange]["_"] =
			models.NewExchangeStatistic(date, exchange, "_")
	}

	if _, ok := stats[exchange][token]; !ok {
		stats[exchange][token] =
			models.NewExchangeStatistic(date, exchange, token)
	}
}

func (db *RawDB) isCharger(address string) (*models.Charger, bool) {
	db.chargersLock.RLock()
	defer db.chargersLock.RUnlock()

	if c, ok := db.chargers[address]; ok && !c.IsFake {
		return c, true
	}

	return nil, false
}

func (db *RawDB) saveMarketPairOriginData(token, data string) {
	date := time.Now().Format("060102")
	hour := time.Now().Format("1504")
	filePath := fmt.Sprintf("/data/market_pairs/%s/%s/", token, date)
	err := os.MkdirAll(filePath, os.ModePerm)
	if err != nil {
		db.logger.Errorf("Create directory error: [%s]", err.Error())
		return
	}

	dataFile, err := os.Create(filePath + hour + ".json")
	if err != nil {
		db.logger.Errorf("Create data file error: [%s]", err.Error())
		return
	}
	defer dataFile.Close()

	dataFile.WriteString(data)
}

func (db *RawDB) saveTokenListingOriginData(data string) {
	date := time.Now().Format("060102")
	hour := time.Now().Format("1504")
	filePath := fmt.Sprintf("/data/token_listings/%s/", date)
	err := os.MkdirAll(filePath, os.ModePerm)
	if err != nil {
		db.logger.Errorf("Create directory error: [%s]", err.Error())
		return
	}

	dataFile, err := os.Create(filePath + hour + ".json")
	if err != nil {
		db.logger.Errorf("Create data file error: [%s]", err.Error())
		return
	}
	defer dataFile.Close()

	dataFile.WriteString(data)
}

func (db *RawDB) marketPairDepthStatisticsLoop() {
	isAllHistoryDone := false
	for {
		select {
		case <-db.quitCh:
			db.logger.Info("market pair depth statistics loop ended")
			db.loopWG.Done()
			return
		case date := <-db.statsCh:
			yesterday, _ := time.Parse("060102", date)
			db.doMarketPairDepthStatistics(yesterday)
		default:
			if !isAllHistoryDone {
				tables, err := db.db.Migrator().GetTables()
				if err != nil {
					db.logger.Warnf("Failed to get tables: %v", err)
					isAllHistoryDone = true
					continue
				}

				db.logger.Info("Start doing market pair depth statistics for history")
				for _, table := range tables {
					if strings.Contains(table, "market_pair_statistics_") {
						month := strings.Split(table, "_")[3]
						date, _ := time.Parse("0601", month)

						for date.Format("0601") == month && date.Before(time.Now().Truncate(24*time.Hour)) {
							db.doMarketPairDepthStatistics(date)
							date = date.AddDate(0, 0, 1)
						}
					}
				}

				isAllHistoryDone = true
				db.logger.Info("Finish doing market pair depth statistics for history")
			}

			time.Sleep(1 * time.Second)
		}
	}
}

func (db *RawDB) doMarketPairDepthStatistics(date time.Time) {
	table := "market_pair_statistics_" + date.Format("0601")

	stat := &models.MarketPairStatistic{}
	res := db.db.Table(table).Where("datetime = ?", date.Format("060102")).First(stat)

	if errors.Is(res.Error, gorm.ErrRecordNotFound) {
		var depthStats []*models.MarketPairStatistic
		db.db.Table(table).
			Select("token", "exchange_name", "pair",
				"AVG(depth_usd_positive_two) as depth_usd_positive_two",
				"AVG(depth_usd_negative_two) as depth_usd_negative_two").
			Where("datetime like ? and percent > 0", date.Format("02")+"%").
			Group("token, exchange_name, pair").
			Find(&depthStats)

		for _, depthStat := range depthStats {
			depthStat.Datetime = date.Format("060102")
			db.db.Create(depthStat)
		}

		db.logger.Infof("Finish market pair depth statistics for date [%s], affected rows: %d", date.Format("060102"), len(depthStats))
	}
}

func (db *RawDB) cacheFlushLoop() {
	for {
		select {
		case <-db.quitCh:
			db.logger.Info("cache flush loop ended")
			db.loopWG.Done()
			return
		case cache := <-db.flushCh:
			db.flushCacheToDB(cache)
		}
	}
}

func (db *RawDB) countLoop() {
	for {
		select {
		case <-db.quitCh:
			db.logger.Info("count loop ended")
			db.loopWG.Done()
			return
		default:
			trackingDate, _ := time.Parse("060102", db.trackingDate)
			countedDate, _ := time.Parse("060102", db.countedDate)

			if trackingDate.Sub(countedDate).Hours() > 24 {
				dateToCount := countedDate.AddDate(0, 0, 1).Format("060102")
				db.countForDate(dateToCount)
				db.countedDate = dateToCount

				if countedDate.Weekday() == time.Saturday {
					db.countedWeek = db.countForWeek(db.countedWeek)
				}
			}

			time.Sleep(1 * time.Second)
		}
	}
}

func (db *RawDB) countForDate(date string) {
	db.logger.Infof("Start counting Transactions for date [%s]", date)

	var (
		txCount              = int64(0)
		results              = make([]*models.Transaction, 0)
		TRXStats             = make(map[string]*models.FungibleTokenStatistic)
		USDTStats            = make(map[string]*models.FungibleTokenStatistic)
		USDTStorageStat      = &models.USDTStorageStatistic{}
		ExchangeSpecialStats = make(map[string]*models.ExchangeStatistic)
	)

	result := db.db.Table("transactions_"+date).
		Where("type = ? or name = ?", 1, "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t").
		FindInBatches(&results, 100, func(tx *gorm.DB, _ int) error {
			for _, result := range results {
				if len(result.ToAddr) == 0 || result.Result != 1 {
					continue
				}

				typeName := fmt.Sprintf("1e%d", len(result.Amount.String()))
				if result.Type == 1 {
					if _, ok := TRXStats[typeName]; !ok {
						TRXStats[typeName] = models.NewFungibleTokenStatistic(date, "TRX", typeName, result)
					} else {
						TRXStats[typeName].Add(result)
					}
				} else {
					if _, ok := USDTStats[typeName]; !ok {
						USDTStats[typeName] = models.NewFungibleTokenStatistic(date, "USDT", typeName, result)
					} else {
						USDTStats[typeName].Add(result)
					}

					if result.Method == "a9059cbb" || result.Method == "23b872dd" {
						USDTStorageStat.Add(result)
					}
				}

				from := result.FromAddr
				to := result.ToAddr

				if charger, ok := db.isCharger(to); ok && db.IsExchange(from) {
					exchange := db.GetExchange(from)
					if _, ok := ExchangeSpecialStats[exchange.Name]; !ok {
						ExchangeSpecialStats[exchange.Name] =
							models.NewExchangeStatistic(date, charger.ExchangeName, "Special")

						ExchangeSpecialStats[exchange.Name].AddWithdraw(result)
					}
				}

			}
			txCount += tx.RowsAffected

			if txCount%500_000 == 0 {
				db.logger.Infof("Counting Transactions for date [%s], current counted txs [%d]", date, txCount)
			}

			return nil
		})

	for _, stats := range TRXStats {
		db.db.Create(stats)
	}

	for _, stats := range USDTStats {
		db.db.Create(stats)
	}

	USDTStorageStat.Date = date
	db.db.Create(USDTStorageStat)

	for _, stats := range ExchangeSpecialStats {
		db.db.Create(stats)
	}

	db.db.Model(&models.Meta{}).Where(models.Meta{Key: models.CountedDateKey}).Update("val", date)
	db.logger.Infof("Finish counting Transactions for date [%s], counted txs [%d]", date, result.RowsAffected)
}

func (db *RawDB) countForWeek(startDate string) string {
	week := generateWeek(startDate)
	db.logger.Infof("Start counting Transactions for week [%s], start date [%s]", week, startDate)

	var (
		countingDate = startDate
		txCount      = int64(0)
		results      = make([]*models.Transaction, 0)
		TRXStats     = make(map[string]*models.FungibleTokenStatistic)
		USDTStats    = make(map[string]*models.FungibleTokenStatistic)
	)

	for generateWeek(countingDate) == week {
		result := db.db.Table("transactions_"+countingDate).
			Where("type = ? or name = ?", 1, "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t").
			FindInBatches(&results, 100, func(tx *gorm.DB, _ int) error {
				for _, result := range results {
					if len(result.ToAddr) == 0 || result.Result != 1 {
						continue
					}

					typeName := fmt.Sprintf("1e%d", len(result.Amount.String()))
					if result.Type == 1 {
						if _, ok := TRXStats[typeName]; !ok {
							TRXStats[typeName] = models.NewFungibleTokenStatistic(week, "TRX", typeName, result)
						} else {
							TRXStats[typeName].Add(result)
						}
					} else {
						if _, ok := USDTStats[typeName]; !ok {
							USDTStats[typeName] = models.NewFungibleTokenStatistic(week, "USDT", typeName, result)
						} else {
							USDTStats[typeName].Add(result)
						}
					}
				}
				txCount += tx.RowsAffected

				if txCount%500_000 == 0 {
					db.logger.Infof("Counting Transactions for week [%s], current counting date [%s], counted txs [%d]", week, countingDate, txCount)
				}

				return nil
			})

		if result.Error != nil {
			db.logger.Errorf("Counting Transactions for week [%s], error [%v]", week, result.Error)
		}

		date, _ := time.Parse("060102", countingDate)
		countingDate = date.AddDate(0, 0, 1).Format("060102")
	}

	for _, stats := range TRXStats {
		db.db.Create(stats)
	}

	for _, stats := range USDTStats {
		db.db.Create(stats)
	}

	db.db.Model(&models.Meta{}).Where(models.Meta{Key: models.CountedWeekKey}).Update("val", countingDate)
	db.logger.Infof("Finish counting Transactions for week [%s], total counted txs [%d]", week, txCount)

	return countingDate
}

func (db *RawDB) flushChargerToDB() {
	chargers := make([]*models.Charger, 0)
	for _, charger := range db.chargersToSave {
		chargers = append(chargers, charger)
	}
	db.db.Save(chargers)
	db.chargersToSave = make(map[string]*models.Charger)
}

func (db *RawDB) flushCacheToDB(cache *dbCache) {
	if len(cache.fromStats) == 0 && len(cache.toStats) == 0 {
		return
	}

	db.logger.Infof("Start flushing cache to DB for date [%s]", cache.date)

	// Start flushing user from statistics
	db.logger.Info("Start flushing from_stats")

	fromStatsDBName := "from_stats_" + cache.date
	db.createTableIfNotExist(fromStatsDBName, models.UserStatistic{})

	reporter := utils.NewReporter(0, 60*time.Second, len(cache.fromStats), makeReporterFunc("from_stats"))

	fromStatsToPersist := make([]*models.UserStatistic, 0)
	for _, stats := range cache.fromStats {
		fromStatsToPersist = append(fromStatsToPersist, stats)
		if len(fromStatsToPersist) == 200 {
			db.db.Table(fromStatsDBName).Create(&fromStatsToPersist)
			fromStatsToPersist = make([]*models.UserStatistic, 0)
		}
		if shouldReport, reportContent := reporter.Add(1); shouldReport {
			db.logger.Info(reportContent)
		}
	}
	db.db.Table(fromStatsDBName).Create(&fromStatsToPersist)

	// End of flushing user from statistics
	db.logger.Info(reporter.Finish("Flushing from_stats"))

	// Start flushing user to statistics
	db.logger.Info("Start flushing to_stats")

	toStatsDBName := "to_stats_" + cache.date
	db.createTableIfNotExist(toStatsDBName, models.UserStatistic{})

	reporter = utils.NewReporter(0, 60*time.Second, len(cache.toStats), makeReporterFunc("to_stats"))

	toStatsToPersist := make([]*models.UserStatistic, 0)
	for _, stats := range cache.toStats {
		toStatsToPersist = append(toStatsToPersist, stats)
		if len(toStatsToPersist) == 200 {
			db.db.Table(toStatsDBName).Create(&toStatsToPersist)
			toStatsToPersist = make([]*models.UserStatistic, 0)
		}
		if shouldReport, reportContent := reporter.Add(1); shouldReport {
			db.logger.Info(reportContent)
		}
	}
	db.db.Table(toStatsDBName).Create(&toStatsToPersist)

	// End of flushing user to statistics
	db.logger.Info(reporter.Finish("Flushing to_stats"))

	// Start flushing token statistics
	db.logger.Info("Start flushing token_stats")

	tokenStatsDBName := "token_stats_" + cache.date
	db.createTableIfNotExist(tokenStatsDBName, models.TokenStatistic{})

	reporter = utils.NewReporter(0, 60*time.Second, len(cache.tokenStats), makeReporterFunc("token_stats"))

	tokenStatsToPersist := make([]*models.TokenStatistic, 0)
	for _, stats := range cache.tokenStats {
		tokenStatsToPersist = append(tokenStatsToPersist, stats)
		if len(tokenStatsToPersist) == 200 {
			db.db.Table(tokenStatsDBName).Create(&tokenStatsToPersist)
			tokenStatsToPersist = make([]*models.TokenStatistic, 0)
		}
		if shouldReport, reportContent := reporter.Add(1); shouldReport {
			db.logger.Info(reportContent)
		}
	}
	db.db.Table(tokenStatsDBName).Create(&tokenStatsToPersist)

	// End of flushing token statistics
	db.logger.Info(reporter.Finish("Flushing token_stats"))

	// Start flushing user token statistics
	db.logger.Info("Start flushing user_token_stats")

	userTokenStatsDBName := "user_token_stats_" + cache.date
	db.createTableIfNotExist(userTokenStatsDBName, models.UserTokenStatistic{})

	reporter = utils.NewReporter(0, 60*time.Second, len(cache.userTokenStats), makeReporterFunc("user_token_stats"))

	userTokenStatsToPersist := make([]*models.UserTokenStatistic, 0)
	for _, stats := range cache.userTokenStats {
		userTokenStatsToPersist = append(userTokenStatsToPersist, stats)
		if len(userTokenStatsToPersist) == 200 {
			db.db.Table(userTokenStatsDBName).Create(&userTokenStatsToPersist)
			userTokenStatsToPersist = make([]*models.UserTokenStatistic, 0)
		}

		// Check need to report
		if shouldReport, reportContent := reporter.Add(1); shouldReport {
			db.logger.Info(reportContent)
		}
	}
	db.db.Table(userTokenStatsDBName).Create(&userTokenStatsToPersist)

	// End of flushing user token statistics
	db.logger.Info(reporter.Finish("Flushing user_token_stats"))

	// Start doing exchange statistics
	db.DoExchangeStatistics(cache.date)

	// Refresh chargers
	db.refreshChargers()

	db.logger.Infof("Complete flushing cache to DB for date [%s]", cache.date)
}

func (db *RawDB) createTableIfNotExist(tableName string, model interface{}) {
	db.tableLock.Lock()
	defer db.tableLock.Unlock()

	if ok, _ := db.isTableMigrated[tableName]; !ok {
		err := db.db.Table(tableName).AutoMigrate(&model)
		if err != nil {
			panic(err)
		}

		db.isTableMigrated[tableName] = true
	}
}

func generateDate(ts int64) string {
	return time.Unix(ts, 0).In(time.FixedZone("UTC", 0)).Format("060102")
}

func generateWeek(date string) string {
	ts, _ := time.Parse("060102", date)
	year, week := ts.ISOWeek()
	return fmt.Sprintf("%d%2d", year, week)
}

func makeReporterFunc(name string) func(utils.ReporterState) string {
	return func(rs utils.ReporterState) string {
		return fmt.Sprintf("Saved [%d] [%s] in [%.2fs], speed [%.2frecords/sec], left [%d/%d] records to save [%.2f%%]",
			rs.CountInc, name, rs.ElapsedTime, float64(rs.CountInc)/rs.ElapsedTime,
			rs.CurrentCount, rs.FinishCount, float64(rs.CurrentCount*100)/float64(rs.FinishCount))
	}
}
