package database

import (
	"bufio"
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
	db          *gorm.DB
	el          *types.ExchangeList
	elUpdatedAt time.Time
	vt          map[string]string

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
		el:          net.GetExchanges(),
		elUpdatedAt: time.Now(),
		vt:          validTokens,

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

	rawDB.loadChargers()

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

func (db *RawDB) Start() {
	db.loopWG.Add(2)
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
	return db.el.Contains(addr)
}

func (db *RawDB) GetLastTrackedBlockNum() uint {
	return db.lastTrackedBlockNum
}

func (db *RawDB) GetLastTrackedBlockTime() int64 {
	return db.lastTrackedBlockTime
}

func (db *RawDB) GetTokenName(addr string) string {
	return db.vt[addr]
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
				resultMap[es.Name][tokenName] = &models.ExchangeStatistic{}
			}
			resultMap[es.Name][tokenName].Merge(&es)

			if _, ok := resultMap["All"][tokenName]; !ok {
				resultMap["All"][tokenName] = &models.ExchangeStatistic{}
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

func (db *RawDB) GetTokenPriceByDate(date time.Time, token string) float64 {
	queryDateDBName := "market_pair_statistics_" + date.Format("0601")

	var stats models.MarketPairStatistic
	db.db.Table(queryDateDBName).
		Where("datetime = ? and token = ?", date.Format("02")+"0010", token).
		Find(&stats)

	return stats.Price
}

func (db *RawDB) GetMarketPairStatisticsByDateAndDaysAndToken(date time.Time, days int, token string) map[string]*models.MarketPairStatistic {
	resultMap := make(map[string]*models.MarketPairStatistic)

	var totalVolume float64
	for i := 0; i < days; i++ {
		today := date.AddDate(0, 0, i)
		todayDBName := "market_pair_statistics_" + today.Format("0601")

		var volumeStats []*models.MarketPairStatistic
		db.db.Table(todayDBName).
			Select("exchange_name", "pair", "volume").
			Where("datetime = ? and token = ? and percent > 0", today.Format("02")+"0000", token).
			Find(&volumeStats)

		for _, dayStat := range volumeStats {
			key := dayStat.ExchangeName + "_" + dayStat.Pair
			if _, ok := resultMap[key]; !ok {
				resultMap[key] = dayStat
			} else {
				resultMap[key].Volume += dayStat.Volume
			}

			totalVolume += dayStat.Volume
		}

		lastDay := today.AddDate(0, 0, -1)
		lastDayDBName := "market_pair_statistics_" + lastDay.Format("0601")

		var depthStats []*models.MarketPairStatistic
		db.db.Table(lastDayDBName).
			Select("exchange_name", "pair",
				"AVG(depth_usd_positive_two) as depth_usd_positive_two",
				"AVG(depth_usd_negative_two) as depth_usd_negative_two").
			Where("datetime like ? and token = ? and percent > 0", lastDay.Format("02")+"%", token).
			Group("exchange_name, pair").
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

	totalVolume /= float64(days)
	for _, stat := range resultMap {
		stat.Datetime = ""
		stat.Token = ""
		stat.Volume /= float64(days)
		stat.Percent = stat.Volume / totalVolume
		stat.DepthUsdPositiveTwo /= float64(days)
		stat.DepthUsdNegativeTwo /= float64(days)
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

		db.trackingDate = trackingDate
		db.cache = newCache()

		db.db.Model(&models.Meta{}).Where(models.Meta{Key: models.TrackingDateKey}).Update("val", trackingDate)
		db.db.Model(&models.Meta{}).Where(models.Meta{Key: models.TrackingStartBlockNumKey}).Update("val", strconv.Itoa(int(block.BlockHeader.RawData.Number)))
	}

	db.lastTrackedBlockNum = block.BlockHeader.RawData.Number
	db.lastTrackedBlockTime = block.BlockHeader.RawData.Timestamp
}

func (db *RawDB) updateExchanges() {
	if time.Now().Sub(db.elUpdatedAt) < 1*time.Hour {
		return
	}

	for i := 0; i < 3; i++ {
		el := net.GetExchanges()
		if len(el.Exchanges) == 0 {
			db.logger.Infof("No exchange found, retry %d times", i+1)
		} else {
			if len(db.el.Exchanges) == len(el.Exchanges) {
				db.logger.Info("Update exchange list successfully, exchange list is not changed")
			} else {
				db.logger.Infof("Update exchange list successfully, exchange list size [%d] => [%d]", len(db.el.Exchanges), len(el.Exchanges))
			}

			db.el = el
			db.elUpdatedAt = time.Now()
			break
		}
	}
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
	if _, ok := db.vt[token]; !ok {
		return
	}

	if charger, ok := db.chargers[from]; !ok && !db.el.Contains(from) && db.el.Contains(to) {
		db.chargersLock.Lock()
		db.chargers[from] = &models.Charger{
			Address:      from,
			ExchangeName: db.el.Get(to).Name,
		}
		db.chargersLock.Unlock()

		db.chargersToSave[from] = db.chargers[from]
	} else if ok && !charger.IsFake {
		if db.el.Contains(to) {
			// Charger interact with other exchange address, so it is a fake charger
			if db.el.Get(to).Name != charger.ExchangeName {
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

				if db.el.Contains(user) {
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

	tronOriginData, tronMarketPairs, err := net.GetMarketPairs("tron")
	if err != nil {
		db.logger.Errorf("Get tron market pairs error: [%s]", err.Error())
		return
	}

	db.saveMarketPairOriginData("tron", tronOriginData)
	db.db.Table(statsDBName).Save(tronMarketPairs)

	steemOriginData, steemMarketPairs, err := net.GetMarketPairs("steem")
	if err != nil {
		db.logger.Errorf("Get steem market pairs error: [%s]", err.Error())
		return
	}

	db.saveMarketPairOriginData("steem", steemOriginData)
	db.db.Table(statsDBName).Save(steemMarketPairs)

	db.logger.Infof("Finish doing market pair statistics")
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

			db.countAmount()
			os.Exit(1)

			// time.Sleep(1 * time.Second)
		}
	}
}

func (db *RawDB) countAmount() {
	db.logger.Infof("Start counting amount")

	var (
		countingDate = "240808"
		results      = make([]*models.Transaction, 0)
	)

	for countingDate != db.trackingDate {
		txCount := int64(0)
		exchangeStats := make(map[string]map[string]*models.ExchangeStatistic)
		result := db.db.Table("transactions_"+countingDate).
			FindInBatches(&results, 100, func(tx *gorm.DB, _ int) error {
				for _, result := range results {
					if _, ok := db.vt[result.Name]; !ok || len(result.ToAddr) == 0 || result.Result != 1 {
						continue
					}

					if db.el.Contains(result.FromAddr) {
						exchangeName := db.el.Get(result.FromAddr).Name
						if _, ok := exchangeStats[exchangeName]; !ok {
							exchangeStats[exchangeName] = make(map[string]*models.ExchangeStatistic)
						}
						if _, ok := exchangeStats[exchangeName][result.Name]; !ok {
							exchangeStats[exchangeName][result.Name] =
								models.NewExchangeStatistic(countingDate, exchangeName, result.Name)
						}
						exchangeStats[exchangeName][result.Name].WithdrawAmount.Add(result.Amount)
					} else if db.el.Contains(result.ToAddr) {
						exchangeName := db.el.Get(result.ToAddr).Name
						if _, ok := exchangeStats[exchangeName]; !ok {
							exchangeStats[exchangeName] = make(map[string]*models.ExchangeStatistic)
						}
						if _, ok := exchangeStats[exchangeName][result.Name]; !ok {
							exchangeStats[exchangeName][result.Name] =
								models.NewExchangeStatistic(countingDate, exchangeName, result.Name)
						}
						exchangeStats[exchangeName][result.Name].CollectAmount.Add(result.Amount)
					} else if charger, ok := db.isCharger(result.ToAddr); ok {
						exchangeName := charger.ExchangeName
						if _, ok := exchangeStats[exchangeName]; !ok {
							exchangeStats[exchangeName] = make(map[string]*models.ExchangeStatistic)
						}
						if _, ok := exchangeStats[exchangeName][result.Name]; !ok {
							exchangeStats[exchangeName][result.Name] =
								models.NewExchangeStatistic(countingDate, exchangeName, result.Name)
						}
						exchangeStats[exchangeName][result.Name].ChargeAmount.Add(result.Amount)
					}
				}

				txCount += tx.RowsAffected

				if txCount%500_000 == 0 {
					db.logger.Infof("Counting amount for date [%s], counted txs [%d]", countingDate, txCount)
				}

				return nil
			})

		for _, stats := range exchangeStats {
			for _, stat := range stats {
				db.db.Where(&models.ExchangeStatistic{
					Date:  stat.Date,
					Name:  stat.Name,
					Token: stat.Token,
				}).Updates(models.ExchangeStatistic{
					ChargeAmount:   stat.ChargeAmount,
					CollectAmount:  stat.CollectAmount,
					WithdrawAmount: stat.WithdrawAmount,
				})
			}
		}

		if result.Error != nil {
			db.logger.Errorf("Counting amount for date [%s], error [%v]", countingDate, result.Error)
		}

		db.logger.Infof("Finish counting amount for date [%s], counted txs [%d]", countingDate, result.RowsAffected)

		date, _ := time.Parse("060102", countingDate)
		countingDate = date.AddDate(0, 0, 1).Format("060102")
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

				if charger, ok := db.isCharger(to); ok && db.el.Contains(from) {
					exchange := db.el.Get(from)
					if _, ok := ExchangeSpecialStats[exchange.Name]; !ok {
						ExchangeSpecialStats[exchange.Name] =
							models.NewExchangeStatistic(date, charger.ExchangeName, "Special")

						ExchangeSpecialStats[exchange.Name].AddWithdrawFromTx(result)
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

	// Flush user from statistics
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

	db.logger.Info(reporter.Finish("Flushing from_stats"))

	// Flush user to statistics
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

	db.logger.Info(reporter.Finish("Flushing to_stats"))

	// Flush token statistics
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

	db.logger.Info(reporter.Finish("Flushing token_stats"))

	// Flush user token statistics
	db.logger.Info("Start flushing user_token_stats")

	userTokenStatsDBName := "user_token_stats_" + cache.date
	db.createTableIfNotExist(userTokenStatsDBName, models.UserTokenStatistic{})

	reporter = utils.NewReporter(0, 60*time.Second, len(cache.userTokenStats), makeReporterFunc("user_token_stats"))

	userTokenStatsToPersist := make([]*models.UserTokenStatistic, 0)
	exchangeStats := make(map[string]map[string]*models.ExchangeStatistic)
	for _, stats := range cache.userTokenStats {
		// First, do exchange statistics
		if _, ok := db.vt[stats.Token]; ok {
			if db.el.Contains(stats.User) {
				exchange := db.el.Get(stats.User)

				if _, ok := exchangeStats[exchange.Name]; !ok {
					exchangeStats[exchange.Name] = make(map[string]*models.ExchangeStatistic)
					exchangeStats[exchange.Name]["_"] =
						models.NewExchangeStatistic(cache.date, exchange.Name, "_")
				}

				exchangeStats[exchange.Name]["_"].AddCollect(stats)
				exchangeStats[exchange.Name]["_"].AddWithdraw(stats)

				if _, ok := exchangeStats[exchange.Name][stats.Token]; !ok {
					exchangeStats[exchange.Name][stats.Token] =
						models.NewExchangeStatistic(cache.date, exchange.Name, stats.Token)
				}

				exchangeStats[exchange.Name][stats.Token].AddCollect(stats)
				exchangeStats[exchange.Name][stats.Token].AddWithdraw(stats)
			} else {
				charger, isCharger := db.isCharger(stats.User)

				if isCharger {
					if db.el.Contains(charger.Address) {
						charger.IsFake = true
						db.db.Save(charger)
						continue
					}

					if _, ok := exchangeStats[charger.ExchangeName]; !ok {
						exchangeStats[charger.ExchangeName] = make(map[string]*models.ExchangeStatistic)
						exchangeStats[charger.ExchangeName]["_"] =
							models.NewExchangeStatistic(cache.date, charger.ExchangeName, "_")
					}

					exchangeStats[charger.ExchangeName]["_"].AddCharge(stats)

					if _, ok := exchangeStats[charger.ExchangeName][stats.Token]; !ok {
						exchangeStats[charger.ExchangeName][stats.Token] =
							models.NewExchangeStatistic(cache.date, charger.ExchangeName, stats.Token)
					}

					exchangeStats[charger.ExchangeName][stats.Token].AddCharge(stats)
				}
			}
		}

		// Second, save it to db
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

	db.logger.Info(reporter.Finish("Flushing user_token_stats"))

	db.logger.Info("Start saving exchange statistic")

	// Flush exchange statistics
	for _, tokenStats := range exchangeStats {
		for _, stats := range tokenStats {
			db.db.Create(stats)
		}
	}

	db.logger.Info("Complete saving exchange statistic")

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
