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

	"github.com/jinzhu/now"
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

	dbErr = db.AutoMigrate(&models.MarketPairStatistic{})
	if dbErr != nil {
		panic(dbErr)
	}

	dbErr = db.AutoMigrate(&models.Meta{})
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

func (db *RawDB) GetLastTrackedBlockNum() uint {
	return db.lastTrackedBlockNum
}

func (db *RawDB) GetLastTrackedBlockTime() int64 {
	return db.lastTrackedBlockTime
}

func (db *RawDB) GetTokenName(addr string) string {
	return db.vt[addr]
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

func (db *RawDB) GetFromStatisticByDateAndUserAndDays(date time.Time, user string, days int) models.UserStatistic {
	result := models.UserStatistic{
		Address: user,
	}

	for i := 0; i < days; i++ {
		queryDate := date.AddDate(0, 0, i).Format("060102")

		var dayStat models.UserStatistic
		db.db.Table("from_stats_"+queryDate).Where("address = ?", user).Limit(1).Find(&dayStat)

		result.Merge(&dayStat)
	}

	return result
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

func (db *RawDB) GetTotalStatisticsByDate(date string) models.UserStatistic {
	var totalStat models.UserStatistic
	db.db.Table("from_stats_"+date).Where("address = ?", "total").Limit(1).Find(&totalStat)
	return totalStat
}

func (db *RawDB) GetTokenStatisticsByDate(date, token string) models.TokenStatistic {
	var tokenStat models.TokenStatistic
	db.db.Table("token_stats_"+date).Where("address = ?", token).Limit(1).Find(&tokenStat)
	return tokenStat
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

func (db *RawDB) GetExchangeTotalStatisticsByDate(date time.Time) []models.ExchangeStatistic {
	return db.GetExchangeStatisticsByDateAndToken(date.Format("060102"), "_")
}

func (db *RawDB) GetExchangeStatisticsByDateAndToken(date, token string) []models.ExchangeStatistic {
	var results []models.ExchangeStatistic
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

func (db *RawDB) GetMarketPairStatisticsByDateAndDays(date time.Time, days int) map[string]*models.MarketPairStatistic {
	resultMap := make(map[string]*models.MarketPairStatistic)

	var totalVolume float64
	for i := 0; i < days; i++ {
		queryDate := date.AddDate(0, 0, i).Format("060102")

		var dayStats []*models.MarketPairStatistic
		db.db.Where("datetime = ?", queryDate+"00").Find(&dayStats)

		for _, dayStat := range dayStats {
			if dayStat.Percent == 0 {
				continue
			}

			key := dayStat.ExchangeName + "_" + dayStat.Pair
			if _, ok := resultMap[key]; !ok {
				resultMap[key] = dayStat
			} else {
				resultMap[key].Volume += dayStat.Volume
			}

			totalVolume += dayStat.Volume
		}
	}

	for _, stat := range resultMap {
		stat.Datetime = ""
		stat.Percent = stat.Volume / totalVolume
	}

	return resultMap
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

func (db *RawDB) UpdateStatistics(tx *models.Transaction) {
	db.updateUserStatistic(tx.FromAddr, tx, db.cache.fromStats)
	db.updateUserStatistic("total", tx, db.cache.fromStats)

	if len(tx.ToAddr) > 0 {
		db.updateUserStatistic(tx.ToAddr, tx, db.cache.toStats)
	}

	if len(tx.Name) > 0 {
		db.updateTokenStatistic(tx.Name, tx, db.cache.tokenStats)
		db.updateUserTokenStatistic(tx, db.cache.userTokenStats)
	}
}

func (db *RawDB) updateUserStatistic(user string, tx *models.Transaction, stats map[string]*models.UserStatistic) {
	db.cache.date = generateDate(tx.Timestamp)
	if _, ok := stats[user]; !ok {
		stats[user] = &models.UserStatistic{
			Address: user,
		}
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
		stats[tx.FromAddr+tx.Name] = &models.UserTokenStatistic{
			User:  tx.FromAddr,
			Token: tx.Name,
		}
	}
	stats[tx.FromAddr+tx.Name].AddFrom(tx)

	if _, ok := stats[tx.ToAddr+tx.Name]; !ok {
		stats[tx.ToAddr+tx.Name] = &models.UserTokenStatistic{
			User:  tx.ToAddr,
			Token: tx.Name,
		}
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

	thisMonday := now.With(date).BeginningOfWeek().AddDate(0, 0, 1).Format("20060102")

	weeklyUsersFile, err := os.Open(fmt.Sprintf("/data/tronlink/week%s.txt", thisMonday))
	if err != nil {
		db.logger.Info("Weekly file has`t been created yet")
		return
	}
	defer weeklyUsersFile.Close()

	statsResultFilePath := fmt.Sprintf("/data/tronlink/week%s_stats.txt", thisMonday)
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

	db.logger.Info("Start count TronLink user fee for week " + thisMonday)

	lastMonday := now.With(date).BeginningOfWeek().AddDate(0, 0, -6)
	db.logger.Infof("Last Monday: %s", lastMonday.Format("2006-01-02"))

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
		queryDate := lastMonday.AddDate(0, 0, i).Format("060102")
		db.logger.Infof("Start querying transactions for date: %s", queryDate)

		txCount := 0
		results := make([]*models.Transaction, 0)
		result := db.db.Table("transactions_"+queryDate).FindInBatches(&results, 100, func(_ *gorm.DB, _ int) error {
			for _, result := range results {
				user := result.FromAddr
				if _, ok := users[user]; !ok {
					continue
				}

				if db.el.Contains(user) {
					withdrawFee += result.Fee
					withdrawEnergy += result.EnergyTotal
				} else if _, ok := db.chargers[user]; ok {
					collectFee += result.Fee
					collectEnergy += result.EnergyTotal
				} else if _, ok := db.chargers[result.ToAddr]; ok {
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
}

func (db *RawDB) DoMarketPairStatistics() {
	db.logger.Infof("Start doing market pair statistics")

	originData, marketPairs, err := net.GetMarketPairs()
	if err != nil {
		db.logger.Errorf("Get market pairs error: [%s]", err.Error())
		return
	}

	db.saveMarketPairOriginData(originData)
	db.db.Save(marketPairs)

	db.logger.Infof("Finish doing market pair statistics")
}

func (db *RawDB) saveMarketPairOriginData(data string) {
	date := time.Now().Format("060102")
	hour := time.Now().Format("15")
	filePath := fmt.Sprintf("/data/market_pairs/%s/", date)
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
		ExchangeSpecialStats = make(map[string]*models.ExchangeStatistic)
	)

	result := db.db.Table("transactions_"+date).Where("type = ?", 1).FindInBatches(&results, 100, func(tx *gorm.DB, _ int) error {
		for _, result := range results {
			typeName := fmt.Sprintf("1e%d", len(result.Amount.String()))
			if _, ok := TRXStats[typeName]; !ok {
				TRXStats[typeName] = models.NewFungibleTokenStatistic(date, "TRX", typeName, result)
			} else {
				TRXStats[typeName].Add(result)
			}

			from := result.FromAddr
			to := result.ToAddr
			if charger, ok := db.chargers[to]; ok && db.el.Contains(from) {
				exchange := db.el.Get(from)
				if _, ok := ExchangeSpecialStats[exchange.Name]; !ok {
					ExchangeSpecialStats[exchange.Name] = &models.ExchangeStatistic{
						Date:  date,
						Name:  charger.ExchangeName,
						Token: "Special",
					}

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
	)

	for generateWeek(countingDate) == week {
		result := db.db.Table("transactions_"+countingDate).Where("type = ?", 1).FindInBatches(&results, 100, func(tx *gorm.DB, _ int) error {
			for _, result := range results {
				typeName := fmt.Sprintf("1e%d", len(result.Amount.String()))
				if _, ok := TRXStats[typeName]; !ok {
					TRXStats[typeName] = models.NewFungibleTokenStatistic(week, "TRX", typeName, result)
				} else {
					TRXStats[typeName].Add(result)
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
					exchangeStats[exchange.Name]["_"] = &models.ExchangeStatistic{
						Date:  cache.date,
						Name:  exchange.Name,
						Token: "_",
					}
				}

				exchangeStats[exchange.Name]["_"].AddCollect(stats)
				exchangeStats[exchange.Name]["_"].AddWithdraw(stats)

				if _, ok := exchangeStats[exchange.Name][stats.Token]; !ok {
					exchangeStats[exchange.Name][stats.Token] = &models.ExchangeStatistic{
						Date:  cache.date,
						Name:  exchange.Name,
						Token: stats.Token,
					}
				}

				exchangeStats[exchange.Name][stats.Token].AddCollect(stats)
				exchangeStats[exchange.Name][stats.Token].AddWithdraw(stats)
			} else {
				db.chargersLock.RLock()
				charger, ok := db.chargers[stats.User]
				db.chargersLock.RUnlock()
				if ok {
					if charger.IsFake {
						continue
					}

					if db.el.Contains(charger.Address) {
						charger.IsFake = true
						db.db.Save(charger)
						continue
					}

					if _, ok := exchangeStats[charger.ExchangeName]; !ok {
						exchangeStats[charger.ExchangeName] = make(map[string]*models.ExchangeStatistic)
						exchangeStats[charger.ExchangeName]["_"] = &models.ExchangeStatistic{
							Date:  cache.date,
							Name:  charger.ExchangeName,
							Token: "_",
						}
					}

					exchangeStats[charger.ExchangeName]["_"].AddCharge(stats)

					if _, ok := exchangeStats[charger.ExchangeName][stats.Token]; !ok {
						exchangeStats[charger.ExchangeName][stats.Token] = &models.ExchangeStatistic{
							Date:  cache.date,
							Name:  charger.ExchangeName,
							Token: stats.Token,
						}
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
