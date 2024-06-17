package database

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"regexp"
	"sort"
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

	lastTrackedBlockNum      uint64
	lastTrackedBlockTime     int64
	lastTrackedEthBlockNum   uint64
	nextDaysStartEthBlockNum uint64
	isTableMigrated          map[string]bool
	tableLock                sync.Mutex
	statsLock                sync.Mutex
	chargers                 map[string]*models.Charger
	chargersLock             sync.RWMutex
	chargersToSave           map[string]*models.Charger

	usdtPhishingRelations map[string]bool

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

	dbErr = db.AutoMigrate(&models.ERC20Statistic{})
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

	var trackedEthBlockNumMeta models.Meta
	db.Where(models.Meta{Key: models.TrackedEthBlockNumKey}).Attrs(models.Meta{Val: strconv.Itoa(4634748)}).FirstOrCreate(&trackedEthBlockNumMeta)
	trackedEthBlockNum, _ := strconv.Atoi(trackedEthBlockNumMeta.Val)

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

		lastTrackedBlockNum:    uint64(trackingStartBlockNum - 1),
		lastTrackedEthBlockNum: uint64(trackedEthBlockNum),
		isTableMigrated:        make(map[string]bool),
		chargers:               make(map[string]*models.Charger),
		chargersToSave:         make(map[string]*models.Charger),

		usdtPhishingRelations: make(map[string]bool),

		flushCh:     make(chan *dbCache),
		cache:       newCache(),
		countedDate: countedDateMeta.Val,
		countedWeek: countedWeekMeta.Val,
		statsCh:     make(chan string),

		quitCh: make(chan struct{}),
		logger: zap.S().Named("[db]"),
	}

	// rawDB.loadChargers()

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
	db.logger.Infof("RawDB start, tron block [%d], eth block [%d]", db.lastTrackedBlockNum, db.lastTrackedEthBlockNum)

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

func (db *RawDB) GetLastTrackedBlockNum() uint64 {
	return db.lastTrackedBlockNum
}

func (db *RawDB) GetLastTrackedBlockTime() int64 {
	return db.lastTrackedBlockTime
}

func (db *RawDB) GetLastTrackedEthBlockNum() uint64 {
	return db.lastTrackedEthBlockNum
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

func (db *RawDB) GetTokenStatisticByDateAndTokenAndDays(date time.Time, days int) map[string]*models.TokenStatistic {
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

func (db *RawDB) GetExchangeTokenStatisticsByDate(date time.Time) []models.ExchangeStatistic {
	var results []models.ExchangeStatistic
	db.db.Where("date = ?", date.Format("060102")).Find(&results)
	return results
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

	db.lastTrackedBlockNum = uint64(block.BlockHeader.RawData.Number)
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

	if len(db.chargersToSave) >= 200 {
		db.flushChargerToDB()
	}
}

func (db *RawDB) DoTronLinkWeeklyStatistics(date time.Time, override bool) {
	db.statsLock.Lock()
	defer db.statsLock.Unlock()

	thisMonday := now.With(date).BeginningOfWeek().AddDate(0, 0, 1).Format("20060102")

	statsResultFilePath := fmt.Sprintf("tronlink/week%s_stats.txt", thisMonday)
	if _, err := os.Stat(statsResultFilePath); err == nil && !override {
		return
	}

	statsResultFile, err := os.Create(statsResultFilePath)
	defer statsResultFile.Close()

	if err != nil {
		db.logger.Errorf("Create stats file error: [%s]", err.Error())
		return
	}

	weeklyUsersFile, err := os.Open(fmt.Sprintf("tronlink/week%s.txt", thisMonday))
	defer weeklyUsersFile.Close()

	if err != nil {
		db.logger.Info("Weekly file has`t been created yet")
		return
	}

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
				// db.countForDate(dateToCount)
				db.countedDate = dateToCount

				if countedDate.Weekday() == time.Saturday {
					// db.countTRXPhishingForWeek(db.countedWeek)
					db.countUSDTPhishingForWeek(db.countedWeek)
					db.countedWeek = db.countForWeek(db.countedWeek)
				}
			}

			time.Sleep(1 * time.Second)
		}
	}
}

func (db *RawDB) countForDate(date string) {
	db.logger.Infof("Start counting TRX&USDT Transactions for date [%s]", date)

	var (
		txCount   = int64(0)
		results   = make([]*models.Transaction, 0)
		TRXStats  = make(map[string]*models.FungibleTokenStatistic)
		USDTStats = make(map[string]*models.FungibleTokenStatistic)
	)

	result := db.db.Table("transactions_"+date).Where("type = ? or name = ?", 1, "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t").FindInBatches(&results, 100, func(tx *gorm.DB, _ int) error {
		for _, result := range results {
			if len(result.ToAddr) == 0 || result.Result != "SUCCESS" {
				continue
			}

			typeName := fmt.Sprintf("1e%d", len(result.Amount.String()))
			if result.Name == "TRX" {
				if _, ok := TRXStats[typeName]; !ok {
					TRXStats[typeName] = models.NewFungibleTokenStatistic(date, "TRX", typeName, result)
				} else {
					TRXStats[typeName].Add(result)
				}
			} else if len(result.ToAddr) > 0 {
				if _, ok := USDTStats[typeName]; !ok {
					USDTStats[typeName] = models.NewFungibleTokenStatistic(date, "USDT", typeName, result)
				} else {
					USDTStats[typeName].Add(result)
				}
			}
		}

		txCount += tx.RowsAffected
		if txCount%500_000 == 0 {
			db.logger.Infof("Counting TRX&USDT Transactions for date [%s], current counted txs [%d]", date, txCount)
		}

		return nil
	})

	for _, stats := range TRXStats {
		db.db.Create(stats)
	}

	for _, stats := range USDTStats {
		db.db.Create(stats)
	}

	db.db.Model(&models.Meta{}).Where(models.Meta{Key: models.CountedDateKey}).Update("val", date)
	db.logger.Infof("Finish counting TRX&USDT Transactions for date [%s], total counted txs [%d]", date, result.RowsAffected)
}

type TRXStatistic struct {
	fromStats  map[string]int
	toStats    map[string]int
	toMap      map[string]bool
	amountMap  map[string]int
	phisherMap map[string]bool
	fakerMap   map[string]bool
}

func newTRXStatistic() *TRXStatistic {
	return &TRXStatistic{
		fromStats:  make(map[string]int),
		toStats:    make(map[string]int),
		toMap:      make(map[string]bool),
		amountMap:  make(map[string]int),
		phisherMap: make(map[string]bool),
		fakerMap:   make(map[string]bool),
	}
}

func (ts *TRXStatistic) getSmallCount() int {
	return ts.fromStats["1e1"] + ts.fromStats["1e2"] + ts.fromStats["1e3"]
}

func (ts *TRXStatistic) isCharger() bool {
	return len(ts.toMap) <= 1
}

func (ts *TRXStatistic) toString() string {
	return fmt.Sprintf("from: %v, to: %v, amount: %v", ts.fromStats, ts.toStats, ts.amountMap)
}

func (db *RawDB) countTRXPhishingForWeek(startDate string) {
	week := generateWeek(startDate)
	db.logger.Infof("Start counting Phishing TRX Transactions for week [%s], start date [%s]", week, startDate)

	var (
		countingDate = startDate
		txCount      = int64(0)
		phishSum     = int64(0)
		phisherMap   = make(map[string]int64)
		victimsMap   = make(map[string]int64)
		results      = make([]*models.Transaction, 0)
		report       = make(map[int]bool)
		stats        = make(map[string]*TRXStatistic)
		tickers      = make(map[string]bool)
		amounts      = make(map[string]int64)
		normals      = make(map[string]bool)
		usdt         = make(map[string]int64)
		rex          = regexp.MustCompile("^10*$")
	)

	for generateWeek(countingDate) == week {
		result := db.db.Table("transactions_"+countingDate).Where("type = ? OR name = ?", 1, "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t").FindInBatches(&results, 100, func(tx *gorm.DB, _ int) error {
			for _, result := range results {
				fromAddr := result.FromAddr
				toAddr := result.ToAddr

				if result.Name == "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t" {
					normals[fromAddr] = true
					usdt[fromAddr] += 1
					continue
				}

				if _, ok := stats[fromAddr]; !ok {
					stats[fromAddr] = newTRXStatistic()
				}
				if _, ok := stats[toAddr]; !ok {
					stats[toAddr] = newTRXStatistic()
				}

				stats[fromAddr].toMap[toAddr] = true

				amountStr := result.Amount.String()
				amountType := fmt.Sprintf("1e%d", len(amountStr))
				stats[toAddr].amountMap[amountStr] += 1

				if result.Fee == 0 && len(amountStr) <= 3 {
					stats[toAddr].phisherMap[fromAddr] = true
				}

				if _, ok := stats[toAddr].phisherMap[fromAddr]; ok && (result.Fee > 0 || len(amountStr) > 3) {
					stats[toAddr].fakerMap[fromAddr] = true
				}

				if _, ok := stats[fromAddr].phisherMap[toAddr]; ok && len(amountStr) >= 6 && !stats[toAddr].isCharger() && usdt[toAddr] < 2 {
					if _, ok := stats[fromAddr].fakerMap[toAddr]; !ok {
						amount := result.Amount.Int64()
						phishSum += amount
						phisherMap[toAddr] += amount
						victimsMap[fromAddr] += amount
						fmt.Printf("Phishing: %s %s %s %s %.1f TRX\n", countingDate, fromAddr, toAddr, result.Hash, float64(amount)/1_000_000)
					}
				}

				// Activate account transfer
				if result.Fee >= 1_000_000 {
					normals[fromAddr] = true
					stats[fromAddr].fromStats[amountType] += 1
					stats[fromAddr].fromStats["1e0"] = 1
					stats[toAddr].toStats["1e0"] = 1
					continue
				}

				if preAmount, ok := amounts[fromAddr]; ok {
					diff := result.Amount.Int64() - preAmount
					if diff < 1_000_000 && rex.MatchString(strconv.Itoa(int(diff))) {
						tickers[fromAddr] = true
					}
				}
				amounts[fromAddr] = result.Amount.Int64()

				// Phishing injection
				if amounts[fromAddr] == 6380 {
					normals[fromAddr] = true
					stats[fromAddr].fromStats[amountType] += 1
					stats[toAddr].toStats["1e0"] = 2
					continue
				}

				if len(result.Amount.String()) >= 6 {
					normals[fromAddr] = true
				}

				stats[fromAddr].fromStats[amountType] += 1
				stats[toAddr].toStats[amountType] += 1
			}
			txCount += tx.RowsAffected

			phase := int(txCount / 500_000)
			if _, ok := report[phase]; !ok {
				report[phase] = true
				db.logger.Infof("Counting Phishing TRX Transactions for week [%s], current counting date [%s], counted txs [%d]", week, countingDate, txCount)
			}

			return nil
		})

		if result.Error != nil {
			db.logger.Errorf("Counting Phishing TRX Transactions for week [%s], error [%v]", week, result.Error)
		}

		date, _ := time.Parse("060102", countingDate)
		countingDate = date.AddDate(0, 0, 1).Format("060102")
	}

	db.logger.Infof("Finish counting Phishing TRX Transactions for week [%s], total counted txs [%d]", week, txCount)

	fmt.Printf("PhishSum: %d\n", phishSum)
	fmt.Printf("PhisherMap: %d\n", len(phisherMap))
	for k, v := range phisherMap {
		if usdt[k] > 0 || len(stats[k].amountMap) > 3 {
			fmt.Printf("maybe ")
		}
		fmt.Printf("%s %d %s %d\n", k, v, stats[k].toString(), usdt[k])
	}
	fmt.Printf("VictimsMap: %d\n", len(victimsMap))
	for k, v := range victimsMap {
		fmt.Printf("%s %d\n", k, v)
	}

	var (
		phishers         = make(map[string]bool)
		victims          = make(map[string]bool)
		phishingSum      = make(map[string]int)
		multiPhishingSum = make(map[string]int)
		circleSum        = make(map[string]int)
		collectSum       = make(map[string]int)
		normalSum        = make(map[string]int)
		tickerSum        = make(map[string]int)
		onlyTo           = 0
	)
	fmt.Printf("Stats size: %d\n", len(stats))
	for addr, stat := range stats {
		if _, ok := normals[addr]; ok {
			if stat.getSmallCount() >= 7 && stat.fromStats["1e0"] == 0 && !stat.isCharger() && len(stat.amountMap) <= 3 {
				phishers[addr] = true
				for k, v := range stat.fromStats {
					phishingSum[k] += v
				}
				for k := range stat.toMap {
					victims[k] = true
				}
				fmt.Printf("%s %s [success]\n", addr, stat.toString())
			} else {
				for k, v := range stat.fromStats {
					normalSum[k] += v
				}
			}
			continue
		}

		if _, ok := tickers[addr]; ok {
			for k, v := range stat.fromStats {
				tickerSum[k] += v
			}
			continue
		}

		if len(stat.fromStats) > 0 && len(stat.toStats) == 0 {
			if len(stat.fromStats) == 1 {
				for k, v := range stat.fromStats {
					if v == 1 {
						collectSum[k] += v
					} else if k == "1e1" || k == "1e2" || k == "1e3" {
						phishingSum[k] += v
						phishers[addr] = true
						for k := range stat.toMap {
							victims[k] = true
						}
					} else {
						fmt.Printf("%s %s [only_from] [single]\n", addr, stat.toString())
					}
				}
			} else if len(stat.fromStats) == 2 || len(stat.fromStats) == 3 {
				if stat.getSmallCount() >= 7 {
					for k, v := range stat.fromStats {
						multiPhishingSum[k] += v
						phishers[addr] = true
						for k := range stat.toMap {
							victims[k] = true
						}
					}
				} else {
					fmt.Printf("%s %s [only_from] [multi]\n", addr, stat.toString())
				}
			} else {
				fmt.Printf("%s %s [only_from] [multi]\n", addr, stat.toString())
			}
		} else if len(stat.fromStats) == 0 && len(stat.toStats) > 0 {
			onlyTo += 1
			// fmt.Printf("%s %s [only_to]\n", addr, stat.toString())
		} else {
			if len(stat.fromStats) == 1 && len(stat.toStats) == 1 {
				if stat.toStats["1e0"] == 1 || stat.toStats["1e0"] == 2 {
					if stat.fromStats["1e1"] > 0 {
						phishingSum["1e1"] += stat.fromStats["1e1"]
						phishers[addr] = true
						for k := range stat.toMap {
							victims[k] = true
						}
					} else if stat.fromStats["1e3"] > 0 {
						phishingSum["1e3"] += stat.fromStats["1e3"]
						phishers[addr] = true
						for k := range stat.toMap {
							victims[k] = true
						}
					} else {
						if stat.toStats["1e0"] == 1 {
							fmt.Printf("%s %s [both] [activated]\n", addr, stat.toString())
						} else {
							fmt.Printf("%s %s [both] [injection]\n", addr, stat.toString())
						}
					}
				} else if stat.fromStats["1e1"] > 10 && stat.toStats["1e1"] > 10 {
					circleSum["1e1"] += stat.fromStats["1e1"]
				} else if stat.fromStats["1e2"]+stat.toStats["1e2"] > 10 {
					circleSum["1e2"] += stat.fromStats["1e2"]
				} else {
					fmt.Printf("%s %s [both] [single]\n", addr, stat.toString())
				}
			} else {
				fmt.Printf("%s %s [both]\n", addr, stat.toString())
			}
		}
	}
	fmt.Printf("PhishingSum: %v\n", phishingSum)
	fmt.Printf("MultiPhishingSum: %v\n", multiPhishingSum)
	fmt.Printf("Phisher: %d\n", len(phishers))
	fmt.Printf("Victim: %d\n", len(victims))
	fmt.Printf("CircleSum: %v\n", circleSum)
	fmt.Printf("CollectSum: %v\n", collectSum)
	fmt.Printf("NormalSum: %v\n", normalSum)
	fmt.Printf("TickerSum: %v\n", tickerSum)
	fmt.Printf("OnlyTo: %d\n", onlyTo)

	recountingDate := startDate

	var (
		weekFromMap    = make(map[string]bool)
		weekToMap      = make(map[string]bool)
		weekTotalCount = int64(0)
		weekPhisherMap = make(map[string]bool)
		weekVictimsMap = make(map[string]bool)
		weekPhishCount = int64(0)
	)

	for generateWeek(recountingDate) == week {
		var (
			dayFromMap    = make(map[string]bool)
			dayToMap      = make(map[string]bool)
			dayTotalCount = int64(0)
			dayPhisherMap = make(map[string]bool)
			dayVictimsMap = make(map[string]bool)
			dayPhishCount = int64(0)
			dailyCount    = int64(0)
		)
		_ = db.db.Table("transactions_"+recountingDate).Where("type = ?", 1).FindInBatches(&results, 100, func(tx *gorm.DB, _ int) error {
			for _, result := range results {
				fromAddr := result.FromAddr
				toAddr := result.ToAddr

				dayFromMap[fromAddr] = true
				dayToMap[toAddr] = true
				dayTotalCount += 1

				weekFromMap[fromAddr] = true
				weekToMap[toAddr] = true
				weekTotalCount += 1

				if _, ok := phishers[fromAddr]; ok {
					dayPhisherMap[fromAddr] = true
					dayPhishCount += 1

					weekPhisherMap[fromAddr] = true
					weekPhishCount += 1
				}

				if _, ok := victims[toAddr]; ok {
					dayVictimsMap[toAddr] = true

					weekVictimsMap[toAddr] = true
				}
			}

			dailyCount += tx.RowsAffected

			if dailyCount%1_000_000 == 0 {
				db.logger.Infof("Recounting Phishing TRX Transactions for week [%s], current counting date [%s], counted txs [%d]", week, recountingDate, dailyCount)
			}
			return nil
		})

		fmt.Printf("TRXDaily %s %d %d %s %d %d %s %d %d %s\n", recountingDate,
			len(dayFromMap), len(dayPhisherMap), utils.FormatOfPercent(int64(len(dayFromMap)), int64(len(dayPhisherMap))),
			len(dayToMap), len(dayVictimsMap), utils.FormatOfPercent(int64(len(dayToMap)), int64(len(dayVictimsMap))),
			dayTotalCount, dayPhishCount, utils.FormatOfPercent(dayTotalCount, dayPhishCount))

		date, _ := time.Parse("060102", recountingDate)
		recountingDate = date.AddDate(0, 0, 1).Format("060102")
	}

	fmt.Printf("TRXWeekly %s %d %d %s %d %d %s %d %d %s\n", recountingDate,
		len(weekFromMap), len(weekPhisherMap), utils.FormatOfPercent(int64(len(weekFromMap)), int64(len(weekPhisherMap))),
		len(weekToMap), len(weekVictimsMap), utils.FormatOfPercent(int64(len(weekToMap)), int64(len(weekVictimsMap))),
		weekTotalCount, weekPhishCount, utils.FormatOfPercent(weekTotalCount, weekPhishCount))
}

type USDTStatistic struct {
	transferIn      map[string]int64
	inFingerPoints  map[string]string
	transferOut     map[string]int64
	outFingerPoints map[string]string
}

func newUSDTStatistic() *USDTStatistic {
	return &USDTStatistic{
		transferIn:      make(map[string]int64),
		inFingerPoints:  make(map[string]string),
		transferOut:     make(map[string]int64),
		outFingerPoints: make(map[string]string),
	}
}

func (db *RawDB) countUSDTPhishingForWeek(startDate string) {
	week := generateWeek(startDate)
	db.logger.Infof("Start counting Phishing USDT Transactions for week [%s], start date [%s]", week, startDate)

	var (
		countingDate = startDate
		txCount      = int64(0)
		results      = make([]*models.Transaction, 0)
		report       = make(map[int]bool)

		USDTStats = make(map[string]*USDTStatistic)

		weekFromMap    = make(map[string]bool)
		weekToMap      = make(map[string]bool)
		weekTotalCount = int64(0)

		weekPhisherMap = make(map[string]bool)
		weekVictimsMap = make(map[string]bool)
		weekPhishCount = int64(0)
		weekUSDTCost   = int64(0)

		weekSuccessPhisherMap = make(map[string]bool)
		weekSuccessVictimsMap = make(map[string]bool)
		weekSuccessPhishCount = int64(0)
		weekSuccessAmount     = int64(0)

		weekStat = &models.TokenStatistic{}
	)

	for generateWeek(countingDate) == week {
		var (
			dayFromMap    = make(map[string]bool)
			dayToMap      = make(map[string]bool)
			dayTotalCount = int64(0)

			dayPhisherMap = make(map[string]bool)
			dayVictimsMap = make(map[string]bool)
			dayPhishCount = int64(0)
			dayUSDTCost   = int64(0)

			daySuccessPhisherMap = make(map[string]bool)
			daySuccessVictimsMap = make(map[string]bool)
			daySuccessPhishCount = int64(0)
			daySuccessAmount     = int64(0)

			dayStat = &models.TokenStatistic{}
		)

		result := db.db.Table("transactions_"+countingDate).Where("type = ? or name = ?", 31, "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t").FindInBatches(&results, 100, func(tx *gorm.DB, _ int) error {
			for _, result := range results {
				if len(result.ToAddr) == 0 || result.Result != "SUCCESS" {
					continue
				}

				fromAddr := result.FromAddr
				toAddr := result.ToAddr
				amountStr := result.Amount.String()

				dayFromMap[fromAddr] = true
				dayToMap[toAddr] = true
				dayTotalCount += 1

				weekFromMap[fromAddr] = true
				weekToMap[toAddr] = true
				weekTotalCount += 1

				if _, ok := USDTStats[fromAddr]; !ok {
					USDTStats[fromAddr] = newUSDTStatistic()
				}

				if _, ok := USDTStats[toAddr]; !ok {
					USDTStats[toAddr] = newUSDTStatistic()
				}

				if result.Method == "23b872dd" && amountStr == "0" {
					if isPhishing(toAddr, result.Timestamp, USDTStats[fromAddr], true) {
						dayPhishCount += 1
						dayUSDTCost += result.Amount.Int64()
						dayPhisherMap[toAddr] = true
						dayVictimsMap[fromAddr] = true
						dayStat.Add(result)

						weekPhishCount += 1
						weekUSDTCost += result.Amount.Int64()
						weekVictimsMap[fromAddr] = true
						weekPhisherMap[toAddr] = true
						weekStat.Add(result)

						db.usdtPhishingRelations[fromAddr+toAddr] = true

						imitator := USDTStats[fromAddr].outFingerPoints[getFp(toAddr)]
						db.logger.Infof("Phishing Zero USDT Transfer: date-[%s] victim-[%s] phisher-[%s] imitator-[%s] hash-[%s] amount-[%s]",
							countingDate, fromAddr, toAddr, imitator, result.Hash, amountStr)
					}
					continue
				}

				if isPhishingAmount(amountStr) && (isPhishing(fromAddr, result.Timestamp, USDTStats[toAddr], true) ||
					isPhishing(fromAddr, result.Timestamp, USDTStats[toAddr], false)) {
					dayPhishCount += 1
					dayUSDTCost += result.Amount.Int64()
					dayPhisherMap[fromAddr] = true
					dayVictimsMap[toAddr] = true
					dayStat.Add(result)

					weekPhishCount += 1
					weekUSDTCost += result.Amount.Int64()
					weekPhisherMap[fromAddr] = true
					weekVictimsMap[toAddr] = true
					weekStat.Add(result)

					db.usdtPhishingRelations[toAddr+fromAddr] = true

					fromFp := getFp(fromAddr)
					in := USDTStats[toAddr].inFingerPoints[fromFp]
					out := USDTStats[toAddr].outFingerPoints[fromFp]
					db.logger.Infof("Phishing Normal USDT Transfer: date-[%s] phisher-[%s] victim-[%s] imitated_in-[%s] imitated_out-[%s] hash-[%s] amount-[%f]",
						countingDate, fromAddr, toAddr, in, out, result.Hash, float64(result.Amount.Int64())/1e6)
					continue
				}

				if _, ok := db.usdtPhishingRelations[fromAddr+toAddr]; ok {
					daySuccessPhishCount += 1
					daySuccessVictimsMap[fromAddr] = true
					daySuccessPhisherMap[toAddr] = true
					daySuccessAmount += result.Amount.Int64()

					weekSuccessPhishCount += 1
					weekSuccessVictimsMap[fromAddr] = true
					weekSuccessPhisherMap[toAddr] = true
					weekSuccessAmount += result.Amount.Int64()

					toFp := getFp(toAddr)
					in := USDTStats[fromAddr].inFingerPoints[toFp]
					out := USDTStats[fromAddr].outFingerPoints[toFp]
					db.logger.Infof("Phishing success USDT Transfer: date-[%s] victim-[%s] phisher-[%s] imitated_in-[%s] imitated_out-[%s] hash-[%s] amount-[%f]",
						countingDate, fromAddr, toAddr, in, out, result.Hash, float64(result.Amount.Int64())/1e6)
					db.logger.Infof("Success: %s %s %s %s %f", countingDate, fromAddr, toAddr, result.Hash, float64(result.Amount.Int64())/1e6)
					continue
				}

				// 100 000 000
				if len(amountStr) >= 9 || amountStr == "1000000" || amountStr == "5000000" {
					USDTStats[fromAddr].transferOut[toAddr] = result.Timestamp
					USDTStats[fromAddr].outFingerPoints[getFp(toAddr)] = toAddr
					USDTStats[toAddr].transferIn[fromAddr] = result.Timestamp
					USDTStats[toAddr].inFingerPoints[getFp(fromAddr)] = fromAddr
				}
			}

			txCount += tx.RowsAffected
			phase := int(txCount / 500_000)
			if _, ok := report[phase]; !ok {
				report[phase] = true
				db.logger.Infof("Counting Phishing USDT Transactions for date [%s], current counted txs [%d]", countingDate, txCount)
			}

			return nil
		})

		db.logger.Infof("Finish counting Phishing USDT Transactions for date [%s], total counted txs [%d]", countingDate, result.RowsAffected)
		fmt.Printf("USDTDaily %s %d %d %s %d %d %s %d %d %s - - %d %d %d %d %d %d\n", countingDate,
			len(dayFromMap), len(dayPhisherMap), utils.FormatOfPercent(int64(len(dayFromMap)), int64(len(dayPhisherMap))),
			len(dayToMap), len(dayVictimsMap), utils.FormatOfPercent(int64(len(dayToMap)), int64(len(dayVictimsMap))),
			dayTotalCount, dayPhishCount, utils.FormatOfPercent(dayTotalCount, dayPhishCount),
			dayUSDTCost/1e6, dayStat.Fee, dayStat.EnergyTotal-dayStat.EnergyFee/420,
			daySuccessPhishCount, len(daySuccessVictimsMap), daySuccessAmount/1e6)

		date, _ := time.Parse("060102", countingDate)
		countingDate = date.AddDate(0, 0, 1).Format("060102")
	}

	db.logger.Infof("Finish counting Phishing USDT Transactions for week [%s], total counted txs [%d]", week, txCount)
	fmt.Printf("USDTWeekly %s %d %d %s %d %d %s %d %d %s - - %d %d %d %d %d %d\n", countingDate,
		len(weekFromMap), len(weekPhisherMap), utils.FormatOfPercent(int64(len(weekFromMap)), int64(len(weekPhisherMap))),
		len(weekToMap), len(weekVictimsMap), utils.FormatOfPercent(int64(len(weekToMap)), int64(len(weekVictimsMap))),
		weekTotalCount, weekPhishCount, utils.FormatOfPercent(weekTotalCount, weekPhishCount),
		weekUSDTCost/1e6, weekStat.Fee, weekStat.EnergyTotal-weekStat.EnergyFee/420,
		weekSuccessPhishCount, len(weekSuccessVictimsMap), weekSuccessAmount/1e6)
}

func isPhishingAmount(amount string) bool {
	if len(amount) <= 7 {
		return true
	}

	return false
}

func isPhishing(addr string, ts int64, stat *USDTStatistic, isPhishingOut bool) bool {
	fp := getFp(addr)

	if isPhishingOut {
		if _, ok := stat.outFingerPoints[fp]; !ok {
			return false
		}

		interactedAddr := stat.outFingerPoints[fp]

		gap := ts - stat.transferOut[interactedAddr]

		_, hasIn := stat.transferIn[addr]

		return addr != interactedAddr && gap < 7*24*60*60 && !hasIn
	} else {
		if _, ok := stat.inFingerPoints[fp]; !ok {
			return false
		}

		interactedAddr := stat.inFingerPoints[fp]

		gap := ts - stat.transferIn[interactedAddr]

		_, hasOut := stat.transferOut[addr]

		return addr != interactedAddr && gap < 7*24*60*60 && !hasOut
	}
}

const FpSize = 5

func getFp(addr string) string {
	fp := []byte(addr[34-FpSize:])
	sort.Slice(fp, func(i, j int) bool {
		return fp[i] < fp[j]
	})
	return string(fp)
}

func (db *RawDB) countForWeek(startDate string) string {
	week := generateWeek(startDate)
	db.logger.Infof("Start counting TRX&USDT Transactions for week [%s], start date [%s]", week, startDate)

	var (
		countingDate = startDate
		txCount      = int64(0)
		// results      = make([]*models.Transaction, 0)
		TRXStats  = make(map[string]*models.FungibleTokenStatistic)
		USDTStats = make(map[string]*models.FungibleTokenStatistic)
	)

	for generateWeek(countingDate) == week {
		// result := db.db.Table("transactions_"+countingDate).Where("type = ? or name = ?", 1, "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t").FindInBatches(&results, 100, func(tx *gorm.DB, _ int) error {
		// 	for _, result := range results {
		// 		if len(result.ToAddr) == 0 || result.Result != "SUCCESS" {
		// 			continue
		// 		}
		//
		// 		typeName := fmt.Sprintf("1e%d", len(result.Amount.String()))
		// 		if result.Name == "TRX" {
		// 			if _, ok := TRXStats[typeName]; !ok {
		// 				TRXStats[typeName] = models.NewFungibleTokenStatistic(week, "TRX", typeName, result)
		// 			} else {
		// 				TRXStats[typeName].Add(result)
		// 			}
		// 		} else if len(result.ToAddr) > 0 {
		// 			if _, ok := USDTStats[typeName]; !ok {
		// 				USDTStats[typeName] = models.NewFungibleTokenStatistic(week, "USDT", typeName, result)
		// 			} else {
		// 				USDTStats[typeName].Add(result)
		// 			}
		// 		}
		// 	}
		// 	txCount += tx.RowsAffected
		//
		// 	if txCount%500_000 == 0 {
		// 		db.logger.Infof("Counting TRX&USDT Transactions for week [%s], current counting date [%s], counted txs [%d]", week, countingDate, txCount)
		// 	}
		//
		// 	return nil
		// })
		//
		// if result.Error != nil {
		// 	db.logger.Errorf("Counting TRX&USDT Transactions for week [%s], error [%v]", week, result.Error)
		// }

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
	db.logger.Infof("Finish counting TRX&USDT Transactions for week [%s], total counted txs [%d]", week, txCount)

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
