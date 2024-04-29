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
	Host        string   `toml:"host"`
	DB          string   `toml:"db"`
	User        string   `toml:"user"`
	Password    string   `toml:"password"`
	StartDate   string   `toml:"start_date"`
	StartNum    int      `toml:"start_num"`
	ValidTokens []string `toml:"valid_tokens"`
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
	vt          map[string]bool

	lastTrackedDate      string
	lastTrackedBlockNum  uint
	lastTrackedBlockTime int64
	isTableMigrated      map[string]bool
	tableLock            sync.Mutex
	statsLock            sync.Mutex
	chargers             map[string]*models.Charger
	chargersToSave       map[string]*models.Charger

	curDate string
	flushCh chan *dbCache
	cache   *dbCache
	statsCh chan struct{}

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

	dbErr = db.AutoMigrate(&models.Meta{})
	if dbErr != nil {
		panic(dbErr)
	}

	var LastTrackedDateMeta models.Meta
	db.Where(models.Meta{Key: models.LastTrackedDateKey}).Attrs(models.Meta{Val: config.StartDate}).FirstOrCreate(&LastTrackedDateMeta)
	db.Migrator().DropTable("transactions_" + LastTrackedDateMeta.Val)
	db.Migrator().DropTable("transfers_" + LastTrackedDateMeta.Val)

	var LastTrackedBlockNumMeta models.Meta
	db.Where(models.Meta{Key: models.LastTrackedBlockNumKey}).Attrs(models.Meta{Val: strconv.Itoa(config.StartNum)}).FirstOrCreate(&LastTrackedBlockNumMeta)
	lastTrackedBlockNum, _ := strconv.Atoi(LastTrackedBlockNumMeta.Val)

	validTokens := make(map[string]bool)
	for _, token := range config.ValidTokens {
		validTokens[token] = true
	}
	// TRX token symbol is "TRX"
	validTokens["TRX"] = true

	rawDB := &RawDB{
		db:          db,
		el:          net.GetExchanges(),
		elUpdatedAt: time.Now(),
		vt:          validTokens,

		lastTrackedDate:     LastTrackedDateMeta.Val,
		lastTrackedBlockNum: uint(lastTrackedBlockNum),
		isTableMigrated:     make(map[string]bool),
		chargers:            make(map[string]*models.Charger),
		chargersToSave:      make(map[string]*models.Charger),

		flushCh: make(chan *dbCache),
		cache:   newCache(),

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
			if i%100_000 == 0 {
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
			Address:         cols[0],
			ExchangeName:    cols[1],
			ExchangeAddress: cols[2],
		})
		if len(chargeToSave) == 1000 {
			db.db.Create(&chargeToSave)
			for _, charger := range chargeToSave {
				db.chargers[charger.Address] = charger
			}
			chargeToSave = make([]*models.Charger, 0)
		}
		count++
		if count%100_000 == 0 {
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
	db.loopWG.Add(1)
	go db.Run()
}

func (db *RawDB) Close() {
	db.quitCh <- struct{}{}
	db.loopWG.Wait()

	db.flushChargerToDB()

	underDB, _ := db.db.DB()
	_ = underDB.Close()
}

func (db *RawDB) GetLastTrackedBlockNum() uint {
	return db.lastTrackedBlockNum
}

func (db *RawDB) GetLastTrackedBlockTime() int64 {
	return db.lastTrackedBlockTime
}

func (db *RawDB) GetFromStatisticByDateAndDays(date time.Time, days int) map[string]*models.UserStatistic {
	userStatisticMap := make(map[string]*models.UserStatistic)

	for i := 0; i < days; i++ {
		dayStatistics := make([]*models.UserStatistic, 0)
		db.db.Table("from_stats_" + date.AddDate(0, 0, i).Format("060102")).Find(&dayStatistics)

		for _, stats := range dayStatistics {
			user := stats.Address

			if _, ok := userStatisticMap[user]; !ok {
				userStatisticMap[user] = stats
			} else {
				userStatisticMap[user].Merge(stats)
			}
		}
	}

	return userStatisticMap
}

func (db *RawDB) GetFromStatisticByDateAndUserAndDays(date time.Time, user string, days int) models.UserStatistic {
	var userStatistic models.UserStatistic
	userStatistic.Address = user

	for i := 0; i < days; i++ {
		dayStatistic := models.UserStatistic{}
		db.db.Table("from_stats_"+date.AddDate(0, 0, i).Format("060102")).Where("address = ?", user).Limit(1).Find(&dayStatistic)
		userStatistic.Merge(&dayStatistic)
	}

	return userStatistic
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
	var totalStatistic models.UserStatistic
	db.db.Table("from_stats_"+date).Where("address = ?", "total").Limit(1).Find(&totalStatistic)
	return totalStatistic
}

func (db *RawDB) GetTokenStatisticsByDate(date, token string) models.TokenStatistic {
	var tokenStatistic models.TokenStatistic
	db.db.Table("token_stats_"+date).Where("address = ?", token).Limit(1).Find(&tokenStatistic)
	return tokenStatistic
}

func (db *RawDB) GetTokenStatisticByDateAndTokenAndDays(date time.Time, token string, days int) models.TokenStatistic {
	var tokenStatistic models.TokenStatistic
	tokenStatistic.Address = token

	for i := 0; i < days; i++ {
		dayStatistic := models.TokenStatistic{}
		db.db.Table("token_stats_"+date.AddDate(0, 0, i).Format("060102")).Where("address = ?", token).Limit(1).Find(&dayStatistic)
		tokenStatistic.Merge(&dayStatistic)
	}

	return tokenStatistic
}

func (db *RawDB) GetExchangeTotalStatisticsByDate(date string) []models.ExchangeStatistic {
	return db.GetExchangeStatisticsByDateAndToken(date, "_")
}

func (db *RawDB) GetExchangeStatisticsByDateAndToken(date, token string) []models.ExchangeStatistic {
	var exchangeStatistic []models.ExchangeStatistic
	db.db.Where("date = ? and token = ?", date, token).Find(&exchangeStatistic)
	return exchangeStatistic
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

	nextDate := generateDate(block.BlockHeader.RawData.Timestamp)
	if db.curDate == "" {
		db.curDate = nextDate
	} else if db.curDate != nextDate {
		db.curDate = nextDate
		db.flushCh <- db.cache
		db.cache = newCache()

		db.db.Model(&models.Meta{}).Where(models.Meta{Key: models.LastTrackedDateKey}).Update("val", nextDate)
		db.db.Model(&models.Meta{}).Where(models.Meta{Key: models.LastTrackedBlockNumKey}).Update("val", strconv.Itoa(int(block.BlockHeader.RawData.Number)))
	}

	db.lastTrackedBlockNum = block.BlockHeader.RawData.Number
	db.lastTrackedBlockTime = block.BlockHeader.RawData.Timestamp
}

func (db *RawDB) SaveTransactions(transactions []*models.Transaction) {
	if transactions == nil || len(transactions) == 0 {
		return
	}

	dbName := "transactions_" + db.curDate
	db.createTableIfNotExist(dbName, models.Transaction{})
	db.db.Table(dbName).Create(transactions)
}

func (db *RawDB) UpdateStatistics(tx *models.Transaction) {
	db.updateUserStatistic(tx.FromAddr, tx, db.cache.fromStats)
	db.updateUserStatistic(tx.ToAddr, tx, db.cache.toStats)
	db.updateUserStatistic("total", tx, db.cache.fromStats)

	if len(tx.Name) > 0 && tx.Name != "_" {
		db.updateTokenStatistic(tx.Name, tx, db.cache.tokenStats)
	}

	if len(tx.Name) > 0 {
		db.updateUserTokenStatistic(tx, db.cache.userTokenStats)
	}
}

func (db *RawDB) updateUserStatistic(user string, tx *models.Transaction, stats map[string]*models.UserStatistic) {
	db.cache.date = generateDate(tx.Timestamp)
	if stat, ok := stats[user]; ok {
		stat.Add(tx)
	} else {
		stats[user] = models.NewUserStatistic(user, tx)
	}
}

func (db *RawDB) updateTokenStatistic(name string, tx *models.Transaction, stats map[string]*models.TokenStatistic) {
	if stat, ok := stats[name]; ok {
		stat.Add(tx)
	} else {
		stats[name] = models.NewTokenStatistic(name, tx)
	}
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
	if !db.vt[token] {
		return
	}

	if charger, ok := db.chargers[from]; !ok && !db.el.Contains(from) && db.el.Contains(to) {
		db.chargers[from] = &models.Charger{
			Address:         from,
			ExchangeName:    db.el.Get(to).Name,
			ExchangeAddress: to,
		}

		db.chargersToSave[from] = db.chargers[from]
	} else if ok && to != charger.ExchangeAddress && !charger.IsFake {
		// If charger interact with other address which is not its exchange address
		// Check if the other address is backup address or the same exchange
		// Otherwise, this charger is not a real charger
		if len(charger.BackupAddress) == 0 {
			charger.BackupAddress = to
			db.chargersToSave[from] = charger
			db.logger.Infof("Set charger [%s] backup address to [%s]", from, to)
		} else if to != charger.BackupAddress && !utils.IsSameExchange(charger.ExchangeName, db.el.Get(to).Name) {
			charger.IsFake = true
			db.chargersToSave[from] = charger
			db.logger.Infof("Set charger [%s] as fake charger", from)
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
	count := 0
	var (
		totalFee    int64
		totalEnergy int64
	)
	lastMonday := now.With(date).BeginningOfWeek().AddDate(0, 0, -6)
	db.logger.Infof("Last Monday: %s", lastMonday.Format("2006-01-02"))

	users := make([]string, 0)
	for scanner.Scan() {
		user := scanner.Text()

		// If user is exchange address, skip it
		if db.el.Contains(user) {
			continue
		}

		users = append(users, user)

		if len(users) == 100 {
			for i := 0; i < 7; i++ {
				date := lastMonday.AddDate(0, 0, i).Format("060102")
				fee, energy := db.GetFeeAndEnergyByDateAndUsers(date, users)
				totalFee += fee
				totalEnergy += energy
			}
			users = make([]string, 0)
		}

		count += 1
		if count%100_000 == 0 {
			db.logger.Infof("Counted [%d] user fee, current total fee [%d]", count, totalFee)
		}
	}

	// Count the rest users
	if len(users) > 0 {
		for i := 0; i < 7; i++ {
			date := lastMonday.AddDate(0, 0, i).Format("060102")
			fee, energy := db.GetFeeAndEnergyByDateAndUsers(date, users)
			totalFee += fee
			totalEnergy += energy
		}
	}

	statsResultFile.WriteString(strconv.FormatInt(totalFee, 10) + "\n")
	statsResultFile.WriteString(strconv.FormatInt(totalEnergy, 10) + "\n")
}

func (db *RawDB) Run() {
	for {
		select {
		case <-db.quitCh:
			db.logger.Info("rawdb closed")
			db.loopWG.Done()
			return
		case cache := <-db.flushCh:
			db.flushCacheToDB(cache)
		}
	}
}

func (db *RawDB) updateExchanges() {
	if time.Now().Sub(db.elUpdatedAt) < 10*time.Minute {
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

				if _, ok := exchangeStats[exchange.Address]; !ok {
					exchangeStats[exchange.Address] = make(map[string]*models.ExchangeStatistic)
					exchangeStats[exchange.Address]["_"] = &models.ExchangeStatistic{
						Date:    cache.date,
						Name:    exchange.Name,
						Address: exchange.Address,
						Token:   "_",
					}
				}

				exchangeStats[exchange.Address]["_"].AddCollect(stats)
				exchangeStats[exchange.Address]["_"].AddWithdraw(stats)

				if _, ok := exchangeStats[exchange.Address][stats.Token]; !ok {
					exchangeStats[exchange.Address][stats.Token] = &models.ExchangeStatistic{
						Date:    cache.date,
						Name:    exchange.Name,
						Address: exchange.Address,
						Token:   stats.Token,
					}
				}

				exchangeStats[exchange.Address][stats.Token].AddCollect(stats)
				exchangeStats[exchange.Address][stats.Token].AddWithdraw(stats)
			} else {
				if charger, ok := db.chargers[stats.User]; ok {
					if charger.IsFake {
						continue
					}

					if db.el.Contains(charger.Address) {
						charger.IsFake = true
						db.db.Delete(charger)
						db.logger.Infof("Delete existed exchange charger [%s](%s) related to [%s](%s)",
							charger.Address, db.el.Get(charger.Address).Name, charger.ExchangeAddress, charger.ExchangeName)
						continue
					}

					if _, ok := exchangeStats[charger.ExchangeAddress]; !ok {
						exchangeStats[charger.ExchangeAddress] = make(map[string]*models.ExchangeStatistic)
						exchangeStats[charger.ExchangeAddress]["_"] = &models.ExchangeStatistic{
							Date:    cache.date,
							Name:    charger.ExchangeName,
							Address: charger.ExchangeAddress,
							Token:   "_",
						}
					}

					exchangeStats[charger.ExchangeAddress]["_"].AddCharge(stats)

					if _, ok := exchangeStats[charger.ExchangeAddress][stats.Token]; !ok {
						exchangeStats[charger.ExchangeAddress][stats.Token] = &models.ExchangeStatistic{
							Date:    cache.date,
							Name:    charger.ExchangeName,
							Address: charger.ExchangeAddress,
							Token:   stats.Token,
						}
					}

					exchangeStats[charger.ExchangeAddress][stats.Token].AddCharge(stats)
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

func makeReporterFunc(name string) func(utils.ReporterState) string {
	return func(rs utils.ReporterState) string {
		return fmt.Sprintf("Saved [%d] [%s] in [%.2fs], speed [%.2frecords/sec], left [%d/%d] records to save [%.2f%%]",
			rs.CountInc, name, rs.ElapsedTime, float64(rs.CountInc)/rs.ElapsedTime,
			rs.CurrentCount, rs.FinishCount, float64(rs.CurrentCount*100)/float64(rs.FinishCount))
	}
}
