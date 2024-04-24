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
	date       string
	fromStats  map[string]*models.UserStatistic
	toStats    map[string]*models.UserStatistic
	tokenStats map[string]*models.TokenStatistic
	chargers   map[string]*models.Charger
}

func newCache() *dbCache {
	return &dbCache{
		fromStats:  make(map[string]*models.UserStatistic),
		toStats:    make(map[string]*models.UserStatistic),
		tokenStats: make(map[string]*models.TokenStatistic),
		chargers:   make(map[string]*models.Charger),
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

	curDate string
	flushCh chan *dbCache
	cache   *dbCache

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
	db.Migrator().DropTable("transaction_" + LastTrackedDateMeta.Val)
	db.Migrator().DropTable("transfer_" + LastTrackedDateMeta.Val)

	var LastTrackedBlockNumMeta models.Meta
	db.Where(models.Meta{Key: models.LastTrackedBlockNumKey}).Attrs(models.Meta{Val: strconv.Itoa(config.StartNum)}).FirstOrCreate(&LastTrackedBlockNumMeta)
	lastTrackedBlockNum, _ := strconv.Atoi(LastTrackedBlockNumMeta.Val)

	validTokens := make(map[string]bool)
	for _, token := range config.ValidTokens {
		validTokens[token] = true
	}
	// TRX token symbol is "_"
	validTokens["_"] = true

	rawDB := &RawDB{
		db:          db,
		el:          net.GetExchanges(),
		elUpdatedAt: time.Now(),
		vt:          validTokens,

		lastTrackedDate:     LastTrackedDateMeta.Val,
		lastTrackedBlockNum: uint(lastTrackedBlockNum),
		isTableMigrated:     make(map[string]bool),

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

	db.logger.Info("Start loading charge")
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
			chargeToSave = make([]*models.Charger, 0)
		}
		count++
		if count%1000000 == 0 {
			db.logger.Infof("Loaded [%d] charge", count)
		}
	}
	db.db.Create(&chargeToSave)
	db.logger.Info("Complete loading charge")

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

func (db *RawDB) GetFromStatisticByDateAndUserAndDays(date, user string, days int) models.UserStatistic {
	var userStatistic models.UserStatistic

	for i := 0; i < days; i++ {
		dayStatistic := models.UserStatistic{}
		db.db.Table("from_stats_"+date).Where("address = ?", user).Limit(1).Find(&dayStatistic)
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

func (db *RawDB) GetExchangeStatisticsByDate(date string) []models.ExchangeStatistic {
	var exchangeStatistic []models.ExchangeStatistic
	db.db.Where("date = ?", date).Find(&exchangeStatistic)
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

func (db *RawDB) GetCachedChargesByAddr(addr string) []*models.Charger {
	charges := make([]*models.Charger, 0)
	for _, charger := range db.cache.chargers {
		if charger.ExchangeAddress == addr {
			charges = append(charges, charger)
		}
	}
	return charges
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

func (db *RawDB) SaveTransfers(transfers []*models.TRC20Transfer) {
	if transfers == nil || len(transfers) == 0 {
		return
	}

	dbName := "transfers_" + db.curDate
	db.createTableIfNotExist(dbName, models.TRC20Transfer{})
	db.db.Table(dbName).Create(transfers)
}

func (db *RawDB) UpdateUserStatistic(tx *models.Transaction) {
	db.updateUserStatistic(tx.FromAddr, tx, db.cache.fromStats)
	db.updateUserStatistic("total", tx, db.cache.fromStats)

	if len(tx.Name) > 0 && tx.Name != "_" {
		db.updateTokenStatistic(tx.Name, tx, db.cache.tokenStats)
	}

	db.updateUserStatistic(tx.ToAddr, tx, db.cache.toStats)
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

func (db *RawDB) SaveCharger(from, to, token, txHash string) {
	// Filter invalid token charger
	if !db.vt[token] {
		return
	}

	if charger, ok := db.cache.chargers[from]; !ok && !db.el.Contains(from) && db.el.Contains(to) {
		db.cache.chargers[from] = &models.Charger{
			Address:         from,
			ExchangeName:    db.el.Get(to).Name,
			ExchangeAddress: to,
		}
	} else if ok && !charger.IsFake {
		// If charger interact with other address which is not its exchange address
		// Check if the other address is backup address or the same exchange
		// Otherwise, this charger is not a real charger
		if len(charger.BackupAddress) == 0 {
			charger.BackupAddress = to
		} else if charger.BackupAddress != to && !utils.IsSameExchange(charger.ExchangeName, db.el.Get(to).Name) {
			db.logger.Infof("Set charger [%s] as fake charger caused by tx - [%s]", from, txHash)
			charger.IsFake = true
		}
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
		if count%10000 == 0 {
			db.logger.Infof("Counted [%d] user fee, current total fee [%d]", count, totalFee)
		}
	}

	statsResultFile.WriteString(strconv.FormatInt(totalFee, 10) + "\n")
	statsResultFile.WriteString(strconv.FormatInt(totalEnergy, 10))
}

func (db *RawDB) Run() {
	for {
		select {
		case <-db.quitCh:
			db.logger.Info("rawdb closed")
			db.loopWG.Done()
			return
		case cache := <-db.flushCh:
			db.persist(cache)
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
			continue
		}

		db.logger.Infof("Update exchange list successfully, exchange list size [%d] => [%d]", len(db.el.Exchanges), len(el.Exchanges))
		db.el = el
		db.elUpdatedAt = time.Now()
	}
}

func (db *RawDB) persist(cache *dbCache) {
	if len(cache.fromStats) == 0 && len(cache.toStats) == 0 && len(cache.chargers) == 0 {
		return
	}

	db.logger.Infof("Start persisting cache for date [%s]", cache.date)

	fromStatsDBName := "from_stats_" + cache.date
	db.createTableIfNotExist(fromStatsDBName, models.UserStatistic{})

	reporter := utils.NewReporter(0, 60*time.Second, "Saved [%d] from statistic in [%.2fs], speed [%.2frecords/sec]")

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

	db.logger.Info(reporter.Finish("Complete saving from statistic for date " + cache.date + ", total count [%d], cost [%.2fs], avg speed [%.2frecords/sec]"))

	toStatsDBName := "to_stats_" + cache.date
	db.createTableIfNotExist(toStatsDBName, models.UserStatistic{})

	reporter = utils.NewReporter(0, 60*time.Second, "Saved [%d] to statistic in [%.2fs], speed [%.2frecords/sec]")

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

	db.logger.Info(reporter.Finish("Complete saving to statistic for date " + cache.date + ", total count [%d], cost [%.2fs], avg speed [%.2frecords/sec]"))

	tokenStatsDBName := "token_stats_" + cache.date
	db.createTableIfNotExist(tokenStatsDBName, models.TokenStatistic{})

	reporter = utils.NewReporter(0, 60*time.Second, "Saved [%d] token statistic in [%.2fs], speed [%.2frecords/sec]")

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

	db.logger.Info(reporter.Finish("Complete saving to statistic for date " + cache.date + ", total count [%d], cost [%.2fs], avg speed [%.2frecords/sec]"))

	reporter = utils.NewReporter(0, 60*time.Second, "Saved [%d] charger in [%.2fs], speed [%.2frecords/sec]")

	for _, charger := range cache.chargers {
		if charger.IsFake {
			charger = &models.Charger{}
			result := db.db.Where("address = ?", charger.Address).First(charger)
			if !errors.Is(result.Error, gorm.ErrRecordNotFound) {
				// If charger is fake, should delete it from database
				db.db.Delete(charger)
				db.logger.Infof("Delete existed fake charger [%s] for exchange - [%s](%s)", charger.Address, charger.ExchangeAddress, charger.ExchangeName)
			}
		} else {
			db.db.Where(models.Charger{Address: charger.Address}).FirstOrCreate(charger)
		}

		if shouldReport, reportContent := reporter.Add(1); shouldReport {
			db.logger.Info(reportContent)
		}
	}

	db.logger.Info(reporter.Finish("Complete saving charger for date " + cache.date + ", total count [%d], cost [%.2fs], avg speed [%.2frecords/sec]"))

	db.logger.Info("Start updating exchange statistic")

	exchangeStats := make(map[string]*models.ExchangeStatistic)
	for _, exchange := range db.el.Exchanges {
		exchangeStats[exchange.Address] = &models.ExchangeStatistic{
			Date:    cache.date,
			Name:    exchange.Name,
			Address: exchange.Address,
		}

		// 归集统计
		if collectStat, ok := cache.toStats[exchange.Address]; ok {
			exchangeStats[exchange.Address].CollectTxCount += collectStat.TRXTotal - collectStat.SmallTRXTotal + collectStat.USDTTotal - collectStat.SmallUSDTTotal
			exchangeStats[exchange.Address].CollectFee += collectStat.Fee
			exchangeStats[exchange.Address].CollectNetFee += collectStat.NetFee
			exchangeStats[exchange.Address].CollectNetUsage += collectStat.NetUsage
			exchangeStats[exchange.Address].CollectEnergyTotal += collectStat.EnergyTotal
			exchangeStats[exchange.Address].CollectEnergyFee += collectStat.EnergyFee
			exchangeStats[exchange.Address].CollectEnergyUsage += collectStat.EnergyUsage + collectStat.EnergyOriginUsage
		}

		// 提币统计
		if withdrawStat, ok := cache.fromStats[exchange.Address]; ok {
			exchangeStats[exchange.Address].WithdrawTxCount += withdrawStat.TRXTotal - withdrawStat.SmallTRXTotal + withdrawStat.USDTTotal - withdrawStat.SmallUSDTTotal
			exchangeStats[exchange.Address].WithdrawFee += withdrawStat.Fee
			exchangeStats[exchange.Address].WithdrawNetFee += withdrawStat.NetFee
			exchangeStats[exchange.Address].WithdrawNetUsage += withdrawStat.NetUsage
			exchangeStats[exchange.Address].WithdrawEnergyTotal += withdrawStat.EnergyTotal
			exchangeStats[exchange.Address].WithdrawEnergyFee += withdrawStat.EnergyFee
			exchangeStats[exchange.Address].WithdrawEnergyUsage += withdrawStat.EnergyUsage + withdrawStat.EnergyOriginUsage
		}
	}

	for _, toStat := range cache.toStats {
		charger, ok := cache.chargers[toStat.Address]
		if ok && charger.IsFake {
			db.logger.Infof("Skip fake charger [%s] for exchange - [%s](%s)", charger.Address, charger.ExchangeAddress, charger.ExchangeName)
			continue
		}

		if !ok {
			charger = &models.Charger{}
			result := db.db.Where("address = ?", toStat.Address).First(charger)
			ok = !errors.Is(result.Error, gorm.ErrRecordNotFound)
		}

		if ok {
			if db.el.Contains(charger.Address) {
				db.db.Delete(charger)
				db.logger.Infof("Delete existed exchange charger [%s](%s) related to [%s](%s)", charger.Address, db.el.Get(charger.Address).Name, charger.ExchangeAddress, charger.ExchangeName)
				continue
			}

			exchangeStats[charger.ExchangeAddress].ChargeTxCount += toStat.TRXTotal - toStat.SmallTRXTotal + toStat.USDTTotal - toStat.SmallUSDTTotal
			exchangeStats[charger.ExchangeAddress].ChargeFee += toStat.Fee
			exchangeStats[charger.ExchangeAddress].ChargeNetFee += toStat.NetFee
			exchangeStats[charger.ExchangeAddress].ChargeNetUsage += toStat.NetUsage
			exchangeStats[charger.ExchangeAddress].ChargeEnergyTotal += toStat.EnergyTotal
			exchangeStats[charger.ExchangeAddress].ChargeEnergyFee += toStat.EnergyFee
			exchangeStats[charger.ExchangeAddress].ChargeEnergyUsage += toStat.EnergyUsage + toStat.EnergyOriginUsage
		}
	}

	for _, statistic := range exchangeStats {
		db.db.Create(statistic)
	}

	db.logger.Info("Complete updating exchange statistic")
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
