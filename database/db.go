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
	date      string
	fromStats map[string]*models.UserStatistic
	toStats   map[string]*models.UserStatistic
	chargers  map[string]*models.Charger
}

func newCache() *dbCache {
	return &dbCache{
		fromStats: make(map[string]*models.UserStatistic),
		toStats:   make(map[string]*models.UserStatistic),
		chargers:  make(map[string]*models.Charger),
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

func (db *RawDB) GetFromStatisticByDateAndUser(date, user string) models.UserStatistic {
	var userStatistic models.UserStatistic
	db.db.Table("from_stats_"+date).Where("address = ?", user).Limit(1).Find(&userStatistic)
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
		db.updateUserStatistic(tx.Name, tx, db.cache.fromStats)
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
			db.logger.Info("No exchange found, retry %d times", i+1)
			continue
		}

		db.el = el
		db.elUpdatedAt = time.Now()
	}
}

func (db *RawDB) persist(cache *dbCache) {
	if len(cache.fromStats) == 0 && len(cache.toStats) == 0 && len(cache.chargers) == 0 {
		return
	}

	db.logger.Infof("Start persisting cache for date [%s]", cache.date)

	fromDBName := "from_stats_" + cache.date
	db.createTableIfNotExist(fromDBName, models.UserStatistic{})

	reporter := utils.NewReporter(0, 60*time.Second, "Saved [%d] from statistic in [%.2fs], speed [%.2frecords/sec]")

	statsToPersist := make([]*models.UserStatistic, 0)
	for _, stats := range cache.fromStats {
		statsToPersist = append(statsToPersist, stats)
		if len(statsToPersist) == 200 {
			db.db.Table(fromDBName).Create(&statsToPersist)
			statsToPersist = make([]*models.UserStatistic, 0)
		}
		if shouldReport, reportContent := reporter.Add(1); shouldReport {
			db.logger.Info(reportContent)
		}
	}
	db.db.Table(fromDBName).Create(&statsToPersist)

	db.logger.Info(reporter.Finish("Complete saving from statistic for date " + cache.date + ", total count [%d], cost [%.2fs], avg speed [%.2frecords/sec]"))

	toDBName := "to_stats_" + cache.date
	db.createTableIfNotExist(toDBName, models.UserStatistic{})

	reporter = utils.NewReporter(0, 60*time.Second, "Saved [%d] to statistic in [%.2fs], speed [%.2frecords/sec]")

	statsToPersist = make([]*models.UserStatistic, 0)
	for _, stats := range cache.toStats {
		statsToPersist = append(statsToPersist, stats)
		if len(statsToPersist) == 200 {
			db.db.Table(toDBName).Create(&statsToPersist)
			statsToPersist = make([]*models.UserStatistic, 0)
		}
		if shouldReport, reportContent := reporter.Add(1); shouldReport {
			db.logger.Info(reportContent)
		}
	}
	db.db.Table(toDBName).Create(&statsToPersist)

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
