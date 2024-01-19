package database

import (
	"bufio"
	"errors"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"tron-tracker/database/models"
	"tron-tracker/net"
	"tron-tracker/types"
	"tron-tracker/utils"
)

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
	db *gorm.DB
	el *types.ExchangeList

	lastTrackedDate     string
	lastTrackedBlockNum uint
	isTableMigrated     map[string]bool

	curDate string
	flushCh chan *dbCache
	cache   *dbCache

	loopWG sync.WaitGroup
	quitCh chan struct{}
}

func New() *RawDB {
	dsn := "root:Root1234!@tcp(127.0.0.1:3306)/tron_tracker?charset=utf8mb4&parseTime=True&loc=Local"
	db, dbErr := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if dbErr != nil {
		panic(dbErr)
	}

	dbErr = db.AutoMigrate(&models.ExchangeStatistic{})
	if dbErr != nil {
		panic(dbErr)
	}

	if !db.Migrator().HasTable(&models.Charger{}) {
		dbErr = db.AutoMigrate(&models.Charger{})
		if dbErr != nil {
			panic(dbErr)
		}

		f, err := os.Open("all")
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()

		scanner := bufio.NewScanner(f)

		zap.L().Info("Start loading charge")
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
				db.Create(&chargeToSave)
				chargeToSave = make([]*models.Charger, 0)
			}
			count++
			if count%1000000 == 0 {
				zap.S().Infof("Loaded [%d] charge", count)
			}
		}
		db.Create(&chargeToSave)
		zap.L().Info("Complete loading charge")

		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}
	}

	dbErr = db.AutoMigrate(&models.Meta{})
	if dbErr != nil {
		panic(dbErr)
	}

	var LastTrackedDateMeta models.Meta
	db.Where(models.Meta{Key: models.LastTrackedDateKey}).Attrs(models.Meta{Val: "231102"}).FirstOrCreate(&LastTrackedDateMeta)
	db.Migrator().DropTable("transaction_" + LastTrackedDateMeta.Val)
	db.Migrator().DropTable("transfer_" + LastTrackedDateMeta.Val)

	var LastTrackedBlockNumMeta models.Meta
	db.Where(models.Meta{Key: models.LastTrackedBlockNumKey}).Attrs(models.Meta{Val: "56084338"}).FirstOrCreate(&LastTrackedBlockNumMeta)
	lastTrackedBlockNum, _ := strconv.Atoi(LastTrackedBlockNumMeta.Val)

	return &RawDB{
		db: db,
		el: net.GetExchanges(),

		lastTrackedBlockNum: uint(lastTrackedBlockNum),
		lastTrackedDate:     LastTrackedDateMeta.Val,
		isTableMigrated:     make(map[string]bool),

		flushCh: make(chan *dbCache),
		cache:   newCache(),

		quitCh: make(chan struct{}),
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

func (db *RawDB) GetFromStatisticByDateAndUser(date, user string) *models.UserStatistic {
	var userStatistic models.UserStatistic
	db.db.Table("from_stats_"+date).Where("address = ?", user).Limit(1).Find(&userStatistic)
	return &userStatistic
}

func (db *RawDB) GetExchangeStatisticsByDate(date string) []*models.ExchangeStatistic {
	var exchangeStatistic []*models.ExchangeStatistic
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

func (db *RawDB) GetCachedChargesByAddr(addr string) []string {
	charges := make([]string, 0)
	for _, charger := range db.cache.chargers {
		if charger.ExchangeAddress == addr {
			charges = append(charges, charger.Address)
		}
	}
	return charges
}

func (db *RawDB) GetTotalStatisticsByDate(date string) *models.UserStatistic {
	var totalStatistic models.UserStatistic
	db.db.Table("from_stats_" + date).Where("address = total").Limit(1).Find(&totalStatistic)
	return &totalStatistic

}

func (db *RawDB) SetLastTrackedBlock(block *types.Block) {
	if block.BlockHeader.RawData.Number%100 == 0 {
		db.el = net.GetExchanges()
	}

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

	zap.S().Debugf("Updated last tracked block num [%d]", db.lastTrackedBlockNum)
}

func (db *RawDB) SaveTransactions(transactions *[]models.Transaction) {
	if transactions == nil || len(*transactions) == 0 {
		return
	}

	dbName := "transactions_" + db.curDate
	db.createTableIfNotExist(dbName, models.Transaction{})
	db.db.Table(dbName).Create(transactions)
}

func (db *RawDB) SaveTransfers(transfers *[]models.TRC20Transfer) {
	if transfers == nil || len(*transfers) == 0 {
		return
	}

	dbName := "transfers_" + db.curDate
	db.createTableIfNotExist(dbName, models.TRC20Transfer{})
	db.db.Table(dbName).Create(transfers)
}

func (db *RawDB) UpdateToStatistic(to string, tx *models.Transaction) {
	db.cache.date = generateDate(tx.Timestamp)
	if stats, ok := db.cache.toStats[to]; ok {
		stats.Add(tx)
	} else {
		db.cache.toStats[to] = models.NewUserStatistic(to, tx)
	}
}

func (db *RawDB) UpdateFromStatistic(tx *models.Transaction) {
	db.updateFromStatistic(tx.FromAddr, tx)
	db.updateFromStatistic("total", tx)

	if len(tx.Name) > 0 && tx.Name != "_" {
		db.updateFromStatistic(tx.Name, tx)
	}
}

func (db *RawDB) updateFromStatistic(user string, tx *models.Transaction) {
	db.cache.date = generateDate(tx.Timestamp)
	if stats, ok := db.cache.fromStats[user]; ok {
		stats.Add(tx)
	} else {
		db.cache.fromStats[user] = models.NewUserStatistic(user, tx)
	}
}

func (db *RawDB) SaveCharger(from, to string) {
	if _, ok := db.cache.chargers[from]; !ok && !db.el.Contains(from) && db.el.Contains(to) {
		db.cache.chargers[from] = &models.Charger{
			Address:         from,
			ExchangeName:    db.el.Get(to).Name,
			ExchangeAddress: to,
		}
	}
}

func (db *RawDB) CheckCharger(address string, exchange types.Exchange) {
	if charger, ok := db.cache.chargers[address]; ok {
		// If charger interact with other address which is not its exchange address
		// Check if the other address is the same exchange
		// Otherwise, this charger is not a real charger
		if !utils.IsSameExchange(charger.ExchangeName, exchange.Name) {
		}
	}
}

func (db *RawDB) Run() {
	for {
		select {
		case <-db.quitCh:
			zap.L().Info("rawdb closed")
			db.loopWG.Done()
			return
		case cache := <-db.flushCh:
			db.persist(cache)
		}
	}
}

func (db *RawDB) persist(cache *dbCache) {
	if len(cache.fromStats) == 0 && len(cache.toStats) == 0 && len(cache.chargers) == 0 {
		return
	}

	zap.S().Infof("Start persisting cache for date [%s]", cache.date)

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
			zap.L().Info(reportContent)
		}
	}
	db.db.Table(fromDBName).Create(&statsToPersist)

	zap.S().Info(reporter.Finish("Complete saving from statistic for date " + cache.date + ", total count [%d], cost [%.2fs], avg speed [%.2frecords/sec]"))

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
			zap.L().Info(reportContent)
		}
	}
	db.db.Table(toDBName).Create(&statsToPersist)

	zap.S().Info(reporter.Finish("Complete saving to statistic for date " + cache.date + ", total count [%d], cost [%.2fs], avg speed [%.2frecords/sec]"))

	reporter = utils.NewReporter(0, 60*time.Second, "Saved [%d] charge in [%.2fs], speed [%.2frecords/sec]")

	for _, charger := range cache.chargers {
		db.db.Where(models.Charger{Address: charger.Address}).FirstOrCreate(&charger)
		if shouldReport, reportContent := reporter.Add(1); shouldReport {
			zap.L().Info(reportContent)
		}
	}

	zap.S().Info(reporter.Finish("Complete saving charge for date " + cache.date + ", total count [%d], cost [%.2fs], avg speed [%.2frecords/sec]"))

	zap.S().Info("Start updating exchange statistic")

	exchangeStats := make(map[string]*models.ExchangeStatistic)
	for _, exchange := range db.el.Exchanges {
		exchangeStats[exchange.Address] = &models.ExchangeStatistic{
			Date:    cache.date,
			Name:    exchange.Name,
			Address: exchange.Address,
		}

		// 归集统计
		if collectStat, ok := cache.toStats[exchange.Address]; ok {
			exchangeStats[exchange.Address].CollectTxCount += collectStat.TXTotal
			exchangeStats[exchange.Address].CollectFee += collectStat.Fee
			exchangeStats[exchange.Address].CollectNetFee += collectStat.NetFee
			exchangeStats[exchange.Address].CollectNetUsage += collectStat.NetUsage
			exchangeStats[exchange.Address].CollectEnergyFee += collectStat.EnergyFee
			exchangeStats[exchange.Address].CollectEnergyUsage += collectStat.EnergyUsage + collectStat.EnergyOriginUsage
		}

		// 提币统计
		if withdrawStat, ok := cache.fromStats[exchange.Address]; ok {
			exchangeStats[exchange.Address].WithdrawTxCount += withdrawStat.TXTotal
			exchangeStats[exchange.Address].WithdrawFee += withdrawStat.Fee
			exchangeStats[exchange.Address].WithdrawNetFee += withdrawStat.NetFee
			exchangeStats[exchange.Address].WithdrawNetUsage += withdrawStat.NetUsage
			exchangeStats[exchange.Address].WithdrawEnergyFee += withdrawStat.EnergyFee
			exchangeStats[exchange.Address].WithdrawEnergyUsage += withdrawStat.EnergyUsage + withdrawStat.EnergyOriginUsage
		}
	}

	for _, toStat := range cache.toStats {
		if ok, charger := db.isCharger(toStat.Address); ok {
			exchangeStats[charger.ExchangeAddress].ChargeTxCount += toStat.TXTotal
			exchangeStats[charger.ExchangeAddress].ChargeFee += toStat.Fee
			exchangeStats[charger.ExchangeAddress].ChargeNetFee += toStat.NetFee
			exchangeStats[charger.ExchangeAddress].ChargeNetUsage += toStat.NetUsage
			exchangeStats[charger.ExchangeAddress].ChargeEnergyFee += toStat.EnergyFee
			exchangeStats[charger.ExchangeAddress].ChargeEnergyUsage += toStat.EnergyUsage + toStat.EnergyOriginUsage
		}
	}

	for _, statistic := range exchangeStats {
		db.db.Create(statistic)
	}

	zap.S().Info("Complete updating exchange statistic")
}

func (db *RawDB) isCharger(address string) (bool, *models.Charger) {
	if charger, ok := db.cache.chargers[address]; ok {
		return true, charger
	}

	charger := models.Charger{}
	result := db.db.Where("address = ?", address).First(&charger)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return false, nil
	}
	return true, &charger
}

func (db *RawDB) createTableIfNotExist(tableName string, model interface{}) {
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
