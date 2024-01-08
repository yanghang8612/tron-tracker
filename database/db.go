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

	if db.Migrator().HasTable(&models.Charger{}) {
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
	db.Where(models.Meta{Key: models.LastTrackedDateKey}).Attrs(models.Meta{Val: "231210"}).FirstOrCreate(&LastTrackedDateMeta)
	db.Migrator().DropTable("transaction_" + LastTrackedDateMeta.Val)
	db.Migrator().DropTable("transfer_" + LastTrackedDateMeta.Val)

	var LastTrackedBlockNumMeta models.Meta
	db.Where(models.Meta{Key: models.LastTrackedBlockNumKey}).Attrs(models.Meta{Val: "57200000"}).FirstOrCreate(&LastTrackedBlockNumMeta)
	lastTrackedBlockNum, _ := strconv.Atoi(LastTrackedBlockNumMeta.Val)

	return &RawDB{
		db:                  db,
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

func (db *RawDB) GetChargers() map[string]*models.Charger {
	return db.cache.chargers
}

func (db *RawDB) GetUserFromStatistic(date, user string) *models.UserStatistic {
	var userStatistic models.UserStatistic
	db.db.Table("from_stats_"+date).Where("address = ?", user).Limit(1).Find(&userStatistic)
	return &userStatistic
}

func (db *RawDB) GetExchangeStatistic(date string) []models.ExchangeStatistic {
	var exchangeStatistic []models.ExchangeStatistic
	db.db.Where("date = ?", date).Find(&exchangeStatistic)
	return exchangeStatistic
}

func (db *RawDB) GetSpecialStatistic(date, addr string) uint {
	res := struct {
		Sum uint
	}{}
	db.db.Table("transactions_"+date).
		Select("SUM(fee)").
		Where("hash IN <?>",
			db.db.Table("transfers_"+date).
				Distinct("hash").
				Where("to_addr = ? and token = ?", addr, "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t")).
		Find(&res)
	return res.Sum
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

func (db *RawDB) SaveCharger(address string, exchange types.Exchange, tx models.Transaction) {
	if _, ok := db.cache.chargers[address]; !ok {
		db.cache.chargers[address] = &models.Charger{
			Address:         address,
			ExchangeName:    exchange.Name,
			ExchangeAddress: exchange.Address,
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

	// reporter = utils.NewReporter(0, 60*time.Second, "Saved [%d] charge in [%.2fs], speed [%.2frecords/sec]")

	// for _, charger := range cache.chargers {
	// 	db.db.Where(models.Charger{Address: charger.Address}).FirstOrCreate(&charger)
	// 	if shouldReport, reportContent := reporter.Add(1); shouldReport {
	// 		zap.L().Info(reportContent)
	// 	}
	// }

	// zap.S().Info(reporter.Finish("Complete saving charge for date " + cache.date + ", total count [%d], cost [%.2fs], avg speed [%.2frecords/sec]"))

	zap.S().Info("Start updating exchange statistic")

	exchangeStats := make(map[string]*models.ExchangeStatistic)
	for address, charger := range cache.chargers {
		if _, ok := exchangeStats[charger.ExchangeAddress]; !ok {
			exchangeStats[charger.ExchangeAddress] = &models.ExchangeStatistic{
				Date:    cache.date,
				Name:    charger.ExchangeName,
				Address: charger.ExchangeAddress,
			}
		}

		// 充币统计
		if chargeStatistic, ok := cache.toStats[address]; ok {
			exchangeStats[charger.ExchangeAddress].ChargeTxCount += chargeStatistic.TXTotal
			exchangeStats[charger.ExchangeAddress].ChargeFee += chargeStatistic.Fee
			exchangeStats[charger.ExchangeAddress].ChargeNetFee += chargeStatistic.NetFee
			exchangeStats[charger.ExchangeAddress].ChargeNetUsage += chargeStatistic.NetUsage
			exchangeStats[charger.ExchangeAddress].ChargeEnergyFee += chargeStatistic.EnergyFee
			exchangeStats[charger.ExchangeAddress].ChargeEnergyUsage += chargeStatistic.EnergyUsage + chargeStatistic.EnergyOriginUsage
		}

		// 归集统计
		if collectStats, ok := cache.fromStats[address]; ok {
			exchangeStats[charger.ExchangeAddress].CollectTxCount += collectStats.TXTotal
			exchangeStats[charger.ExchangeAddress].CollectFee += collectStats.Fee
			exchangeStats[charger.ExchangeAddress].CollectNetFee += collectStats.NetFee
			exchangeStats[charger.ExchangeAddress].CollectNetUsage += collectStats.NetUsage
			exchangeStats[charger.ExchangeAddress].CollectEnergyFee += collectStats.EnergyFee
			exchangeStats[charger.ExchangeAddress].CollectEnergyUsage += collectStats.EnergyUsage + collectStats.EnergyOriginUsage
		}
	}
	for address := range exchangeStats {
		// 提币统计
		if withdrawStats, ok := cache.fromStats[address]; ok {
			exchangeStats[address].WithdrawTxCount += withdrawStats.TXTotal
			exchangeStats[address].WithdrawFee += withdrawStats.Fee
			exchangeStats[address].WithdrawNetFee += withdrawStats.NetFee
			exchangeStats[address].WithdrawNetUsage += withdrawStats.NetUsage
			exchangeStats[address].WithdrawEnergyFee += withdrawStats.EnergyFee
			exchangeStats[address].WithdrawEnergyUsage += withdrawStats.EnergyUsage + withdrawStats.EnergyOriginUsage
		}

		db.db.Create(exchangeStats[address])
	}

	zap.S().Info("Complete updating exchange statistic")
}

func (db *RawDB) isCharger(address string) bool {
	if _, ok := db.cache.chargers[address]; ok {
		return true
	}
	result := db.db.Where("address = ?", address).First(&models.Charger{})
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return false
	}
	return true
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
