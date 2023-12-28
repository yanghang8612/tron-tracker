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

type chargerStatistic struct {
	txCount     uint
	netFee      uint
	netUsage    uint
	energyFee   uint
	energyUsage uint
	otherFee    uint
}

type dbCache struct {
	date      string
	userStats map[string]*models.UserStatistic
	chargers  map[string]*models.Charger
	toStats   map[string]*chargerStatistic
}

func newCache() *dbCache {
	return &dbCache{
		userStats: make(map[string]*models.UserStatistic),
		chargers:  make(map[string]*models.Charger),
		toStats:   make(map[string]*chargerStatistic),
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

func (db *RawDB) GetUserStatistic(date, user string) *models.UserStatistic {
	var userStatistic models.UserStatistic
	db.db.Table("stats_"+date).Where("address = ?", user).Limit(1).Find(&userStatistic)
	return &userStatistic
}

func (db *RawDB) GetExchangeStatistic(date string) []models.ExchangeStatistic {
	var exchangeStatistic []models.ExchangeStatistic
	db.db.Where("date = ?", date).Find(&exchangeStatistic)
	return exchangeStatistic
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

func (db *RawDB) SaveChargeEnergyConsumption(to string, tx *models.Transaction) {
	if _, ok := db.cache.toStats[to]; !ok {
		db.cache.toStats[to] = &chargerStatistic{
			txCount:     1,
			netFee:      tx.NetFee,
			netUsage:    tx.NetUsage,
			energyFee:   tx.EnergyFee,
			energyUsage: tx.EnergyUsage + tx.EnergyOriginUsage,
			otherFee:    tx.Fee - tx.EnergyFee - tx.NetFee,
		}
	} else {
		db.cache.toStats[to].txCount += 1
		db.cache.toStats[to].netFee += tx.NetFee
		db.cache.toStats[to].netUsage += tx.NetUsage
		db.cache.toStats[to].energyFee += tx.EnergyFee
		db.cache.toStats[to].energyUsage += tx.EnergyUsage + tx.EnergyOriginUsage
		db.cache.toStats[to].otherFee += tx.Fee - tx.EnergyFee - tx.NetFee
	}
}

func (db *RawDB) UpdateStatistic(tx *models.Transaction) {
	db.updateStatistic(tx.Owner, tx)
	db.updateStatistic("total", tx)
}

func (db *RawDB) updateStatistic(owner string, tx *models.Transaction) {
	db.cache.date = generateDate(tx.Timestamp)
	if ownerStats, ok := db.cache.userStats[owner]; ok {
		ownerStats.Add(tx)
	} else {
		db.cache.userStats[owner] = models.NewUserStatistic(owner, tx)
	}
}

func (db *RawDB) SaveCharger(address string, exchange types.Exchange) {
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
	if len(cache.userStats) == 0 && len(cache.chargers) == 0 {
		return
	}

	zap.S().Infof("Start persisting cache for date [%s]", cache.date)

	dbName := "stats_" + cache.date
	db.createTableIfNotExist(dbName, models.UserStatistic{})

	reporter := utils.NewReporter(0, 3*time.Second, "Saved [%d] user statistic in [%.2fs], speed [%.2frecords/sec]")

	statsToPersist := make([]*models.UserStatistic, 0)
	for _, stats := range cache.userStats {
		statsToPersist = append(statsToPersist, stats)
		if len(statsToPersist) == 1000 {
			db.db.Table(dbName).Create(&statsToPersist)
			statsToPersist = make([]*models.UserStatistic, 0)
		}
		if shouldReport, reportContent := reporter.Add(1); shouldReport {
			zap.L().Info(reportContent)
		}
	}
	db.db.Table(dbName).Create(&statsToPersist)

	zap.S().Info(reporter.Finish("Complete saving user statistic for date " + cache.date + ", total count [%d], cost [%.2fs], avg speed [%.2frecords/sec]"))

	reporter = utils.NewReporter(0, 3*time.Second, "Saved [%d] charge in [%.2fs], speed [%.2frecords/sec]")

	for _, charger := range cache.chargers {
		db.db.Where(models.Charger{Address: charger.Address}).FirstOrCreate(&charger)
		if shouldReport, reportContent := reporter.Add(1); shouldReport {
			zap.L().Info(reportContent)
		}
	}

	zap.S().Info(reporter.Finish("Complete saving charge for date " + cache.date + ", total count [%d], cost [%.2fs], avg speed [%.2frecords/sec]"))

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
			exchangeStats[charger.ExchangeAddress].ChargeTxCount += chargeStatistic.txCount
			exchangeStats[charger.ExchangeAddress].ChargeNetFee += chargeStatistic.netFee
			exchangeStats[charger.ExchangeAddress].ChargeNetUsage += chargeStatistic.netUsage
			exchangeStats[charger.ExchangeAddress].ChargeEnergyFee += chargeStatistic.energyFee
			exchangeStats[charger.ExchangeAddress].ChargeEnergyUsage += chargeStatistic.energyUsage
			exchangeStats[charger.ExchangeAddress].ChargeOtherFee += chargeStatistic.otherFee
		}

		// 归集统计
		if collectStats, ok := cache.userStats[address]; ok {
			exchangeStats[charger.ExchangeAddress].CollectTxCount += collectStats.TransactionTotal
			exchangeStats[charger.ExchangeAddress].CollectNetFee += collectStats.NetFee
			exchangeStats[charger.ExchangeAddress].CollectNetUsage += collectStats.NetUsage
			exchangeStats[charger.ExchangeAddress].CollectEnergyFee += collectStats.EnergyFee
			exchangeStats[charger.ExchangeAddress].CollectEnergyUsage += collectStats.EnergyUsage + collectStats.EnergyOriginUsage
			exchangeStats[charger.ExchangeAddress].CollectOtherFee += collectStats.OtherFee
		}
	}
	for address := range exchangeStats {
		// 提币统计
		if withdrawStats, ok := cache.userStats[address]; ok {
			exchangeStats[address].WithdrawTxCount += withdrawStats.TransactionTotal
			exchangeStats[address].WithdrawNetFee += withdrawStats.NetFee
			exchangeStats[address].WithdrawNetUsage += withdrawStats.NetUsage
			exchangeStats[address].WithdrawEnergyFee += withdrawStats.EnergyFee
			exchangeStats[address].WithdrawEnergyUsage += withdrawStats.EnergyUsage + withdrawStats.EnergyOriginUsage
			exchangeStats[address].WithdrawOtherFee += withdrawStats.OtherFee
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
