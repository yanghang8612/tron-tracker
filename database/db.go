package database

import (
	"bufio"
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
	date   string
	stats  map[string]*models.Statistic
	charge map[string]*models.Charge
}

func newCache() *dbCache {
	return &dbCache{
		stats:  make(map[string]*models.Statistic),
		charge: make(map[string]*models.Charge),
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

	if !db.Migrator().HasTable(&models.Charge{}) {
		dbErr = db.AutoMigrate(&models.Charge{})
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
		var chargeToSave []*models.Charge
		for scanner.Scan() {
			line := scanner.Text()
			cols := strings.Split(line, ",")
			chargeToSave = append(chargeToSave, &models.Charge{
				Address:         cols[0],
				ExchangeName:    cols[1],
				ExchangeAddress: cols[2],
			})
			if len(chargeToSave) == 1000 {
				db.Create(&chargeToSave)
				chargeToSave = make([]*models.Charge, 0)
			}
			count++
			if count%100000 == 0 {
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

func (db *RawDB) UpdateStats(tx *models.Transaction) {
	db.updateStats(tx.Owner, tx)
	db.updateStats("total", tx)
}

func (db *RawDB) updateStats(owner string, tx *models.Transaction) {
	db.cache.date = generateDate(tx.Timestamp)
	if ownerStats, ok := db.cache.stats[owner]; ok {
		ownerStats.Add(tx)
	} else {
		db.cache.stats[owner] = models.NewStats(owner, tx)
	}
}

func (db *RawDB) SaveCharge(address string, exchange types.Exchange) {
	if _, ok := db.cache.charge[address]; !ok {
		db.cache.charge[address] = &models.Charge{
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
	if len(cache.stats) == 0 && len(cache.charge) == 0 {
		return
	}

	zap.S().Infof("Start persisting cache for date [%s]", cache.date)

	dbName := "stats_" + cache.date
	db.createTableIfNotExist(dbName, models.Statistic{})

	reporter := utils.NewReporter(0, 3*time.Second, "Saved [%d] stats in [%.2fs], speed [%.2frecords/sec]")

	statsToPersist := make([]*models.Statistic, 0)
	for _, stats := range cache.stats {
		statsToPersist = append(statsToPersist, stats)
		if len(statsToPersist) == 1000 {
			db.db.Table(dbName).Create(&statsToPersist)
			statsToPersist = make([]*models.Statistic, 0)
		}
		if shouldReport, reportContent := reporter.Add(1); shouldReport {
			zap.L().Info(reportContent)
		}
	}
	db.db.Table(dbName).Create(&statsToPersist)

	zap.S().Info(reporter.Finish("Complete saving charge for date " + cache.date + ", total count [%d], cost [%.2fs], avg speed [%.2frecords/sec]"))

	reporter = utils.NewReporter(0, 3*time.Second, "Saved [%d] charge in [%.2fs], speed [%.2frecords/sec]")

	for _, charge := range cache.charge {
		db.db.Where(models.Charge{Address: charge.Address}).FirstOrCreate(&charge)
		if shouldReport, reportContent := reporter.Add(1); shouldReport {
			zap.L().Info(reportContent)
		}
	}
	db.db.Table(dbName).Create(&statsToPersist)

	zap.S().Info(reporter.Finish("Complete saving charge for date " + cache.date + ", total count [%d], cost [%.2fs], avg speed [%.2frecords/sec]"))
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
