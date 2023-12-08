package database

import (
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"tron-tracker/database/models"
	"tron-tracker/types"
)

type RawDB struct {
	db *gorm.DB

	lastTrackedBlockNum uint
	isTableMigrated     map[string]bool

	preDate    string
	curDate    string
	statsLock  sync.Mutex
	statsCh    chan map[string]*models.Stats
	statsCache map[string]*models.Stats

	loopWG sync.WaitGroup
	quitCh chan struct{}
}

func New() *RawDB {
	dsn := "root@tcp(127.0.0.1:3306)/tron_tracker?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	err = db.AutoMigrate(&models.Meta{})
	if err != nil {
		panic(err)
	}

	var LastTrackedBlockNumMeta models.Meta
	db.Where(models.Meta{Key: models.LastTrackedBlockNumKey}).Attrs(models.Meta{Val: "56084338"}).FirstOrCreate(&LastTrackedBlockNumMeta)

	lastTrackedBlockNum, _ := strconv.Atoi(LastTrackedBlockNumMeta.Val)
	return &RawDB{
		db:                  db,
		lastTrackedBlockNum: uint(lastTrackedBlockNum),
		isTableMigrated:     make(map[string]bool),

		preDate:    "",
		statsCh:    make(chan map[string]*models.Stats),
		statsCache: make(map[string]*models.Stats),

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
		db.statsCh <- db.statsCache
		db.statsCache = make(map[string]*models.Stats)
	}

	db.lastTrackedBlockNum = block.BlockHeader.RawData.Number
	db.db.Model(&models.Meta{}).Where(models.Meta{Key: models.LastTrackedBlockNumKey}).Update("val", strconv.Itoa(int(db.lastTrackedBlockNum)))

	zap.S().Debugf("Updated last tracked block num [%d]", db.lastTrackedBlockNum)
}

func (db *RawDB) SaveTransactions(transactions *[]models.Transaction) {
	if transactions == nil || len(*transactions) == 0 {
		return
	}

	dbName := "transaction_" + db.curDate
	if ok, _ := db.isTableMigrated[dbName]; !ok {
		err := db.db.Table(dbName).AutoMigrate(&models.Transaction{})
		if err != nil {
			panic(err)
		}

		db.isTableMigrated[dbName] = true
	}

	db.db.Table(dbName).Create(transactions)
}

func (db *RawDB) SaveTransfers(transfers *[]models.TRC20Transfer) {
	if transfers == nil || len(*transfers) == 0 {
		return
	}

	dbName := "transfer_" + db.curDate
	if ok, _ := db.isTableMigrated[dbName]; !ok {
		err := db.db.Table(dbName).AutoMigrate(&models.TRC20Transfer{})
		if err != nil {
			panic(err)
		}

		db.isTableMigrated[dbName] = true
	}

	db.db.Table(dbName).Create(transfers)
}

func (db *RawDB) UpdateStats(tx *models.Transaction) {
	db.updateStats(tx.Owner, tx)
	db.updateStats("total", tx)
}

func (db *RawDB) updateStats(owner string, tx *models.Transaction) {
	if ownerStats, ok := db.statsCache[owner]; ok {
		ownerStats.Add(tx)
	} else {
		db.statsCache[owner] = models.NewStats(owner, tx)
	}

	if len(db.statsCache) == 1000 {
		db.statsCh <- db.statsCache
		db.statsCache = make(map[string]*models.Stats)
	}
}

func (db *RawDB) Run() {
	for {
		select {
		case <-db.quitCh:
			zap.L().Info("DB quit, start saving the stats cache")
			db.saveStats(db.statsCache)
			zap.L().Info("DB quit, complete saving the stats cache")
			db.loopWG.Done()
			return
		case statsToBeProcessed := <-db.statsCh:
			startTime := time.Now()
			db.saveStats(statsToBeProcessed)
			elapsedTime := time.Since(startTime).Seconds()
			zap.S().Infof("Complete saving stats, cost [%.2fs], avg speed [%.2frecords/sec]", elapsedTime, float64(len(statsToBeProcessed))/elapsedTime)
		}
	}
}

func (db *RawDB) saveStats(statsToBeSaved map[string]*models.Stats) {
	if len(statsToBeSaved) == 0 {
		return
	}

	var date string
	for _, stats := range statsToBeSaved {
		date = generateDate(stats.Date)
		break
	}
	// zap.S().Infof("Start saving stats for date [%s]", date)

	dbName := "stats_" + date
	err := db.db.Table(dbName).AutoMigrate(&models.Stats{})
	if err != nil {
		panic(err)
	}

	// reporter := utils.NewReporter(0, 3*time.Second, "Saved [%d] stats in [%ds], speed [%.2frecords/sec]")
	for owner, stats := range statsToBeSaved {
		var ownerStats models.Stats
		db.db.Table(dbName).Where(&models.Stats{Date: stats.Date, Owner: owner}).Limit(1).Find(&ownerStats)
		ownerStats.Date = stats.Date
		ownerStats.Owner = stats.Owner
		ownerStats.Merge(stats)
		db.db.Table(dbName).Save(&ownerStats)
		// if errors.Is(result.Error, gorm.ErrRecordNotFound) || result.RowsAffected == 0 {
		// 	db.db.Table(dbName).Create(stats)
		// } else {
		// 	db.db.Table(dbName).Model(stats).Updates(stats)
		// }
		// db.db.Table(dbName).Create(stats)
		// if shouldReport, reportContent := reporter.Add(1); shouldReport {
		// 	zap.L().Info(reportContent)
		// }
	}

	// zap.S().Info(reporter.Finish("Complete saving stats for date " + date + ", total count [%d], cost [%ds], avg speed [%.2frecords/sec]"))

}

func generateDate(ts int64) string {
	return time.Unix(ts, 0).Add(-8 * time.Hour).Format("060102")
}
