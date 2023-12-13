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
	"tron-tracker/utils"
)

type statsSet struct {
	isComplete bool
	date       string
	val        map[string]*models.Stats
}

type RawDB struct {
	db *gorm.DB

	lastTrackedBlockNum uint
	isTableMigrated     map[string]bool

	curDate    string
	statsLock  sync.Mutex
	statsCh    chan *statsSet
	statsCache *statsSet
	statsAll   map[string]map[string]*models.Stats

	loopWG sync.WaitGroup
	quitCh chan struct{}
}

func New() *RawDB {
	dsn := "root:Root1234!@tcp(127.0.0.1:3306)/tron_tracker?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	err = db.AutoMigrate(&models.Meta{})
	if err != nil {
		panic(err)
	}

	var LastTrackedBlockNumMeta models.Meta
	db.Where(models.Meta{Key: models.LastTrackedBlockNumKey}).Attrs(models.Meta{Val: "57264000"}).FirstOrCreate(&LastTrackedBlockNumMeta)

	lastTrackedBlockNum, _ := strconv.Atoi(LastTrackedBlockNumMeta.Val)
	return &RawDB{
		db:                  db,
		lastTrackedBlockNum: uint(lastTrackedBlockNum),
		isTableMigrated:     make(map[string]bool),

		statsCh: make(chan *statsSet),
		statsCache: &statsSet{
			val: make(map[string]*models.Stats),
		},
		statsAll: make(map[string]map[string]*models.Stats),

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
		db.statsCache = &statsSet{
			val: make(map[string]*models.Stats),
		}
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
	db.createTableIfNotExist(dbName, models.Transaction{})

	db.db.Table(dbName).Create(transactions)
}

func (db *RawDB) SaveTransfers(transfers *[]models.TRC20Transfer) {
	if transfers == nil || len(*transfers) == 0 {
		return
	}

	dbName := "transfer_" + db.curDate
	db.createTableIfNotExist(dbName, models.TRC20Transfer{})
	db.db.Table(dbName).Create(transfers)
}

func (db *RawDB) UpdateStats(tx *models.Transaction) {
	db.updateStats(tx.Owner, tx)
	db.updateStats("total", tx)
}

func (db *RawDB) updateStats(owner string, tx *models.Transaction) {
	db.statsCache.date = generateDate(tx.Timestamp)
	if ownerStats, ok := db.statsCache.val[owner]; ok {
		ownerStats.Add(tx)
	} else {
		db.statsCache.val[owner] = models.NewStats(owner, tx)
	}

	// if len(db.statsCache.val) == 3000 {
	// 	db.statsCh <- db.statsCache
	// 	db.statsCache = &statsSet{
	// 		val: make(map[string]*models.Stats),
	// 	}
	// }
}

func (db *RawDB) Run() {
	for {
		select {
		case <-db.quitCh:
			zap.L().Info("DB quit, start saving the stats cache")
			// db.saveStats(db.statsCache)
			zap.L().Info("DB quit, complete saving the stats cache")
			db.loopWG.Done()
			return
		case ss := <-db.statsCh:
			startTime := time.Now()
			db.saveStats(ss)
			elapsedTime := time.Since(startTime).Seconds()
			zap.S().Infof("Complete saving stats, cost [%.2fs], avg speed [%.2frecords/sec]", elapsedTime, float64(len(ss.val))/elapsedTime)
		}
	}
}

func (db *RawDB) saveStats(ss *statsSet) {
	if len(ss.val) == 0 {
		return
	}

	zap.S().Infof("Start saving stats for date [%s]", ss.date)

	dbName := "stats_" + ss.date
	db.createTableIfNotExist(dbName, models.Stats{})
	// if _, ok := db.statsAll[ss.date]; !ok {
	// 	db.statsAll[ss.date] = make(map[string]*models.Stats)
	// }
	// statsAll := db.statsAll[ss.date]

	reporter := utils.NewReporter(0, 3*time.Second, "Saved [%d] stats in [%.2fs], speed [%.2frecords/sec]")
	statsToCreate := make([]*models.Stats, 0)
	for _, stats := range ss.val {

		// if preStats, ok := statsAll[stats.Owner]; ok {
		// 	preStats.Merge(stats)
		// 	db.db.Table(dbName).Model(&preStats).Updates(&preStats)
		// } else {
		// 	var ownerStats models.Stats
		// 	result := db.db.Table(dbName).Where(&models.Stats{Owner: stats.Owner}).Limit(1).Find(&ownerStats)
		// 	if errors.Is(result.Error, gorm.ErrRecordNotFound) || result.RowsAffected == 0 {
		// 		statsToCreate = append(statsToCreate, stats)
		// 	} else {
		// 		ownerStats.Merge(stats)
		// 		statsAll[stats.Owner] = &ownerStats
		// 	}
		// }

		statsToCreate = append(statsToCreate, stats)
		if len(statsToCreate) == 1000 {
			db.db.Table(dbName).Create(&statsToCreate)
			statsToCreate = make([]*models.Stats, 0)
		}
		if shouldReport, reportContent := reporter.Add(1); shouldReport {
			zap.L().Info(reportContent)
		}
	}
	db.db.Table(dbName).Create(&statsToCreate)
	// for _, stats := range statsToCreate {
	// 	statsAll[stats.Owner] = stats
	// }

	zap.S().Info(reporter.Finish("Complete saving stats for date " + ss.date + ", total count [%d], cost [%.2fs], avg speed [%.2frecords/sec]"))

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
