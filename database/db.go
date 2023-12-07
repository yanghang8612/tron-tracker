package database

import (
	"errors"
	"strconv"
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
	statsCh    chan string
	statsCache map[string]map[string]*models.Stats
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

	err = db.AutoMigrate(&models.Stats{})
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
		statsCh:    make(chan string),
		statsCache: make(map[string]map[string]*models.Stats),
	}
}

func (db *RawDB) GetLastTrackedBlockNum() uint {
	return db.lastTrackedBlockNum
}

func (db *RawDB) SetLastTrackedBlock(block *types.Block) {
	nextDate := generateDate(block.BlockHeader.RawData.Timestamp)
	if db.preDate != "" && db.preDate != nextDate {
		db.statsCh <- db.preDate
	}
	db.preDate = nextDate

	db.lastTrackedBlockNum = block.BlockHeader.RawData.Number
	db.db.Model(&models.Meta{}).Where(models.Meta{Key: models.LastTrackedBlockNumKey}).Update("val", strconv.Itoa(int(db.lastTrackedBlockNum)))

	zap.S().Debugf("Updated last tracked block num [%d]", db.lastTrackedBlockNum)
}

func (db *RawDB) SaveTransactions(transactions *[]models.Transaction) {
	if transactions == nil || len(*transactions) == 0 {
		return
	}

	dbName := "transaction_" + generateDate((*transactions)[0].Timestamp)

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

	dbName := "transfer_" + generateDate((*transfers)[0].Timestamp)
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
	date := generateDate(tx.Timestamp)

	if _, ok := db.statsCache[date]; !ok {
		db.statsCache[date] = make(map[string]*models.Stats)
	}

	if ownerStats, ok := db.statsCache[date][owner]; ok {
		ownerStats.Add(tx)
	} else {
		db.statsCache[date][owner] = models.NewStats(owner, tx)
	}
}

func (db *RawDB) Run() {
	for {
		select {
		case date := <-db.statsCh:
			zap.S().Infof("Start saving stats for date [%s], total user [%d]", date, len(db.statsCache[date]))
			count := 0
			for owner, stats := range db.statsCache[date] {
				var ownerStats models.Stats
				result := db.db.Where(&models.Stats{Date: stats.Date, Owner: owner}).Limit(1).Find(&ownerStats)
				ownerStats.Merge(stats)
				if errors.Is(result.Error, gorm.ErrRecordNotFound) || result.RowsAffected == 0 {
					db.db.Create(&ownerStats)
				} else {
					db.db.Model(&ownerStats).Updates(ownerStats)
				}
				count += 1
				if count%1000 == 0 {
					zap.S().Infof("Saved stats for date [%s], count [%d]", date, count)
				}
			}
		}
	}
}

func generateDate(ts int64) string {
	return time.Unix(ts, 0).Add(-8 * time.Hour).Format("060102")
}
