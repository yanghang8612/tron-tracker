package database

import (
	"errors"
	"strconv"
	"time"

	"go.uber.org/zap"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"tron-tracker/database/models"
)

type RawDB struct {
	db *gorm.DB

	lastTrackedBlockNum uint
	isTableMigrated     map[string]bool
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
	}
}

func (db *RawDB) GetLastTrackedBlockNum() uint {
	return db.lastTrackedBlockNum
}

func (db *RawDB) SetLastTrackedBlockNum(blockNum uint) {
	db.lastTrackedBlockNum = blockNum
	db.db.Model(&models.Meta{}).Where(models.Meta{Key: models.LastTrackedBlockNumKey}).Update("val", strconv.Itoa(int(blockNum)))
	zap.S().Debugf("Updated last tracked block num [%d]", blockNum)
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

func generateDate(blockTimestamp int64) string {
	return time.Unix(blockTimestamp, 0).Add(-8 * time.Hour).Format("060102")
}

func (db *RawDB) UpdateStats(tx *models.Transaction) {
	db.updateStats(tx.Owner, tx)
	db.updateStats("total", tx)
}

func (db *RawDB) updateStats(owner string, tx *models.Transaction) {
	date := time.Unix(tx.Timestamp, 0).Add(-8 * time.Hour).Truncate(24 * time.Hour)

	var ownerStats models.Stats
	result := db.db.Where(&models.Stats{Date: &date, Owner: owner}).Limit(1).Find(&ownerStats)
	ownerStats.Date = &date
	ownerStats.Owner = tx.Owner
	ownerStats.EnergyTotal += tx.EnergyTotal
	ownerStats.EnergyFee += tx.EnergyFee
	ownerStats.EnergyUsage += tx.EnergyUsage
	ownerStats.EnergyOriginUsage += tx.EnergyOriginUsage
	ownerStats.NetUsage += tx.NetUsage
	ownerStats.NetFee += tx.NetFee
	ownerStats.TransactionTotal++
	switch tx.Type {
	case 1:
		ownerStats.TRXTotal++
	case 2:
		ownerStats.TRC10Total++
	case 30, 31:
		ownerStats.SCTotal++
	}
	if errors.Is(result.Error, gorm.ErrRecordNotFound) || result.RowsAffected == 0 {
		db.db.Create(&ownerStats)
	} else {
		db.db.Model(&ownerStats).Updates(ownerStats)
	}
}
