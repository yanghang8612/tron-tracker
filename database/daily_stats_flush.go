package database

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"hash/maphash"
	"os"
	"strings"
	"time"

	"gorm.io/gorm"

	"tron-tracker/database/models"
)

type dbCache struct {
	fromStats      map[string]*models.UserStatistic
	toStats        map[string]*models.UserStatistic
	tokenStats     map[string]*models.TokenStatistic
	userTokenStats map[string]*models.UserTokenStatistic
}

func newCache() *dbCache {
	return &dbCache{
		fromStats:      make(map[string]*models.UserStatistic),
		toStats:        make(map[string]*models.UserStatistic),
		tokenStats:     make(map[string]*models.TokenStatistic),
		userTokenStats: make(map[string]*models.UserTokenStatistic),
	}
}

func (cache *dbCache) accumulate(mask dailyStatMask, tx *models.Transaction) {
	if mask&statOwner != 0 {
		if cache.fromStats[tx.OwnerAddr] == nil {
			cache.fromStats[tx.OwnerAddr] = models.NewUserStatistic(tx.OwnerAddr)
		}
		cache.fromStats[tx.OwnerAddr].Add(tx)
	}
	if mask&statRecipient != 0 {
		if cache.toStats[tx.ToAddr] == nil {
			cache.toStats[tx.ToAddr] = models.NewUserStatistic(tx.ToAddr)
		}
		cache.toStats[tx.ToAddr].Add(tx)
	}
	if mask&statToken != 0 {
		if cache.tokenStats[tx.Name] == nil {
			cache.tokenStats[tx.Name] = &models.TokenStatistic{Address: tx.Name}
		}
		cache.tokenStats[tx.Name].Add(tx)
	}
	if mask&statTokenSender != 0 {
		cache.userToken(tx.FromAddr, tx.Name).AddFrom(tx)
	}
	if mask&statTokenRecipient != 0 {
		cache.userToken(tx.ToAddr, tx.Name).AddTo(tx)
	}
}

func (cache *dbCache) userToken(address, token string) *models.UserTokenStatistic {
	key := address + token
	stat := cache.userTokenStats[key]
	if stat == nil {
		stat = models.NewUserTokenStatistic(address, token)
		cache.userTokenStats[key] = stat
	}
	return stat
}

type dailyStatTotals struct {
	fee   *models.FeeStatistic
	from  *models.UserStatistic
	token *models.TokenStatistic
}

func (totals *dailyStatTotals) add(tx *models.Transaction) {
	if totals.from == nil {
		totals.from = models.NewUserStatistic("total")
	}
	totals.from.Add(tx)
	if tx.OwnerAddr == "total" {
		totals.from.Add(tx) // Same key as the synthetic total in the old cache.
	}
	if tx.Name != "" {
		if totals.token == nil {
			totals.token = &models.TokenStatistic{Address: "total"}
		}
		totals.token.Add(tx)
		if tx.Name == "total" {
			totals.token.Add(tx)
		}
	}
	if tx.Fee > 0 {
		totals.fee.Add(tx)
	}
}

func createInBatches[T any](gdb *gorm.DB, table string, items map[string]*T) error {
	const batch = 200
	buf := make([]*T, 0, min(len(items), batch))
	for _, item := range items {
		buf = append(buf, item)
		if len(buf) == batch {
			if err := gdb.Table(table).Create(buf).Error; err != nil {
				return err
			}
			buf = buf[:0]
		}
	}
	if len(buf) > 0 {
		return gdb.Table(table).Create(buf).Error
	}
	return nil
}

const (
	fromStatsPrefix      = "from_stats_"
	toStatsPrefix        = "to_stats_"
	tokenStatsPrefix     = "token_stats_"
	userTokenStatsPrefix = "user_token_stats_"
	statsStagePrefix     = "_tracker_stage_"
	statsPreviousPrefix  = "_tracker_previous_"
)

var dailyStatsTables = []struct {
	prefix string
	model  any
}{
	{fromStatsPrefix, &models.UserStatistic{}},
	{toStatsPrefix, &models.UserStatistic{}},
	{tokenStatsPrefix, &models.TokenStatistic{}},
	{userTokenStatsPrefix, &models.UserTokenStatistic{}},
}

func (db *RawDB) flushDailyStats(date string) error {
	if _, err := time.Parse("060102", date); err != nil || len(date) != 6 {
		return fmt.Errorf("invalid flush date %q", date)
	}
	var sourceExists int
	if err := db.db.Raw("SELECT COUNT(*) FROM information_schema.tables "+
		"WHERE table_schema = DATABASE() AND table_name = ? AND table_type = 'BASE TABLE'", "transactions_"+date).
		Scan(&sourceExists).Error; err != nil {
		return fmt.Errorf("check transaction table %s: %w", date, err)
	}
	if sourceExists == 0 {
		db.logger.Infof("No transactions table for [%s], nothing to flush", date)
		return nil
	}

	if err := db.db.Connection(func(conn *gorm.DB) error {
		gdb := conn.Session(&gorm.Session{})
		// The server session guards deterministic staging names across workers.
		var namespace string
		if err := gdb.Raw("SELECT CONCAT(@@hostname, ':', @@port, ':', DATABASE())").Scan(&namespace).Error; err != nil {
			return err
		}
		hash := sha256.Sum256([]byte(namespace))
		name := fmt.Sprintf("tracker-flush-%x-%s", hash[:16], date)
		var acquired int
		if err := gdb.Raw("SELECT GET_LOCK(?, 0)", name).Scan(&acquired).Error; err != nil {
			return err
		}
		if acquired != 1 {
			return fmt.Errorf("daily statistics for %s are already being flushed", date)
		}
		defer func() {
			var released int
			if err := gdb.Raw("SELECT RELEASE_LOCK(?)", name).Scan(&released).Error; err != nil {
				db.logger.Warnf("Release daily statistics lock for %s: %v", date, err)
			}
		}()
		return db.flushDailyStatsLocked(gdb, date, "."+name)
	}); err != nil {
		return err
	}
	if err := db.DropStaleStatsTables(dailyTableRetentionCutoff(time.Now())); err != nil {
		db.logger.Errorf("drop stale daily tables: %v", err)
	}
	db.reconcileTxIndexes(txIndexRetainDays)
	db.logger.Infof("Flushed daily stats for [%s]", date)
	return nil
}

func (db *RawDB) flushDailyStatsLocked(gdb *gorm.DB, date, spoolDir string) error {
	// Keep the spill on the working-directory filesystem (which can be a data
	// disk), and clean up files from an interrupted attempt for this date.
	if err := os.RemoveAll(spoolDir); err != nil {
		return fmt.Errorf("clean stale statistics spill: %w", err)
	}
	if err := os.Mkdir(spoolDir, 0700); err != nil {
		return fmt.Errorf("create statistics spill: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(spoolDir); err != nil {
			db.logger.Warnf("Remove daily statistics spill for %s: %v", date, err)
		}
	}()

	// Reclaim an interrupted attempt's staging tables before writing the spill.
	if err := dropDailyStatsScratchTables(gdb, date); err != nil {
		return err
	}

	start := time.Now()
	spill := newDailyStatSpill(spoolDir, maphash.MakeSeed(), 0, dailyStatsBucketBits)
	totals := dailyStatTotals{fee: models.NewFeeStatistic(date)}
	var batch []*models.Transaction
	table := "transactions_" + date
	scan := gdb.Table(table).Where("type <> ?", models.TransferType).
		FindInBatches(&batch, 500, func(_ *gorm.DB, _ int) error {
			for _, tx := range batch {
				totals.add(tx)
				if err := spill.add(newDailyStatRecord(tx)); err != nil {
					return err
				}
			}
			return nil
		})
	if err := errors.Join(scan.Error, spill.close()); err != nil {
		return fmt.Errorf("scan and partition %s: %w", table, err)
	}
	db.logger.Infof("Partitioned [%d] transactions for [%s] in %s", scan.RowsAffected, date, time.Since(start))

	defer func() {
		if err := dropDailyStatsScratchTables(gdb, date); err != nil {
			db.logger.Warnf("Clean up daily statistics staging tables for %s: %v", date, err)
		}
	}()
	for _, spec := range dailyStatsTables {
		stage := statsStagePrefix + spec.prefix + date
		if err := gdb.Table(stage).Migrator().CreateTable(spec.model); err != nil {
			return fmt.Errorf("create %s: %w", stage, err)
		}
	}

	fromTable := statsStagePrefix + fromStatsPrefix + date
	tokenTable := statsStagePrefix + tokenStatsPrefix + date
	start = time.Now()
	if err := spill.drain(dailyStatsBucketKeys, func(cache *dbCache) error {
		if err := createInBatches(gdb, fromTable, cache.fromStats); err != nil {
			return fmt.Errorf("write from stats: %w", err)
		}
		if err := createInBatches(gdb, statsStagePrefix+toStatsPrefix+date, cache.toStats); err != nil {
			return fmt.Errorf("write to stats: %w", err)
		}
		if err := createInBatches(gdb, tokenTable, cache.tokenStats); err != nil {
			return fmt.Errorf("write token stats: %w", err)
		}
		if err := createInBatches(gdb, statsStagePrefix+userTokenStatsPrefix+date, cache.userTokenStats); err != nil {
			return fmt.Errorf("write user token stats: %w", err)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("aggregate and write daily statistics %s: %w", date, err)
	}
	if totals.from != nil {
		if err := gdb.Table(fromTable).Create(totals.from).Error; err != nil {
			return fmt.Errorf("write total from stats: %w", err)
		}
	}
	if totals.token != nil {
		if err := gdb.Table(tokenTable).Create(totals.token).Error; err != nil {
			return fmt.Errorf("write total token stats: %w", err)
		}
	}
	if err := gdb.Exec("ALTER TABLE `" + fromTable +
		"` ADD INDEX idx_fee_addr (fee, address), ADD INDEX idx_energy_total (energy_total)").Error; err != nil {
		return fmt.Errorf("index %s: %w", fromTable, err)
	}
	db.logger.Infof("Staged daily statistics for [%s] in %s", date, time.Since(start))

	if err := db.backfillResourceTransactionsForDate(date); err != nil {
		return fmt.Errorf("finalize resource transactions %s: %w", date, err)
	}
	if err := db.refreshChargers(); err != nil {
		return fmt.Errorf("refresh chargers: %w", err)
	}
	exchangeStats, err := db.calculateExchangeStatistics(date)
	if err != nil {
		return err
	}
	if err := gdb.Transaction(func(tx *gorm.DB) error {
		if err := tx.Delete(&models.FeeStatistic{}, "date = ?", date).Error; err != nil {
			return fmt.Errorf("delete fee stats %s: %w", date, err)
		}
		if err := tx.Create(totals.fee).Error; err != nil {
			return fmt.Errorf("write fee stats %s: %w", date, err)
		}
		return replaceExchangeStatistics(tx, date, exchangeStats)
	}); err != nil {
		return err
	}
	return publishDailyStatsTables(gdb, date)
}

func dropDailyStatsScratchTables(gdb *gorm.DB, date string) error {
	var names []string
	for _, spec := range dailyStatsTables {
		name := spec.prefix + date
		names = append(names, "`"+statsStagePrefix+name+"`", "`"+statsPreviousPrefix+name+"`")
	}
	if err := gdb.Exec("DROP TABLE IF EXISTS " + strings.Join(names, ", ")).Error; err != nil {
		return fmt.Errorf("drop daily statistics scratch tables for %s: %w", date, err)
	}
	return nil
}

func publishDailyStatsTables(gdb *gorm.DB, date string) error {
	var renames []string
	for _, spec := range dailyStatsTables {
		name := spec.prefix + date
		if gdb.Migrator().HasTable(name) {
			renames = append(renames, "`"+name+"` TO `"+statsPreviousPrefix+name+"`")
		}
		renames = append(renames, "`"+statsStagePrefix+name+"` TO `"+name+"`")
	}
	if err := gdb.Exec("RENAME TABLE " + strings.Join(renames, ", ")).Error; err != nil {
		return fmt.Errorf("publish daily statistics for %s: %w", date, err)
	}
	return nil
}
