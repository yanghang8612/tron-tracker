//go:build integration

package database

import (
	"testing"

	"go.uber.org/zap"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"tron-tracker/database/models"
)

// newFlushTestDB spins a throwaway MySQL schema and a minimal RawDB wired for
// flush. Skips when no local MySQL (root, empty password) is available.
func newFlushTestDB(t *testing.T) *RawDB {
	const base = "root:@tcp(127.0.0.1:3306)/"
	root, err := gorm.Open(mysql.Open(base+"?charset=utf8mb4&parseTime=True&loc=Local"), &gorm.Config{Logger: logger.Discard})
	if err != nil {
		t.Skipf("no local mysql: %v", err)
	}
	if err := root.Exec("CREATE DATABASE IF NOT EXISTS tracker_flush_itest").Error; err != nil {
		t.Skipf("cannot create test db: %v", err)
	}
	t.Cleanup(func() { root.Exec("DROP DATABASE IF EXISTS tracker_flush_itest") })

	gdb, err := gorm.Open(mysql.Open(base+"tracker_flush_itest?charset=utf8mb4&parseTime=True&loc=Local"),
		&gorm.Config{SkipDefaultTransaction: true, Logger: logger.Discard})
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	if err := gdb.AutoMigrate(&models.FeeStatistic{}, &models.ExchangeStatistic{}, &models.Charger{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	rdb := &RawDB{
		db:              gdb,
		exchanges:       map[string]*models.Exchange{},
		validTokens:     map[string]string{"TRX": "TRX", USDT: "USDT"},
		isTableMigrated: map[string]bool{},
		logger:          zap.NewNop().Sugar(),
	}
	rdb.chargerStore = NewChargerStore(gdb, rdb.logger)
	return rdb
}

// seedTxs creates transactions_<date> and inserts rows (some type=255 transfers
// that must be excluded from stats).
func seedTxs(t *testing.T, db *RawDB, date string, txs []*models.Transaction) {
	table := "transactions_" + date
	if err := db.db.Table(table).AutoMigrate(&models.Transaction{}); err != nil {
		t.Fatalf("migrate %s: %v", table, err)
	}
	if err := db.db.Table(table).Create(txs).Error; err != nil {
		t.Fatalf("seed %s: %v", table, err)
	}
}

func countTable(t *testing.T, db *RawDB, table string) int64 {
	var n int64
	if err := db.db.Table(table).Count(&n).Error; err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func TestPersistTrackingMetaAtomic(t *testing.T) {
	db := newFlushTestDB(t)
	if err := db.db.AutoMigrate(&models.Meta{}); err != nil {
		t.Fatalf("migrate meta: %v", err)
	}
	db.db.Create(&models.Meta{Key: models.TrackingDateKey, Val: "250101"})
	db.db.Create(&models.Meta{Key: models.TrackingStartBlockNumKey, Val: "100"})

	if err := db.persistTrackingMeta("250102", 200); err != nil {
		t.Fatalf("persistTrackingMeta: %v", err)
	}

	get := func(k string) string {
		var m models.Meta
		db.db.Where("`key` = ?", k).Take(&m)
		return m.Val
	}
	if get(models.TrackingDateKey) != "250102" || get(models.TrackingStartBlockNumKey) != "200" {
		t.Fatalf("both meta must be updated together: date=%s num=%s",
			get(models.TrackingDateKey), get(models.TrackingStartBlockNumKey))
	}
}

func TestFlushDailyStatsEquivalenceAndIdempotency(t *testing.T) {
	db := newFlushTestDB(t)
	// Use a recent date so DropStaleStatsTables (cutoff = now-1yr) does not
	// drop the freshly-written test tables during the same flush call.
	const date = "260101"

	seedTxs(t, db, date, []*models.Transaction{
		mkTx(1, "A", "A", "B", "TRX", 2_000_000, 100), // counted
		mkTx(1, "A", "A", "C", "TRX", 3_000_000, 200), // counted
		mkTx(255, "X", "A", "B", USDT, 9_000_000, 0),  // TransferType -> excluded
	})

	if err := db.flushDailyStats(date); err != nil {
		t.Fatalf("flushDailyStats: %v", err)
	}

	// from_stats_<date>: A, total (2 rows); excluded transfer doesn't add X
	if got := countTable(t, db, "from_stats_"+date); got != 2 {
		t.Fatalf("from_stats rows = %d, want 2 (A,total)", got)
	}
	var a models.UserStatistic
	db.db.Table("from_stats_"+date).Where("address = ?", "A").Take(&a)
	if a.Fee != 300 || a.TxTotal != 2 {
		t.Fatalf("A fee=%d tx=%d, want 300/2 (transfer excluded)", a.Fee, a.TxTotal)
	}

	// idempotency: second flush must not double rows or values
	if err := db.flushDailyStats(date); err != nil {
		t.Fatalf("second flushDailyStats: %v", err)
	}
	if got := countTable(t, db, "from_stats_"+date); got != 2 {
		t.Fatalf("after re-flush from_stats rows = %d, want 2", got)
	}
	db.db.Table("from_stats_"+date).Where("address = ?", "A").Take(&a)
	if a.Fee != 300 {
		t.Fatalf("after re-flush A fee=%d, want 300 (no doubling)", a.Fee)
	}
	if got := countTable(t, db, "fee_statistics"); got != 1 {
		t.Fatalf("fee_statistics rows = %d, want 1 (delete-before-write)", got)
	}
}

func TestSaveTransactionsPropagatesError(t *testing.T) {
	db := newFlushTestDB(t)
	db.trackingDate = "250101"
	// 预置已迁移标记，跳过 createTableIfNotExist 的 AutoMigrate（其在出错时 panic 而非返回
	// error），从而让失败发生在我们关心的 Create 上、被 SaveTransactions 返回。
	db.isTableMigrated["transactions_250101"] = true

	// 关闭底层连接，强制 Create 失败
	under, err := db.db.DB()
	if err != nil {
		t.Fatalf("get sql.DB: %v", err)
	}
	_ = under.Close()

	if err := db.SaveTransactions([]*models.Transaction{{Height: 1, Type: 1, OwnerAddr: "A"}}); err == nil {
		t.Fatal("SaveTransactions must return error when the insert fails")
	}
}

func TestResumeBlockNum(t *testing.T) {
	db := newFlushTestDB(t)

	// 表不存在 -> 回退到 fallback-1
	if n, resumed := resumeBlockNum(db.db, "250101", 5000); resumed || n != 4999 {
		t.Fatalf("missing table: got (%d,%v), want (4999,false)", n, resumed)
	}

	// 有数据 -> MAX(height)
	seedTxs(t, db, "250101", []*models.Transaction{
		mkTx(1, "A", "A", "B", "TRX", 1, 1),
	})
	db.db.Table("transactions_250101").Create([]*models.Transaction{
		{Height: 102, Type: 1, OwnerAddr: "A"},
	})
	if n, resumed := resumeBlockNum(db.db, "250101", 5000); !resumed || n != 102 {
		t.Fatalf("with data: got (%d,%v), want (102,true)", n, resumed)
	}

	// 空表 -> 回退
	if err := db.db.Table("transactions_250102").AutoMigrate(&models.Transaction{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if n, resumed := resumeBlockNum(db.db, "250102", 5000); resumed || n != 4999 {
		t.Fatalf("empty table: got (%d,%v), want (4999,false)", n, resumed)
	}
}

func TestFlushDailyStatsMissingTableNoError(t *testing.T) {
	db := newFlushTestDB(t)
	const date = "260301" // within the last year; no transactions_<date> table created

	// Missing transactions table => nothing to flush => nil (cursor can advance),
	// must NOT error inside DoExchangeStatistics nor leave empty stat tables behind.
	if err := db.flushDailyStats(date); err != nil {
		t.Fatalf("flushDailyStats on a missing transactions table must return nil, got: %v", err)
	}
	if db.db.Migrator().HasTable("from_stats_" + date) {
		t.Fatal("flushDailyStats must not create stat tables when there is no transactions table")
	}
}
