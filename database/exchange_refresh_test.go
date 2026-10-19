package database

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"tron-tracker/database/models"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestExchangeRefreshBackoffKeepsCacheAndRecovers(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	old := &models.Exchange{Address: "a", Name: "Binance", OriginName: "Binance Hot Wallet"}
	db := &RawDB{exchanges: map[string]*models.Exchange{"a": old}, logger: zap.New(core).Sugar()}
	now := time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)
	calls := 0
	fail := func() (*models.Exchanges, error) { calls++; return nil, errors.New("HTTP 503") }
	for i, delay := range []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 15 * time.Minute, 15 * time.Minute} {
		db.updateExchangesAt(now, fail)
		if calls != i+1 || db.exchangesRetryAt != now.Add(delay) {
			t.Fatalf("attempt %d: calls=%d retry=%s", i, calls, db.exchangesRetryAt)
		}
		for n := 0; n < 100; n++ {
			db.updateExchangesAt(now.Add(delay-time.Nanosecond), fail)
		}
		if calls != i+1 || db.exchanges["a"] != old || old.OriginName != "Binance Hot Wallet" || !db.exchangesUpdatedAt.IsZero() {
			t.Fatal("failed refresh retried early or damaged cached data")
		}
		now = now.Add(delay)
	}
	if logs.FilterLevelExact(zapcore.WarnLevel).Len() != 6 {
		t.Fatalf("warnings=%d; want one per attempt", logs.FilterLevelExact(zapcore.WarnLevel).Len())
	}
	succeed := func() (*models.Exchanges, error) {
		calls++
		return &models.Exchanges{Val: []*models.Exchange{{Address: "a", Name: "Binance"}}}, nil
	}
	db.updateExchangesAt(now, succeed)
	if db.exchangesFailures != 0 || !db.exchangesRetryAt.IsZero() || !db.exchangesUpdatedAt.Equal(now) || old.OriginName != "Binance Hot Wallet" || old.Name != "Binance" {
		t.Fatal("successful refresh did not reset backoff/preserve cached names")
	}
	before := calls
	db.updateExchangesAt(now.Add(time.Hour-time.Nanosecond), succeed)
	if calls != before {
		t.Fatal("success must restore hourly refresh interval")
	}
	db.updateExchangesAt(now.Add(time.Hour), succeed)
	if calls != before+1 {
		t.Fatal("hourly refresh was not due")
	}
}

func TestExchangeRefreshEmptyListIsFailure(t *testing.T) {
	for _, data := range []*models.Exchanges{nil, {}} {
		db := &RawDB{exchanges: map[string]*models.Exchange{}, logger: zap.NewNop().Sugar()}
		db.updateExchangesAt(time.Now(), func() (*models.Exchanges, error) { return data, nil })
		if db.exchangesFailures != 1 || db.exchangesRetryAt.IsZero() {
			t.Fatal("empty response bypassed backoff")
		}
	}
}

func TestExchangeRefreshMissingOriginPreservesNamesAndBoundsWarnings(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	db := &RawDB{exchanges: map[string]*models.Exchange{}, logger: zap.New(core).Sugar()}
	var snapshot []*models.Exchange
	for i := 0; i < 400; i++ {
		addr := fmt.Sprintf("address%03d", i)
		db.exchanges[addr] = &models.Exchange{Address: addr, OriginName: "Binance Hot Wallet", Name: "Binance"}
		snapshot = append(snapshot, &models.Exchange{Address: addr, Name: "Binance"})
	}
	db.updateExchangesAt(time.Now(), func() (*models.Exchanges, error) { return &models.Exchanges{Val: snapshot}, nil })
	if logs.FilterLevelExact(zapcore.WarnLevel).Len() != 0 || logs.Len() != 1 {
		t.Fatalf("missing origins generated per-row logs: %d", logs.Len())
	}
	for _, row := range db.exchanges {
		if row.OriginName != "Binance Hot Wallet" || row.Name != "Binance" {
			t.Fatal("missing origin erased a cached name")
		}
	}

	// Real conflicts/omissions remain visible, but in at most two summaries.
	logs.TakeAll()
	db.exchangesUpdatedAt = time.Time{}
	db.updateExchangesAt(time.Now(), func() (*models.Exchanges, error) {
		return &models.Exchanges{Val: []*models.Exchange{{Address: "address000", Name: "Other", OriginName: "Other"}}}, nil
	})
	if logs.FilterLevelExact(zapcore.WarnLevel).Len() != 2 {
		t.Fatalf("warnings=%d, want conflict and omission summaries", logs.FilterLevelExact(zapcore.WarnLevel).Len())
	}
	for _, entry := range logs.FilterLevelExact(zapcore.WarnLevel).All() {
		if len(entry.Message) > 350 {
			t.Fatal("summary is unbounded")
		}
	}
}

func TestExchangeRefreshRepairsEmptyNamesFromCurrentSchema(t *testing.T) {
	gdb, err := gorm.Open(mysql.New(mysql.Config{DSN: "test:test@tcp(127.0.0.1:3306)/unused", SkipInitializeWithVersion: true}), &gorm.Config{DryRun: true, DisableAutomaticPing: true, SkipDefaultTransaction: true, Logger: logger.Discard})
	if err != nil {
		t.Fatal(err)
	}
	db := &RawDB{db: gdb, exchanges: map[string]*models.Exchange{"a": {ID: 1, Address: "a"}}, logger: zap.NewNop().Sugar()}
	db.updateExchangesAt(time.Now(), func() (*models.Exchanges, error) {
		return &models.Exchanges{Val: []*models.Exchange{{Address: "a", Name: "Binance 8"}, {Address: "b", Name: "Kraken"}}}, nil
	})
	if db.exchanges["a"].OriginName != "Binance 8" || db.exchanges["a"].Name != "Binance" || db.exchanges["b"].OriginName != "Kraken" || db.exchanges["b"].Name != "Kraken" {
		t.Fatalf("metadata not repaired: %+v %+v", db.exchanges["a"], db.exchanges["b"])
	}
}
