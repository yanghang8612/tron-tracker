package tron

import (
	"fmt"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestBlockFetchRetryTransientAndPersistentLogging(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	logger := zap.New(core).Sugar()
	start := time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)
	var state blockFetchRetry
	state.failure(logger, start, 123, "transaction info count=0, block transaction count=100")
	state.recovered(logger, start.Add(time.Second), 123)
	if logs.FilterLevelExact(zapcore.WarnLevel).Len() != 0 || logs.FilterLevelExact(zapcore.InfoLevel).Len() != 0 {
		t.Fatal("brief indexer lag must not create warning/recovery noise")
	}
	logs.TakeAll()
	for i := 0; i <= 70; i++ {
		state.failure(logger, start.Add(time.Duration(i)*time.Second), uint(123+i%2), fmt.Sprintf("changing failure %d", i))
	}
	if logs.FilterLevelExact(zapcore.WarnLevel).Len() != 3 {
		t.Fatalf("warnings=%d, want at 10s/40s/70s", logs.FilterLevelExact(zapcore.WarnLevel).Len())
	}
	if logs.FilterLevelExact(zapcore.DebugLevel).Len() != 1 {
		t.Fatal("retry loop emitted per-attempt debug logs")
	}
	state.recovered(logger, start.Add(71*time.Second), 123)
	if logs.FilterLevelExact(zapcore.InfoLevel).Len() != 1 || state.attempts != 0 {
		t.Fatal("persistent episode must log one recovery and reset")
	}
	state.failure(logger, start.Add(72*time.Second), 124, "new episode")
	if logs.FilterLevelExact(zapcore.WarnLevel).Len() != 3 {
		t.Fatal("new transient episode inherited the old warning state")
	}
}
