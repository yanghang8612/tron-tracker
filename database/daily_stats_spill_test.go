package database

import (
	"fmt"
	"hash/maphash"
	"math/big"
	"reflect"
	"testing"

	"tron-tracker/database/models/types"
)

func TestDailyStatSpillSplitKeepsCompleteKeys(t *testing.T) {
	const limit = 7
	spill := newDailyStatSpill(t.TempDir(), maphash.MakeSeed(), 0, 1)
	t.Cleanup(func() { spill.close() })

	want := newCache()
	for i := range 160 {
		owner := fmt.Sprintf("owner-%d", i%31)
		from := fmt.Sprintf("from-%d", i%29)
		to := fmt.Sprintf("to-%d", i%23)
		tx := mkTx(131, owner, from, to, USDT, int64(i+1), int64(i%3))
		if i%3 == 0 {
			tx.FromAddr = tx.ToAddr // Both contributions for the same token key.
			tx.Amount = types.NewBigInt(new(big.Int).Lsh(big.NewInt(1), 250))
		}
		if i%5 == 0 {
			tx.Result = 0
		}
		want.accumulate(statOwner|statRecipient|statToken|statTokenSender|statTokenRecipient, tx)
		if err := spill.add(newDailyStatRecord(tx)); err != nil {
			t.Fatal(err)
		}
	}
	if err := spill.close(); err != nil {
		t.Fatal(err)
	}
	// The test deliberately uses two initial buckets for >100 keys.
	got := newCache()
	if err := spill.drain(limit, func(bucket *dbCache) error {
		count := len(bucket.fromStats) + len(bucket.toStats) + len(bucket.tokenStats) + len(bucket.userTokenStats)
		if count > limit {
			t.Fatalf("bucket has %d keys, budget is %d", count, limit)
		}
		mergeSpillTestKeys(t, got.fromStats, bucket.fromStats)
		mergeSpillTestKeys(t, got.toStats, bucket.toStats)
		mergeSpillTestKeys(t, got.tokenStats, bucket.tokenStats)
		mergeSpillTestKeys(t, got.userTokenStats, bucket.userTokenStats)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("spilled statistics differ from the original transaction order")
	}
}

func mergeSpillTestKeys[T any](t *testing.T, dst, src map[string]*T) {
	t.Helper()
	for key, value := range src {
		if _, exists := dst[key]; exists {
			t.Fatalf("key %q was emitted from multiple buckets", key)
		}
		dst[key] = value
	}
}
