package types

import (
	"math/bits"
	"math/rand"
	"testing"
)

func TestTransferStats_Add(t *testing.T) {
	ts1 := NewTransferStats()
	for i := 1; i < 10; i++ {
		ts1.Add(i, uint(i*i))
	}
	for i := 1; i < 10; i++ {
		if ts1.Get(i) != uint(i*i) {
			t.Errorf("Add failed: %d", ts1.Get(i))
		}
	}

	ts2 := NewTransferStats()
	for i := 10; i > 0; i-- {
		ts2.Add(i, uint(i*i))
	}
	for i := 10; i > 0; i-- {
		if ts2.Get(i) != uint(i*i) {
			t.Errorf("Add failed: %d", ts1.Get(i))
		}
	}

	ts2.Merge(&ts1)
	for i := 1; i < 10; i++ {
		if ts2.Get(i) != uint(i*i*2) {
			t.Errorf("Merge failed: %d", ts2.Get(i))
		}
	}

	ts3 := NewTransferStats()
	for i := 0; i < 100; i++ {
		length := rand.Int() % 18
		preCount := ts3.Get(length)
		count := rand.Int() % 100

		ts3.Add(length, uint(count))

		if ts3.Get(length) != preCount+uint(count) {
			t.Errorf("Add failed: %d", ts3.Get(length))
		}
	}
}

func TestTransferStatsDoesNotGrowForUnrepresentableLength(t *testing.T) {
	stats := NewTransferStats()
	stats.Add(6, 3)
	for range 100 {
		stats.Add(bits.UintSize, 1)
		stats.Add(78, 1)
	}
	if len(stats.vals) != 2 || stats.Get(6) != 3 {
		t.Fatalf("unexpected counters: %v", stats.vals)
	}
}
