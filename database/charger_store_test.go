package database

import (
	"sync"
	"testing"

	"go.uber.org/zap"

	"tron-tracker/database/models"
)

// addrN deterministically builds a valid base58check TRON address from a byte,
// so tests don't hardcode checksums. Distinct b -> distinct address.
func addrN(b byte) string {
	var p [20]byte
	for i := range p {
		p[i] = b
	}
	return encodeAddr(p)
}

func newTestStore() *ChargerStore {
	return &ChargerStore{
		chargers:    map[[20]byte]compactCharger{},
		addrBackups: map[[20]byte][20]byte{},
		confBackups: map[[20]byte]uint16{},
		nameToIdx:   map[string]uint16{},
		dirty:       map[[20]byte]struct{}{},
		logger:      zap.NewNop().Sugar(),
	}
}

func TestEncodeDecodeAddrRoundTrip(t *testing.T) {
	// 真实 TRON 地址（USDT-TRC20 合约）
	const b58 = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
	key, ok := decodeAddr(b58)
	if !ok {
		t.Fatalf("decode failed for valid addr %s", b58)
	}
	if got := encodeAddr(key); got != b58 {
		t.Fatalf("round trip mismatch: got %s want %s", got, b58)
	}
	if _, ok := decodeAddr("not-a-valid-address"); ok {
		t.Fatalf("expected decode to reject invalid input")
	}
	if _, ok := decodeAddr(""); ok {
		t.Fatalf("expected decode to reject empty input")
	}
}

func TestInternNameDedup(t *testing.T) {
	s := newTestStore()
	a := s.internName("Binance")
	b := s.internName("Okex")
	again := s.internName("Binance")
	if a != again {
		t.Fatalf("same name must map to same idx: %d vs %d", a, again)
	}
	if a == b {
		t.Fatalf("different names must differ")
	}
	if s.names[a] != "Binance" {
		t.Fatalf("names[idx] mismatch: %q", s.names[a])
	}
}

func TestLookupExcludesFake(t *testing.T) {
	s := newTestStore()
	addr := addrN(1)
	key, _ := decodeAddr(addr)
	s.chargers[key] = compactCharger{exchangeIdx: s.internName("Binance"), isFake: true}
	if _, ok := s.Lookup(addr); ok {
		t.Fatalf("fake charger must not be reported by Lookup")
	}
	s.chargers[key] = compactCharger{exchangeIdx: s.internName("Binance"), isFake: false}
	if name, ok := s.Lookup(addr); !ok || name != "Binance" {
		t.Fatalf("non-fake charger expected, got %q ok=%v", name, ok)
	}
	if _, ok := s.Lookup(addrN(99)); ok {
		t.Fatalf("absent address must return ok=false")
	}
}

func TestGetReconstructsBackup(t *testing.T) {
	s := newTestStore()
	addr := addrN(1)
	key, _ := decodeAddr(addr)
	bkAddr := addrN(7)
	bkKey, _ := decodeAddr(bkAddr)

	// address backup (orthogonal to isFake)
	s.chargers[key] = compactCharger{exchangeIdx: s.internName("Binance"), isFake: false}
	s.addrBackups[key] = bkKey
	if v, ok := s.Get(addr); !ok || v.ExchangeName != "Binance" || v.BackupAddress != bkAddr || v.IsFake {
		t.Fatalf("address backup: got %+v ok=%v", v, ok)
	}

	// conflicting-exchange backup
	delete(s.addrBackups, key)
	s.confBackups[key] = s.internName("Okex")
	s.chargers[key] = compactCharger{exchangeIdx: s.internName("Binance"), isFake: true}
	if v, _ := s.Get(addr); v.BackupAddress != "Okex" || !v.IsFake {
		t.Fatalf("conf backup: got %+v", v)
	}

	// no backup
	delete(s.confBackups, key)
	if v, _ := s.Get(addr); v.BackupAddress != "" {
		t.Fatalf("no backup: expected empty, got %q", v.BackupAddress)
	}
}

func TestApplyChargeBranches(t *testing.T) {
	s := newTestStore()
	c1 := addrN(1)
	exAddr := addrN(100) // an exchange address (only its toIsExchange flag + name matter)
	u1 := addrN(2)
	u2 := addrN(3)

	// new charger: to is an exchange -> non-fake
	s.ApplyCharge(c1, exAddr, true, "Binance")
	if name, ok := s.Lookup(c1); !ok || name != "Binance" {
		t.Fatalf("new charger expected Binance, got %q ok=%v", name, ok)
	}

	// first unknown address -> record backup, not fake
	s.ApplyCharge(c1, u1, false, "")
	if v, _ := s.Get(c1); v.IsFake || v.BackupAddress != u1 {
		t.Fatalf("first unknown: expect backup=%s !fake, got backup=%s fake=%v", u1, v.BackupAddress, v.IsFake)
	}

	// same unknown address again -> still not fake
	s.ApplyCharge(c1, u1, false, "")
	if v, _ := s.Get(c1); v.IsFake {
		t.Fatalf("repeated same unknown addr must not fake")
	}

	// second, different unknown address -> fake, backup stays u1
	s.ApplyCharge(c1, u2, false, "")
	if v, _ := s.Get(c1); !v.IsFake || v.BackupAddress != u1 {
		t.Fatalf("second unknown: expect fake & backup stays %s, got backup=%s fake=%v", u1, v.BackupAddress, v.IsFake)
	}

	// conflicting exchange -> fake + conflict name
	ex2 := addrN(50)
	s.ApplyCharge(ex2, exAddr, true, "Binance")
	s.ApplyCharge(ex2, addrN(101), true, "Okex")
	if v, _ := s.Get(ex2); !v.IsFake || v.BackupAddress != "Okex" {
		t.Fatalf("conflict: expect fake & backup=Okex, got backup=%s fake=%v", v.BackupAddress, v.IsFake)
	}

	// invariant: a non-fake charger is never in confBackups
	c3 := addrN(4)
	s.ApplyCharge(c3, exAddr, true, "Binance")
	key3, _ := decodeAddr(c3)
	if _, in := s.confBackups[key3]; in {
		t.Fatalf("non-fake charger must not be in confBackups")
	}

	// a non-existent charger paired with a non-exchange to is a no-op
	cAbsent := addrN(5)
	s.ApplyCharge(cAbsent, u1, false, "")
	if _, ok := s.Get(cAbsent); ok {
		t.Fatalf("non-existent charger with non-exchange to must stay absent")
	}
}

func TestModelRoundTrip(t *testing.T) {
	cases := []models.Charger{
		{ID: 1, Address: addrN(1), ExchangeName: "Binance"},                                       // no backup
		{ID: 2, Address: addrN(2), ExchangeName: "Okex", BackupAddress: addrN(20)},                // addr backup, !fake
		{ID: 3, Address: addrN(3), ExchangeName: "Huobi", BackupAddress: addrN(30), IsFake: true}, // addr backup, fake
		{ID: 4, Address: addrN(4), ExchangeName: "Gate", BackupAddress: "Binance", IsFake: true},  // name backup, fake
	}
	s := newTestStore()
	for i := range cases {
		s.applyModel(&cases[i])
	}
	for _, want := range cases {
		key, _ := decodeAddr(want.Address)
		got := s.toModel(key, s.chargers[key])
		if got.ID != want.ID || got.Address != want.Address || got.ExchangeName != want.ExchangeName ||
			got.BackupAddress != want.BackupAddress || got.IsFake != want.IsFake {
			t.Fatalf("round trip mismatch for id %d:\n got  %+v\n want %+v", want.ID, *got, want)
		}
	}
}

func TestApplyModelDirtyFakeZeroBackup(t *testing.T) {
	// is_fake=0 + non-empty backup + decode failure -> treated as no backup
	s := newTestStore()
	m := &models.Charger{ID: 9, Address: addrN(1), ExchangeName: "Binance", BackupAddress: "garbage-not-addr"}
	s.applyModel(m)
	key, _ := decodeAddr(m.Address)
	if _, has := s.addrBackups[key]; has {
		t.Fatalf("dirty backup on !fake must be dropped, not stored")
	}
	if _, has := s.confBackups[key]; has {
		t.Fatalf("dirty backup on !fake must not pollute confBackups")
	}
	if got := s.toModel(key, s.chargers[key]); got.BackupAddress != "" {
		t.Fatalf("expect empty backup, got %q", got.BackupAddress)
	}
}

func TestRefreshCancelsFakeWhenNoBackup(t *testing.T) {
	s := newTestStore()
	c := addrN(1)
	key, _ := decodeAddr(c)
	s.chargers[key] = compactCharger{exchangeIdx: s.internName("Binance Old"), isFake: true} // fake, no backup
	resolve := func(n string) string {
		if n == "Binance Old" {
			return "Binance"
		}
		return n
	}
	isExchange := func(string) (string, bool) { return "", false }
	s.refreshLocked(resolve, isExchange)
	if v, _ := s.Get(c); v.IsFake || v.ExchangeName != "Binance" {
		t.Fatalf("expect name normalized & fake cancelled, got name=%s fake=%v", v.ExchangeName, v.IsFake)
	}
}

func TestRefreshDoesNotCancelFakeWhenNameUnchanged(t *testing.T) {
	s := newTestStore()
	c := addrN(1)
	key, _ := decodeAddr(c)
	s.chargers[key] = compactCharger{exchangeIdx: s.internName("Binance"), isFake: true} // fake, no backup, name stable
	resolve := func(n string) string { return n }                                        // no change
	isExchange := func(string) (string, bool) { return "", false }
	s.refreshLocked(resolve, isExchange)
	if v, _ := s.Get(c); !v.IsFake {
		t.Fatalf("fake must NOT be cancelled when name unchanged (mirrors db.go nesting)")
	}
}

func TestRefreshBackupBecameConflictingExchange(t *testing.T) {
	s := newTestStore()
	c := addrN(1)
	bk := addrN(2)
	key, _ := decodeAddr(c)
	bkKey, _ := decodeAddr(bk)
	s.chargers[key] = compactCharger{exchangeIdx: s.internName("Binance"), isFake: false}
	s.addrBackups[key] = bkKey
	resolve := func(n string) string { return n }
	isExchange := func(b58 string) (string, bool) {
		if b58 == bk {
			return "Okex", true // backup address is now a different exchange
		}
		return "", false
	}
	s.refreshLocked(resolve, isExchange)
	if v, _ := s.Get(c); !v.IsFake {
		t.Fatalf("backup became conflicting exchange -> expect fake")
	}
}

func TestRefreshBackupBecameSameExchangeClears(t *testing.T) {
	s := newTestStore()
	c := addrN(1)
	bk := addrN(2)
	key, _ := decodeAddr(c)
	bkKey, _ := decodeAddr(bk)
	s.chargers[key] = compactCharger{exchangeIdx: s.internName("Okex"), isFake: false}
	s.addrBackups[key] = bkKey
	resolve := func(n string) string { return n }
	isExchange := func(b58 string) (string, bool) {
		if b58 == bk {
			return "Okex", true // same exchange as the charger
		}
		return "", false
	}
	s.refreshLocked(resolve, isExchange)
	if _, has := s.addrBackups[key]; has {
		t.Fatalf("backup became same exchange -> should be cleared")
	}
	if v, _ := s.Get(c); v.IsFake {
		t.Fatalf("same-exchange clear must not fake")
	}
}

func TestConcurrentAccess(t *testing.T) {
	// Exercises the lock under -race: concurrent writers (ApplyCharge) and
	// readers (Lookup/Get). ApplyCharge does not touch the DB, so a nil db is fine.
	s := newTestStore()
	exAddr := addrN(200)
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(3)
		go func(n int) { defer wg.Done(); s.ApplyCharge(addrN(byte(n)), exAddr, true, "Binance") }(i)
		go func(n int) { defer wg.Done(); s.Lookup(addrN(byte(n))) }(i)
		go func(n int) { defer wg.Done(); s.Get(addrN(byte(n))) }(i)
	}
	wg.Wait()
}
