package database

import (
	"bufio"
	"math"
	"os"
	"strings"
	"sync"

	"github.com/btcsuite/btcd/btcutil/base58"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"tron-tracker/common"
	"tron-tracker/database/models"
)

// tronAddrVersion is the TRON base58check version byte (0x41).
const tronAddrVersion = 0x41

// compactCharger is the zero-pointer in-memory representation of a charger.
// Keeping it pointer-free lets the GC skip the whole map bucket during marking.
type compactCharger struct {
	id          uint32 // DB primary key; 0 means not yet persisted (flush inserts and back-fills)
	exchangeIdx uint16 // index into ChargerStore.names
	isFake      bool
}

// chargerView is a reconstructed, business-facing view of a charger.
type chargerView struct {
	ExchangeName  string
	BackupAddress string // rebuilt from the side maps: base58 address / exchange name / ""
	IsFake        bool
}

// ChargerStore holds all chargers in a compact, pointer-free form and owns
// their persistence. All access is guarded by mu.
type ChargerStore struct {
	mu          sync.RWMutex
	chargers    map[[20]byte]compactCharger // main table, pointer-free
	addrBackups map[[20]byte][20]byte       // backup is a real address (orthogonal to isFake)
	confBackups map[[20]byte]uint16         // backup is a conflicting exchange name (idx into names)
	names       []string                    // idx -> exchange name (deduped)
	nameToIdx   map[string]uint16
	dirty       map[[20]byte]struct{} // pending-persist set, replaces chargersToSave
	skipped     int                   // rows skipped during Load (invalid address)
	db          *gorm.DB
	logger      *zap.SugaredLogger
}

// decodeAddr converts a base58check TRON address into its 20-byte payload.
// Returns ok=false for any malformed input or wrong version byte.
func decodeAddr(b58 string) ([20]byte, bool) {
	var key [20]byte
	payload, version, err := base58.CheckDecode(b58)
	if err != nil || version != tronAddrVersion || len(payload) != 20 {
		return key, false
	}
	copy(key[:], payload)
	return key, true
}

// encodeAddr converts a 20-byte payload back into a base58check TRON address.
func encodeAddr(key [20]byte) string {
	return base58.CheckEncode(key[:], tronAddrVersion)
}

// internName maps an exchange name to a stable uint16 index, appending on first
// sight. Caller must hold the write lock (it mutates names/nameToIdx).
func (s *ChargerStore) internName(name string) uint16 {
	if idx, ok := s.nameToIdx[name]; ok {
		return idx
	}
	if len(s.names) > 65535 {
		// uint16 index would overflow; fail loudly rather than silently mismap.
		s.logger.Fatalf("exchange name dictionary overflow: > 65535 distinct names")
	}
	idx := uint16(len(s.names))
	s.names = append(s.names, name)
	s.nameToIdx[name] = idx
	return idx
}

// rebuildBackupLocked reconstructs the BackupAddress string per spec §6.
// Caller holds at least the read lock.
func (s *ChargerStore) rebuildBackupLocked(key [20]byte) string {
	if bk, ok := s.addrBackups[key]; ok {
		return encodeAddr(bk)
	}
	if idx, ok := s.confBackups[key]; ok {
		return s.names[idx]
	}
	return ""
}

// Lookup reports the exchange name of a non-fake charger. Fake chargers and
// absent addresses return ok=false (the !isFake semantics of isCharger).
func (s *ChargerStore) Lookup(b58 string) (string, bool) {
	key, ok := decodeAddr(b58)
	if !ok {
		return "", false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if cc, ok := s.chargers[key]; ok && !cc.isFake {
		return s.names[cc.exchangeIdx], true
	}
	return "", false
}

// Get returns the full reconstructed view of a charger (including fakes).
func (s *ChargerStore) Get(b58 string) (chargerView, bool) {
	key, ok := decodeAddr(b58)
	if !ok {
		return chargerView{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	cc, ok := s.chargers[key]
	if !ok {
		return chargerView{}, false
	}
	return chargerView{
		ExchangeName:  s.names[cc.exchangeIdx],
		BackupAddress: s.rebuildBackupLocked(key),
		IsFake:        cc.isFake,
	}, true
}

// ApplyCharge atomically applies one charge observation, faithfully mirroring
// the SaveCharger state machine (db.go:1577-1604). The caller (db.go) resolves
// the exchange info for `to` and passes it in; this method never touches
// db.exchanges. A single write lock makes the compound check-then-act atomic,
// eliminating the TOCTOU that a Get-then-Set split would have.
func (s *ChargerStore) ApplyCharge(fromB58, toB58 string, toIsExchange bool, toExchangeName string) {
	key, ok := decodeAddr(fromB58)
	if !ok {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	cc, exists := s.chargers[key]
	if !exists {
		if toIsExchange {
			s.chargers[key] = compactCharger{exchangeIdx: s.internName(toExchangeName)}
			s.dirty[key] = struct{}{}
		}
		return
	}
	if cc.isFake {
		return
	}

	if toIsExchange {
		// charger interacts with a different exchange -> fake. The conflict
		// exchange NAME replaces any prior backup (mirrors db.go's single
		// BackupAddress field), so drop a stale address backup first to keep
		// the two side maps mutually exclusive.
		if toExchangeName != s.names[cc.exchangeIdx] {
			delete(s.addrBackups, key)
			s.confBackups[key] = s.internName(toExchangeName)
			cc.isFake = true
			s.chargers[key] = cc
			s.dirty[key] = struct{}{}
		}
		return
	}

	// to is a normal address. For !isFake the key cannot be in confBackups, so
	// addrBackups presence is exactly "has a backup".
	if _, has := s.addrBackups[key]; !has {
		if bk, ok := decodeAddr(toB58); ok {
			s.addrBackups[key] = bk // first unknown counterparty address
			s.dirty[key] = struct{}{}
		}
		return
	}
	if toB58 != encodeAddr(s.addrBackups[key]) {
		cc.isFake = true // second, different unknown address -> fake (backup stays)
		s.chargers[key] = cc
		s.dirty[key] = struct{}{}
	}
}

// NewChargerStore constructs an empty store bound to a DB and logger.
func NewChargerStore(db *gorm.DB, logger *zap.SugaredLogger) *ChargerStore {
	return &ChargerStore{
		chargers:    make(map[[20]byte]compactCharger),
		addrBackups: make(map[[20]byte][20]byte),
		confBackups: make(map[[20]byte]uint16),
		nameToIdx:   make(map[string]uint16),
		dirty:       make(map[[20]byte]struct{}),
		db:          db,
		logger:      logger,
	}
}

// Len reports the number of chargers held.
func (s *ChargerStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.chargers)
}

// DirtyLen reports how many chargers are pending persistence.
func (s *ChargerStore) DirtyLen() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.dirty)
}

// applyModel loads one DB row into the in-memory maps (spec §6/§10).
// Caller holds the write lock.
func (s *ChargerStore) applyModel(m *models.Charger) {
	key, ok := decodeAddr(m.Address)
	if !ok {
		s.skipped++
		s.logger.Warnf("skip charger with invalid address: %q", m.Address)
		return
	}
	// The address column is not unique; if the same address appears in more
	// than one row, clear any side-map backup from a prior row so the two maps
	// stay mutually exclusive and consistent with the row we keep.
	delete(s.addrBackups, key)
	delete(s.confBackups, key)
	if m.ID > math.MaxUint32 {
		s.logger.Fatalf("charger ID %d exceeds uint32 (address %s)", m.ID, m.Address)
	}
	s.chargers[key] = compactCharger{
		id:          uint32(m.ID),
		exchangeIdx: s.internName(m.ExchangeName),
		isFake:      m.IsFake,
	}
	if m.BackupAddress == "" {
		return
	}
	if bk, ok := decodeAddr(m.BackupAddress); ok {
		s.addrBackups[key] = bk // backup is a real address
		return
	}
	if m.IsFake {
		s.confBackups[key] = s.internName(m.BackupAddress) // conflicting exchange name
		return
	}
	// is_fake=0 + non-empty + decode failed = dirty data; treat as no backup to
	// preserve the "!isFake -> not in confBackups" invariant (spec §6/§10).
	s.logger.Errorf("dirty backup on non-fake charger %s: %q (dropped)", m.Address, m.BackupAddress)
}

// toModel rebuilds a *models.Charger from the compact form (spec §10).
// Caller holds at least the read lock.
func (s *ChargerStore) toModel(key [20]byte, cc compactCharger) *models.Charger {
	return &models.Charger{
		ID:            uint(cc.id),
		Address:       encodeAddr(key),
		ExchangeName:  s.names[cc.exchangeIdx],
		BackupAddress: s.rebuildBackupLocked(key),
		IsFake:        cc.isFake,
	}
}

// Load populates the store from the chargers table, or bootstraps from the
// legacy "all" file when the table does not yet exist (spec §10).
func (s *ChargerStore) Load() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db.Migrator().HasTable(&models.Charger{}) {
		s.loadFromDBLocked()
		return
	}
	if err := s.db.AutoMigrate(&models.Charger{}); err != nil {
		panic(err)
	}
	s.bootstrapFromFileLocked()
}

func (s *ChargerStore) loadFromDBLocked() {
	s.logger.Info("Start loading chargers from db")
	var batch []*models.Charger
	loaded := 0
	result := s.db.FindInBatches(&batch, 10000, func(tx *gorm.DB, _ int) error {
		for _, m := range batch {
			s.applyModel(m)
			loaded++
			if loaded%1_000_000 == 0 {
				s.logger.Infof("Loaded [%d] chargers", loaded)
			}
		}
		return nil
	})
	if result.Error != nil {
		s.logger.Fatalf("Load chargers from db failed after [%d]: %v", loaded, result.Error)
	}
	s.logger.Infof("Complete loading chargers: [%d] in memory, [%d] skipped", len(s.chargers), s.skipped)
}

func (s *ChargerStore) bootstrapFromFileLocked() {
	f, err := os.Open("all")
	if err != nil {
		s.logger.Errorf("Load charger file error: [%s]", err.Error())
		return
	}
	defer f.Close()

	s.logger.Info("Start loading chargers from file")
	scanner := bufio.NewScanner(f)
	batch := make([]*models.Charger, 0, 1000)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		s.db.Create(&batch)
		for _, m := range batch {
			s.applyModel(m)
		}
		batch = batch[:0]
	}
	count := 0
	for scanner.Scan() {
		cols := strings.Split(scanner.Text(), ",")
		if len(cols) < 2 {
			continue
		}
		batch = append(batch, &models.Charger{Address: cols[0], ExchangeName: common.TrimExchangeName(cols[1])})
		if len(batch) == 1000 {
			flush()
		}
		count++
	}
	flush()
	if err := scanner.Err(); err != nil {
		s.logger.Fatal(err)
	}
	s.logger.Infof("Complete loading chargers from file: [%d]", count)
}

// doFlushLocked writes all dirty chargers back and back-fills inserted IDs.
// Caller holds the write lock (RWMutex is non-reentrant, so Refresh calls this
// directly rather than FlushDirty).
func (s *ChargerStore) doFlushLocked() {
	if len(s.dirty) == 0 {
		return
	}
	batch := make([]*models.Charger, 0, len(s.dirty))
	keys := make([][20]byte, 0, len(s.dirty))
	for key := range s.dirty {
		batch = append(batch, s.toModel(key, s.chargers[key]))
		keys = append(keys, key)
	}
	if err := s.db.Save(batch).Error; err != nil {
		// Keep dirty for retry on the next flush; do not back-fill or clear,
		// otherwise id==0 rows would be lost and re-inserted later.
		s.logger.Errorf("Flush %d chargers failed, keeping dirty for retry: %v", len(batch), err)
		return
	}
	// back-fill auto-incremented IDs for freshly inserted rows
	for i, m := range batch {
		id := uint32(m.ID)
		if id == 0 {
			continue
		}
		if cc := s.chargers[keys[i]]; cc.id != id {
			cc.id = id
			s.chargers[keys[i]] = cc
		}
	}
	s.dirty = make(map[[20]byte]struct{})
}

// FlushDirty persists pending chargers (spec §10).
func (s *ChargerStore) FlushDirty() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.doFlushLocked()
}

// Refresh re-normalizes exchange names and reconciles address backups that have
// since become exchanges (mirrors db.go:266-289). Holds the write lock
// throughout and persists via doFlushLocked directly (RWMutex is non-reentrant,
// so it must not call FlushDirty).
func (s *ChargerStore) Refresh(resolve func(string) string, isExchange func(b58 string) (string, bool)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refreshLocked(resolve, isExchange)
	s.doFlushLocked()
}

// refreshLocked performs the in-memory part of Refresh. Caller holds the write lock.
func (s *ChargerStore) refreshLocked(resolve func(string) string, isExchange func(b58 string) (string, bool)) {
	for key, cc := range s.chargers {
		changed := false

		// step 1: normalize the exchange name; cancel fake only when the name
		// actually changes AND there is no backup (db.go:267-275).
		current := s.names[cc.exchangeIdx]
		newName := resolve(common.TrimExchangeName(current))
		if newName != current {
			cc.exchangeIdx = s.internName(newName)
			if cc.isFake {
				_, hasAddr := s.addrBackups[key]
				_, hasConf := s.confBackups[key]
				if !hasAddr && !hasConf {
					cc.isFake = false
				}
			}
			changed = true
		}

		// step 2: an address backup that has since become an exchange (db.go:278-288).
		if bk, ok := s.addrBackups[key]; ok {
			if exName, isEx := isExchange(encodeAddr(bk)); isEx {
				if exName != s.names[cc.exchangeIdx] {
					if !cc.isFake {
						cc.isFake = true
						changed = true
					}
				} else {
					delete(s.addrBackups, key)
					changed = true
				}
			}
		}

		if changed {
			s.chargers[key] = cc
			s.dirty[key] = struct{}{}
		}
	}
}
