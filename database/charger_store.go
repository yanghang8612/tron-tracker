package database

import (
	"bufio"
	"fmt"
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
	if err := s.streamChargersLocked(); err != nil {
		s.logger.Fatalf("Load chargers from db failed: %v", err)
	}
	s.logger.Infof("Complete loading chargers: [%d] in memory, [%d] skipped", len(s.chargers), s.skipped)
}

func (s *ChargerStore) streamChargersLocked() error {
	// InnoDB's estimate avoids a second full scan. The map can still grow
	// when the estimate is low or unavailable.
	var estimate int
	if len(s.chargers) == 0 &&
		s.db.Raw("SELECT COALESCE(MAX(table_rows), 0) FROM information_schema.tables "+
			"WHERE table_schema = DATABASE() AND table_name = 'chargers'").Scan(&estimate).Error == nil && estimate > 0 {
		s.chargers = make(map[[20]byte]compactCharger, estimate)
	}

	// NULL fields in older rows were read as zero values by GORM.
	rows, err := s.db.Raw("SELECT id, COALESCE(address, ''), COALESCE(exchange_name, ''), " +
		"COALESCE(backup_address, ''), COALESCE(is_fake, 0) FROM chargers ORDER BY id").Rows()
	if err != nil {
		return err
	}
	defer rows.Close()

	var charger models.Charger
	loaded := 0
	for rows.Next() {
		if err := rows.Scan(&charger.ID, &charger.Address, &charger.ExchangeName, &charger.BackupAddress, &charger.IsFake); err != nil {
			return fmt.Errorf("scan charger after %d rows: %w", loaded, err)
		}
		s.applyModel(&charger)
		loaded++
		if loaded%1_000_000 == 0 {
			s.logger.Infof("Loaded [%d] chargers", loaded)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("stream chargers after %d rows: %w", loaded, err)
	}
	return nil
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

const chargerWriteBatch = 500

// doFlushLocked writes bounded batches. Only a successful batch can back-fill
// IDs and remove dirty keys, so a later failure leaves the rest retryable.
func (s *ChargerStore) doFlushLocked() error {
	if len(s.dirty) == 0 {
		return nil
	}
	capacity := min(len(s.dirty), chargerWriteBatch)
	batch := make([]models.Charger, 0, capacity)
	keys := make([][20]byte, 0, capacity)
	flush := func() error {
		if err := s.db.Save(&batch).Error; err != nil {
			return fmt.Errorf("flush %d chargers: %w", len(batch), err)
		}
		for i, key := range keys {
			if cc, id := s.chargers[key], uint32(batch[i].ID); id != 0 && id != cc.id {
				cc.id = id
				s.chargers[key] = cc
			}
			delete(s.dirty, key)
		}
		batch, keys = batch[:0], keys[:0]
		return nil
	}
	for key := range s.dirty {
		batch = append(batch, *s.toModel(key, s.chargers[key]))
		keys = append(keys, key)
		if len(batch) == capacity {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if len(batch) > 0 {
		return flush()
	}
	return nil
}

func (s *ChargerStore) FlushDirty() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.doFlushLocked(); err != nil {
		s.logger.Errorf("Keeping dirty chargers for retry: %v", err)
	}
}

func (s *ChargerStore) Refresh(resolve func(string) string, isExchange func([20]byte) (string, bool)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.refreshLocked(resolve, isExchange, s.doFlushLocked)
}

// refreshLocked performs the in-memory part of Refresh. Caller holds the write lock.
func (s *ChargerStore) refreshLocked(resolve func(string) string, isExchange func([20]byte) (string, bool), flush func() error) error {
	if err := flush(); err != nil {
		return err
	}
	// Resolve only names used by chargers, not historical dictionary entries
	// or conflict backups. The resolver can promote a spelling in the exchange
	// registry, so settle those promotions before using the cached results.
	active := make([]bool, len(s.names))
	for _, cc := range s.chargers {
		active[cc.exchangeIdx] = true
	}
	trimmed := make([]string, len(active))
	for idx, used := range active {
		if used {
			trimmed[idx] = common.TrimExchangeName(s.names[idx])
			resolve(trimmed[idx])
		}
	}
	normalized := make([]uint16, len(active))
	for idx, used := range active {
		if used {
			normalized[idx] = s.internName(resolve(trimmed[idx]))
		}
	}

	for key, cc := range s.chargers {
		changed := false

		// Cancel fake only on a name change and when there is no backup.
		normalizedIdx := normalized[cc.exchangeIdx]
		if normalizedIdx != cc.exchangeIdx {
			cc.exchangeIdx = normalizedIdx
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
			if exName, isEx := isExchange(bk); isEx {
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
			if len(s.dirty) == chargerWriteBatch {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
	return flush()
}
