package database

import (
	"bufio"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/maphash"
	"io"
	"math/big"
	"os"
	"path/filepath"

	"tron-tracker/database/models"
	"tron-tracker/database/models/types"
)

type dailyStatMask uint8

const (
	statOwner dailyStatMask = 1 << iota
	statRecipient
	statToken
	statTokenSender
	statTokenRecipient
)

// Each record carries only the fields needed by the per-key aggregators. A
// transaction is written once per destination bucket, with a mask of the keys
// it contributes to there. Order within a key matches the source ID order.
type dailyStatRecord struct {
	Mask                                                                          dailyStatMask
	Owner, From, To, Token, Amount                                                string
	Type, Result, SigCount                                                        uint8
	Fee, EnergyTotal, EnergyFee, EnergyUsage, EnergyOriginUsage, NetUsage, NetFee int64
}

func newDailyStatRecord(tx *models.Transaction) dailyStatRecord {
	record := dailyStatRecord{
		Owner: tx.OwnerAddr, From: tx.FromAddr, To: tx.ToAddr, Token: tx.Name, Amount: tx.Amount.String(),
		Type: tx.Type, Result: tx.Result, SigCount: tx.SigCount,
		Fee: tx.Fee, EnergyTotal: tx.EnergyTotal, EnergyFee: tx.EnergyFee, EnergyUsage: tx.EnergyUsage,
		EnergyOriginUsage: tx.EnergyOriginUsage, NetUsage: tx.NetUsage, NetFee: tx.NetFee,
	}
	if tx.OwnerAddr != "total" {
		record.Mask |= statOwner
	}
	if tx.ToAddr != "" {
		record.Mask |= statRecipient
	}
	if tx.Name != "" {
		if tx.Name != "total" {
			record.Mask |= statToken
		}
		if tx.FromAddr != "" && tx.ToAddr != "" {
			record.Mask |= statTokenSender | statTokenRecipient
		}
	}
	return record
}

func (r dailyStatRecord) transaction() (models.Transaction, error) {
	var amount *big.Int
	if r.Amount != "<nil>" {
		var ok bool
		if amount, ok = new(big.Int).SetString(r.Amount, 10); !ok {
			return models.Transaction{}, fmt.Errorf("invalid amount %q in daily stats spill", r.Amount)
		}
	}
	return models.Transaction{
		OwnerAddr: r.Owner, FromAddr: r.From, ToAddr: r.To, Name: r.Token, Amount: types.NewBigInt(amount),
		Type: r.Type, Result: r.Result, SigCount: r.SigCount,
		Fee: r.Fee, EnergyTotal: r.EnergyTotal, EnergyFee: r.EnergyFee, EnergyUsage: r.EnergyUsage,
		EnergyOriginUsage: r.EnergyOriginUsage, NetUsage: r.NetUsage, NetFee: r.NetFee,
	}, nil
}

const (
	dailyStatsBucketBits  = 7
	dailyStatsSplitBits   = 4
	dailyStatsBucketKeys  = 125_000
	dailyStatsSpillBuffer = 32 * 1024
)

type dailyStatBucket struct {
	file    *os.File
	buf     *bufio.Writer
	encoder *gob.Encoder
	path    string
}

type dailyStatSpill struct {
	dir         string
	seed        maphash.Seed
	shift, bits uint
	buckets     []dailyStatBucket
}

func newDailyStatSpill(dir string, seed maphash.Seed, shift, bits uint) *dailyStatSpill {
	return &dailyStatSpill{dir: dir, seed: seed, shift: shift, bits: bits, buckets: make([]dailyStatBucket, 1<<bits)}
}

func (s *dailyStatSpill) add(record dailyStatRecord) error {
	keys := [...]string{record.Owner, record.To, record.Token, record.From + record.Token, record.To + record.Token}
	var masks [1 << dailyStatsBucketBits]dailyStatMask
	var used [len(keys)]int
	n := 0
	for i, key := range keys {
		mask := dailyStatMask(1 << i)
		if record.Mask&mask == 0 {
			continue
		}
		bucket := int(maphash.String(s.seed, key)>>s.shift) & (len(s.buckets) - 1)
		if masks[bucket] == 0 {
			used[n] = bucket
			n++
		}
		masks[bucket] |= mask
	}
	for _, index := range used[:n] {
		bucket := &s.buckets[index]
		if bucket.file == nil {
			path := filepath.Join(s.dir, fmt.Sprintf("%03d", index))
			file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			if err != nil {
				return err
			}
			bucket.path = path
			bucket.file = file
			bucket.buf = bufio.NewWriterSize(file, dailyStatsSpillBuffer)
			bucket.encoder = gob.NewEncoder(bucket.buf)
		}
		record.Mask = masks[index]
		if err := bucket.encoder.Encode(&record); err != nil {
			return err
		}
	}
	return nil
}

func (s *dailyStatSpill) close() error {
	var errs []error
	for i := range s.buckets {
		bucket := &s.buckets[i]
		if bucket.file != nil {
			errs = append(errs, bucket.buf.Flush(), bucket.file.Close())
			bucket.file, bucket.buf, bucket.encoder = nil, nil, nil
		}
	}
	return errors.Join(errs...)
}

func (s *dailyStatSpill) drain(limit int, write func(*dbCache) error) error {
	for _, bucket := range s.buckets {
		if bucket.path == "" {
			continue
		}
		cache, err := aggregateDailyStatBucket(bucket.path, limit)
		if errors.Is(err, errDailyStatsBucketFull) {
			if err := s.split(bucket.path, limit, write); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		if err := write(cache); err != nil {
			return err
		}
		if err := os.Remove(bucket.path); err != nil {
			return err
		}
	}
	return nil
}

func (s *dailyStatSpill) split(path string, limit int, write func(*dbCache) error) error {
	shift := s.shift + s.bits
	if shift >= 64 {
		return fmt.Errorf("daily stats bucket %s still exceeds %d keys after splitting", path, limit)
	}
	dir := path + ".split"
	if err := os.Mkdir(dir, 0700); err != nil {
		return err
	}
	child := newDailyStatSpill(dir, s.seed, shift, min(dailyStatsSplitBits, 64-shift))
	if err := errors.Join(readDailyStatBucket(path, child.add), child.close()); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	return child.drain(limit, write)
}

var errDailyStatsBucketFull = errors.New("daily stats bucket full")

func aggregateDailyStatBucket(path string, limit int) (*dbCache, error) {
	cache := newCache()
	err := readDailyStatBucket(path, func(record dailyStatRecord) error {
		tx, err := record.transaction()
		if err != nil {
			return err
		}
		cache.accumulate(record.Mask, &tx)
		if len(cache.fromStats)+len(cache.toStats)+len(cache.tokenStats)+len(cache.userTokenStats) > limit {
			return errDailyStatsBucketFull
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return cache, nil
}

func readDailyStatBucket(path string, apply func(dailyStatRecord) error) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	decoder := gob.NewDecoder(bufio.NewReaderSize(file, dailyStatsSpillBuffer))
	for {
		var record dailyStatRecord
		if err := decoder.Decode(&record); err == io.EOF {
			return nil
		} else if err != nil {
			return fmt.Errorf("read daily stats bucket %s: %w", path, err)
		}
		if err := apply(record); err != nil {
			return err
		}
	}
}
