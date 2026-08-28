package database

import (
	"fmt"
	"time"

	"tron-tracker/database/models"
)

// Aggregated daily statistics pushed down to MySQL. These back the
// /token_amount_stats, /type_fee_stats and /addr_activity_stats endpoints and
// replace ad-hoc /q raw queries with the same semantics. Every method operates
// on a single UTC day table and returns aggregate rows only — the per-day
// transaction tables are never streamed into Go memory.
//
// Amount sums: amount is a decimal string in a uint256 domain — up to 78
// digits (production holds real uint256-max rows, 77 digits), beyond
// DECIMAL's 65-digit ceiling, where CAST saturates to all-nines and one row
// poisons the whole sum. Every SUM therefore clamps via
// IF(amount = '' OR amount = '<nil>' OR LENGTH(amount) > 65, '0', amount)
// cast to DECIMAL(65,0): malformed or overflow-range amounts count as zero
// toward the sum while the row still counts toward tx_count/fee. The same
// guard (with a tighter uint64 bound) is the established pattern in the
// top-delegate queries.

// TokenAmountBucket is one LENGTH(amount) bucket of a token's transfers for one
// day. digit_len N (N>=2) covers raw amounts in [10^(N-1), 10^N); digit_len 1
// covers 0-9. Amount is a decimal string: a full-day SUM over CAST(amount AS
// DECIMAL) can exceed comfortable JSON integer range.
type TokenAmountBucket struct {
	DigitLen    int    `json:"digit_len"`
	TxCount     int64  `json:"tx_count"`
	Fee         int64  `json:"fee"`
	StakeEnergy int64  `json:"stake_energy"`
	Amount      string `json:"amount"`
}

// GetTokenAmountBucketsByDate groups one day's transfers of the given token
// contract by LENGTH(amount), returning per-bucket count, burned fee (sun),
// stake-covered energy (energy_usage + energy_origin_usage) and raw amount sum.
// Mirrors the per-bucket token statistic filters (statistic.go USDTStats.Add):
// TriggerSmartContract only (type 31 and its +100 energy variant), non-empty
// sender, successful result only — failed calls transfer nothing, and
// excluding them keeps these buckets reconcilable with the existing
// transfer-distribution statistic.
func (db *RawDB) GetTokenAmountBucketsByDate(date time.Time, tokenAddr string) ([]TokenAmountBucket, error) {
	table := "transactions_" + date.Format("060102")
	var buckets []TokenAmountBucket
	err := db.db.Raw(fmt.Sprintf(`
		SELECT LENGTH(amount)                                            AS digit_len,
		       COUNT(*)                                                  AS tx_count,
		       COALESCE(SUM(fee), 0)                                     AS fee,
		       COALESCE(SUM(energy_usage + energy_origin_usage), 0)      AS stake_energy,
		       CAST(COALESCE(SUM(CAST(IF(amount = '' OR amount = '<nil>' OR LENGTH(amount) > 65, '0', amount) AS DECIMAL(65,0))), 0) AS CHAR) AS amount
		FROM %s
		WHERE name = ? AND type IN (31, 131) AND from_addr <> '' AND result = 1
		GROUP BY LENGTH(amount)
		ORDER BY digit_len`, table), tokenAddr).Scan(&buckets).Error
	return buckets, err
}

// TypeFeeStat is one transaction-type row of a day's fee breakdown. Type is the
// raw stored code (energy-resource variants carry base+100, e.g. 131 for 31);
// callers merge by type%100 when rendering.
type TypeFeeStat struct {
	Type    int   `json:"type"`
	TxCount int64 `json:"tx_count"`
	Fee     int64 `json:"fee"`
}

// GetTypeFeeStatsByDate groups one day's transaction table by type, returning
// per-type count and burned fee (sun). Synthesized transfer rows (type 255,
// models.TransferType) are excluded — their fee duplicates the originating
// transaction's, and 255%100 would masquerade as type 55; this mirrors the
// `type <> 255` scan in flushDailyStats, so the per-type fee sum stays
// reconcilable with total_statistics. Failed transactions are deliberately
// kept: their fee is genuinely burned and total_statistics counts them too.
func (db *RawDB) GetTypeFeeStatsByDate(date time.Time) ([]TypeFeeStat, error) {
	table := "transactions_" + date.Format("060102")
	var stats []TypeFeeStat
	err := db.db.Raw(fmt.Sprintf(`
		SELECT type, COUNT(*) AS tx_count, COALESCE(SUM(fee), 0) AS fee
		FROM %s
		WHERE type <> %d
		GROUP BY type
		ORDER BY type`, table, models.TransferType)).Scan(&stats).Error
	return stats, err
}

// AddrActivityStat is one day's address-activity scalar set.
type AddrActivityStat struct {
	FromAddrCount      int64  `json:"from_addr_count"`
	AddrTxMedian       int64  `json:"addr_tx_median"`
	ChargerActiveCount int64  `json:"charger_active_count"`
	TrxTransferCount   int64  `json:"trx_transfer_count"`
	TrxAmount          string `json:"trx_amount"`
}

// GetAddrActivityByDate computes one day's address-activity scalars:
//   - from_addr_count: distinct originating addresses (from_stats rows)
//   - addr_tx_median: exact median of per-address daily tx counts
//     (ORDER BY tx_total OFFSET n/2 — n is in the millions, so this stays in SQL)
//   - charger_active_count: distinct active addresses known as exchange
//     chargers (fake chargers excluded)
//   - trx_transfer_count / trx_amount: native TRX transfers (type 1) count and
//     raw sun sum (decimal string)
func (db *RawDB) GetAddrActivityByDate(date time.Time) (*AddrActivityStat, error) {
	day := date.Format("060102")
	fromStats := "from_stats_" + day
	txTable := "transactions_" + day
	stat := &AddrActivityStat{}

	if err := db.db.Raw(fmt.Sprintf(
		`SELECT COUNT(*) FROM %s WHERE address <> 'total'`, fromStats)).
		Scan(&stat.FromAddrCount).Error; err != nil {
		return nil, err
	}

	if stat.FromAddrCount > 0 {
		// Lower median: OFFSET (n-1)/2 picks the smaller middle element on even
		// n, keeping the value an integer an address actually has (an arithmetic
		// mean of the two middles would be fractional and match no address).
		if err := db.db.Raw(fmt.Sprintf(
			`SELECT tx_total FROM %s WHERE address <> 'total' ORDER BY tx_total LIMIT 1 OFFSET %d`,
			fromStats, (stat.FromAddrCount-1)/2)).
			Scan(&stat.AddrTxMedian).Error; err != nil {
			return nil, err
		}
	}

	if err := db.db.Raw(fmt.Sprintf(`
		SELECT COUNT(DISTINCT f.address)
		FROM %s f JOIN chargers c ON f.address = c.address
		WHERE f.address <> 'total' AND (c.is_fake = 0 OR c.is_fake IS NULL)`, fromStats)).
		Scan(&stat.ChargerActiveCount).Error; err != nil {
		return nil, err
	}

	var trx struct {
		TrxTransferCount int64
		TrxAmount        string
	}
	// result = 1: failed transfers move no TRX, so counting them would
	// overstate both the transfer count and the amount.
	if err := db.db.Raw(fmt.Sprintf(`
		SELECT COUNT(*)                                                      AS trx_transfer_count,
		       CAST(COALESCE(SUM(CAST(IF(amount = '' OR amount = '<nil>' OR LENGTH(amount) > 65, '0', amount) AS DECIMAL(65,0))), 0) AS CHAR) AS trx_amount
		FROM %s WHERE type = 1 AND result = 1`, txTable)).
		Scan(&trx).Error; err != nil {
		return nil, err
	}
	stat.TrxTransferCount = trx.TrxTransferCount
	stat.TrxAmount = trx.TrxAmount

	return stat, nil
}

// CollectEnergyProvider is one (exchange, provider) row of a day's energy
// delegation inflow to exchange charger addresses. DelegatedAmount is raw sun
// as a decimal string (per-provider daily sums exceed comfortable JSON ints).
type CollectEnergyProvider struct {
	Exchange        string `json:"exchange"`
	Provider        string `json:"provider"`
	TxCount         int64  `json:"tx_count"`
	DelegatedAmount string `json:"delegated_amount"`
}

// GetCollectEnergyProvidersByDate aggregates one day's ENERGY resource
// delegations (type 157, provider in owner_addr, successful only) that target
// known exchange charger addresses (fake chargers excluded), grouped by
// exchange and provider. This answers "who supplies the energy behind each
// exchange's collect sweeps": chargers hold no stake of their own, so collect
// energy arrives almost entirely through these just-in-time delegations.
//
// The join assumes chargers.address is unique — ChargerStore's in-memory map
// is keyed by address and production currently holds zero duplicates
// (COUNT(*) = COUNT(DISTINCT address)); a duplicate row would double-count
// that address's delegations. Deduplicating in-query (derived GROUP BY over
// the ~40M-row chargers table) measured out at >8 min per request, so the
// invariant is better enforced with a unique index on chargers(address).
func (db *RawDB) GetCollectEnergyProvidersByDate(date time.Time) ([]CollectEnergyProvider, error) {
	table := "transactions_" + date.Format("060102")
	var providers []CollectEnergyProvider
	err := db.db.Raw(fmt.Sprintf(`
		SELECT c.exchange_name                                               AS exchange,
		       t.owner_addr                                                  AS provider,
		       COUNT(*)                                                      AS tx_count,
		       CAST(COALESCE(SUM(CAST(IF(t.amount = '' OR t.amount = '<nil>' OR LENGTH(t.amount) > 65, '0', t.amount) AS DECIMAL(65,0))), 0) AS CHAR) AS delegated_amount
		FROM %s t
		JOIN chargers c ON t.to_addr = c.address
		WHERE t.type = 157 AND t.result = 1 AND (c.is_fake = 0 OR c.is_fake IS NULL)
		GROUP BY c.exchange_name, t.owner_addr
		ORDER BY exchange, SUM(CAST(IF(t.amount = '' OR t.amount = '<nil>' OR LENGTH(t.amount) > 65, '0', t.amount) AS DECIMAL(65,0))) DESC`, table)).
		Scan(&providers).Error
	return providers, err
}
