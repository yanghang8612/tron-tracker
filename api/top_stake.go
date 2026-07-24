package api

import (
	"math/big"
	"sort"

	"github.com/gin-gonic/gin"

	"tron-tracker/database/models"
)

// maxTopStakeDays caps the /top_stake window so a single request never sweeps an
// unbounded number of per-day transaction tables.
const maxTopStakeDays = 90

// topStake ranks the top-n staking and unstaking accounts over a [start_date,
// start_date+days) window, recomputed on the fly from the per-day transaction
// tables. Read-only: it persists nothing. Mirrors java-tron's /top_stake but
// aggregates by day instead of by maintenance cycle.
func (s *Server) topStake(c *gin.Context) {
	startDate, days, ok := prepareStartDateAndDays(c, yesterday(), 1)
	if !ok {
		return
	}

	n, ok := getIntParam(c, "n", 20)
	if !ok {
		return
	}

	if days < 1 {
		days = 1
	}
	if days > maxTopStakeDays {
		days = maxTopStakeDays
	}

	txs := s.db.GetStakeRelatedTxsByDateDays(startDate, days)
	topStake, topUnstake := buildTopStake(txs, n)

	c.JSON(200, gin.H{
		"start_date":  startDate.Format("2006-01-02"),
		"days":        days,
		"n":           n,
		"top_stake":   topStake,
		"top_unstake": topUnstake,
	})
}

// StakeEntry is one row of a /top_stake ranking. Amounts are raw sun, rendered
// as decimal strings (they can exceed int64-friendly JSON numbers).
type StakeEntry struct {
	Address  string `json:"address"`
	Stake2   string `json:"stake2"`
	Cancel   string `json:"cancel"`
	Unstake  string `json:"unstake"`
	Unstake2 string `json:"unstake2"`
	Score    string `json:"score"`
	// Per-resource splits of the components above. ENERGY variants carry
	// type = base + 100 in the transaction tables, so the split is exact and
	// needs no schema change; bandwidth = total - energy by construction.
	// Cancel (59) has no resource dimension (it cancels all pending
	// unfreezes at once) and is deliberately not split.
	Stake2Energy      string `json:"stake2_energy"`
	Stake2Bandwidth   string `json:"stake2_bandwidth"`
	UnstakeEnergy     string `json:"unstake_energy"`
	UnstakeBandwidth  string `json:"unstake_bandwidth"`
	Unstake2Energy    string `json:"unstake2_energy"`
	Unstake2Bandwidth string `json:"unstake2_bandwidth"`
}

type stakeAgg struct {
	addr     string
	stake2   *big.Int
	cancel   *big.Int
	unstake  *big.Int
	unstake2 *big.Int
	// ENERGY-resource portions of stake2/unstake/unstake2. The BANDWIDTH
	// portion is derived as total - energy when rendering, so only one extra
	// accumulator per component is needed.
	stake2E   *big.Int
	unstakeE  *big.Int
	unstake2E *big.Int
}

// buildTopStake aggregates freeze/unfreeze transactions by owner and returns the
// top-n stake board (ranked by stake2+cancel) and top-n unstake board (ranked by
// unstake+unstake2), mirroring java-tron's TopStakeServlet.
func buildTopStake(txs []*models.Transaction, n int) (topStake, topUnstake []StakeEntry) {
	aggs := make(map[string]*stakeAgg)
	get := func(addr string) *stakeAgg {
		a := aggs[addr]
		if a == nil {
			a = &stakeAgg{
				addr:   addr,
				stake2: new(big.Int), cancel: new(big.Int),
				unstake: new(big.Int), unstake2: new(big.Int),
				stake2E: new(big.Int), unstakeE: new(big.Int), unstake2E: new(big.Int),
			}
			aggs[addr] = a
		}
		return a
	}

	for _, tx := range txs {
		amt := big.NewInt(tx.Amount.Int64())
		a := get(tx.OwnerAddr)
		// ENERGY-resource variants carry type = base + 100; fold by type%100 so
		// they accumulate into the same component as their BANDWIDTH counterpart.
		isEnergy := tx.Type >= 100
		switch tx.Type % 100 {
		case 54:
			a.stake2.Add(a.stake2, amt)
			if isEnergy {
				a.stake2E.Add(a.stake2E, amt)
			}
		case 59:
			a.cancel.Add(a.cancel, amt)
		case 55:
			a.unstake2.Add(a.unstake2, amt)
			if isEnergy {
				a.unstake2E.Add(a.unstake2E, amt)
			}
		case 12:
			a.unstake.Add(a.unstake, amt)
			if isEnergy {
				a.unstakeE.Add(a.unstakeE, amt)
			}
		}
	}

	rank := func(score func(a *stakeAgg) *big.Int) []StakeEntry {
		list := make([]*stakeAgg, 0, len(aggs))
		for _, a := range aggs {
			list = append(list, a)
		}
		sort.Slice(list, func(i, j int) bool {
			// Tie-break by address so identical requests return identical order
			// (scores alone leave ties at the mercy of random map iteration).
			if c := score(list[i]).Cmp(score(list[j])); c != 0 {
				return c > 0
			}
			return list[i].addr < list[j].addr
		})
		out := make([]StakeEntry, 0, n)
		for i := 0; i < n && i < len(list); i++ {
			a := list[i]
			out = append(out, StakeEntry{
				Address:  a.addr,
				Stake2:   a.stake2.String(),
				Cancel:   a.cancel.String(),
				Unstake:  a.unstake.String(),
				Unstake2: a.unstake2.String(),
				Score:    score(a).String(),

				Stake2Energy:      a.stake2E.String(),
				Stake2Bandwidth:   new(big.Int).Sub(a.stake2, a.stake2E).String(),
				UnstakeEnergy:     a.unstakeE.String(),
				UnstakeBandwidth:  new(big.Int).Sub(a.unstake, a.unstakeE).String(),
				Unstake2Energy:    a.unstake2E.String(),
				Unstake2Bandwidth: new(big.Int).Sub(a.unstake2, a.unstake2E).String(),
			})
		}
		return out
	}

	stakeScore := func(a *stakeAgg) *big.Int { return new(big.Int).Add(a.stake2, a.cancel) }
	unstakeScore := func(a *stakeAgg) *big.Int { return new(big.Int).Add(a.unstake, a.unstake2) }
	return rank(stakeScore), rank(unstakeScore)
}
