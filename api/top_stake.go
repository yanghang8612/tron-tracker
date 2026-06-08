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
}

type stakeAgg struct {
	addr     string
	stake2   *big.Int
	cancel   *big.Int
	unstake  *big.Int
	unstake2 *big.Int
}

// buildTopStake aggregates freeze/unfreeze transactions by owner and returns the
// top-n stake board (ranked by stake2+cancel) and top-n unstake board (ranked by
// unstake+unstake2), mirroring java-tron's TopStakeServlet.
func buildTopStake(txs []*models.Transaction, n int) (topStake, topUnstake []StakeEntry) {
	aggs := make(map[string]*stakeAgg)
	get := func(addr string) *stakeAgg {
		a := aggs[addr]
		if a == nil {
			a = &stakeAgg{addr: addr, stake2: new(big.Int), cancel: new(big.Int), unstake: new(big.Int), unstake2: new(big.Int)}
			aggs[addr] = a
		}
		return a
	}

	for _, tx := range txs {
		amt := big.NewInt(tx.Amount.Int64())
		a := get(tx.OwnerAddr)
		// ENERGY-resource variants carry type = base + 100; fold by type%100 so
		// they accumulate into the same component as their BANDWIDTH counterpart.
		switch tx.Type % 100 {
		case 54:
			a.stake2.Add(a.stake2, amt)
		case 59:
			a.cancel.Add(a.cancel, amt)
		case 55:
			a.unstake2.Add(a.unstake2, amt)
		case 12:
			a.unstake.Add(a.unstake, amt)
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
			})
		}
		return out
	}

	stakeScore := func(a *stakeAgg) *big.Int { return new(big.Int).Add(a.stake2, a.cancel) }
	unstakeScore := func(a *stakeAgg) *big.Int { return new(big.Int).Add(a.unstake, a.unstake2) }
	return rank(stakeScore), rank(unstakeScore)
}
