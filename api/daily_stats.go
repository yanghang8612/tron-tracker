package api

import (
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"tron-tracker/database"
)

// maxDailyStatsDays caps the per-day aggregate endpoints below so one request
// never fans out over an unbounded number of per-day tables. Each covered day
// is one heavy aggregate query (up to ~2.5 min measured on production-sized
// tables), so the cap bounds worst-case runtime to roughly 20 minutes; callers
// needing longer ranges paginate by start_date.
const maxDailyStatsDays = 7

// forEachStatsDay iterates the [start_date, start_date+days) window, invoking
// visit once per day. Days whose backing tables are missing (outside the
// retention window) are collected into the returned missing list instead of
// failing the whole request; any other error aborts and is reported.
func (s *Server) forEachStatsDay(c *gin.Context, visit func(date time.Time, day string) error) (days int, missing []string, ok bool) {
	startDate, days, prepared := prepareStartDateAndDays(c, yesterday(), 1)
	if !prepared {
		return 0, nil, false
	}
	if days < 1 {
		days = 1
	}
	if days > maxDailyStatsDays {
		days = maxDailyStatsDays
	}

	for i := 0; i < days; i++ {
		date := startDate.AddDate(0, 0, i)
		day := date.Format("2006-01-02")
		if err := visit(date, day); err != nil {
			// Error 1146: table doesn't exist — the day is outside the raw-table
			// retention window (or not yet built). Report it, keep the rest.
			if strings.Contains(err.Error(), "1146") {
				missing = append(missing, day)
				continue
			}
			c.JSON(200, gin.H{"code": 500, "error": err.Error()})
			return 0, nil, false
		}
	}
	return days, missing, true
}

// tokenAmountStats groups a token's transfers by amount digit length
// (LENGTH(amount)) per day: transfer count, burned fee, stake-covered energy
// and raw amount sum per bucket. Replaces the equivalent /q raw query used by
// downstream dashboards for USDT transfer-size cost structure.
func (s *Server) tokenAmountStats(c *gin.Context) {
	token := c.DefaultQuery("token", "USDT")
	tokenAddr := s.db.GetTokenAddress(token)
	if tokenAddr == "" {
		c.JSON(200, gin.H{"code": 400, "error": "unknown token [" + token + "]"})
		return
	}

	type dayBuckets struct {
		Date    string                       `json:"date"`
		Buckets []database.TokenAmountBucket `json:"buckets"`
	}
	daily := make([]dayBuckets, 0)
	days, missing, ok := s.forEachStatsDay(c, func(date time.Time, day string) error {
		buckets, err := s.db.GetTokenAmountBucketsByDate(date, tokenAddr)
		if err != nil {
			return err
		}
		daily = append(daily, dayBuckets{Date: day, Buckets: buckets})
		return nil
	})
	if !ok {
		return
	}

	c.JSON(200, gin.H{
		"token":         token,
		"token_address": tokenAddr,
		"days":          days,
		"daily":         daily,
		"missing_dates": missing,
	})
}

// typeFeeStats groups each day's full transaction table by type: count and
// burned fee per transaction type. Types are raw stored codes (energy-resource
// variants carry base+100); consumers merge by type%100. The per-day fee sum
// reconciles exactly with /total_statistics fee.
func (s *Server) typeFeeStats(c *gin.Context) {
	type dayTypes struct {
		Date  string                 `json:"date"`
		Types []database.TypeFeeStat `json:"types"`
	}
	daily := make([]dayTypes, 0)
	days, missing, ok := s.forEachStatsDay(c, func(date time.Time, day string) error {
		stats, err := s.db.GetTypeFeeStatsByDate(date)
		if err != nil {
			return err
		}
		daily = append(daily, dayTypes{Date: day, Types: stats})
		return nil
	})
	if !ok {
		return
	}

	c.JSON(200, gin.H{
		"days":          days,
		"daily":         daily,
		"missing_dates": missing,
	})
}

// addrActivityStats returns per-day address-activity scalars: distinct
// originating addresses, exact median of per-address tx counts, active
// exchange-charger addresses (fake excluded) and native TRX transfer
// count/amount. The charger count joins from_stats against the chargers table,
// so it needs both tables for the day.
func (s *Server) addrActivityStats(c *gin.Context) {
	type dayActivity struct {
		Date string `json:"date"`
		*database.AddrActivityStat
	}
	daily := make([]dayActivity, 0)
	days, missing, ok := s.forEachStatsDay(c, func(date time.Time, day string) error {
		stat, err := s.db.GetAddrActivityByDate(date)
		if err != nil {
			return err
		}
		daily = append(daily, dayActivity{Date: day, AddrActivityStat: stat})
		return nil
	})
	if !ok {
		return
	}

	c.JSON(200, gin.H{
		"days":          days,
		"daily":         daily,
		"missing_dates": missing,
	})
}

// collectEnergyProviders returns, per day, who delegated ENERGY to each
// exchange's charger addresses (type 157, provider = owner_addr, fake chargers
// excluded). Chargers hold no stake, so their collect sweeps run on these
// just-in-time delegations — the flow-level answer to "whose energy do
// exchange collects burn".
func (s *Server) collectEnergyProviders(c *gin.Context) {
	type dayProviders struct {
		Date      string                           `json:"date"`
		Providers []database.CollectEnergyProvider `json:"providers"`
	}
	daily := make([]dayProviders, 0)
	days, missing, ok := s.forEachStatsDay(c, func(date time.Time, day string) error {
		providers, err := s.db.GetCollectEnergyProvidersByDate(date)
		if err != nil {
			return err
		}
		daily = append(daily, dayProviders{Date: day, Providers: providers})
		return nil
	})
	if !ok {
		return
	}

	c.JSON(200, gin.H{
		"days":          days,
		"daily":         daily,
		"missing_dates": missing,
	})
}
