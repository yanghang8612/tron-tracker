package api

import (
	"sort"

	"tron-tracker/common"
	"tron-tracker/database/models"

	"github.com/gin-gonic/gin"
)

// ConcentrationEntry is one address's share of a metric within a direction.
type ConcentrationEntry struct {
	Address string `json:"address"`
	Value   int64  `json:"value"`
	Percent string `json:"percent"`
	Tag     string `json:"tag,omitempty"`
}

// buildConcentration ranks the given head rows by metric and reports each entry's
// share of total — the network-wide denominator passed in by the caller (from the
// total row / SUM), since stats here are only the per-day top-n candidates, not
// the whole network. list is the top n (n clamped to [0, len]); topNSum is the
// combined value of the returned entries.
func buildConcentration(stats []*models.UserStatistic, metric string, total int64, n int) (topNSum int64, list []*ConcentrationEntry) {
	list = make([]*ConcentrationEntry, 0, len(stats))
	for _, s := range stats {
		v, _ := s.Metric(metric)
		list = append(list, &ConcentrationEntry{Address: s.Address, Value: v})
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Value > list[j].Value })

	if n < 0 {
		n = 0
	}
	if len(list) > n {
		list = list[:n]
	}

	for _, e := range list {
		e.Percent = common.FormatOfPercent(total, e.Value)
		topNSum += e.Value
	}
	return topNSum, list
}

// topAddrs ranks the most concentrated from/to addresses by a
// chosen UserStatistic metric (default trx_total) over [start_date, +days),
// returning each address's share plus the top-N combined share. It reads the
// pre-aggregated from_stats_/to_stats_ tables, so callers don't need the raw
// /q SQL endpoint.
func (s *Server) topAddrs(c *gin.Context) {
	startDate, days, ok := prepareStartDateAndDays(c, lastWeek(), 7)
	if !ok {
		return
	}

	n, ok := getIntParam(c, "n", 50)
	if !ok {
		return
	}

	direction := c.DefaultQuery("direction", "from")
	if direction != "from" && direction != "to" {
		c.JSON(200, gin.H{"code": 400, "error": "direction must be 'from' or 'to'"})
		return
	}

	metric := c.DefaultQuery("metric", "trx_total")
	// Validate against the UserStatistic whitelist before querying, so an unknown
	// column can't silently rank everything by zero.
	if _, ok := (&models.UserStatistic{}).Metric(metric); !ok {
		c.JSON(200, gin.H{"code": 400, "error": "unsupported metric: " + metric})
		return
	}

	// Denominator and head are fetched separately: the total is exact (total row /
	// SUM), while the list is each day's DB-side top-n merged — so we never pull
	// the full multi-million-row table into memory.
	total := s.db.GetTransferTotalByDateDays(startDate, days, direction, metric)
	topStats := s.db.GetTopTransferStatsByDateDays(startDate, days, direction, metric, n)
	topNSum, list := buildConcentration(topStats, metric, total, n)

	for _, e := range list {
		if s.db.IsExchange(e.Address) {
			e.Tag = s.db.GetExchange(e.Address).Name
		}
	}

	c.JSON(200, gin.H{
		"direction":     direction,
		"metric":        metric,
		"date_range":    startDate.Format("060102") + "~" + startDate.AddDate(0, 0, days-1).Format("060102"),
		"total":         total,
		"top_n_sum":     topNSum,
		"top_n_percent": common.FormatOfPercent(total, topNSum),
		"list":          list,
	})
}
