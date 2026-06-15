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

// buildConcentration ranks addresses by the given metric and reports each
// entry's share of the network-wide total.
//
// total is summed over ALL stats BEFORE truncation, so it stays the true
// denominator; list is the top n by metric (n clamped to [0, len]); topNSum is
// the combined value of the returned entries — the head's aggregate share.
func buildConcentration(stats []*models.UserStatistic, metric string, n int) (total, topNSum int64, list []*ConcentrationEntry) {
	list = make([]*ConcentrationEntry, 0, len(stats))
	for _, s := range stats {
		v, _ := s.Metric(metric)
		total += v
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
	return total, topNSum, list
}

// topTransferConcentration ranks the most concentrated from/to addresses by a
// chosen UserStatistic metric (default trx_total) over [start_date, +days),
// returning each address's share plus the top-N combined share. It reads the
// pre-aggregated from_stats_/to_stats_ tables, so callers don't need the raw
// /q SQL endpoint.
func (s *Server) topTransferConcentration(c *gin.Context) {
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

	statsMap := s.db.GetTransferStatisticByDateDays(startDate, days, direction)
	stats := make([]*models.UserStatistic, 0, len(statsMap))
	for _, st := range statsMap {
		stats = append(stats, st)
	}

	total, topNSum, list := buildConcentration(stats, metric, n)

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
