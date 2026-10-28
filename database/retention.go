package database

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"tron-tracker/database/models"
)

// Daily tables are partitioned by UTC date. Keep the anniversary date itself,
// including today's partial table, rather than using a fixed 365-day duration.
func dailyTableRetentionCutoff(now time.Time) time.Time {
	now = now.UTC()
	return time.Date(now.Year()-1, now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
}

// transactionRetentionCutoff also protects source tables still needed by the
// tracker and daily/weekly statistics during a historical catch-up. The daily
// cursors are completed dates; countedWeek is the next unprocessed week's start.
func transactionRetentionCutoff(cutoff string, cursors map[string]string) (string, error) {
	for _, cursor := range []struct {
		name      string
		value     string
		completed bool
	}{
		{models.TrackingDateKey, cursors[models.TrackingDateKey], false},
		{models.FlushedDateKey, cursors[models.FlushedDateKey], true},
		{models.CountedDateKey, cursors[models.CountedDateKey], true},
		{models.CountedWeekKey, cursors[models.CountedWeekKey], false},
	} {
		date, err := time.Parse("060102", cursor.value)
		if err != nil || len(cursor.value) != 6 {
			return "", fmt.Errorf("invalid %s %q, refusing transaction table cleanup", cursor.name, cursor.value)
		}
		if cursor.completed {
			date = date.AddDate(0, 0, 1)
		}
		if value := date.Format("060102"); value < cutoff {
			cutoff = value
		}
	}
	return cutoff, nil
}

// DropStaleStatsTables drops expired UTC daily transaction and address-statistic
// tables after a daily statistics flush. Only exact PREFIX_YYMMDD base-table
// names are eligible; cutoff and newer dates are kept. Other table families keep
// their existing retention policy. Errors remain non-fatal to ingestion, but are
// returned so cleanup failures are visible and can be retried on the next flush.
func (db *RawDB) DropStaleStatsTables(cutoff time.Time) error {
	if cutoff.After(time.Now()) {
		return fmt.Errorf("cutoff %s is in the future, refusing to drop", cutoff.Format("060102"))
	}
	cutoffStr := cutoff.UTC().Format("060102")
	var cleanupErrors []error
	for _, prefix := range []string{"from_stats_", "to_stats_", "user_token_stats_", "transactions_"} {
		tableCutoff := cutoffStr
		if prefix == "transactions_" {
			// Read persisted cursors in one query. The counting loops update their
			// in-memory strings concurrently; cleanup must not race those writes.
			var meta []models.Meta
			if err := db.db.Where("`key` IN ?", []string{
				models.TrackingDateKey, models.FlushedDateKey, models.CountedDateKey, models.CountedWeekKey,
			}).Find(&meta).Error; err != nil {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("read retention cursors: %w", err))
				continue
			}
			cursors := make(map[string]string, len(meta))
			for _, row := range meta {
				cursors[row.Key] = row.Val
			}
			var err error
			tableCutoff, err = transactionRetentionCutoff(cutoffStr, cursors)
			if err != nil {
				cleanupErrors = append(cleanupErrors, err)
				continue
			}
		}
		var tables []string
		like := strings.ReplaceAll(prefix, "_", `\_`) + "%"
		if err := db.db.Raw(
			"SELECT table_name FROM information_schema.tables "+
				"WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE' AND table_name LIKE ? ORDER BY table_name",
			like,
		).Scan(&tables).Error; err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("list %s tables: %w", prefix, err))
			continue
		}
		for _, table := range tables {
			if !strings.HasPrefix(table, prefix) {
				continue
			}
			suffix := strings.TrimPrefix(table, prefix)
			if len(suffix) != 6 {
				continue
			}
			if _, err := time.Parse("060102", suffix); err != nil || suffix >= tableCutoff {
				continue
			}
			db.logger.Infof("Dropping stale table %s (cutoff=%s)", table, tableCutoff)
			if err := db.db.Exec("DROP TABLE IF EXISTS `" + table + "`").Error; err != nil {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("drop %s: %w", table, err))
				continue
			}
			db.tableLock.Lock()
			delete(db.isTableMigrated, table)
			db.tableLock.Unlock()
		}
	}
	return errors.Join(cleanupErrors...)
}
