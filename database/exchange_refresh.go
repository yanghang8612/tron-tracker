package database

import (
	"errors"
	"time"

	"tron-tracker/common"
	"tron-tracker/database/models"
	"tron-tracker/net"
)

func (db *RawDB) updateExchanges() {
	db.updateExchangesAt(time.Now(), net.GetExchanges)
}

func exchangeRefreshRetryDelay(failures uint) time.Duration {
	if failures >= 5 {
		return 15 * time.Minute
	}
	if failures == 0 {
		return time.Minute
	}
	return time.Minute << (failures - 1)
}

func (db *RawDB) updateExchangesAt(now time.Time, fetch func() (*models.Exchanges, error)) {
	if now.Sub(db.exchangesUpdatedAt) < time.Hour || now.Before(db.exchangesRetryAt) {
		return
	}
	exchanges, err := fetch()
	if err == nil && (exchanges == nil || len(exchanges.Val) == 0) {
		err = errors.New("empty exchange list")
	}
	if err != nil {
		db.exchangesFailures++
		delay := exchangeRefreshRetryDelay(db.exchangesFailures)
		db.exchangesRetryAt = now.Add(delay)
		db.logger.Warnf("Exchange metadata refresh failed; keeping %d cached entries, failure_count=%d retry_in=%s: %v",
			len(db.exchanges), db.exchangesFailures, delay, err)
		return
	}

	missingOrigin, conflicts, removed := 0, 0, 0
	conflictSamples, removedSamples := make([]string, 0, 3), make([]string, 0, 3)
	seen := make(map[string]bool, len(exchanges.Val))
	for _, incoming := range exchanges.Val {
		// Work on a copy so the fetched snapshot retains its missing-field data.
		newExchange := *incoming
		seen[newExchange.Address] = true
		if newExchange.OriginName == "" {
			missingOrigin++
		}
		if oldExchange, ok := db.exchanges[newExchange.Address]; ok {
			if newExchange.OriginName == "" {
				if oldExchange.OriginName != "" {
					continue
				}
				newExchange.OriginName = newExchange.Name
			}
			if oldExchange.OriginName != newExchange.OriginName {
				if oldExchange.OriginName != "" && !common.IsSameExchange(oldExchange.OriginName, newExchange.OriginName) {
					conflicts++
					if len(conflictSamples) < 3 {
						conflictSamples = append(conflictSamples, oldExchange.Address)
					}
					continue
				}
				db.logger.Infof("Exchange origin name updated: [%s] - [%s -> %s]",
					oldExchange.Address, oldExchange.OriginName, newExchange.OriginName)
				oldExchange.OriginName = newExchange.OriginName
				db.db.Model(oldExchange).Update("origin_name", oldExchange.OriginName)
				if oldExchange.Name == "" {
					oldExchange.Name = db.resolveExchangeName(common.TrimExchangeName(oldExchange.OriginName))
					db.db.Model(oldExchange).Update("name", oldExchange.Name)
				}
			}
		} else {
			if newExchange.OriginName == "" {
				newExchange.OriginName = newExchange.Name
			}
			newExchange.Name = db.resolveExchangeName(common.TrimExchangeName(newExchange.OriginName))
			db.db.Create(&newExchange)
			db.exchanges[newExchange.Address] = &newExchange
			db.logger.Infof("New exchange added: [%s - %s] - [%s]", newExchange.Address, newExchange.OriginName, newExchange.Name)
		}
	}
	for addr, oldExchange := range db.exchanges {
		if !seen[addr] && !oldExchange.FromAsuka {
			removed++
			if len(removedSamples) < 3 {
				removedSamples = append(removedSamples, oldExchange.Address)
			}
		}
	}
	if conflicts > 0 {
		db.logger.Warnf("Exchange metadata has %d conflicting origin names; keeping cached names, sample_addresses=%v", conflicts, conflictSamples)
	}
	if removed > 0 {
		db.logger.Warnf("Exchange metadata omitted %d cached addresses; keeping them, sample_addresses=%v", removed, removedSamples)
	}
	db.logger.Infof("Exchange metadata refreshed: entries=%d missing_origin_name=%d previous_failures=%d", len(exchanges.Val), missingOrigin, db.exchangesFailures)
	db.exchangesUpdatedAt = now
	db.exchangesRetryAt = time.Time{}
	db.exchangesFailures = 0
}
