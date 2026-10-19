package tron

import (
	"time"

	"go.uber.org/zap"
)

// Fetching a block and its receipts is one retry episode. The upstream indexer
// can briefly lag a block, or return an empty array through a lagging replica.
// Keep short episodes at DEBUG; warn after 10s and summarize every 30s until
// success. Dynamic block numbers/error strings must not bypass the rate limit.
type blockFetchRetry struct {
	started, lastWarning time.Time
	attempts             uint64
	warned               bool
}

func (r *blockFetchRetry) failure(logger *zap.SugaredLogger, now time.Time, height uint, reason string) {
	if r.attempts == 0 {
		r.started = now
		logger.Debugf("Block fetch temporarily unavailable: block=%d reason=%s", height, reason)
	}
	r.attempts++
	if now.Sub(r.started) >= 10*time.Second && (!r.warned || now.Sub(r.lastWarning) >= 30*time.Second) {
		logger.Warnf("Block fetch still retrying: block=%d attempts=%d elapsed=%s reason=%s", height, r.attempts, now.Sub(r.started).Round(time.Second), reason)
		r.lastWarning = now
		r.warned = true
	}
}

func (r *blockFetchRetry) recovered(logger *zap.SugaredLogger, now time.Time, height uint) {
	if r.warned {
		logger.Infof("Block fetch recovered: block=%d attempts=%d elapsed=%s", height, r.attempts, now.Sub(r.started).Round(time.Second))
	}
	*r = blockFetchRetry{}
}
