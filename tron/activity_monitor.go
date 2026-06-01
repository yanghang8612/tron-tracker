package tron

import (
	"strings"

	"tron-tracker/config"
	"tron-tracker/database/models"
	"tron-tracker/net"
)

const (
	baseUnitsPerToken int64 = 1_000_000
	maxThreshold      int64 = 9_223_372_036_854
)

type ActivityMonitor struct {
	webhook         string
	detectors       []ActivityDetector
	accumulatorByID map[string]*activityAccumulator
}

type ActivityDetector interface {
	Detect(tx *models.Transaction, txID string) (MonitoredActivity, bool)
}

type MonitoredActivity struct {
	Type       string
	ActorLabel string
	Actor      string
	Amount     int64
	Unit       string
	Threshold  int64
	Height     uint
	Index      uint16
	TxID       string
	BucketKey  string
	Fields     []activityField
	Breakdown  []activityAmount
}

type activityField struct {
	Label string
	Value string
}

type activityAmount struct {
	Label  string
	Amount int64
	Unit   string
}

type activityAccumulator struct {
	Type       string
	ActorLabel string
	Actor      string
	Amount     int64
	Unit       string
	TxCount    int
	FirstBlock uint
	LastBlock  uint
	FirstTxID  string
	LastTxID   string
	Breakdown  map[string]activityAmount
}

func NewActivityMonitor(cfg *config.OnChainMonitorConfig) *ActivityMonitor {
	if cfg == nil || !cfg.Enabled {
		return nil
	}

	detectors := make([]ActivityDetector, 0, 3)
	if threshold := thresholdToBaseUnits(cfg.StakeThresholdTRX); threshold > 0 {
		detectors = append(detectors, StakeActivityDetector{threshold: threshold})
	}
	if threshold := thresholdToBaseUnits(cfg.TRXTransferThresholdTRX); threshold > 0 {
		detectors = append(detectors, TRXTransferActivityDetector{threshold: threshold})
	}
	if threshold := thresholdToBaseUnits(cfg.USDTTransferThresholdUSDT); threshold > 0 {
		detectors = append(detectors, USDTTransferActivityDetector{threshold: threshold})
	}

	if len(detectors) == 0 {
		return nil
	}

	return &ActivityMonitor{
		webhook:         cfg.SlackWebhook,
		detectors:       detectors,
		accumulatorByID: make(map[string]*activityAccumulator),
	}
}

func (m *ActivityMonitor) ReportIfLarge(tx *models.Transaction, txID string) {
	if msg, ok := m.buildAlertIfLarge(tx, txID); ok {
		net.ReportOnChainMonitorMessageToSlack(m.webhook, msg)
	}
}

func (m *ActivityMonitor) buildAlertIfLarge(tx *models.Transaction, txID string) (net.SlackMessage, bool) {
	if m == nil || tx == nil || tx.Result != 1 {
		return net.SlackMessage{}, false
	}

	activity, ok := m.detect(tx, txID)
	if !ok || activity.Amount <= 0 || activity.Threshold <= 0 {
		return net.SlackMessage{}, false
	}

	if activity.Amount >= activity.Threshold {
		delete(m.accumulatorByID, activity.BucketKey)
		return formatActivityAlert(activity), true
	}

	if m.accumulatorByID == nil {
		m.accumulatorByID = make(map[string]*activityAccumulator)
	}

	accumulator := m.accumulatorByID[activity.BucketKey]
	if accumulator == nil {
		accumulator = newActivityAccumulator(activity)
		m.accumulatorByID[activity.BucketKey] = accumulator
	}
	accumulator.Add(activity)

	if accumulator.Amount < activity.Threshold {
		return net.SlackMessage{}, false
	}

	msg := formatCumulativeActivityAlert(accumulator)
	delete(m.accumulatorByID, activity.BucketKey)
	return msg, true
}

func (m *ActivityMonitor) detect(tx *models.Transaction, txID string) (MonitoredActivity, bool) {
	for _, detector := range m.detectors {
		if activity, ok := detector.Detect(tx, txID); ok {
			return activity, true
		}
	}
	return MonitoredActivity{}, false
}

func newActivityAccumulator(activity MonitoredActivity) *activityAccumulator {
	return &activityAccumulator{
		Type:       activity.Type,
		ActorLabel: activity.ActorLabel,
		Actor:      activity.Actor,
		Unit:       activity.Unit,
		FirstBlock: activity.Height,
		FirstTxID:  activity.TxID,
		Breakdown:  make(map[string]activityAmount),
	}
}

func (a *activityAccumulator) Add(activity MonitoredActivity) {
	a.Amount += activity.Amount
	a.TxCount++
	a.LastBlock = activity.Height
	a.LastTxID = activity.TxID

	for _, item := range activity.Breakdown {
		key := item.Label + "\x00" + item.Unit
		existing := a.Breakdown[key]
		existing.Label = item.Label
		existing.Unit = item.Unit
		existing.Amount += item.Amount
		a.Breakdown[key] = existing
	}
}

func thresholdToBaseUnits(threshold int64) int64 {
	if threshold <= 0 {
		return 0
	}
	if threshold > maxThreshold {
		threshold = maxThreshold
	}
	return threshold * baseUnitsPerToken
}

func activityBucketKey(parts ...string) string {
	return strings.Join(parts, "\x00")
}
