package tron

import (
	"fmt"
	"strings"

	"tron-tracker/database"
	"tron-tracker/database/models"
)

type StakeActivityDetector struct {
	threshold int64
}

type TRXTransferActivityDetector struct {
	threshold int64
}

type USDTTransferActivityDetector struct {
	threshold int64
}

func (d StakeActivityDetector) Detect(tx *models.Transaction, txID string) (MonitoredActivity, bool) {
	action, resource, ok := classifyStakeActivity(tx.Type)
	if !ok {
		return MonitoredActivity{}, false
	}

	amount := tx.Amount.Int64()
	return MonitoredActivity{
		Type:       action,
		ActorLabel: "Owner",
		Actor:      tx.OwnerAddr,
		Amount:     amount,
		Unit:       "TRX",
		Threshold:  d.threshold,
		Height:     tx.Height,
		Index:      tx.Index,
		TxID:       txID,
		BucketKey:  activityBucketKey("stake", action, tx.OwnerAddr),
		Fields: []activityField{
			{Label: "Action", Value: action},
			{Label: "Amount", Value: formatAmount(amount, "TRX")},
			{Label: "Resource", Value: resource},
			{Label: "Owner", Value: "`" + tx.OwnerAddr + "`"},
			{Label: "Block / Index", Value: fmt.Sprintf("`%d / %d`", tx.Height, tx.Index)},
		},
		Breakdown: []activityAmount{
			{Label: titleCase(resource), Amount: amount, Unit: "TRX"},
		},
	}, true
}

func (d TRXTransferActivityDetector) Detect(tx *models.Transaction, txID string) (MonitoredActivity, bool) {
	if tx.Type != 1 || tx.Name != "TRX" || tx.FromAddr == "" || tx.ToAddr == "" {
		return MonitoredActivity{}, false
	}

	amount := tx.Amount.Int64()
	return newTransferActivity("TRX Transfer", "TRX", d.threshold, amount, tx, txID), true
}

func (d USDTTransferActivityDetector) Detect(tx *models.Transaction, txID string) (MonitoredActivity, bool) {
	if tx.Name != database.USDT || tx.FromAddr == "" || tx.ToAddr == "" {
		return MonitoredActivity{}, false
	}

	amount := tx.Amount.Int64()
	return newTransferActivity("USDT Transfer", "USDT", d.threshold, amount, tx, txID), true
}

func newTransferActivity(activityType, unit string, threshold, amount int64, tx *models.Transaction, txID string) MonitoredActivity {
	return MonitoredActivity{
		Type:       activityType,
		ActorLabel: "From",
		Actor:      tx.FromAddr,
		Amount:     amount,
		Unit:       unit,
		Threshold:  threshold,
		Height:     tx.Height,
		Index:      tx.Index,
		TxID:       txID,
		BucketKey:  activityBucketKey("transfer", unit, tx.FromAddr),
		Fields: []activityField{
			{Label: "Token", Value: unit},
			{Label: "Amount", Value: formatAmount(amount, unit)},
			{Label: "From", Value: "`" + tx.FromAddr + "`"},
			{Label: "To", Value: "`" + tx.ToAddr + "`"},
			{Label: "Block / Index", Value: fmt.Sprintf("`%d / %d`", tx.Height, tx.Index)},
		},
	}
}

func classifyStakeActivity(txType uint8) (string, string, bool) {
	resource := "BANDWIDTH"
	if txType >= 100 {
		txType -= 100
		resource = "ENERGY"
	}

	switch txType {
	case 11, 54:
		return "Stake", resource, true
	case 12, 55:
		return "Unstake", resource, true
	default:
		return "", "", false
	}
}

func titleCase(text string) string {
	if text == "" {
		return ""
	}
	return strings.ToUpper(text[:1]) + strings.ToLower(text[1:])
}
