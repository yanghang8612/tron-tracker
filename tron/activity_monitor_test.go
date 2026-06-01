package tron

import (
	"strings"
	"testing"

	"tron-tracker/database"
	"tron-tracker/database/models"
	"tron-tracker/net"
)

func TestClassifyStakeActivity(t *testing.T) {
	tests := []struct {
		name     string
		txType   uint8
		action   string
		resource string
		ok       bool
	}{
		{name: "v1 bandwidth stake", txType: 11, action: "Stake", resource: "BANDWIDTH", ok: true},
		{name: "v1 energy stake", txType: 111, action: "Stake", resource: "ENERGY", ok: true},
		{name: "v2 bandwidth stake", txType: 54, action: "Stake", resource: "BANDWIDTH", ok: true},
		{name: "v2 energy stake", txType: 154, action: "Stake", resource: "ENERGY", ok: true},
		{name: "v1 bandwidth unstake", txType: 12, action: "Unstake", resource: "BANDWIDTH", ok: true},
		{name: "v1 energy unstake", txType: 112, action: "Unstake", resource: "ENERGY", ok: true},
		{name: "v2 bandwidth unstake", txType: 55, action: "Unstake", resource: "BANDWIDTH", ok: true},
		{name: "v2 energy unstake", txType: 155, action: "Unstake", resource: "ENERGY", ok: true},
		{name: "delegate is ignored", txType: 57, ok: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			action, resource, ok := classifyStakeActivity(tt.txType)
			if ok != tt.ok {
				t.Fatalf("ok = %v, want %v", ok, tt.ok)
			}
			if action != tt.action {
				t.Fatalf("action = %q, want %q", action, tt.action)
			}
			if resource != tt.resource {
				t.Fatalf("resource = %q, want %q", resource, tt.resource)
			}
		})
	}
}

func TestFormatBaseUnits(t *testing.T) {
	tests := []struct {
		name   string
		amount int64
		want   string
	}{
		{name: "whole", amount: 123_456_789_000_000, want: "123,456,789"},
		{name: "fractional", amount: 1_234_567, want: "1.234567"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatBaseUnits(tt.amount); got != tt.want {
				t.Fatalf("formatBaseUnits() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestActivityMonitorBuildAlertIfLargeCumulativeStake(t *testing.T) {
	monitor := newActivityMonitorForTest(StakeActivityDetector{threshold: 100 * baseUnitsPerToken})

	msg, ok := monitor.buildAlertIfLarge(newStakeTx("TOWNER", 54, 60, 100), "tx1")
	if ok {
		t.Fatalf("first tx alert = true, msg = %#v", msg)
	}

	msg, ok = monitor.buildAlertIfLarge(newStakeTx("TOWNER", 154, 40, 101), "tx2")
	if !ok {
		t.Fatal("second tx alert = false, want true")
	}

	msgText := slackMessageText(msg)
	wantParts := []string{
		"TRON Cumulative Stake Alert",
		"*Total*\n100 TRX",
		"*Owner*\n`TOWNER`",
		"*Transactions*\n`2`",
		"*Block Range*\n`100 - 101`",
		"*Bandwidth*\n60 TRX",
		"*Energy*\n40 TRX",
		"First :clippy:<https://tronscan.io/#/transaction/tx1|TxHash>",
		"Last :clippy:<https://tronscan.io/#/transaction/tx2|TxHash>",
	}
	for _, want := range wantParts {
		if !strings.Contains(msgText, want) {
			t.Fatalf("alert msg missing %q: %s", want, msgText)
		}
	}

	if len(monitor.accumulatorByID) != 0 {
		t.Fatalf("accumulator count = %d, want 0 after alert", len(monitor.accumulatorByID))
	}
}

func TestActivityMonitorBuildAlertIfLargeSeparatesActivityType(t *testing.T) {
	monitor := newActivityMonitorForTest(StakeActivityDetector{threshold: 100 * baseUnitsPerToken})

	if _, ok := monitor.buildAlertIfLarge(newStakeTx("TOWNER", 54, 60, 100), "stake1"); ok {
		t.Fatal("stake alert = true, want false")
	}
	if _, ok := monitor.buildAlertIfLarge(newStakeTx("TOWNER", 55, 60, 101), "unstake1"); ok {
		t.Fatal("unstake alert = true, want false")
	}
	if _, ok := monitor.buildAlertIfLarge(newStakeTx("TOWNER", 54, 40, 102), "stake2"); !ok {
		t.Fatal("second stake alert = false, want true")
	}

	if len(monitor.accumulatorByID) != 1 {
		t.Fatalf("accumulator count = %d, want 1 pending unstake bucket", len(monitor.accumulatorByID))
	}
	if got := monitor.accumulatorByID[activityBucketKey("stake", "Unstake", "TOWNER")].Amount; got != 60*baseUnitsPerToken {
		t.Fatalf("pending unstake amount = %d, want %d", got, 60*baseUnitsPerToken)
	}
}

func TestActivityMonitorBuildAlertIfLargeSingleTxClearsPendingBucket(t *testing.T) {
	monitor := newActivityMonitorForTest(StakeActivityDetector{threshold: 100 * baseUnitsPerToken})

	if _, ok := monitor.buildAlertIfLarge(newStakeTx("TOWNER", 54, 60, 100), "stake1"); ok {
		t.Fatal("first stake alert = true, want false")
	}

	msg, ok := monitor.buildAlertIfLarge(newStakeTx("TOWNER", 54, 120, 101), "stake2")
	if !ok {
		t.Fatal("single large stake alert = false, want true")
	}
	if strings.Contains(strings.ToLower(slackMessageText(msg)), "cumulative") {
		t.Fatalf("single large alert should not be cumulative: %s", slackMessageText(msg))
	}
	if len(monitor.accumulatorByID) != 0 {
		t.Fatalf("accumulator count = %d, want 0 after single large alert", len(monitor.accumulatorByID))
	}
}

func TestActivityMonitorBuildAlertIfLargeTRXTransfer(t *testing.T) {
	monitor := newActivityMonitorForTest(TRXTransferActivityDetector{threshold: 100 * baseUnitsPerToken})

	msg, ok := monitor.buildAlertIfLarge(newTRXTransferTx("TFROM", "TTO", 120, 100), "tx1")
	if !ok {
		t.Fatal("trx transfer alert = false, want true")
	}

	msgText := slackMessageText(msg)
	for _, want := range []string{
		"TRON Large TRX Transfer Alert",
		"*Token*\nTRX",
		"*Amount*\n120 TRX",
		"*From*\n`TFROM`",
		"*To*\n`TTO`",
	} {
		if !strings.Contains(msgText, want) {
			t.Fatalf("alert msg missing %q: %s", want, msgText)
		}
	}
}

func TestActivityMonitorBuildAlertIfLargeUSDTTransferCumulative(t *testing.T) {
	monitor := newActivityMonitorForTest(USDTTransferActivityDetector{threshold: 100 * baseUnitsPerToken})

	if _, ok := monitor.buildAlertIfLarge(newUSDTTransferTx("TFROM", "TTO1", 45, 100), "tx1"); ok {
		t.Fatal("first usdt transfer alert = true, want false")
	}
	msg, ok := monitor.buildAlertIfLarge(newUSDTTransferTx("TFROM", "TTO2", 55, 101), "tx2")
	if !ok {
		t.Fatal("second usdt transfer alert = false, want true")
	}

	msgText := slackMessageText(msg)
	for _, want := range []string{
		"TRON Cumulative USDT Transfer Alert",
		"*Total*\n100 USDT",
		"*From*\n`TFROM`",
		"*Transactions*\n`2`",
	} {
		if !strings.Contains(msgText, want) {
			t.Fatalf("alert msg missing %q: %s", want, msgText)
		}
	}
}

func newActivityMonitorForTest(detectors ...ActivityDetector) *ActivityMonitor {
	return &ActivityMonitor{
		detectors:       detectors,
		accumulatorByID: make(map[string]*activityAccumulator),
	}
}

func newStakeTx(owner string, txType uint8, amount int64, height uint) *models.Transaction {
	tx := &models.Transaction{
		OwnerAddr: owner,
		Type:      txType,
		Result:    1,
		Height:    height,
	}
	tx.SetAmount(amount * baseUnitsPerToken)
	return tx
}

func newTRXTransferTx(from, to string, amount int64, height uint) *models.Transaction {
	tx := &models.Transaction{
		Type:     1,
		Name:     "TRX",
		FromAddr: from,
		ToAddr:   to,
		Result:   1,
		Height:   height,
	}
	tx.SetAmount(amount * baseUnitsPerToken)
	return tx
}

func newUSDTTransferTx(from, to string, amount int64, height uint) *models.Transaction {
	tx := &models.Transaction{
		Type:     models.TransferType,
		Name:     database.USDT,
		FromAddr: from,
		ToAddr:   to,
		Result:   1,
		Height:   height,
	}
	tx.SetAmount(amount * baseUnitsPerToken)
	return tx
}

func slackMessageText(msg net.SlackMessage) string {
	var b strings.Builder
	b.WriteString(msg.Text)
	for _, block := range msg.Blocks {
		if block.Text != nil {
			b.WriteString("\n")
			b.WriteString(block.Text.Text)
		}
		for _, field := range block.Fields {
			b.WriteString("\n")
			b.WriteString(field.Text)
		}
		for _, element := range block.Elements {
			b.WriteString("\n")
			b.WriteString(element.Text)
		}
	}
	return b.String()
}
