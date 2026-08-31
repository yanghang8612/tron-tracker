package tron

import (
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"tron-tracker/common"
	"tron-tracker/config"
	"tron-tracker/tron/types"
)

func TestNewActivityMonitorEnablesHighRiskMonitoringWithoutThresholds(t *testing.T) {
	monitor := NewActivityMonitor(&config.OnChainMonitorConfig{
		Enabled:      true,
		AIOpsAppKeys: "app-one, app-two, app-one, ,",
	})
	if monitor == nil {
		t.Fatal("monitor = nil, want high-risk monitoring with zero amount thresholds")
	}
	if len(monitor.detectors) != 0 {
		t.Fatalf("detector count = %d, want 0", len(monitor.detectors))
	}
	if got := strings.Join(monitor.aiopsAppKeys, ","); got != "app-one,app-two" {
		t.Fatalf("AIOps app keys = %q, want app-one,app-two", got)
	}
}

func TestDetectSuicideWithStake2(t *testing.T) {
	contract1 := "41" + strings.Repeat("11", 20)
	contract2 := "41" + strings.Repeat("22", 20)
	internalTxs := []types.InternalTx{
		{Note: encodeInternalNote("suicide"), From: contract1},
		{Note: encodeInternalNote("freezeBalanceV2ForBandwidth"), From: contract1},
		{Note: encodeInternalNote("delegateResourceOfEnergy"), From: contract2},
		// Rejected internals must not affect either count.
		{Note: encodeInternalNote("suicide"), From: contract2, Rejected: true},
		{Note: encodeInternalNote("unfreezeBalanceV2ForEnergy"), From: contract2, Rejected: true},
	}

	activity, ok := detectSuicideWithStake2(internalTxs, 123, 7, "tx-hash")
	if !ok {
		t.Fatal("detectSuicideWithStake2() = false, want true")
	}
	if activity.SuicideCount != 1 || activity.Stake2Count != 2 {
		t.Fatalf("counts = suicide:%d stake2:%d, want 1/2", activity.SuicideCount, activity.Stake2Count)
	}
	if got, want := strings.Join(activity.Stake2Actions, ","),
		"freezeBalanceV2ForBandwidth,delegateResourceOfEnergy"; got != want {
		t.Fatalf("actions = %q, want %q", got, want)
	}
	if got, want := strings.Join(activity.Contracts, ","),
		common.EncodeToBase58(contract1)+","+common.EncodeToBase58(contract2); got != want {
		t.Fatalf("contracts = %q, want %q", got, want)
	}

	message := slackMessageText(formatSuicideStake2Alert(activity))
	for _, want := range []string{
		"TRON High-risk Transaction Alert",
		"*Risk Level*\n`HIGH`",
		"*Block / Index*\n`123 / 7`",
		"https://tronscan.io/#/transaction/tx-hash",
	} {
		if !strings.Contains(message, want) {
			t.Fatalf("alert message missing %q: %s", want, message)
		}
	}
	for _, sensitive := range []string{
		"suicide",
		"Stake 2.0",
		"freezeBalanceV2ForBandwidth",
		"delegateResourceOfEnergy",
		common.EncodeToBase58(contract1),
		common.EncodeToBase58(contract2),
	} {
		if strings.Contains(message, sensitive) {
			t.Fatalf("alert message contains sensitive detail %q: %s", sensitive, message)
		}
	}

	aiopsAlert := formatSuicideStake2AIOpsAlert(activity)
	if aiopsAlert.EventID != "tx-hash" || aiopsAlert.EventType != "trigger" || aiopsAlert.Priority != 5 {
		t.Fatalf("AIOps alert = %#v, want fatal trigger for tx-hash", aiopsAlert)
	}
	for _, want := range []string{
		"高风险链上交易",
		"123/7",
		"tx-hash",
	} {
		if !strings.Contains(aiopsAlert.AlarmContent, want) {
			t.Fatalf("AIOps content missing %q: %s", want, aiopsAlert.AlarmContent)
		}
	}
	for _, sensitive := range []string{
		"suicide",
		"Stake 2.0",
		"freezeBalanceV2ForBandwidth",
		"delegateResourceOfEnergy",
		common.EncodeToBase58(contract1),
		common.EncodeToBase58(contract2),
	} {
		if strings.Contains(aiopsAlert.AlarmName+aiopsAlert.AlarmContent, sensitive) {
			t.Fatalf("AIOps alert contains sensitive detail %q: %#v", sensitive, aiopsAlert)
		}
	}
	if len(aiopsAlert.Contexts) != 1 || !strings.HasSuffix(aiopsAlert.Contexts[0].Href, "/tx-hash") {
		t.Fatalf("AIOps contexts = %#v, want Tronscan tx link", aiopsAlert.Contexts)
	}
}

func TestDetectSuicideWithStake2RequiresBothSuccessfulKindsInSameTransaction(t *testing.T) {
	tests := []struct {
		name        string
		internalTxs []types.InternalTx
	}{
		{
			name:        "suicide only",
			internalTxs: []types.InternalTx{{Note: encodeInternalNote("suicide")}},
		},
		{
			name:        "stake2 only",
			internalTxs: []types.InternalTx{{Note: encodeInternalNote("unfreezeBalanceV2ForEnergy")}},
		},
		{
			name: "rejected suicide",
			internalTxs: []types.InternalTx{
				{Note: encodeInternalNote("suicide"), Rejected: true},
				{Note: encodeInternalNote("cancelAllUnfreezeV2")},
			},
		},
		{
			name: "rejected stake2",
			internalTxs: []types.InternalTx{
				{Note: encodeInternalNote("suicide")},
				{Note: encodeInternalNote("withdrawExpireUnfreeze"), Rejected: true},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if activity, ok := detectSuicideWithStake2(tt.internalTxs, 1, 0, "tx"); ok {
				t.Fatalf("detectSuicideWithStake2() = true, activity = %#v", activity)
			}
		})
	}
}

func TestSuicideOnlyInternalTransactionIsNotHighRisk(t *testing.T) {
	activity, highRisk := detectSuicideWithStake2([]types.InternalTx{
		{Note: encodeInternalNote("suicide")},
		{Note: encodeInternalNote("suicide"), Rejected: true},
	}, 456, 8, "suicide-tx-hash")
	if highRisk {
		t.Fatal("highRisk = true, want false for suicide-only transaction")
	}
	if activity.SuicideCount != 1 {
		t.Fatalf("suicide count = %d, want 1 successful internal", activity.SuicideCount)
	}
}

func TestFormatSuicideInternalSummarySortsCalledContracts(t *testing.T) {
	location := time.FixedZone("CST", 8*60*60)
	end := time.Date(2026, 8, 12, 12, 0, 0, 0, location)
	start := end.Add(-24 * time.Hour)
	message := formatSuicideInternalSummary(start, end, []suicideContractStatistic{
		{CalledContract: "TSecond", TxCount: 2, SuicideCount: 3},
		{CalledContract: "TFirst", TxCount: 8, SuicideCount: 10},
		{CalledContract: "", TxCount: 1, SuicideCount: 1},
	})

	for _, want := range []string{
		"TRON internal transaction daily summary",
		"Period: `2026-08-11 12:00:00` - `2026-08-12 12:00:00` (CST)",
		"Transactions: `11`",
		"Internal activities: `14`",
		"Called contracts: `3`",
		"1. `TFirst`: `8` tx (72.73%), `10` internal",
		"2. `TSecond`: `2` tx (18.18%), `3` internal",
		"3. `unknown`: `1` tx (9.09%), `1` internal",
	} {
		if !strings.Contains(message, want) {
			t.Fatalf("daily summary missing %q: %s", want, message)
		}
	}
	for _, unwanted := range []string{"<!channel>", "suicide", "Stake 2.0", "freezeBalanceV2", "delegateResource"} {
		if strings.Contains(message, unwanted) {
			t.Fatalf("daily summary contains sensitive detail %q: %s", unwanted, message)
		}
	}
}

func TestSuicideContractStatisticsAccumulateAndResetInMemory(t *testing.T) {
	monitor := NewActivityMonitor(&config.OnChainMonitorConfig{Enabled: true})
	monitor.ReportSuicideWithStake2([]types.InternalTx{
		{Note: encodeInternalNote("suicide")},
		{Note: encodeInternalNote("suicide"), Rejected: true},
	}, 1, 0, "tx-1", "TContract")
	monitor.ReportSuicideWithStake2([]types.InternalTx{
		{Note: encodeInternalNote("suicide")},
		{Note: encodeInternalNote("suicide")},
	}, 2, 0, "tx-2", "TContract")

	stats := monitor.takeSuicideContractStatistics()
	if len(stats) != 1 {
		t.Fatalf("statistics count = %d, want 1", len(stats))
	}
	if got := stats[0]; got.CalledContract != "TContract" || got.TxCount != 2 || got.SuicideCount != 3 {
		t.Fatalf("statistics = %#v, want 2 transactions and 3 internals", got)
	}
	if got := monitor.takeSuicideContractStatistics(); len(got) != 0 {
		t.Fatalf("statistics after reset = %#v, want empty", got)
	}
}

func TestCalledContractAddressUsesOuterTriggerContract(t *testing.T) {
	hexAddress := "41" + strings.Repeat("ab", 20)
	var tx types.Transaction
	tx.RawData.Contract = append(tx.RawData.Contract, struct {
		Parameter struct {
			Value   map[string]interface{} `json:"value"`
			TypeUrl string                 `json:"type_url"`
		} `json:"parameter"`
		Type string `json:"type"`
	}{})
	tx.RawData.Contract[0].Parameter.Value = map[string]interface{}{
		"contract_address": hexAddress,
	}

	if got, want := calledContractAddress(tx, "fallback"), common.EncodeToBase58(hexAddress); got != want {
		t.Fatalf("called contract = %q, want %q", got, want)
	}
	if got := calledContractAddress(types.Transaction{}, "fallback"); got != "fallback" {
		t.Fatalf("missing called contract = %q, want fallback", got)
	}
}

func TestIsStake2InternalNote(t *testing.T) {
	for _, note := range []string{
		"freezeBalanceV2ForBandwidth",
		"unfreezeBalanceV2ForEnergy",
		"withdrawExpireUnfreeze",
		"withdrawExpireUnfreezeWhileSuiciding",
		"cancelAllUnfreezeV2",
		"delegateResourceOfBandwidth",
		"unDelegateResourceOfEnergy",
	} {
		if !isStake2InternalNote(note) {
			t.Errorf("isStake2InternalNote(%q) = false, want true", note)
		}
	}

	for _, note := range []string{"suicide", "call", "freezeForEnergy", "unfreezeForBandwidth"} {
		if isStake2InternalNote(note) {
			t.Errorf("isStake2InternalNote(%q) = true, want false", note)
		}
	}
}

func TestDecodeInternalNoteAcceptsHexAndPlainText(t *testing.T) {
	if got := decodeInternalNote(encodeInternalNote("suicide")); got != "suicide" {
		t.Fatalf("decoded hex note = %q, want suicide", got)
	}
	if got := decodeInternalNote("suicide"); got != "suicide" {
		t.Fatalf("decoded plain note = %q, want suicide", got)
	}
}

func TestTruncateRunesPreservesUTF8(t *testing.T) {
	if got := truncateRunes("中文告警内容", 5); got != "中文告警…" {
		t.Fatalf("truncateRunes() = %q, want 中文告警…", got)
	}
}

func encodeInternalNote(note string) string {
	return hex.EncodeToString([]byte(note))
}
