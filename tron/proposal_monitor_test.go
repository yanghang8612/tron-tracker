package tron

import (
	"encoding/json"
	"strings"
	"testing"

	"tron-tracker/tron/types"
)

func TestHasProposalCreate(t *testing.T) {
	for _, tt := range []struct {
		name      string
		contracts string
		want      bool
	}{
		{"empty", `[]`, false},
		{"create without parameters", `[{"type":"ProposalCreateContract"}]`, true},
		{"approve", `[{"type":"ProposalApproveContract"}]`, false},
		{"delete", `[{"type":"ProposalDeleteContract"}]`, false},
		{"later contract", `[{"type":"TransferContract"},{"type":"ProposalCreateContract"}]`, true},
		{"multiple proposals", `[{"type":"ProposalCreateContract"},{"type":"ProposalCreateContract"}]`, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var tx types.Transaction
			if err := json.Unmarshal([]byte(`{"raw_data":{"contract":`+tt.contracts+`}}`), &tx); err != nil {
				t.Fatal(err)
			}
			if got := hasProposalCreate(tx); got != tt.want {
				t.Fatalf("hasProposalCreate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFormatProposalCreateAIOpsAlert(t *testing.T) {
	alert := formatProposalCreateAIOpsAlert(123, 7, "tx-hash")
	if alert.EventID != "tx-hash" || alert.EventType != "trigger" || alert.Priority != 5 {
		t.Fatalf("expected highest-priority trigger: %#v", alert)
	}
	for _, want := range []string{"ProposalCreateContract", "123/7", "tx-hash"} {
		if !strings.Contains(alert.AlarmContent, want) {
			t.Fatalf("content missing %q: %s", want, alert.AlarmContent)
		}
	}
	if len(alert.Contexts) != 1 || alert.Contexts[0].Href != "https://tronscan.io/#/transaction/tx-hash" {
		t.Fatalf("unexpected transaction link: %#v", alert.Contexts)
	}
}
