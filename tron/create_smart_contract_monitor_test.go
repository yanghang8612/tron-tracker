package tron

import (
	"encoding/json"
	"strings"
	"testing"

	"tron-tracker/tron/types"
)

func TestDetectCreateSmartContractHashFields(t *testing.T) {
	tests := []struct {
		name       string
		contract   string
		wantFields string
		want       bool
	}{
		{
			name: "code hash",
			contract: `{
				"type":"CreateSmartContract",
				"parameter":{"value":{"new_contract":{"code_hash":"aabb"}}}
			}`,
			wantFields: "code_hash",
			want:       true,
		},
		{
			name: "trx hash",
			contract: `{
				"type":"CreateSmartContract",
				"parameter":{"value":{"new_contract":{"trx_hash":"ccdd"}}}
			}`,
			wantFields: "trx_hash",
			want:       true,
		},
		{
			name: "both fields",
			contract: `{
				"type":"CreateSmartContract",
				"parameter":{"value":{"new_contract":{"trx_hash":"ccdd","code_hash":"aabb"}}}
			}`,
			wantFields: "code_hash,trx_hash",
			want:       true,
		},
		{
			name: "field presence is monitored even if JSON value is empty",
			contract: `{
				"type":"CreateSmartContract",
				"parameter":{"value":{"new_contract":{"code_hash":""}}}
			}`,
			wantFields: "code_hash",
			want:       true,
		},
		{
			name: "ordinary contract creation",
			contract: `{
				"type":"CreateSmartContract",
				"parameter":{"value":{"new_contract":{"name":"example","bytecode":"00"}}}
			}`,
			want: false,
		},
		{
			name: "hash fields on another contract type",
			contract: `{
				"type":"TriggerSmartContract",
				"parameter":{"value":{"new_contract":{"code_hash":"aabb","trx_hash":"ccdd"}}}
			}`,
			want: false,
		},
		{
			name: "hash fields outside new contract",
			contract: `{
				"type":"CreateSmartContract",
				"parameter":{"value":{"code_hash":"aabb","trx_hash":"ccdd","new_contract":{}}}
			}`,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx := transactionWithRawContract(t, tt.contract)
			activity, ok := detectCreateSmartContractHashFields(tx, 123, 7, "tx-hash")
			if ok != tt.want {
				t.Fatalf("detectCreateSmartContractHashFields() = %v, want %v; activity = %#v", ok, tt.want, activity)
			}
			if got := strings.Join(activity.Fields, ","); got != tt.wantFields {
				t.Fatalf("fields = %q, want %q", got, tt.wantFields)
			}
			if activity.Height != 123 || activity.Index != 7 || activity.TxID != "tx-hash" {
				t.Fatalf("activity metadata = %#v, want block/index/tx 123/7/tx-hash", activity)
			}
		})
	}
}

func TestFormatCreateSmartContractHashAlerts(t *testing.T) {
	activity := createSmartContractHashActivity{
		Height: 123,
		Index:  7,
		TxID:   "tx-hash",
		Fields: []string{"code_hash", "trx_hash"},
	}

	message := slackMessageText(formatCreateSmartContractHashAlert(activity))
	for _, want := range []string{
		"TRON High-risk Contract Creation Alert",
		"*Risk Level*\n`HIGH`",
		"*Contract Type*\n`CreateSmartContract`",
		"*Unexpected Fields*\n`code_hash`, `trx_hash`",
		"*Block / Index*\n`123 / 7`",
		"https://tronscan.io/#/transaction/tx-hash",
	} {
		if !strings.Contains(message, want) {
			t.Fatalf("Slack alert missing %q: %s", want, message)
		}
	}

	aiopsAlert := formatCreateSmartContractHashAIOpsAlert(activity)
	if aiopsAlert.EventID != "tx-hash" || aiopsAlert.EventType != "trigger" || aiopsAlert.Priority != 5 {
		t.Fatalf("AIOps alert = %#v, want fatal trigger for tx-hash", aiopsAlert)
	}
	for _, want := range []string{"CreateSmartContract", "code_hash,trx_hash", "123/7", "tx-hash"} {
		if !strings.Contains(aiopsAlert.AlarmContent, want) {
			t.Fatalf("AIOps content missing %q: %s", want, aiopsAlert.AlarmContent)
		}
	}
	if len(aiopsAlert.Contexts) != 1 || !strings.HasSuffix(aiopsAlert.Contexts[0].Href, "/tx-hash") {
		t.Fatalf("AIOps contexts = %#v, want Tronscan tx link", aiopsAlert.Contexts)
	}
}

func TestDetectCreateSmartContractHashFieldsScansAllContracts(t *testing.T) {
	var tx types.Transaction
	err := json.Unmarshal([]byte(`{
		"raw_data":{"contract":[
			{"type":"TransferContract","parameter":{"value":{}}},
			{"type":"CreateSmartContract","parameter":{"value":{"new_contract":{"trx_hash":"ccdd"}}}}
		]}
	}`), &tx)
	if err != nil {
		t.Fatalf("decode transaction: %v", err)
	}

	activity, ok := detectCreateSmartContractHashFields(tx, 1, 0, "tx")
	if !ok || strings.Join(activity.Fields, ",") != "trx_hash" {
		t.Fatalf("activity = %#v, ok = %v; want trx_hash detection", activity, ok)
	}
}

func transactionWithRawContract(t *testing.T, rawContract string) types.Transaction {
	t.Helper()

	var tx types.Transaction
	payload := `{"raw_data":{"contract":[` + rawContract + `]}}`
	if err := json.Unmarshal([]byte(payload), &tx); err != nil {
		t.Fatalf("decode transaction: %v", err)
	}
	return tx
}
