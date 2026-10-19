package net

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"tron-tracker/config"

	"github.com/go-resty/resty/v2"
)

func TestGetNetworkStakeTotals(t *testing.T) {
	oldConfig, oldGrid := configs, gridClient
	t.Cleanup(func() { configs, gridClient = oldConfig, oldGrid })
	for _, tc := range []struct {
		name, body string
		status     int
		want       NetworkStakeTotals
		wantError  bool
	}{
		{"integer TRX weights", `{"TotalNetWeight":26780000000,"TotalEnergyWeight":19540000000}`, 200, NetworkStakeTotals{26780000000, 19540000000}, false},
		{"string TRX weights", `{"TotalNetWeight":"26780000000","TotalEnergyWeight":"19540000000"}`, 200, NetworkStakeTotals{26780000000, 19540000000}, false},
		{"missing weights", `{}`, 200, NetworkStakeTotals{}, true},
		{"partial weights", `{"TotalNetWeight":123}`, 200, NetworkStakeTotals{}, true},
		{"zero weight", `{"TotalNetWeight":0,"TotalEnergyWeight":123}`, 200, NetworkStakeTotals{}, true},
		{"negative weight", `{"TotalNetWeight":-1,"TotalEnergyWeight":123}`, 200, NetworkStakeTotals{}, true},
		{"fractional weight", `{"TotalNetWeight":1.5,"TotalEnergyWeight":123}`, 200, NetworkStakeTotals{}, true},
		{"node error in HTTP 200", `{"Error":"query failed"}`, 200, NetworkStakeTotals{}, true},
		{"HTTP error with valid-looking body", `{"TotalNetWeight":123,"TotalEnergyWeight":456}`, 503, NetworkStakeTotals{}, true},
		{"invalid JSON", `not json`, 200, NetworkStakeTotals{}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/wallet/getaccountresource" {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			gridClient = resty.New()
			configs = &config.NetConfig{FullNode: server.URL}
			got, err := GetNetworkStakeTotals()
			if (err != nil) != tc.wantError || got != tc.want {
				t.Fatalf("got %+v, %v; want %+v, error=%t", got, err, tc.want, tc.wantError)
			}
		})
	}
}
