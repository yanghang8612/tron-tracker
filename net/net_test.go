package net

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"tron-tracker/config"

	"github.com/go-playground/assert/v2"
)

func TestDate(t *testing.T) {
	endDateStr := "2025-07-22"
	endDate, _ := time.Parse("2006-01-02", endDateStr)
	startDate := subtractBusinessDays(endDate, 10).Format("2006-01-02")
	assert.Equal(t, startDate, "2025-07-08")
}

func TestGetAccountFrozenResources(t *testing.T) {
	cases := []struct {
		name          string
		resp          string
		wantBandwidth int64
		wantEnergy    int64
	}{
		{
			name:          "empty account returns zero",
			resp:          `{}`,
			wantBandwidth: 0,
			wantEnergy:    0,
		},
		{
			name: "v1 frozen for bandwidth only",
			resp: `{
				"frozen": [
					{"frozen_balance": 1000000, "expire_time": 0},
					{"frozen_balance": 2500000, "expire_time": 0}
				]
			}`,
			wantBandwidth: 3_500_000,
			wantEnergy:    0,
		},
		{
			name: "v1 frozen for energy only",
			resp: `{
				"account_resource": {
					"frozen_balance_for_energy": {"frozen_balance": 7000000, "expire_time": 0}
				}
			}`,
			wantBandwidth: 0,
			wantEnergy:    7_000_000,
		},
		{
			name: "v2 mixed: bandwidth (type omitted), energy, tron_power ignored",
			resp: `{
				"frozenV2": [
					{"amount": 1100000},
					{"type": "ENERGY", "amount": 2200000},
					{"type": "TRON_POWER", "amount": 9999999},
					{"amount": 300000}
				]
			}`,
			wantBandwidth: 1_400_000,
			wantEnergy:    2_200_000,
		},
		{
			name: "v2 explicit BANDWIDTH type string also counts",
			resp: `{
				"frozenV2": [
					{"type": "BANDWIDTH", "amount": 500000},
					{"amount": 700000}
				]
			}`,
			wantBandwidth: 1_200_000,
			wantEnergy:    0,
		},
		{
			name: "acquired delegations v1 and v2 for both",
			resp: `{
				"acquired_delegated_frozen_balance_for_bandwidth": 11,
				"acquired_delegated_frozenV2_balance_for_bandwidth": 22,
				"account_resource": {
					"acquired_delegated_frozen_balance_for_energy": 33,
					"acquired_delegated_frozenV2_balance_for_energy": 44
				}
			}`,
			wantBandwidth: 33,
			wantEnergy:    77,
		},
		{
			name: "combined full payload",
			resp: `{
				"frozen": [{"frozen_balance": 1000}],
				"acquired_delegated_frozen_balance_for_bandwidth": 2000,
				"acquired_delegated_frozenV2_balance_for_bandwidth": 3000,
				"frozenV2": [
					{"amount": 4000},
					{"type": "ENERGY", "amount": 50000},
					{"type": "TRON_POWER", "amount": 999999}
				],
				"account_resource": {
					"frozen_balance_for_energy": {"frozen_balance": 60000},
					"acquired_delegated_frozen_balance_for_energy": 70000,
					"acquired_delegated_frozenV2_balance_for_energy": 80000
				}
			}`,
			wantBandwidth: 1000 + 2000 + 3000 + 4000,
			wantEnergy:    50000 + 60000 + 70000 + 80000,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != GetAccountPath {
					t.Fatalf("unexpected request path: %s", r.URL.Path)
				}
				body, _ := io.ReadAll(r.Body)
				if !strings.Contains(string(body), `"visible":true`) {
					t.Fatalf("expected visible:true in request, got: %s", string(body))
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, tc.resp)
			}))
			defer srv.Close()

			configs = &config.NetConfig{FullNode: srv.URL}

			bandwidth, energy, err := GetAccountFrozenResources("TLsV52sRDL79HXGGm9yzwKibb6BeruhUzy")
			assert.Equal(t, err, nil)
			assert.Equal(t, bandwidth, tc.wantBandwidth)
			assert.Equal(t, energy, tc.wantEnergy)
		})
	}
}
