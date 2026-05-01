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
		name string
		resp string
		want FrozenResources
	}{
		{
			name: "empty account returns zero",
			resp: `{}`,
			want: FrozenResources{},
		},
		{
			name: "v1 frozen for bandwidth only",
			resp: `{
				"frozen": [
					{"frozen_balance": 1000000, "expire_time": 0},
					{"frozen_balance": 2500000, "expire_time": 0}
				]
			}`,
			want: FrozenResources{SelfBandwidth: 3_500_000},
		},
		{
			name: "v1 frozen for energy only",
			resp: `{
				"account_resource": {
					"frozen_balance_for_energy": {"frozen_balance": 7000000, "expire_time": 0}
				}
			}`,
			want: FrozenResources{SelfEnergy: 7_000_000},
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
			want: FrozenResources{SelfBandwidth: 1_400_000, SelfEnergy: 2_200_000},
		},
		{
			name: "v2 explicit BANDWIDTH type string also counts",
			resp: `{
				"frozenV2": [
					{"type": "BANDWIDTH", "amount": 500000},
					{"amount": 700000}
				]
			}`,
			want: FrozenResources{SelfBandwidth: 1_200_000},
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
			want: FrozenResources{AcquiredBandwidth: 33, AcquiredEnergy: 77},
		},
		{
			name: "delegated out v1 and v2 for both",
			resp: `{
				"delegated_frozen_balance_for_bandwidth": 1,
				"delegated_frozenV2_balance_for_bandwidth": 2,
				"account_resource": {
					"delegated_frozen_balance_for_energy": 3,
					"delegated_frozenV2_balance_for_energy": 4
				}
			}`,
			want: FrozenResources{DelegatedOutBandwidth: 3, DelegatedOutEnergy: 7},
		},
		{
			name: "combined full payload",
			resp: `{
				"frozen": [{"frozen_balance": 1000}],
				"acquired_delegated_frozen_balance_for_bandwidth": 2000,
				"acquired_delegated_frozenV2_balance_for_bandwidth": 3000,
				"delegated_frozen_balance_for_bandwidth": 11,
				"delegated_frozenV2_balance_for_bandwidth": 22,
				"frozenV2": [
					{"amount": 4000},
					{"type": "ENERGY", "amount": 50000},
					{"type": "TRON_POWER", "amount": 999999}
				],
				"account_resource": {
					"frozen_balance_for_energy": {"frozen_balance": 60000},
					"acquired_delegated_frozen_balance_for_energy": 70000,
					"acquired_delegated_frozenV2_balance_for_energy": 80000,
					"delegated_frozen_balance_for_energy": 33,
					"delegated_frozenV2_balance_for_energy": 44
				}
			}`,
			want: FrozenResources{
				SelfBandwidth:         1000 + 4000,
				AcquiredBandwidth:     2000 + 3000,
				DelegatedOutBandwidth: 11 + 22,
				SelfEnergy:            60000 + 50000,
				AcquiredEnergy:        70000 + 80000,
				DelegatedOutEnergy:    33 + 44,
			},
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

			got, err := GetAccountFrozenResources("TLsV52sRDL79HXGGm9yzwKibb6BeruhUzy")
			assert.Equal(t, err, nil)
			assert.Equal(t, got, tc.want)
		})
	}
}

func TestGetAccountDelegatedTo(t *testing.T) {
	cases := []struct {
		name string
		resp string
		want []string
	}{
		{
			name: "no delegations",
			resp: `{"account": "TXxx"}`,
			want: nil,
		},
		{
			name: "single recipient",
			resp: `{"account": "TXxx", "toAccounts": ["TYyy"]}`,
			want: []string{"TYyy"},
		},
		{
			name: "multiple recipients ignore fromAccounts",
			resp: `{"account":"TXxx","fromAccounts":["TZzz"],"toAccounts":["TAaa","TBbb","TCcc"]}`,
			want: []string{"TAaa", "TBbb", "TCcc"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != GetDelegatedResourceAccountIndexV2Path {
					t.Fatalf("unexpected request path: %s", r.URL.Path)
				}
				body, _ := io.ReadAll(r.Body)
				if !strings.Contains(string(body), `"visible":true`) {
					t.Fatalf("expected visible:true, got: %s", string(body))
				}
				if !strings.Contains(string(body), `"value":"TXxx"`) {
					t.Fatalf("expected value:TXxx, got: %s", string(body))
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, tc.resp)
			}))
			defer srv.Close()

			configs = &config.NetConfig{FullNode: srv.URL}

			got, err := GetAccountDelegatedTo("TXxx")
			assert.Equal(t, err, nil)
			assert.Equal(t, got, tc.want)
		})
	}
}
