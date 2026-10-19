package net

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGetExchangesSchemasAndFailures(t *testing.T) {
	for _, tc := range []struct {
		name, body, wantName, wantOrigin string
		status                           int
		wantError                        bool
	}{
		{"current schema", `{"exchanges":[{"address":"address1","name":"Binance Hot Wallet"}]}`, "Binance Hot Wallet", "", 200, false},
		{"legacy schema", `{"exchanges":[{"address":"address1","tag":"Binance","origin_name":"Binance Hot Wallet"}]}`, "Binance", "Binance Hot Wallet", 200, false},
		{"origin only", `{"exchanges":[{"address":"address1","origin_name":"Binance"}]}`, "Binance", "Binance", 200, false},
		{"empty", `{"exchanges":[]}`, "", "", 200, true},
		{"missing list", `{}`, "", "", 200, true},
		{"null row", `{"exchanges":[null]}`, "", "", 200, true},
		{"unnamed row", `{"exchanges":[{"address":"a"}]}`, "", "", 200, true},
		{"invalid row mixed with valid", `{"exchanges":[{"address":"a","name":"A"},{"name":"B"}]}`, "", "", 200, true},
		{"invalid JSON", `<html>Bad gateway</html>`, "", "", 200, true},
		{"HTTP error", `{"exchanges":[{"address":"a","name":"A"}]}`, "", "", 502, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			got, err := getExchanges(server.URL)
			if (err != nil) != tc.wantError {
				t.Fatalf("got %v, want error=%t", err, tc.wantError)
			}
			if tc.wantError {
				if got != nil {
					t.Fatal("invalid response returned a usable snapshot")
				}
			} else if len(got.Val) != 1 || got.Val[0].Name != tc.wantName || got.Val[0].OriginName != tc.wantOrigin {
				t.Fatalf("unexpected exchanges: %+v", got)
			}
		})
	}
}

func TestGetExchangesKeepsUnnamedAddressMembershipInPartialResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"exchanges":[{"address":"a","name":"Binance"},{"address":"b"}]}`))
	}))
	defer server.Close()
	got, err := getExchanges(server.URL)
	if err != nil || len(got.Val) != 2 || got.Val[1].Address != "b" || got.Val[1].Name != "" {
		t.Fatalf("partial metadata response lost membership: %+v, %v", got, err)
	}
}
