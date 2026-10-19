package net

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"tron-tracker/config"
)

func TestTrackerRequestsDoNotDisguiseFailuresAsEmptyData(t *testing.T) {
	oldConfig := configs
	t.Cleanup(func() { configs = oldConfig })
	for _, tc := range []struct {
		name, body string
		status     int
	}{
		{"HTTP 503", `[]`, 503},
		{"HTTP 429", `{}`, 429},
		{"API error", `{"Error":"index unavailable"}`, 200},
		{"invalid JSON", `bad gateway`, 200},
		{"null data", `null`, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			configs = &config.NetConfig{FullNode: server.URL}
			if _, err := GetTransactionInfoList(1); err == nil {
				t.Fatal("receipt request masked an upstream error")
			}
			if _, err := GetBlockByHeight(1); err == nil {
				t.Fatal("block request masked an upstream error")
			}
		})
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`[]`)) }))
	defer server.Close()
	configs = &config.NetConfig{FullNode: server.URL}
	if got, err := GetTransactionInfoList(1); err != nil || len(got) != 0 {
		t.Fatalf("valid empty array = %v, %v", got, err)
	}
}
