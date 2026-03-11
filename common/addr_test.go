package common

import (
	"testing"
)

func TestTrimExchangeName(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{name: "Kraken:", want: "Kraken"},
		{name: "Kraken: Hot Wallet", want: "Kraken"},
		{name: "Kraken - Hot Wallet", want: "Kraken"},
		{name: "Bitpie Hot Wallet", want: "Bitpie"},
		{name: "OKX Deposit Wallet", want: "Okex"},
	}

	for _, tt := range tests {
		if got := TrimExchangeName(tt.name); got != tt.want {
			t.Fatalf("TrimExchangeName(%q) = %q, want %q", tt.name, got, tt.want)
		}
	}
}
