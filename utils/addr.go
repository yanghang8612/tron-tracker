package utils

import (
	"encoding/hex"
	"regexp"
	"strings"

	"github.com/btcsuite/btcd/btcutil/base58"
)

func EncodeToBase58(addrInHex string) string {
	addressBytes, _ := hex.DecodeString(addrInHex)
	if len(addressBytes) == 21 {
		return base58.CheckEncode(addressBytes[1:], 0x41)
	}
	return base58.CheckEncode(addressBytes, 0x41)
}

var trimRules = regexp.MustCompile(` Global| Reserves| Exchange| Deposit| wallet| Wallet|-hot|-Hot| hot| Hot|-cold|-Cold| Cold| cold| warm| Warm|\s+\d+$`)

func TrimExchangeName(name string) string {
	if strings.Contains(name, "Bitpie") || strings.Contains(name, "bitpie") {
		return "Bitpie"
	}
	if strings.Contains(name, "OKX") || strings.Contains(name, "okx") {
		return "Okex"
	}
	return trimRules.ReplaceAllString(name, ``)
}

func IsSameExchange(name1, name2 string) bool {
	return TrimExchangeName(name1) == TrimExchangeName(name2)
}
