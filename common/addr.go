package common

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

var trimRules = regexp.MustCompile(`[.:\s-]+(?:` +
	// exchange/wallet types
	`Global|Reserves|Exchange|Deposit(?:AndWithdraw)?|[Ww]allet|[Hh]ot|[Cc]old|[Ww]arm|` +
	// function/service descriptors
	`cross-chain|Bridge|Funder|Marketing|Autobot|Earn|[Dd]e[Ff]i|` +
	// roles/identifiers
	`SR|Partner|Service|Provider|Batch|Sender|Multisig|` +
	// resource related
	`Energy|Rent|Bank|Fee` +
	`)\d*|\s+\d+$|[.:\s-]+$`)

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
	return strings.EqualFold(TrimExchangeName(name1), TrimExchangeName(name2))
}
