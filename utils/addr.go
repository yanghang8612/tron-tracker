package utils

import (
	"encoding/hex"
	"regexp"

	"github.com/btcsuite/btcd/btcutil/base58"
)

func EncodeToBase58(addrInHex string) string {
	addressBytes, _ := hex.DecodeString(addrInHex)
	if len(addressBytes) == 21 {
		return base58.CheckEncode(addressBytes[1:], 0x41)
	}
	return base58.CheckEncode(addressBytes, 0x41)
}

func TrimExchangeName(name string) string {
	return regexp.MustCompile(`-hot|-Hot|\s\d+$`).ReplaceAllString(name, ``)
}

func IsSameExchange(name1 string, name2 string) bool {
	return TrimExchangeName(name1) == TrimExchangeName(name2)
}
