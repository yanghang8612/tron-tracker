package utils

import (
	"encoding/hex"

	"github.com/btcsuite/btcd/btcutil/base58"
)

func EncodeToBase58(addrInHex string) string {
	addressBytes, _ := hex.DecodeString(addrInHex)
	if len(addressBytes) == 21 {
		return base58.CheckEncode(addressBytes[1:], 0x41)
	}
	return base58.CheckEncode(addressBytes, 0x41)
}
