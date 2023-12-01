package utils

import (
	"encoding/hex"
	"math/big"
)

func DecodeString(hexStr string) []byte {
	bytes, _ := hex.DecodeString(hexStr)
	return bytes
}

func ConvertHexToBigInt(hexStr string) *big.Int {
	res, _ := new(big.Int).SetString(hexStr, 16)
	return res
}
