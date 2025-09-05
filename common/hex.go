package common

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

func ConvertDecToBigInt(decStr string) *big.Int {
	res, _ := new(big.Int).SetString(decStr, 10)
	return res
}

func WithDecimal(value *big.Int, decimals int) *big.Int {
	ten := big.NewInt(10)
	exp := big.NewInt(int64(decimals))
	multiplier := new(big.Int).Exp(ten, exp, nil)
	return new(big.Int).Mul(value, multiplier)
}

func DropDecimal(value *big.Int, decimals int) *big.Int {
	ten := big.NewInt(10)
	exp := big.NewInt(int64(decimals))
	divisor := new(big.Int).Exp(ten, exp, nil)
	return new(big.Int).Div(value, divisor)
}
