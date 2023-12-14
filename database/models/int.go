package models

import (
	"database/sql/driver"
	"math/big"
)

type BigInt struct {
	val *big.Int
}

func NewBigInt(val *big.Int) BigInt {
	return BigInt{val: val}
}

func (bi *BigInt) Scan(value interface{}) error {
	bi.val = new(big.Int)
	bi.val, _ = bi.val.SetString(string(value.([]byte)), 10)
	return nil
}

func (bi BigInt) Value() (driver.Value, error) {
	return bi.val.String(), nil
}
