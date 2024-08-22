package types

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

func (b *BigInt) Scan(value interface{}) error {
	b.val = new(big.Int)
	b.val, _ = b.val.SetString(string(value.([]byte)), 10)
	return nil
}

func (b BigInt) Value() (driver.Value, error) {
	return b.val.String(), nil
}

func (b BigInt) Int64() int64 {
	return b.val.Int64()
}

func (b BigInt) Neg() {
	b.val.Neg(b.val)
}

func (b BigInt) Add(other BigInt) {
	b.val.Add(b.val, other.val)
}

func (b BigInt) String() string {
	return b.val.String()
}
