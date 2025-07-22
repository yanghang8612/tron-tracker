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
	if other.val != nil {
		if b.val == nil {
			b.val = big.NewInt(0)
		}
		b.val.Add(b.val, other.val)
	}
}

func (b BigInt) String() string {
	return b.val.String()
}

func (b BigInt) Cmp(other BigInt) int {
	switch {
	case b.val == nil && other.val == nil:
		return 0
	case b.val == nil:
		return -1
	case other.val == nil:
		return 1
	default:
		return b.val.Cmp(other.val)
	}
}

func (b BigInt) Length() int {
	return len(b.val.String())
}

func (b *BigInt) MarshalJSON() ([]byte, error) {
	if b.val == nil {
		return []byte("0"), nil
	}
	return []byte(`"` + b.val.String() + `"`), nil
}
