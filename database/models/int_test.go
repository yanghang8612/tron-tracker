package models

import (
	"math/big"
	"testing"
)

func TestBigInt_Add(t *testing.T) {
	a := NewBigInt(big.NewInt(1))
	b := NewBigInt(big.NewInt(2))
	c := a
	c.Add(b)
	if c.String() != "3" {
		t.Errorf("Add failed: %s", a.String())
	}
}
