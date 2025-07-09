package models

import (
	"math/big"

	"tron-tracker/database/models/types"
)

const TransferType = 255

type Transaction struct {
	ID                uint `gorm:"primaryKey"`
	Height            uint
	Index             uint16
	Type              uint8        `gorm:"index"`
	Name              string       `gorm:"size:34"`
	OwnerAddr         string       `gorm:"size:34"`
	FromAddr          string       `gorm:"size:34"`
	ToAddr            string       `gorm:"size:34"`
	Amount            types.BigInt `gorm:"size:80"`
	Fee               int64
	EnergyTotal       int64
	EnergyFee         int64
	EnergyUsage       int64
	EnergyOriginUsage int64
	NetUsage          int64
	NetFee            int64
	Result            uint8
	SigCount          uint8
	Method            string `gorm:"size:8;index"`
}

func (tx *Transaction) SetAmount(amount int64) {
	tx.Amount = types.NewBigInt(big.NewInt(amount))
}
