package models

import "math/big"

type Transaction struct {
	ID                uint   `gorm:"primaryKey"`
	Hash              string `gorm:"size:64"`
	FromAddr          string `gorm:"size:34;index:addr"`
	ToAddr            string `gorm:"size:34;index:addr"`
	Height            uint
	Timestamp         int64
	Type              uint8  `gorm:"index"`
	Name              string `gorm:"size:34;index"`
	Amount            BigInt `gorm:"size:80;index"`
	Fee               int64
	EnergyTotal       int64
	EnergyFee         int64
	EnergyUsage       int64
	EnergyOriginUsage int64
	NetUsage          int64
	NetFee            int64
	Result            string
	SigCount          uint8
	Method            string `gorm:"size:8;index"`
}

func (tx *Transaction) SetAmount(amount int64) {
	tx.Amount = NewBigInt(big.NewInt(amount))
}
