package models

import "math/big"

type Transaction struct {
	ID                uint   `gorm:"primaryKey"`
	Hash              string `gorm:"size:64;index"`
	FromAddr          string `gorm:"size:34;index"`
	ToAddr            string `gorm:"size:34;index"`
	Height            uint
	Timestamp         int64
	Type              uint8  `gorm:"index:idx_key"`
	Name              string `gorm:"index:idx_key"`
	Amount            BigInt
	Fee               uint
	EnergyTotal       uint
	EnergyFee         uint
	EnergyUsage       uint
	EnergyOriginUsage uint
	NetUsage          uint
	NetFee            uint
	Result            string
	SigCount          uint
	Method            string `gorm:"size:8;"`
}

func (tx Transaction) SetAmount(amount int64) {
	tx.Amount = NewBigInt(big.NewInt(amount))
}

type TRC20Transfer struct {
	ID        uint   `gorm:"primaryKey"`
	Hash      string `gorm:"size:64;index"`
	Token     string `gorm:"size:34;index"`
	FromAddr  string `gorm:"size:34;index"`
	ToAddr    string `gorm:"size:34;index"`
	Timestamp int64
	Amount    BigInt
}
