package models

type Transaction struct {
	ID                uint64 `gorm:"primaryKey"`
	Hash              string
	Owner             string `gorm:"index"`
	To                string
	Timestamp         uint  `gorm:"index"`
	Type              uint8 `gorm:"index"`
	Name              string
	Amount            int64
	Fee               uint
	EnergyTotal       uint
	EnergyFee         uint
	EnergyUsage       uint
	EnergyOriginUsage uint
	NetUsage          uint
	NetFee            uint
	Result            string
}
