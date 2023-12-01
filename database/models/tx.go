package models

type Transaction struct {
	ID                uint64 `gorm:"primaryKey"`
	Hash              string
	Owner             string `gorm:"index"`
	To                string `gorm:"index"`
	Height            uint
	Timestamp         int64
	Type              uint8  `gorm:"index"`
	Name              string `gorm:"index"`
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

type TRC20Transfer struct {
	ID        uint64 `gorm:"primaryKey"`
	Hash      string
	Token     string `gorm:"index"`
	From      string `gorm:"index"`
	To        string `gorm:"index"`
	Timestamp int64
	Amount    BigInt
}
