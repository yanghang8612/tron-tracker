package models

type Transaction struct {
	ID                uint64 `gorm:"primaryKey"`
	Hash              string
	Owner             string `gorm:"index:idx_key"`
	To                string `gorm:"index:idx_key"`
	Height            uint
	Timestamp         int64
	Type              uint8  `gorm:"index:idx_key"`
	Name              string `gorm:"index:idx_key"`
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
	Token     string `gorm:"index:idx_key"`
	From      string `gorm:"index:idx_key"`
	To        string `gorm:"index:idx_key"`
	Timestamp int64
	Amount    BigInt
}
