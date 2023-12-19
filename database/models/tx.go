package models

type Transaction struct {
	ID                uint   `gorm:"primaryKey"`
	Hash              string `gorm:"size:64"`
	Owner             string `gorm:"size:34;index"`
	To                string `gorm:"size:34;index"`
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
	ID        uint   `gorm:"primaryKey"`
	Hash      string `gorm:"size:64"`
	Token     string `gorm:"size:34;index"`
	From      string `gorm:"size:34;index"`
	To        string `gorm:"size:34;index"`
	Timestamp int64
	Amount    BigInt
}
