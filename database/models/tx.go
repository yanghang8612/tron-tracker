package models

type Transaction struct {
	ID                uint `gorm:"primaryKey"`
	Hash              string
	Owner             string `gorm:"char(21),index"`
	To                string `gorm:"char(21),index"`
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
	Hash      string `gorm:"char(32)"`
	Token     string `gorm:"char(21),index"`
	From      string `gorm:"char(21),index"`
	To        string `gorm:"char(21),index"`
	Timestamp int64
	Amount    BigInt
}
