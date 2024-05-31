package models

type Charger struct {
	ID            uint   `gorm:"primaryKey"`
	Address       string `gorm:"size:34;index"`
	ExchangeName  string
	BackupAddress string `gorm:"size:34"`
	IsFake        bool
}

type Phisher struct {
	ID      uint   `gorm:"primaryKey"`
	Address string `gorm:"size:34;index"`
}

type EthUSDTUser struct {
	ID          uint   `gorm:"primaryKey"`
	Address     string `gorm:"size:42;index"`
	Amount      int64  `gorm:"index"`
	TransferIn  uint
	TransferOut uint
}
