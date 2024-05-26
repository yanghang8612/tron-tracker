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
