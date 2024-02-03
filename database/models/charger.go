package models

type Charger struct {
	ID              uint   `gorm:"primaryKey"`
	Created         int64  `gorm:"autoCreateTime"`
	Address         string `gorm:"size:34;index"`
	ExchangeName    string
	ExchangeAddress string `gorm:"size:34"`
	BackupAddress   string `gorm:"size:34"`
	IsFake          bool   `gorm:"-:all"`
}
