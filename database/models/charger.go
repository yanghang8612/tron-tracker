package models

type Charger struct {
	ID              uint   `gorm:"primaryKey"`
	Address         string `gorm:"size:34;uniqueIndex"`
	ExchangeName    string
	ExchangeAddress string `gorm:"size:34;index"`
	BackupAddress   string `gorm:"size:34"`
	IsFake          bool
}
