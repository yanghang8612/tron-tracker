package models

type Charger struct {
	ID              uint   `gorm:"primaryKey"`
	Address         string `gorm:"size:34;index"`
	ExchangeName    string
	ExchangeAddress string
}
