package models

type Charger struct {
	ID              uint   `gorm:"primaryKey"`
	Address         string `gorm:"size:21;uniqueIndex"`
	ExchangeName    string
	ExchangeAddress string
}
