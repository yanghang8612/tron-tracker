package models

type Charge struct {
	ID              uint   `gorm:"primaryKey"`
	Address         string `gorm:"char(21),uniqueIndex"`
	ExchangeName    string
	ExchangeAddress string
}
