package models

type Charger struct {
	ID              uint   `gorm:"primaryKey"`
	Created         int64  `gorm:"autoCreateTime"`
	Address         string `gorm:"size:34;index"`
	ExchangeName    string
	ExchangeAddress string
	IsFake          bool `gorm:"-:all"`
}
