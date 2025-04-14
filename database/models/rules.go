package models

type Rule struct {
	ID                  uint    `gorm:"primaryKey"`
	ExchangeName        string  `gorm:"size:32" json:"exchange_name,omitempty"`
	Pair                string  `gorm:"size:16" json:"pair,omitempty"`
	Volume              float64 `json:"volume,omitempty"`
	DepthUsdPositiveTwo float64 `json:"depth_usd_positive_two,omitempty"`
	DepthUsdNegativeTwo float64 `json:"depth_usd_negative_two,omitempty"`
}
