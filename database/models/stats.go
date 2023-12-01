package models

import (
	"time"

	"gorm.io/gorm"
)

type Stats struct {
	gorm.Model
	Date              *time.Time `gorm:"index"`
	Owner             string     `gorm:"index"`
	EnergyTotal       uint
	EnergyFee         uint
	EnergyUsage       uint
	EnergyOriginUsage uint
	NetUsage          uint
	NetFee            uint
	TransactionTotal  uint
	TRXTotal          uint
	TRC10Total        uint
	SCTotal           uint
}
