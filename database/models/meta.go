package models

import (
	"gorm.io/gorm"
)

const (
	TrackingDateKey          = "tracking_date"
	CountedDateKey           = "counted_date"
	CountedWeekKey           = "counted_week"
	TrackingStartBlockNumKey = "tracking_start_block_num"
	TrackedEthBlockNumKey    = "tracked_eth_block_num"
)

type Meta struct {
	gorm.Model
	Key string `gorm:"unique"`
	Val string
}
