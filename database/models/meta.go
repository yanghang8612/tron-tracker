package models

import (
	"gorm.io/gorm"
)

const (
	TrackingDateKey          = "tracking_date"
	CountedDateKey           = "counted_date"
	TrackingStartBlockNumKey = "tracking_start_block_num"
)

type Meta struct {
	gorm.Model
	Key string `gorm:"unique"`
	Val string
}
