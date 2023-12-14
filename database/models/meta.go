package models

import (
	"gorm.io/gorm"
)

const (
	LastTrackedDateKey     = "last_tracked_date"
	LastTrackedBlockNumKey = "last_tracked_block_num"
)

type Meta struct {
	gorm.Model
	Key string `gorm:"unique"`
	Val string
}
