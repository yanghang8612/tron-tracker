package models

import (
	"strconv"

	"gorm.io/gorm"
)

const (
	LastTrackedBlockNumKey = "last_tracked_block_num"
)

type Meta struct {
	gorm.Model
	Key string `gorm:"unique"`
	Val string
}

func NewMeta(blockNum uint) *Meta {
	return &Meta{
		Key: LastTrackedBlockNumKey,
		Val: strconv.Itoa(int(blockNum)),
	}
}
