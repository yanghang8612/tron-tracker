package models

import (
	"gorm.io/gorm"
)

const (
	TrackingDateKey          = "tracking_date"
	CountedDateKey           = "counted_date"
	CountedWeekKey           = "counted_week"
	TrackingStartBlockNumKey = "tracking_start_block_num"
	TelegramBotChatID        = "telegram_bot_chat_id"
	VolumeReminders          = "volume_reminders"
)

type Meta struct {
	gorm.Model
	Key string `gorm:"unique"`
	Val string
}
