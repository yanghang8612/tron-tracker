package net

import (
	"testing"
	"time"

	"github.com/go-playground/assert/v2"
)

func TestDate(t *testing.T) {
	endDateStr := "2025-07-22"
	endDate, _ := time.Parse("2006-01-02", endDateStr)
	startDate := subtractBusinessDays(endDate, 10).Format("2006-01-02")
	assert.Equal(t, startDate, "2025-07-08")
}
