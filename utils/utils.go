package utils

import "fmt"

func FormatChangePercent(oldValue, newValue int64) string {
	if oldValue == 0 {
		return "∞%"
	} else {
		change := newValue - oldValue
		changePercent := float64(change) / float64(oldValue) * 100.0
		return fmt.Sprintf("%.2f%%", changePercent)
	}
}

func FormatOfPercent(total, part int64) string {
	if total == 0 {
		return "∞%"
	} else {
		percent := float64(part) / float64(total) * 100.0
		return fmt.Sprintf("%.2f%%", percent)
	}
}
