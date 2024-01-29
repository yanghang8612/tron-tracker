package utils

import "fmt"

func FormatChangePercent(oldValue uint64, newValue uint64) string {
	if oldValue == 0 {
		return "∞%"
	} else {
		change := int64(newValue - oldValue)
		changePercent := float64(change) / float64(oldValue) * 100.0
		return fmt.Sprintf("%.2f%%", changePercent)
	}
}

func FormatOfPercent(total uint64, part uint64) string {
	if total == 0 {
		return "∞%"
	} else {
		percent := float64(part) / float64(total) * 100.0
		return fmt.Sprintf("%.2f%%", percent)
	}
}

func FormatReadableNumber(num uint64) string {
	return fmt.Sprintf("%.0f", float64(num))
}
