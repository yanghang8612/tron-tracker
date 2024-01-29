package utils

import "fmt"

func FormatChangePercent(oldValue uint64, newValue uint64) string {
	if oldValue == 0 {
		return "∞%"
	} else {
		change := newValue - oldValue
		changePercent := float64(change) / float64(oldValue) * 100
		return fmt.Sprintf("%.2f%%", changePercent)
	}
}
