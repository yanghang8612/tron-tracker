package utils

import "fmt"

func FormatChangePercent(oldValue, newValue int64) string {
	return FormatFloatChangePercent(float64(oldValue), float64(newValue))
}

func FormatFloatChangePercent(oldValue, newValue float64) string {
	if oldValue == 0 {
		return "∞%"
	} else {
		change := newValue - oldValue
		changePercent := change / oldValue * 100.0
		if changePercent <= 0 {
			return fmt.Sprintf("%.2f%%", changePercent)
		} else {
			return fmt.Sprintf("+%.2f%%", changePercent)
		}
	}
}

func FormatPercentWithSign(percent float64) string {
	if percent == 0 {
		return "0.00%"
	} else if percent > 0 {
		return fmt.Sprintf("+%.2f%%", percent)
	} else {
		return fmt.Sprintf("%.2f%%", percent)
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
