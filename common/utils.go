package common

import (
	"fmt"
	"math"
	"strings"
)

func FormatWithSignAndUnits(n float64) string {
	if n > 0 {
		return "+" + FormatWithUnits(n)
	}

	return FormatWithUnits(n)
}

func FormatWithUnits(n float64) string {
	abs := math.Abs(n)
	switch {
	case abs >= 1e12:
		return fmt.Sprintf("%.2f T", n/1e12)
	case abs >= 1e9:
		return fmt.Sprintf("%.2f B", n/1e9)
	case abs >= 1e5:
		return fmt.Sprintf("%.2f M", n/1e6)
	case abs >= 1e2:
		return fmt.Sprintf("%.2f K", n/1e3)
	default:
		return fmt.Sprintf("%.2f", n)
	}
}

func FormatAbChangePercent(prev, curr float64) string {
	if prev == 0 {
		return "∞%"
	}
	return fmt.Sprintf("%+.2f%%", (prev-curr)/math.Abs(prev)*100)
}

func FormatChangePercent(prev, curr int64) string {
	return FormatFloatChangePercent(float64(prev), float64(curr))
}

func FormatFloatChangePercent(prev, curr float64) string {
	if prev == 0 {
		return "∞%"
	}

	return fmt.Sprintf("%+.2f%%", (curr-prev)/prev*100.0)
}

func FormatPercentWithSign(percent float64) string {
	if percent == 0 {
		return "0.00%"
	}

	return fmt.Sprintf("%+.2f%%", percent)
}

func FormatOfPercent(total, part int64) string {
	if total == 0 {
		return "∞%"
	}

	percent := float64(part) / float64(total) * 100.0
	return fmt.Sprintf("%.2f%%", percent)
}

func FormatUSDTSupplyReport(data [][]interface{}) string {
	title := "Multi-Chain USDT Supply\n"
	var content strings.Builder
	for _, row := range data {
		if len(row[0].(string)) > 6 {
			content.WriteString(
				fmt.Sprintf("\t%s\t%s\t%s\n", row[0], row[1], row[2]))
		} else {
			content.WriteString(
				fmt.Sprintf("\t%s\t\t%s\t%s\n", row[0], row[1], row[2]))
		}
	}
	return title + content.String()
}

func EscapeMarkdownV2(text string) string {
	specialCharacters := "_*[]()~`>#+-=|{}.!"

	var builder strings.Builder

	for _, char := range text {
		if strings.ContainsRune(specialCharacters, char) {
			builder.WriteRune('\\')
		}
		builder.WriteRune(char)
	}

	return builder.String()
}
