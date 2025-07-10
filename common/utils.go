package common

import (
	"fmt"
	"math"
	"strings"

	"github.com/dustin/go-humanize"
	"tron-tracker/database/models"
)

func FormatWithUnits(n float64) string {
	abs := math.Abs(n)
	switch {
	case abs >= 1e12:
		return fmt.Sprintf("%.2f T", n/1e12)
	case abs >= 1e9:
		return fmt.Sprintf("%.2f B", n/1e9)
	case abs >= 1e6:
		return fmt.Sprintf("%.2f M", n/1e6)
	case abs >= 1e3:
		return fmt.Sprintf("%.2f K", n/1e3)
	default:
		return fmt.Sprintf("%.2f", n)
	}
}

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

func FormatStorageDiffReport(curStats, lastStats *models.USDTStorageStatistic) string {
	return fmt.Sprintf("SetStorage:\n"+
		"\tAverage Fee Per Tx: %.2f TRX (%s)\n"+
		"\tDaily transactions: %s (%s)\n"+
		"\tDaily total energy: %s (%s)\n"+
		"\tDaily energy with staking: %s (%s)\n"+
		"\tDaily energy fee: %s TRX (%s)\n"+
		"\tBurn energy: %.2f%%\n"+
		"ResetStorage:\n"+
		"\tAverage Fee Per Tx: %.2f TRX (%s)\n"+
		"\tDaily transactions: %s (%s)\n"+
		"\tDaily total energy: %s (%s)\n"+
		"\tDaily energy with staking: %s (%s)\n"+
		"\tDaily energy fee: %s TRX (%s)\n"+
		"\tBurn energy: %.2f%%\n",
		float64(curStats.SetEnergyFee)/float64(curStats.SetTxCount)/1e6,
		FormatChangePercent(int64(lastStats.SetEnergyFee/uint64(lastStats.SetTxCount)), int64(curStats.SetEnergyFee/uint64(curStats.SetTxCount))),
		humanize.Comma(int64(curStats.SetTxCount/7)),
		FormatChangePercent(int64(lastStats.SetTxCount), int64(curStats.SetTxCount)),
		humanize.Comma(int64(curStats.SetEnergyTotal/7)),
		FormatChangePercent(int64(lastStats.SetEnergyTotal), int64(curStats.SetEnergyTotal)),
		humanize.Comma(int64(curStats.SetEnergyUsage+curStats.SetEnergyOriginUsage)/7),
		FormatChangePercent(int64(lastStats.SetEnergyUsage+lastStats.SetEnergyOriginUsage), int64(curStats.SetEnergyUsage+curStats.SetEnergyOriginUsage)),
		humanize.Comma(int64(curStats.SetEnergyFee/7_000_000)),
		FormatChangePercent(int64(lastStats.SetEnergyFee), int64(curStats.SetEnergyFee)),
		100.0-float64(curStats.SetEnergyUsage+curStats.SetEnergyOriginUsage)/float64(curStats.SetEnergyTotal)*100,
		float64(curStats.ResetEnergyFee)/float64(curStats.ResetTxCount)/1e6,
		FormatChangePercent(int64(lastStats.ResetEnergyFee/uint64(lastStats.ResetTxCount)), int64(curStats.ResetEnergyFee/uint64(curStats.ResetTxCount))),
		humanize.Comma(int64(curStats.ResetTxCount/7)),
		FormatChangePercent(int64(lastStats.ResetTxCount), int64(curStats.ResetTxCount)),
		humanize.Comma(int64(curStats.ResetEnergyTotal/7)),
		FormatChangePercent(int64(lastStats.ResetEnergyTotal), int64(curStats.ResetEnergyTotal)),
		humanize.Comma(int64(curStats.ResetEnergyUsage+curStats.ResetEnergyOriginUsage)/7),
		FormatChangePercent(int64(lastStats.ResetEnergyUsage+lastStats.ResetEnergyOriginUsage), int64(curStats.ResetEnergyUsage+curStats.ResetEnergyOriginUsage)),
		humanize.Comma(int64(curStats.ResetEnergyFee/7_000_000)),
		FormatChangePercent(int64(lastStats.ResetEnergyFee), int64(curStats.ResetEnergyFee)),
		100.0-float64(curStats.ResetEnergyUsage+curStats.ResetEnergyOriginUsage)/float64(curStats.ResetEnergyTotal)*100)
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
