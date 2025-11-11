package common

import (
	"fmt"
	"math"
	"strings"

	"tron-tracker/database/models"

	"github.com/dustin/go-humanize"
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
	case abs >= 1e11:
		return fmt.Sprintf("%.2f T", n/1e12)
	case abs >= 1e8:
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
