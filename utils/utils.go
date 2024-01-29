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

func main() {
	a := uint64(74388855307020)
	b := uint64(79693086114680)
	println(FormatChangePercent(b, a))
	println(FormatChangePercent(a, b))
}
