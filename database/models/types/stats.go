package types

import (
	"database/sql/driver"
	"strconv"
	"strings"
)

type TransferStats struct {
	txCounts [18]uint
}

func (b *TransferStats) Scan(value interface{}) error {
	vals := strings.Split(string(value.([]byte)), ",")
	for i, val := range vals {
		count, _ := strconv.Atoi(val)
		b.txCounts[i] = uint(count)
	}
	return nil
}

func (b TransferStats) Value() (driver.Value, error) {
	res := strings.Builder{}
	for i, count := range b.txCounts {
		res.WriteString(strconv.Itoa(int(count)))
		if i != len(b.txCounts)-1 {
			res.WriteString(",")
		}
	}
	return res.String(), nil
}

func (b TransferStats) Merge(other TransferStats) {
	for i := 0; i < len(b.txCounts); i++ {
		b.txCounts[i] += other.txCounts[i]
	}
}

func (b TransferStats) Add(amount string) {
	amountType := len(amount)
	b.txCounts[amountType]++
}
