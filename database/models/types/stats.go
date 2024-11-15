package types

import (
	"database/sql/driver"
	"strconv"
	"strings"
)

type TransferStats struct {
	vals []uint
}

func NewTransferStats() TransferStats {
	return TransferStats{vals: []uint{0}}
}

func (ts *TransferStats) Scan(value interface{}) error {
	valStr := string(value.([]byte))
	vals := strings.Split(string(value.([]byte)), ",")

	if strings.HasPrefix(valStr, ",") {
		for i, val := range vals[1:] {
			count, _ := strconv.Atoi(val)
			ts.vals[i] = uint(count)
		}
	} else {
		for i, val := range vals {
			count, _ := strconv.Atoi(val)
			if count > 0 {
				ts.Add(i, uint(count))
			}
		}
	}

	return nil
}

func (ts TransferStats) Value() (driver.Value, error) {
	res := strings.Builder{}
	res.WriteString(",")

	for i, count := range ts.vals {
		res.WriteString(strconv.Itoa(int(count)))
		if i != len(ts.vals)-1 {
			res.WriteString(",")
		}
	}

	return res.String(), nil
}

func (ts *TransferStats) Add(length int, count uint) {
	ts.vals[ts.getOrMarkIndex(length)] += count
}

func (ts *TransferStats) Get(length int) uint {
	index := ts.getOrMarkIndex(length)
	return ts.vals[index]
}

func (ts *TransferStats) Merge(other *TransferStats) {
	for length := 1; length <= 18; length++ {
		if other.isLengthMarked(length) {
			count := other.Get(length)
			ts.Add(length, count)
		}
	}
}

func (ts *TransferStats) getOrMarkIndex(length int) int {
	if (ts.vals[0] & (1 << length)) == 0 {
		ts.vals[0] |= 1 << length
		index := 1
		for i := 1; i < length; i++ {
			if (ts.vals[0] & (1 << i)) != 0 {
				index++
			}
		}
		for len(ts.vals) <= index {
			ts.vals = append(ts.vals, 0)
		}
		return index
	}

	index := 1
	for i := 1; i < length; i++ {
		if (ts.vals[0] & (1 << i)) != 0 {
			index++
		}
	}
	return index
}

func (ts *TransferStats) isLengthMarked(length int) bool {
	return (ts.vals[0] & (1 << length)) != 0
}
