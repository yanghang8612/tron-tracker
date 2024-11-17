package types

import (
	"database/sql/driver"
	"slices"
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
		for _, val := range vals[1:] {
			count, _ := strconv.Atoi(val)
			ts.vals = append(ts.vals, uint(count))
		}
	} else {
		ts.vals = []uint{0}
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
	if !ts.isMarked(length) {
		ts.mark(length)
	}
	ts.vals[ts.index(length)] += count
}

func (ts *TransferStats) Get(length int) uint {
	if !ts.isMarked(length) {
		return 0
	}
	return ts.vals[ts.index(length)]
}

func (ts *TransferStats) Merge(other *TransferStats) {
	for length := 1; length <= 18; length++ {
		if other.isMarked(length) {
			count := other.Get(length)
			ts.Add(length, count)
		}
	}
}

func (ts *TransferStats) isMarked(length int) bool {
	return (ts.vals[0] & (1 << length)) != 0
}

func (ts *TransferStats) mark(length int) {
	ts.vals[0] |= 1 << length
	index := ts.index(length)
	ts.vals = slices.Insert(ts.vals, index, 0)
}

func (ts *TransferStats) index(length int) int {
	index := 1
	for i := 1; i < length; i++ {
		if ts.isMarked(i) {
			index++
		}
	}
	return index
}
