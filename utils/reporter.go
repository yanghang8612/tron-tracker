package utils

import (
	"fmt"
	"time"
)

type Reporter struct {
	reportCountThreshold int
	reportInterval       time.Duration
	reportFormat         string
	count                int
	startTime            time.Time

	lastReportTime  time.Time
	lastReportCount int
}

func NewReporter(reportCountThreshold int, reportInterval time.Duration, reportFormat string) *Reporter {
	return &Reporter{
		reportCountThreshold: reportCountThreshold,
		reportInterval:       reportInterval,
		reportFormat:         reportFormat,
		startTime:            time.Now(),
		lastReportTime:       time.Now(),
	}
}

func (r *Reporter) Add(count int) (bool, string) {
	r.count += count

	countIncrement := r.count - r.lastReportCount
	elapsedTime := time.Since(r.lastReportTime).Seconds()
	if (r.reportCountThreshold != 0 && countIncrement >= r.reportCountThreshold) || elapsedTime >= r.reportInterval.Seconds() {
		reportStr := fmt.Sprintf(r.reportFormat, countIncrement, elapsedTime, float64(countIncrement)/elapsedTime)
		r.lastReportTime = time.Now()
		r.lastReportCount = r.count
		return true, reportStr
	}
	return false, ""
}

func (r *Reporter) Finish(format string) string {
	return fmt.Sprintf(format, r.count, int64(time.Since(r.startTime).Seconds()), float64(r.count)/time.Since(r.startTime).Seconds())
}
