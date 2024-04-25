package utils

import (
	"fmt"
	"time"
)

type Reporter struct {
	reportCountThreshold int
	reportInterval       time.Duration
	reportFunc           func(ReporterState) string

	count       int
	finishCount int
	startTime   time.Time

	lastReportCount int
	lastReportTime  time.Time
}

type ReporterState struct {
	CountInc    int
	ElapsedTime float64

	CurrentCount int
	FinishCount  int
}

func NewReporter(reportCountThreshold int, reportInterval time.Duration, finishCount int, reportFunc func(ReporterState) string) *Reporter {
	return &Reporter{
		reportCountThreshold: reportCountThreshold,
		reportInterval:       reportInterval,
		reportFunc:           reportFunc,

		count:       0,
		finishCount: finishCount,
		startTime:   time.Now(),

		lastReportTime: time.Now(),
	}
}

func (r *Reporter) Add(count int) (bool, string) {
	r.count += count

	countIncrement := r.count - r.lastReportCount
	elapsedTime := time.Since(r.lastReportTime).Seconds()

	if (r.reportCountThreshold != 0 && countIncrement >= r.reportCountThreshold) || elapsedTime >= r.reportInterval.Seconds() {
		reportStr := r.reportFunc(ReporterState{
			CountInc:    countIncrement,
			ElapsedTime: elapsedTime,

			CurrentCount: r.count,
			FinishCount:  r.finishCount,
		})
		r.lastReportTime = time.Now()
		r.lastReportCount = r.count
		return true, reportStr
	}
	return false, ""
}

func (r *Reporter) Finish(title string) string {
	return fmt.Sprintf("%s finished, total count [%d], cost [%.2fs], speed [%.2f/sec]",
		title, r.count, time.Since(r.startTime).Seconds(), float64(r.count)/time.Since(r.startTime).Seconds())
}
