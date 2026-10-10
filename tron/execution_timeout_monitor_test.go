package tron

import (
	"strings"
	"testing"

	"tron-tracker/tron/types"
)

func TestExecutionTimeoutWarning(t *testing.T) {
	for _, result := range []string{"OUT_OF_TIME", "SUCCESS", "OUT_OF_ENERGY", "REVERT", "", "FAILED"} {
		t.Run(result, func(t *testing.T) {
			info := types.TransactionInfo{ID: "timeout-tx", Receipt: types.Receipt{Result: result}}
			message := executionTimeoutWarning(&info, 123, 7)
			if result != "OUT_OF_TIME" {
				if message != "" {
					t.Fatalf("unexpected alert for %q: %s", result, message)
				}
				return
			}
			for _, want := range []string{"OUT_OF_TIME", "123 / 7", "https://tronscan.io/#/transaction/timeout-tx"} {
				if !strings.Contains(message, want) {
					t.Fatalf("warning missing %q: %s", want, message)
				}
			}
		})
	}
}

func TestReportExecutionTimeoutWithDisabledMonitor(t *testing.T) {
	var monitor *ActivityMonitor
	monitor.ReportExecutionTimeout(&types.TransactionInfo{Receipt: types.Receipt{Result: "OUT_OF_TIME"}}, 123, 7)
}
