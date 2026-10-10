package tron

import (
	"fmt"

	"tron-tracker/net"
	"tron-tracker/tron/types"
)

// ReportExecutionTimeout sends each timed-out transaction to the warning
// webhook with the existing personal Slack mention.
func (m *ActivityMonitor) ReportExecutionTimeout(info *types.TransactionInfo, height uint, index uint16) {
	if m == nil {
		return
	}
	if message := executionTimeoutWarning(info, height, index); message != "" {
		net.ReportWarningToSlack(message, true)
	}
}

func executionTimeoutWarning(info *types.TransactionInfo, height uint, index uint16) string {
	if info == nil || info.Receipt.Result != "OUT_OF_TIME" {
		return ""
	}
	return fmt.Sprintf("TRON 交易执行超时告警\n执行结果：`OUT_OF_TIME`\n区块 / 索引：`%d / %d`\n交易：<https://tronscan.io/#/transaction/%s|%s>",
		height, index, info.ID, info.ID)
}
