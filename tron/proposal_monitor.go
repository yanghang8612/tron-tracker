package tron

import (
	"fmt"

	"tron-tracker/net"
	"tron-tracker/tron/types"

	"go.uber.org/zap"
)

// ReportProposalCreate immediately alerts on any proposal creation transaction,
// regardless of its parameters or execution result. Each transaction is reported once.
func (m *ActivityMonitor) ReportProposalCreate(tx types.Transaction, height uint, index uint16, txID string) {
	if m == nil || !hasProposalCreate(tx) {
		return
	}
	if txID == "" {
		txID = tx.TxID
	}
	if err := net.ReportAIOpsAlert(m.aiopsAppKeys, formatProposalCreateAIOpsAlert(height, index, txID)); err != nil {
		zap.S().Errorf("report ProposalCreateContract alert to AIOps failed: %v", err)
	}
}

func hasProposalCreate(tx types.Transaction) bool {
	for _, contract := range tx.RawData.Contract {
		if contract.Type == "ProposalCreateContract" {
			return true
		}
	}
	return false
}

func formatProposalCreateAIOpsAlert(height uint, index uint16, txID string) net.AIOpsAlert {
	return net.AIOpsAlert{
		EventID:      txID,
		EventType:    "trigger",
		AlarmName:    "TRON 提案发起交易告警",
		AlarmContent: fmt.Sprintf("TRON 检测到提案发起交易 ProposalCreateContract；区块/索引：%d/%d；交易哈希：%s", height, index, txID),
		EntityName:   "tron-tracker",
		EntityID:     "tron-transaction-" + txID,
		Priority:     5,
		Service:      "tron-mainnet",
		Contexts: []net.AIOpsContext{
			{Type: "link", Text: "Tronscan 交易详情", Href: "https://tronscan.io/#/transaction/" + txID},
		},
	}
}
