package tron

import (
	"fmt"
	"strings"

	"tron-tracker/net"
	"tron-tracker/tron/types"

	"go.uber.org/zap"
)

var monitoredCreateSmartContractHashFields = []string{"code_hash", "trx_hash"}

type createSmartContractHashActivity struct {
	Height uint
	Index  uint16
	TxID   string
	Fields []string
}

// ReportCreateSmartContractHashFields reports contract-creation transactions
// that populate fields which are normally assigned after contract deployment.
func (m *ActivityMonitor) ReportCreateSmartContractHashFields(
	tx types.Transaction, height uint, index uint16, txID string,
) {
	if m == nil {
		return
	}

	activity, ok := detectCreateSmartContractHashFields(tx, height, index, txID)
	if !ok {
		return
	}

	net.ReportWarningChannelMessageToSlack(formatCreateSmartContractHashAlert(activity))
	if err := net.ReportAIOpsAlert(m.aiopsAppKeys, formatCreateSmartContractHashAIOpsAlert(activity)); err != nil {
		zap.S().Errorf("report suspicious CreateSmartContract alert to AIOps failed: %v", err)
	}
}

func detectCreateSmartContractHashFields(
	tx types.Transaction, height uint, index uint16, txID string,
) (createSmartContractHashActivity, bool) {
	activity := createSmartContractHashActivity{
		Height: height,
		Index:  index,
		TxID:   txID,
	}
	seen := make(map[string]struct{}, len(monitoredCreateSmartContractHashFields))

	for _, contract := range tx.RawData.Contract {
		if contract.Type != "CreateSmartContract" {
			continue
		}

		newContract, ok := contract.Parameter.Value["new_contract"].(map[string]interface{})
		if !ok {
			continue
		}
		for _, field := range monitoredCreateSmartContractHashFields {
			if _, exists := newContract[field]; !exists {
				continue
			}
			if _, exists := seen[field]; exists {
				continue
			}
			seen[field] = struct{}{}
			activity.Fields = append(activity.Fields, field)
		}
	}

	return activity, len(activity.Fields) > 0
}

func formatCreateSmartContractHashAlert(activity createSmartContractHashActivity) net.SlackMessage {
	fields := []activityField{
		{Label: "Risk Level", Value: "`HIGH`"},
		{Label: "Contract Type", Value: "`CreateSmartContract`"},
		{Label: "Unexpected Fields", Value: "`" + strings.Join(activity.Fields, "`, `") + "`"},
		{Label: "Block / Index", Value: fmt.Sprintf("`%d / %d`", activity.Height, activity.Index)},
	}

	return net.SlackMessage{
		Text: fmt.Sprintf("TRON suspicious CreateSmartContract transaction detected: %s", activity.TxID),
		Blocks: alertBlocks(
			"TRON High-risk Contract Creation Alert",
			slackFields(fields),
			contextElements(activity.TxID, ""),
		),
	}
}

func formatCreateSmartContractHashAIOpsAlert(activity createSmartContractHashActivity) net.AIOpsAlert {
	content := fmt.Sprintf(
		"TRON 检测到携带异常哈希字段的 CreateSmartContract 交易；字段：%s；区块/索引：%d/%d；交易哈希：%s",
		strings.Join(activity.Fields, ","),
		activity.Height,
		activity.Index,
		activity.TxID,
	)
	content = truncateRunes(content, 800)

	return net.AIOpsAlert{
		EventID:      activity.TxID,
		EventType:    "trigger",
		AlarmName:    "TRON 高风险智能合约创建交易告警",
		AlarmContent: content,
		EntityName:   "tron-tracker",
		EntityID:     "tron-transaction-" + activity.TxID,
		Priority:     5,
		Service:      "tron-mainnet",
		Contexts: []net.AIOpsContext{
			{Type: "link", Text: "Tronscan 交易详情", Href: "https://tronscan.io/#/transaction/" + activity.TxID},
		},
	}
}
