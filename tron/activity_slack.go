package tron

import (
	"fmt"
	"strings"

	"tron-tracker/net"

	"github.com/dustin/go-humanize"
)

func formatActivityAlert(activity MonitoredActivity) net.SlackMessage {
	return net.SlackMessage{
		Text: fmt.Sprintf("TRON large %s: %s by %s",
			strings.ToLower(activity.Type), formatAmount(activity.Amount, activity.Unit), activity.Actor),
		Blocks: alertBlocks("TRON Large "+activity.Type+" Alert",
			slackFields(activity.Fields),
			contextElements(activity.TxID, "")),
	}
}

func formatCumulativeActivityAlert(accumulator *activityAccumulator) net.SlackMessage {
	fields := []activityField{
		{Label: "Activity", Value: accumulator.Type},
		{Label: "Total", Value: formatAmount(accumulator.Amount, accumulator.Unit)},
		{Label: accumulator.ActorLabel, Value: "`" + accumulator.Actor + "`"},
		{Label: "Transactions", Value: fmt.Sprintf("`%d`", accumulator.TxCount)},
		{Label: "Block Range", Value: fmt.Sprintf("`%d - %d`", accumulator.FirstBlock, accumulator.LastBlock)},
	}

	for _, item := range accumulator.Breakdown {
		fields = append(fields, activityField{
			Label: item.Label,
			Value: formatAmount(item.Amount, item.Unit),
		})
	}

	return net.SlackMessage{
		Text: fmt.Sprintf("TRON cumulative large %s: %s by %s across %d txs",
			strings.ToLower(accumulator.Type), formatAmount(accumulator.Amount, accumulator.Unit),
			accumulator.Actor, accumulator.TxCount),
		Blocks: alertBlocks("TRON Cumulative "+accumulator.Type+" Alert",
			slackFields(fields),
			contextElements(accumulator.FirstTxID, accumulator.LastTxID)),
	}
}

func formatAmount(amount int64, unit string) string {
	return formatBaseUnits(amount) + " " + unit
}

func formatBaseUnits(amount int64) string {
	whole := amount / baseUnitsPerToken
	rem := amount % baseUnitsPerToken
	if rem == 0 {
		return humanize.Comma(whole)
	}

	return fmt.Sprintf("%s.%06d", humanize.Comma(whole), rem)
}

func formatTronTxURL(txHash string) string {
	return fmt.Sprintf(":clippy:<https://tronscan.io/#/transaction/%s|TxHash>", txHash)
}

func alertBlocks(title string, fields, contextElements []net.SlackTextObject) []net.SlackBlock {
	blocks := []net.SlackBlock{
		headerBlock(title),
		sectionFieldsBlock(fields...),
	}
	if len(contextElements) > 0 {
		blocks = append(blocks, contextBlock(contextElements...))
	}
	return blocks
}

func headerBlock(text string) net.SlackBlock {
	return net.SlackBlock{
		Type: "header",
		Text: &net.SlackTextObject{
			Type:  "plain_text",
			Text:  text,
			Emoji: true,
		},
	}
}

func sectionFieldsBlock(fields ...net.SlackTextObject) net.SlackBlock {
	return net.SlackBlock{
		Type:   "section",
		Fields: fields,
	}
}

func contextBlock(elements ...net.SlackTextObject) net.SlackBlock {
	return net.SlackBlock{
		Type:     "context",
		Elements: elements,
	}
}

func slackFields(fields []activityField) []net.SlackTextObject {
	objects := make([]net.SlackTextObject, 0, len(fields))
	for _, field := range fields {
		objects = append(objects, mrkdwnField(field.Label, field.Value))
	}
	return objects
}

func mrkdwnField(label, value string) net.SlackTextObject {
	return net.SlackTextObject{
		Type: "mrkdwn",
		Text: fmt.Sprintf("*%s*\n%s", label, value),
	}
}

func contextElements(firstTxID, lastTxID string) []net.SlackTextObject {
	if firstTxID == "" {
		return []net.SlackTextObject{}
	}

	if lastTxID == "" || lastTxID == firstTxID {
		return []net.SlackTextObject{mrkdwnText(formatTronTxURL(firstTxID))}
	}

	return []net.SlackTextObject{
		mrkdwnText("First " + formatTronTxURL(firstTxID)),
		mrkdwnText("Last " + formatTronTxURL(lastTxID)),
	}
}

func mrkdwnText(text string) net.SlackTextObject {
	return net.SlackTextObject{
		Type: "mrkdwn",
		Text: text,
	}
}
