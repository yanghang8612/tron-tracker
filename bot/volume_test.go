package bot

import (
	"testing"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

func TestVolumeRuleInputIsScopedToInitiatingUserAndChat(t *testing.T) {
	otherUser := routingTestMessage("Binance TRX/USDT 1M 60k", "allowed", false)
	otherUser.From.ID = 11
	otherChat := routingTestMessage("Binance TRX/USDT 1M 60k", "allowed", false)
	otherChat.Chat.ID = -201
	withoutSender := routingTestMessage("Binance TRX/USDT 1M 60k", "allowed", false)
	withoutSender.From = nil
	withoutChat := routingTestMessage("Binance TRX/USDT 1M 60k", "allowed", false)
	withoutChat.Chat = nil

	tests := []struct {
		name    string
		message *tgbotapi.Message
		want    bool
	}{
		{name: "owner rule text", message: routingTestMessage("Binance TRX/USDT 1M 60k", "allowed", false), want: true},
		{name: "other user", message: otherUser},
		{name: "other chat", message: otherChat},
		{name: "owner command", message: routingTestMessage("/listrules", "allowed", true)},
		{name: "other bot command", message: routingTestMessage("/start@OtherBot", "allowed", true)},
		{name: "empty text", message: routingTestMessage("", "allowed", false)},
		{name: "nil message"},
		{name: "missing sender", message: withoutSender},
		{name: "missing chat", message: withoutChat},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bot := &VolumeBot{pendingRuleInput: &ruleInputSession{chatID: -200, userID: 10}}
			if got := bot.isRuleInput(tt.message); got != tt.want {
				t.Fatalf("isRuleInput() = %v, want %v", got, tt.want)
			}
			if bot.pendingRuleInput == nil {
				t.Fatal("checking a message consumed the pending rule session")
			}
		})
	}
	bot := &VolumeBot{}
	if bot.isRuleInput(routingTestMessage("Binance TRX/USDT 1M 60k", "allowed", false)) {
		t.Fatal("rule text was accepted without an active session")
	}
}

func TestVolumeUnrelatedMessagesStaySilentDuringRuleInput(t *testing.T) {
	base, client := newRoutingTestBot(t)
	bot := &VolumeBot{Bot: base, pendingRuleInput: &ruleInputSession{chatID: -200, userID: 10}}
	foreignText := routingTestMessage("hello", "outsider", false)
	foreignText.From.ID = 11
	foreignChat := routingTestMessage("hello", "outsider", false)
	foreignChat.Chat.ID = -201
	for _, message := range []*tgbotapi.Message{
		foreignText,
		foreignChat,
		routingTestMessage("/start@OtherBot", "outsider", true),
	} {
		if bot.authorizeMessage(message, bot.isRuleInput(message)) {
			t.Fatal("unrelated message was accepted during rule input")
		}
	}
	if len(client.sent) != 0 {
		t.Fatalf("unrelated messages generated %d Telegram replies", len(client.sent))
	}
	ownerText := routingTestMessage("Binance TRX/USDT 1M 60k", "allowed", false)
	if !bot.authorizeMessage(ownerText, bot.isRuleInput(ownerText)) {
		t.Fatal("unrelated messages prevented the owner's rule input")
	}
}

func TestAddRuleRejectsMalformedFields(t *testing.T) {
	bot := &VolumeBot{}
	for _, fields := range [][]string{
		nil,
		{},
		{"Binance"},
		{"Binance", "TRX/USDT"},
		{"Binance", "TRX/USDT", "1M"},
		{"Binance", "TRX/USDT", "1M", "60k", "extra"},
	} {
		if ok, message := bot.addRule(fields); ok || message == "" {
			t.Errorf("addRule(%q) = (%v, %q), want rejection with an explanation", fields, ok, message)
		}
	}
}
