package bot

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"go.uber.org/zap"
)

type recordingTelegramClient struct {
	sent []url.Values
}

func (c *recordingTelegramClient) Do(req *http.Request) (*http.Response, error) {
	var body string
	switch {
	case strings.HasSuffix(req.URL.Path, "/getMe"):
		body = `{"ok":true,"result":{"id":100,"is_bot":true,"first_name":"Tracker","username":"TrackerBot"}}`
	case strings.HasSuffix(req.URL.Path, "/sendMessage"):
		if err := req.ParseForm(); err != nil {
			return nil, err
		}
		c.sent = append(c.sent, req.PostForm)
		body = `{"ok":true,"result":{"message_id":1,"chat":{"id":-200,"type":"supergroup"}}}`
	default:
		return nil, fmt.Errorf("unexpected Telegram request: %s", req.URL.Path)
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}, nil
}

func newRoutingTestBot(t *testing.T) (*Bot, *recordingTelegramClient) {
	t.Helper()
	client := &recordingTelegramClient{}
	api, err := tgbotapi.NewBotAPIWithClient("test-token", "https://telegram.invalid/bot%s/%s", client)
	if err != nil {
		t.Fatal(err)
	}
	return &Bot{
		botApi:     api,
		logger:     zap.NewNop().Sugar(),
		validUsers: map[string]bool{"allowed": true},
	}, client
}

func routingTestMessage(text, username string, command bool) *tgbotapi.Message {
	message := &tgbotapi.Message{
		MessageID: 1,
		Text:      text,
		From:      &tgbotapi.User{ID: 10, UserName: username},
		Chat:      &tgbotapi.Chat{ID: -200, Type: "supergroup"},
	}
	if command {
		message.Entities = []tgbotapi.MessageEntity{{Type: "bot_command", Offset: 0, Length: len(strings.Fields(text)[0])}}
	}
	return message
}

func TestAuthorizeMessageIgnoresUnrelatedMessages(t *testing.T) {
	privateText := routingTestMessage("hello", "outsider", false)
	privateText.Chat = &tgbotapi.Chat{ID: 200, Type: "private"}
	photo := routingTestMessage("", "outsider", false)
	photo.Photo = []tgbotapi.PhotoSize{{FileID: "photo"}}
	service := routingTestMessage("", "outsider", false)
	service.NewChatTitle = "new group title"
	withoutSender := routingTestMessage("/start", "outsider", true)
	withoutSender.From = nil
	withoutChat := routingTestMessage("/start", "outsider", true)
	withoutChat.Chat = nil

	tests := []struct {
		name      string
		message   *tgbotapi.Message
		allowText bool
	}{
		{name: "ordinary group text", message: routingTestMessage("hello", "outsider", false)},
		{name: "ordinary private text", message: privateText},
		{name: "photo", message: photo},
		{name: "service message", message: service},
		{name: "nil message"},
		{name: "missing sender", message: withoutSender},
		{name: "missing chat", message: withoutChat},
		{name: "other bot command", message: routingTestMessage("/start@OtherBot", "outsider", true)},
		{name: "other bot command during text input", message: routingTestMessage("/start@OtherBot", "outsider", true), allowText: true},
		{name: "empty text during text input", message: service, allowText: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bot, client := newRoutingTestBot(t)
			if bot.authorizeMessage(tt.message, tt.allowText) {
				t.Fatal("unrelated message was accepted")
			}
			if len(client.sent) != 0 {
				t.Fatalf("unrelated message caused %d Telegram replies", len(client.sent))
			}
		})
	}
}

func TestAuthorizeMessagePreservesCommandAuthorization(t *testing.T) {
	tests := []struct {
		name       string
		text       string
		username   string
		command    bool
		allowText  bool
		wantAccept bool
		wantSent   int
	}{
		{name: "authorized bare command", text: "/start", username: "allowed", command: true, wantAccept: true},
		{name: "authorized addressed command", text: "/start@TrackerBot", username: "allowed", command: true, wantAccept: true},
		{name: "case insensitive bot target", text: "/start@tRaCkErBoT", username: "allowed", command: true, wantAccept: true},
		{name: "unauthorized bare command", text: "/start", username: "outsider", command: true, wantSent: 1},
		{name: "unauthorized addressed command", text: "/start@TrackerBot", username: "outsider", command: true, wantSent: 1},
		{name: "authorized follow-up text", text: "Binance TRX/USDT 1M 60k", username: "allowed", allowText: true, wantAccept: true},
		{name: "follow-up text still requires authorization", text: "Binance TRX/USDT 1M 60k", username: "outsider", allowText: true, wantSent: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bot, client := newRoutingTestBot(t)
			if got := bot.authorizeMessage(routingTestMessage(tt.text, tt.username, tt.command), tt.allowText); got != tt.wantAccept {
				t.Fatalf("authorizeMessage() = %v, want %v", got, tt.wantAccept)
			}
			if len(client.sent) != tt.wantSent {
				t.Fatalf("got %d Telegram replies, want %d", len(client.sent), tt.wantSent)
			}
			for _, sent := range client.sent {
				if got := sent.Get("text"); got != "You are not authorized to use this bot." {
					t.Errorf("unexpected reply: %q", got)
				}
				if got := sent.Get("chat_id"); got != "-200" {
					t.Errorf("reply chat ID = %s, want -200", got)
				}
			}
		})
	}
}
