package net

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"tron-tracker/config"
)

func TestReportWarningChannelMessageToSlackAddsChannelMention(t *testing.T) {
	warningMessages := make(chan SlackMessage, 1)
	warningServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var msg SlackMessage
		if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
			t.Errorf("decode Slack message: %v", err)
		}
		warningMessages <- msg
		w.WriteHeader(http.StatusOK)
	}))
	defer warningServer.Close()

	previousConfigs := configs
	configs = &config.NetConfig{WarningWebhook: warningServer.URL}
	t.Cleanup(func() { configs = previousConfigs })

	msg := SlackMessage{
		Text: "combined internal transaction alert",
		Blocks: []SlackBlock{{
			Type: "header",
			Text: &SlackTextObject{Type: "plain_text", Text: "Alert"},
		}},
	}
	ReportWarningChannelMessageToSlack(msg)

	warningMsg := <-warningMessages
	if !strings.Contains(warningMsg.Text, "<!channel>") {
		t.Fatalf("warning message text does not mention channel: %#v", warningMsg)
	}
	if len(warningMsg.Blocks) != 2 || warningMsg.Blocks[1].Text == nil || warningMsg.Blocks[1].Text.Text != "<!channel>" {
		t.Fatalf("warning message blocks do not visibly mention channel: %#v", warningMsg.Blocks)
	}
}

func TestReportWarningMessageToSlackDoesNotAddChannelMention(t *testing.T) {
	messages := make(chan SlackMessage, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var msg SlackMessage
		if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
			t.Errorf("decode Slack message: %v", err)
		}
		messages <- msg
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	previousConfigs := configs
	configs = &config.NetConfig{WarningWebhook: server.URL}
	t.Cleanup(func() { configs = previousConfigs })

	ReportWarningMessageToSlack(SlackMessage{Text: "simple internal transaction alert"})
	msg := <-messages
	if msg.Text != "simple internal transaction alert" {
		t.Fatalf("warning message = %q, want unchanged simple message", msg.Text)
	}
	if strings.Contains(msg.Text, "<!channel>") || len(msg.Blocks) != 0 {
		t.Fatalf("simple warning unexpectedly notifies channel: %#v", msg)
	}
}

func TestReportWarningToSlackMentionsMeAtWarningWebhook(t *testing.T) {
	messages := make(chan SlackMessage, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var msg SlackMessage
		if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
			t.Errorf("decode Slack message: %v", err)
		}
		messages <- msg
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	previousConfigs := configs
	configs = &config.NetConfig{WarningWebhook: server.URL}
	t.Cleanup(func() { configs = previousConfigs })

	ReportWarningToSlack("TRON transaction OUT_OF_TIME", true)
	msg := <-messages
	if msg.Text != "TRON transaction OUT_OF_TIME <@U01DFGWQ2JK>" {
		t.Fatalf("unexpected personal warning: %#v", msg)
	}
}
