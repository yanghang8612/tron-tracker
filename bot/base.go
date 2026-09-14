package bot

import (
	"fmt"
	"strings"

	"tron-tracker/database"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"go.uber.org/zap"
)

type Bot struct {
	Name string

	botApi *tgbotapi.BotAPI
	chatID int64

	db     *database.RawDB
	logger *zap.SugaredLogger

	validUsers map[string]bool
}

func NewBot(name string, botToken string, chatID int64, db *database.RawDB, validUsers []string) *Bot {
	botApi, err := tgbotapi.NewBotAPI(botToken)
	if err != nil {
		panic(err)
	}

	bot := &Bot{
		botApi: botApi,
		chatID: chatID,

		db:     db,
		logger: zap.S().Named(fmt.Sprintf("[%s_bot]", name)),

		validUsers: make(map[string]bool),
	}

	for _, user := range validUsers {
		bot.validUsers[user] = true
	}

	bot.logger.Infof("Telegram volume bot authorized on account [%s]", botApi.Self.UserName)

	return bot
}

// authorizeMessage filters unrelated chat traffic before authorization can reply.
// allowText is reserved for a pending interaction owned by this sender and chat.
func (b *Bot) authorizeMessage(message *tgbotapi.Message, allowText bool) bool {
	if message == nil || message.From == nil || message.Chat == nil {
		return false
	}

	if message.IsCommand() {
		if _, target, addressed := strings.Cut(message.CommandWithAt(), "@"); addressed &&
			!strings.EqualFold(target, b.botApi.Self.UserName) {
			return false
		}
	} else if !allowText || strings.TrimSpace(message.Text) == "" {
		return false
	}

	return b.isAuthorizedUser(message.From.UserName, message.Chat.ID)
}

func (b *Bot) isAuthorizedUser(username string, chatID int64) bool {
	if _, ok := b.validUsers[username]; !ok {
		b.logger.Warnf("Unauthorized user %s tried to access the bot", username)

		b.sendPlainMessage(chatID, "You are not authorized to use this bot.")
		return false
	}

	return true
}

func (b *Bot) sendMessageToChannel(textMsg string) {
	b.sendMessage(b.chatID, 0, "", textMsg, nil)
}

func (b *Bot) sendPlainMessage(chatID int64, textMsg string) {
	b.sendMessage(chatID, 0, "", textMsg, nil)
}

func (b *Bot) sendMessage(chatID int64, msgID int, mode, textMsg string, replyMarkup *tgbotapi.InlineKeyboardMarkup) {
	if chatID == 0 {
		b.logger.Errorf("Telegram chat ID is zero")
		return
	}

	msg := tgbotapi.NewMessage(chatID, textMsg)
	msg.ParseMode = mode
	msg.DisableWebPagePreview = true
	if msgID != 0 {
		msg.ReplyToMessageID = msgID
	}
	if replyMarkup != nil {
		msg.ReplyMarkup = replyMarkup
	}

	_, err := b.botApi.Send(msg)

	if err != nil {
		b.logger.Errorf("Error sending message: %v", err)
	}
}
