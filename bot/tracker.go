package bot

import (
	"fmt"
	"strings"
	"time"

	"tron-tracker/config"
	"tron-tracker/database"
	"tron-tracker/google"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

type TrackerBot struct {
	*Bot

	updater *google.Updater

	isUpdatingPPT bool // Flag to indicate if the bot is currently updating PPT
}

func NewTrackerBot(cfg *config.BotConfig, db *database.RawDB, updater *google.Updater) *TrackerBot {
	trackerBot := &TrackerBot{
		Bot: NewBot("tracker", cfg.TrackerBotToken, -1, db, cfg.ValidUsers),

		updater: updater,
	}

	return trackerBot
}

func (tb *TrackerBot) Start() {
	tb.logger.Infof("Started telegram tracker bot")

	// Start tracker bot loop
	go func() {
		u := tgbotapi.NewUpdate(0)
		u.Timeout = 60

		updates := tb.botApi.GetUpdatesChan(u)
		for update := range updates {
			if update.Message == nil {
				continue
			}

			// Check if the user is valid
			if !tb.isAuthorizedUser(update.Message.From.UserName, update.Message.Chat.ID) {
				continue
			}

			textMsg := ""
			chatID := update.Message.Chat.ID

			if update.Message.IsCommand() {
				switch update.Message.Command() {
				case "start":
					textMsg = "Hi! I am a bot for chen-dve. I can do something useful for you.\n" +
						"Available commands: /update_ppt"
				case "update_ppt":
					data := strings.Fields(update.Message.Text)

					updateDate := time.Now()
					if len(data) == 2 {
						var err error
						updateDate, err = time.Parse("2006-01-02", data[1])
						if err != nil {
							textMsg = "Invalid date format. Please use YYYY-MM-DD format."
							tb.logger.Warnf("User %s provided invalid date format: %s", update.Message.From.UserName, data[1])
							break
						}
					} else if len(data) > 2 {
						textMsg = "Too many arguments. Please use /update_ppt [YYYY-MM-DD] format."
						break
					}

					if tb.isUpdatingPPT {
						textMsg = "PPT updating is already in progress. Please wait until it finishes."
						tb.logger.Warnf("User %s tried to start PPT update while it was already in progress", update.Message.From.UserName)
						break
					}

					tb.logger.Infof("User %s started PPT update for [%s]", update.Message.From.UserName, updateDate.Format("2006-01-02"))
					textMsg = "Updating PPT..."

					tb.isUpdatingPPT = true
					go func() {
						tb.updater.Update(updateDate)
						tb.isUpdatingPPT = false

						tb.logger.Infof("User %s finished PPT update", update.Message.From.UserName)
						textMsg = fmt.Sprintf("PPT updated successfully for date %s", updateDate.Format("2006-01-02"))

						tb.sendPlainMessage(chatID, textMsg)
					}()
				case "q":
					sql := strings.TrimSpace(strings.TrimPrefix(update.Message.Text, "/q"))
					if len(sql) == 0 {
						textMsg = "Please provide a SQL query to execute."
						break
					}

					messages, err := tb.db.RawSQLQuery(sql)
					if err != nil {
						textMsg = fmt.Sprintf("SQL query error: %s", err.Error())
						tb.logger.Warnf("User %s provided invalid SQL query: %s, error: %s", update.Message.From.UserName, sql, err.Error())
						break
					}

					if len(messages) == 0 {
						textMsg = "Query executed successfully. No results to show."
						break
					} else if len(messages) > 10 {
						textMsg = fmt.Sprintf("Query executed successfully. Result set too large (%d rows). Please refine your query.", len(messages))
						break
					}

					for _, msg := range messages {
						tb.sendMessage(chatID, 0, "", msg, nil)
					}
				default:
					textMsg = "Unknown command. Available commands: /update_ppt"
				}
			}

			if textMsg != "" {
				tb.sendMessage(chatID, 0, "", textMsg, nil)
			}
		}
	}()
}
