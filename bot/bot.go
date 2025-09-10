package bot

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"tron-tracker/common"
	"tron-tracker/config"
	"tron-tracker/database"
	"tron-tracker/database/models"
	"tron-tracker/google"
	"tron-tracker/net"

	"github.com/dustin/go-humanize"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/renderer"
	"github.com/olekukonko/tablewriter/tw"
	"go.uber.org/zap"
)

type TelegramBot struct {
	validUsers map[string]bool

	trackerBotApi *tgbotapi.BotAPI
	volumeBotApi  *tgbotapi.BotAPI
	volumeChatID  int64

	db      *database.RawDB
	updater *google.Updater
	logger  *zap.SugaredLogger

	tokens []string
	slugs  []string

	isUpdatingPPT bool // Flag to indicate if the bot is currently updating PPT
	isAddingRules bool // Flag to indicate if the bot is currently adding rules
}

func New(cfg *config.BotConfig, db *database.RawDB, updater *google.Updater) *TelegramBot {
	trackerBotApi, err := tgbotapi.NewBotAPI(cfg.TrackerBotToken)
	if err != nil {
		panic(err)
	}

	volumeBotApi, err := tgbotapi.NewBotAPI(cfg.VolumeBotToken)
	if err != nil {
		panic(err)
	}

	tgBot := &TelegramBot{
		validUsers: make(map[string]bool),

		trackerBotApi: trackerBotApi,
		volumeBotApi:  volumeBotApi,
		volumeChatID:  db.GetTelegramBotChatID(),

		db:      db,
		updater: updater,
		logger:  zap.S().Named("[bot]"),

		tokens: []string{"TRX", "STEEM", "SUN", "BTT", "JST", "WIN", "NFT", "HTX", "USDD", "sTRX"},
		slugs:  []string{"tron", "steem", "sun-token", "bittorrent-new", "just", "wink", "apenft", "htx", "usdd", "staked-trx"},
	}

	for _, user := range cfg.ValidUsers {
		tgBot.validUsers[user] = true
	}

	tgBot.logger.Infof("Telegram tracker bot authorized on account [%s]", trackerBotApi.Self.UserName)
	tgBot.logger.Infof("Telegram volume bot authorized on account [%s]", volumeBotApi.Self.UserName)

	return tgBot
}

func (tb *TelegramBot) Start() {
	tb.logger.Infof("Started telegram volume bot with chat ID [%d]", tb.volumeChatID)

	// Start tracker bot loop
	go func() {
		u := tgbotapi.NewUpdate(0)
		u.Timeout = 60

		updates := tb.trackerBotApi.GetUpdatesChan(u)
		for update := range updates {
			if update.Message == nil {
				continue
			}

			// Check if the user is valid
			if _, ok := tb.validUsers[update.Message.From.UserName]; !ok {
				tb.logger.Warnf("Unauthorized user %s tried to access the bot", update.Message.From.UserName)
				// tb.sendMessage(-1, update.Message.MessageID, "", "You are not authorized to use this bot.", nil)
				continue
			}

			textMsg := ""
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
					tb.sendTrackerMessage(update.Message.Chat.ID, 0, "", "Updating PPT...", nil)

					tb.isUpdatingPPT = true
					go func() {
						tb.updater.Update(updateDate)
						tb.isUpdatingPPT = false

						tb.logger.Infof("User %s finished PPT update", update.Message.From.UserName)
						textMsg = fmt.Sprintf("PPT updated successfully for date %s", updateDate.Format("2006-01-02"))

						tb.sendTrackerMessage(update.Message.Chat.ID, 0, "", "Updating PPT...", nil)
					}()
				default:
					textMsg = "Unknown command. Available commands: /update_ppt"
				}
			}

			if textMsg != "" {
				tb.sendTrackerMessage(update.Message.Chat.ID, 0, "", textMsg, nil)
			}
		}
	}()

	// Start volume bot loop
	go func() {
		u := tgbotapi.NewUpdate(0)
		u.Timeout = 60

		updates := tb.volumeBotApi.GetUpdatesChan(u)
		for update := range updates {
			if update.Message == nil {
				continue
			}

			textMsg := ""
			if update.Message.IsCommand() {
				switch update.Message.Command() {
				case "start":
					tb.volumeChatID = update.Message.Chat.ID
					tb.db.SaveTelegramChatID(update.Message.Chat.ID)
					tb.logger.Infof("Started at chat %s-[%d]", update.Message.Chat.Title, update.Message.Chat.ID)
					textMsg = "Hi! I am a bot that can track and report volumes for tron tokens"
				case "listrules":
					rules := tb.db.GetAllMarketPairRules()
					if len(rules) == 0 {
						textMsg = "No rules found"
					} else {
						tb.reportRules(rules, false)
					}
				case "addrule":
					data := strings.Fields(update.Message.Text)
					if len(data) == 1 {
						tb.isAddingRules = true
						textMsg = "OK. Send me a list of rules. Please use this format:\n\n[exchange_name pair volume +/-2%depth]\n"
						break
					}

					if len(data) != 5 {
						textMsg = "You need to specify the exchange_name, pari, volume and +/-2% depth"
						break
					}

					_, textMsg = tb.addRule(data[1:])
				case "editrule":
					data := strings.Fields(update.Message.Text)
					if len(data) != 4 {
						textMsg = "You need to specify the rule_id, volume and +/-2% depth"
						break
					}

					ruleID, err := strconv.Atoi(data[1])
					if err != nil {
						textMsg = "Invalid rule ID"
						break
					}
					volume, _, err := humanize.ParseSI(data[2])
					if err != nil {
						textMsg = "Invalid volume format, please use SI format like 1k, 1M, 1G"
						break
					}
					depth, _, err := humanize.ParseSI(data[3])
					if err != nil {
						textMsg = "Invalid depth format, please use SI format like 1k, 1M, 1G"
						break
					}

					if rule, ok := tb.db.GetMarketPairRuleByID(ruleID); !ok {
						textMsg = fmt.Sprintf("Rule with ID %d not found", ruleID)
						break
					} else {
						rule.Volume = volume
						rule.DepthUsdPositiveTwo = depth
						rule.DepthUsdNegativeTwo = depth

						tb.db.SaveMarketPairRule(rule)
						textMsg = fmt.Sprintf("Edited rule [%2d] for %s-%s [volume]: $%s, [+/-2%% Depth]: $%s",
							rule.ID, rule.ExchangeName, rule.Pair,
							humanize.SIWithDigits(rule.Volume, 0, ""),
							humanize.SIWithDigits(rule.DepthUsdPositiveTwo, 0, ""))
					}
				case "report":
					// data := strings.Fields(update.Message.Text)
					tb.ReportMarketPairStatistics()
				default:
					textMsg = "Unknown command. Available commands: /start, /listrules, /addrule, /editrule, /report"
				}
			} else if update.Message.Text != "" {
				if !tb.isAddingRules {
					continue
				}

				// Handle rules list to add
				tb.isAddingRules = false
				lines := strings.Split(update.Message.Text, "\n")
				addedCount := 0
				if len(lines) > 1 {
					for _, line := range lines {
						data := strings.Fields(line)
						if ok, _ := tb.addRule(data); ok {
							addedCount++
						}
					}
				}
				textMsg = fmt.Sprintf("Input rules: %d, total added: %d, invalid or duplicate: %d", len(lines), addedCount, len(lines)-addedCount)
			}

			if textMsg != "" {
				tb.sendVolumeMessage(update.Message.MessageID, tgbotapi.ModeMarkdownV2, common.EscapeMarkdownV2(textMsg), nil)
			}
		}
	}()
}

func (tb *TelegramBot) sendTrackerMessage(chatID int64, msgID int, mode, textMsg string, replyMarkup *tgbotapi.InlineKeyboardMarkup) int {
	return tb.sendMessage(tb.trackerBotApi, chatID, msgID, mode, textMsg, replyMarkup)
}

func (tb *TelegramBot) sendVolumeMessage(msgID int, mode, textMsg string, replyMarkup *tgbotapi.InlineKeyboardMarkup) int {
	return tb.sendMessage(tb.volumeBotApi, tb.volumeChatID, msgID, mode, textMsg, replyMarkup)
}

func (tb *TelegramBot) sendMessage(botApi *tgbotapi.BotAPI, chatID int64, msgID int, mode, textMsg string, replyMarkup *tgbotapi.InlineKeyboardMarkup) int {
	if chatID == 0 {
		return -1
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

	msgSent, err := botApi.Send(msg)

	if err != nil {
		tb.logger.Errorf("Error sending message: %v", err)
		return 0
	}

	return msgSent.MessageID
}

func (tb *TelegramBot) addRule(data []string) (bool, string) {
	exchangeName := strings.ReplaceAll(data[0], "#", " ")
	pair := data[1]

	if !tb.db.ContainExchangeAndPairInMarketPairStatistics(exchangeName, pair) {
		return false, fmt.Sprintf("Exchange %s and pair %s not found in database", exchangeName, pair)
	}

	volume, _, err := humanize.ParseSI(data[2])
	if err != nil {
		return false, "Invalid volume format, please use SI format like 1k, 1M, 1G"
	}
	depth, _, err := humanize.ParseSI(data[3])
	if err != nil {
		return false, "Invalid depth format, please use SI format like 1k, 1M, 1G"
	}

	rule := &models.Rule{
		ExchangeName:        exchangeName,
		Pair:                pair,
		Volume:              volume,
		DepthUsdPositiveTwo: depth,
		DepthUsdNegativeTwo: depth,
	}

	if tb.db.GetMarketPairRuleByExchangePair(exchangeName, pair).ID == 0 {
		tb.db.SaveMarketPairRule(rule)
		return true, fmt.Sprintf("Added rule %s-%s [volume]: $%s, [+/-2%% Depth]: $%s", exchangeName, pair, data[2], data[3])
	} else {
		return false, fmt.Sprintf("Rule for %s-%s aleady exists", exchangeName, pair)
	}
}

func (tb *TelegramBot) DoMarketPairStatistics() {
	tb.logger.Infof("Start doing market pair statistics")

	for i, token := range tb.tokens {
		originData, marketPairs, err := net.GetMarketPairs(token, tb.slugs[i])
		if err != nil {
			tb.logger.Errorf("Get %s market pairs error: [%s]", token, err.Error())
			return
		}

		tb.db.SaveMarketPairStatistics(token, originData, marketPairs)

		time.Sleep(time.Second * 1)
	}

	tb.logger.Infof("Finish doing market pair statistics")
}

func (tb *TelegramBot) DoTokenListingStatistics() {
	tb.logger.Infof("Start doing token listing statistics")

	originData, tokenListings, err := net.GetTokenListings()
	if err != nil {
		tb.logger.Errorf("Get token listing error: [%s]", err.Error())
		return
	}

	tb.db.SaveTokenListingStatistics(originData, tokenListings)

	tb.logger.Infof("Finish doing token listing statistics")
}

func (tb *TelegramBot) DoHoldingsStatistics() {
	tb.logger.Infof("Start doing holdings statistics")

	user := "TEySEZLJf6rs2mCujGpDEsgoMVWKLAk9mT"
	sTRX := "TU3kjFuhtEo42tsCBtfYUAZxoqQ4yuSLQ5"
	originData, err := net.Trigger(sTRX, "balanceOf(address)", "00000000000000000000000036e3acd0ad0533e30ee61e194818344c9d2a09b0")
	if err != nil {
		tb.logger.Errorf("Get holdings error: [%s]", err.Error())
		return
	}

	tb.db.SaveHoldingsStatistics(user, sTRX, common.ConvertHexToBigInt(originData).Text(10))

	tb.logger.Infof("Finish doing holdings statistics")
}

func (tb *TelegramBot) CheckMarketPairs() {
	tb.logger.Infof("Start checking market pairs")

	textMsg := "<strong>[Scheduled checks from the past week]</strong>\n\n"
	tb.sendVolumeMessage(0, tgbotapi.ModeHTML, textMsg, nil)

	brokenRulesStats := make([]*models.Rule, 0)
	lastWeek := time.Now().AddDate(0, 0, -7)
	for _, token := range tb.tokens {
		rulesStats := tb.db.GetMarketPairRulesStatsByTokenAndStartDateAndDays(token, lastWeek, 7)
		if len(rulesStats) == 0 {
			continue
		}

		for _, ruleStat := range rulesStats {
			if ruleStat.HitsCount > 0 && 100*ruleStat.VolumeBrokenCount/ruleStat.HitsCount >= 40 {
				brokenRulesStats = append(brokenRulesStats, ruleStat)
			}
		}

		time.Sleep(time.Second * 1)
	}

	if len(brokenRulesStats) == 0 {
		tb.sendVolumeMessage(0, tgbotapi.ModeHTML, "All rules are complied. No issues found.\n", nil)
	} else {
		tb.sendVolumeMessage(0, tgbotapi.ModeHTML, formatTable("", brokenRulesStats, false), nil)
		reminders := tb.db.GetVolumeReminders()
		var mentions strings.Builder
		if len(reminders) > 0 {
			for _, reminder := range reminders {
				mentions.WriteString("@" + reminder + " ")
			}
		}
		tb.sendVolumeMessage(0, "", mentions.String()+" Broken rules found.\n", nil)
	}

	tb.logger.Infof("Finish checking market pairs")
}

func (tb *TelegramBot) ReportMarketPairStatistics() {
	tb.logger.Infof("Start reporting market pair statistics")

	textMsg := "<strong>[Statistics for the past 7 days]</strong>\n\n"
	tb.sendVolumeMessage(0, tgbotapi.ModeHTML, textMsg, nil)

	lastWeek := time.Now().AddDate(0, 0, -7)
	for _, token := range tb.tokens {
		rulesStats := tb.db.GetMarketPairRulesStatsByTokenAndStartDateAndDays(token, lastWeek, 7)
		if len(rulesStats) == 0 {
			continue
		}

		sortedRulesStats := make([]*models.Rule, 0, len(rulesStats))
		for _, ruleStat := range rulesStats {
			if ruleStat.HitsCount > 0 {
				sortedRulesStats = append(sortedRulesStats, ruleStat)
			}
		}

		tb.sendVolumeMessage(0, tgbotapi.ModeHTML, formatTable(token, sortedRulesStats, true), nil)

		time.Sleep(time.Second * 1)
	}

	tb.logger.Infof("Finish reporting market pair statistics")
}

func formatTable(token string, rulesStats []*models.Rule, sortRules bool) string {
	if sortRules {
		sort.Slice(rulesStats, func(i, j int) bool {
			return rulesStats[i].ID < rulesStats[j].ID
		})
	}

	var data [][]string
	for _, ruleStat := range rulesStats {
		data = append(data, []string{
			strings.Split(ruleStat.ExchangeName, " ")[0] + "-" + ruleStat.Pair,
			fmt.Sprintf("%s(%s/%s)",
				formatComplianceRate(ruleStat.HitsCount, ruleStat.DepthPositiveBrokenCount),
				formatFloatWithUnit(ruleStat.DepthUsdPositiveTwoSum/float64(ruleStat.HitsCount)),
				formatFloatWithUnit(ruleStat.DepthUsdPositiveTwo)),
			fmt.Sprintf("%s(%s/%s)",
				formatComplianceRate(ruleStat.HitsCount, ruleStat.DepthNegativeBrokenCount),
				formatFloatWithUnit(ruleStat.DepthUsdNegativeTwoSum/float64(ruleStat.HitsCount)),
				formatFloatWithUnit(ruleStat.DepthUsdNegativeTwo)),
			fmt.Sprintf("%s(%s/%s)",
				formatComplianceRate(ruleStat.HitsCount, ruleStat.VolumeBrokenCount),
				formatFloatWithUnit(ruleStat.VolumeSum/float64(ruleStat.HitsCount)),
				formatFloatWithUnit(ruleStat.Volume)),
		})
	}

	md := renderer.NewMarkdown(
		tw.Rendition{
			Borders: tw.Border{
				Left:   tw.Off,
				Right:  tw.Off,
				Top:    tw.Off,
				Bottom: tw.Off,
			},
		},
	)

	var tableString bytes.Buffer
	table := tablewriter.NewTable(&tableString,
		tablewriter.WithRenderer(md),
		tablewriter.WithHeaderAutoFormat(tw.Off),
		tablewriter.WithHeaderAlignment(tw.AlignNone),
		tablewriter.WithRowAlignment(tw.AlignNone),
	)
	table.Header([]string{"Exchange-Pair", "+2% Depth", "-2% Depth", "Volume"})
	table.Bulk(data)
	table.Render()

	var statsMsg string
	if len(token) > 0 {
		statsMsg += fmt.Sprintf("<b>%s</b>\n", token)
	}
	statsMsg += "<pre>\n" + tableString.String() + "</pre>\n\n"

	return statsMsg
}

func formatComplianceRate(hitsCount, brokenCount int) string {
	if brokenCount > hitsCount/10 {
		return fmt.Sprintf("✘ %.0f%%", 100-math.Ceil(float64(brokenCount)*100/float64(hitsCount)))
	}
	return fmt.Sprintf("✓ %.0f%%", 100-math.Ceil(float64(brokenCount)*100/float64(hitsCount)))
}

func formatFloatWithUnit(f float64) string {
	return strings.ReplaceAll(humanize.SIWithDigits(f, 0, ""), " ", "")
}

func (tb *TelegramBot) reportRules(allRules []*models.Rule, byExchange bool) {
	if byExchange {
		rulesMap := make(map[string][]*models.Rule)
		for _, rule := range allRules {
			if _, ok := rulesMap[rule.ExchangeName]; !ok {
				rulesMap[rule.ExchangeName] = make([]*models.Rule, 0)
			}
			rulesMap[rule.ExchangeName] = append(rulesMap[rule.ExchangeName], rule)
		}

		for exchange, rules := range rulesMap {
			var sb strings.Builder
			sb.WriteString(fmt.Sprintf(">Exchange: *%s*\n", exchange))

			for _, rule := range rules {
				sb.WriteString(fmt.Sprintf(">\\[%2d\\] %s\\-%s, \\[Vol\\]: *$%s*, \\[\\+/\\-2%%Dp\\]: *$%s*\n",
					rule.ID, rule.ExchangeName, rule.Pair,
					humanize.SIWithDigits(rule.Volume, 0, ""),
					humanize.SIWithDigits(rule.DepthUsdPositiveTwo, 0, "")))
			}

			tb.sendVolumeMessage(0, tgbotapi.ModeMarkdownV2, sb.String(), nil)
		}
	} else {
		rulesMap := make(map[string][]*models.Rule)
		for _, rule := range allRules {
			// Special handling for APENFT pair
			token, _ := strings.CutPrefix(strings.Split(rule.Pair, "/")[0], "APE")
			if _, ok := rulesMap[token]; !ok {
				rulesMap[token] = make([]*models.Rule, 0)
			}
			rulesMap[token] = append(rulesMap[token], rule)
		}

		for _, token := range tb.tokens {
			if _, ok := rulesMap[token]; !ok {
				continue
			}
			var sb strings.Builder
			sb.WriteString(fmt.Sprintf(">Token: *%s*\n\n", token))

			for _, rule := range rulesMap[token] {
				sb.WriteString(fmt.Sprintf(">\\[%2d\\] %s\\-%s, \\[Vol\\]: *$%s*, \\[\\+/\\-2%%Dp\\]: *$%s*\n",
					rule.ID, rule.ExchangeName, rule.Pair,
					humanize.SIWithDigits(rule.Volume, 0, ""),
					humanize.SIWithDigits(rule.DepthUsdPositiveTwo, 0, "")))
			}

			tb.sendVolumeMessage(0, tgbotapi.ModeMarkdownV2, sb.String(), nil)
		}
	}
}
