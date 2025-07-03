package bot

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/renderer"
	"go.uber.org/zap"
	"tron-tracker/common"
	"tron-tracker/database"
	"tron-tracker/database/models"
	"tron-tracker/net"
)

type ruleStats struct {
	Rule                     *models.Rule
	ExchangeName             string
	Pair                     string
	DepthUsdPositiveTwoSum   float64
	DepthUsdNegativeTwoSum   float64
	VolumeSum                float64
	HitsCount                int
	DepthPositiveBrokenCount int
	DepthNegativeBrokenCount int
	VolumeBrokenCount        int
}

type TelegramBot struct {
	api    *tgbotapi.BotAPI
	chatID int64

	db     *database.RawDB
	logger *zap.SugaredLogger

	tokens []string
	slugs  []string
}

func New(token string, db *database.RawDB) *TelegramBot {
	botApi, err := tgbotapi.NewBotAPI(token)
	if err != nil {
		panic(err)
	}

	botApi.Debug = true

	tgBot := &TelegramBot{
		api:    botApi,
		chatID: db.GetTelegramBotChatID(),

		db:     db,
		logger: zap.S().Named("[bot]"),

		tokens: []string{"TRX", "STEEM", "SUN", "BTT", "JST", "WIN", "NFT", "HTX", "USDD"},
		slugs:  []string{"tron", "steem", "sun-token", "bittorrent-new", "just", "wink", "apenft", "htx", "usdd"},
	}

	tgBot.logger.Infof("Telegram bot authorized on account %s", botApi.Self.UserName)

	return tgBot
}

func (tb *TelegramBot) Start() {
	tb.logger.Infof("Started telegram bot with chat ID [%d]", tb.chatID)

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60

	go func() {
		updates := tb.api.GetUpdatesChan(u)
		for update := range updates {
			if update.Message == nil {
				continue
			}

			textMsg := ""
			if update.Message.IsCommand() {
				switch update.Message.Command() {
				case "start":
					tb.chatID = update.Message.Chat.ID
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
				// Handle rules list to add
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
				tb.sendMessage(update.Message.MessageID, tgbotapi.ModeMarkdownV2, common.EscapeMarkdownV2(textMsg), nil)
			}
		}
	}()
}

func (tb *TelegramBot) sendMessage(msgID int, mode, textMsg string, replyMarkup *tgbotapi.InlineKeyboardMarkup) int {
	if tb.chatID == 0 {
		return -1
	}

	msg := tgbotapi.NewMessage(tb.chatID, textMsg)
	msg.ParseMode = mode
	msg.DisableWebPagePreview = true
	if msgID != 0 {
		msg.ReplyToMessageID = msgID
	}
	if replyMarkup != nil {
		msg.ReplyMarkup = replyMarkup
	}

	msgSent, err := tb.api.Send(msg)

	if err != nil {
		log.Println(err)
		log.Println(textMsg)
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

func (tb *TelegramBot) ReportMarketPairStatistics() {
	tb.logger.Infof("Start reporting market pair statistics")

	textMsg := "<strong>[Statistics for the past 7 days]</strong>\n\n"
	tb.sendMessage(0, tgbotapi.ModeHTML, textMsg, nil)

	lastWeek := time.Now().AddDate(0, 0, -7)
	for _, token := range tb.tokens {
		statsMsg := ""

		rulesStats := make(map[string]*ruleStats)
		for _, rule := range tb.db.GetMarketPairRuleByToken(token) {
			// Special handling for APENFT pair
			pair, _ := strings.CutPrefix(rule.Pair, "APE")
			rulesStats[rule.ExchangeName+"-"+pair] = &ruleStats{
				Rule:         rule,
				ExchangeName: rule.ExchangeName,
				Pair:         rule.Pair,
			}
		}

		if len(rulesStats) == 0 {
			continue
		}

		var data [][]string
		for _, marketPair := range tb.db.GetMarketPairStatistics(lastWeek, 7, token) {
			key := marketPair.ExchangeName + "-" + marketPair.Pair
			if ruleStat, ok := rulesStats[key]; ok {
				ruleStat.DepthUsdPositiveTwoSum += marketPair.DepthUsdPositiveTwo
				ruleStat.DepthUsdNegativeTwoSum += marketPair.DepthUsdNegativeTwo
				ruleStat.VolumeSum += marketPair.Volume
				ruleStat.HitsCount++

				if marketPair.DepthUsdPositiveTwo < ruleStat.Rule.DepthUsdPositiveTwo {
					ruleStat.DepthPositiveBrokenCount++
				}
				if marketPair.DepthUsdNegativeTwo < ruleStat.Rule.DepthUsdNegativeTwo {
					ruleStat.DepthNegativeBrokenCount++
				}
				if marketPair.Volume < ruleStat.Rule.Volume {
					ruleStat.VolumeBrokenCount++
				}
			}
		}

		sortedRulesStats := make([]*ruleStats, 0, len(rulesStats))
		for _, ruleStat := range rulesStats {
			if ruleStat.HitsCount > 0 {
				sortedRulesStats = append(sortedRulesStats, ruleStat)
			}
		}
		sort.Slice(sortedRulesStats, func(i, j int) bool {
			return sortedRulesStats[i].Rule.ID < sortedRulesStats[j].Rule.ID
		})

		for _, ruleStat := range sortedRulesStats {
			data = append(data, []string{
				strings.Split(ruleStat.ExchangeName, " ")[0] + "-" + ruleStat.Pair,
				fmt.Sprintf("%s(%s/%s)",
					formatComplianceRate(ruleStat.HitsCount, ruleStat.DepthPositiveBrokenCount),
					formatFloatWithUnit(ruleStat.DepthUsdPositiveTwoSum/float64(ruleStat.HitsCount)),
					formatFloatWithUnit(ruleStat.Rule.DepthUsdPositiveTwo)),
				fmt.Sprintf("%s(%s/%s)",
					formatComplianceRate(ruleStat.HitsCount, ruleStat.DepthNegativeBrokenCount),
					formatFloatWithUnit(ruleStat.DepthUsdNegativeTwoSum/float64(ruleStat.HitsCount)),
					formatFloatWithUnit(ruleStat.Rule.DepthUsdNegativeTwo)),
				fmt.Sprintf("%s(%s/%s)",
					formatComplianceRate(ruleStat.HitsCount, ruleStat.VolumeBrokenCount),
					formatFloatWithUnit(ruleStat.VolumeSum/float64(ruleStat.HitsCount)),
					formatFloatWithUnit(ruleStat.Rule.Volume)),
			})
		}

		var tableString bytes.Buffer
		table := tablewriter.NewTable(&tableString, tablewriter.WithRenderer(renderer.NewMarkdown()))
		table.Header([]string{"Exchange-Pair", "+2% Depth", "-2% Depth", "Volume"})
		table.Bulk(data)
		table.Render()

		statsMsg += fmt.Sprintf("<b>%s</b>\n", token)
		statsMsg += "<pre>\n" + tableString.String() + "</pre>\n\n"

		tb.sendMessage(0, tgbotapi.ModeHTML, statsMsg, nil)

		time.Sleep(time.Second * 1)
	}

	tb.logger.Infof("Finish reporting market pair statistics")
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

			tb.sendMessage(0, tgbotapi.ModeMarkdownV2, sb.String(), nil)
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

			tb.sendMessage(0, tgbotapi.ModeMarkdownV2, sb.String(), nil)
		}
	}
}
