package bot

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"go.uber.org/zap"
	"tron-tracker/database"
	"tron-tracker/database/models"
	"tron-tracker/net"
	"tron-tracker/utils"
)

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

		tokens: []string{"TRX", "STEEM", "SUN", "BTT", "JST", "WIN", "NFT"},
		slugs:  []string{"tron", "steem", "sun-token", "bittorrent-new", "just", "wink", "apenft"},
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

			if update.Message.IsCommand() {
				textMsg := ""
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
						rulesMsg := "Rules:\n"
						for _, rule := range rules {
							rulesMsg += fmt.Sprintf(">\\[%2d\\] %s\\-%s, \\[Volume\\]: *$%s*, \\[\\+/\\-2%% Depth\\]: *$%s*\n",
								rule.ID, rule.ExchangeName, rule.Pair,
								humanize.SIWithDigits(rule.Volume, 0, ""),
								humanize.SIWithDigits(rule.DepthUsdPositiveTwo, 0, ""))
						}
						tb.SendMessage(update.Message.MessageID, rulesMsg, nil)
					}
				case "addrule":
					data := strings.Fields(update.Message.Text)
					if len(data) != 5 {
						textMsg = "You need to specify the exchange_name, pari, volume and +/-2% depth"
						break
					}

					exchangeName := data[1]
					pair := data[2]
					volume, _, err := humanize.ParseSI(data[3])
					if err != nil {
						textMsg = "Invalid volume format, please use SI format like 1k, 1M, 1G"
						break
					}
					depth, _, err := humanize.ParseSI(data[4])
					if err != nil {
						textMsg = "Invalid depth format, please use SI format like 1k, 1M, 1G"
						break
					}

					rule := &models.Rule{
						ExchangeName:        exchangeName,
						Pair:                pair,
						Volume:              volume,
						DepthUsdPositiveTwo: depth,
						DepthUsdNegativeTwo: depth,
					}

					if tb.db.GetMarketPairRuleByExchangeNameAndPair(exchangeName, pair).ID == 0 {
						tb.db.SaveMarketPairRule(rule)
						textMsg = fmt.Sprintf("Added rule %s-%s [volume]: $%s, [+/-2%% Depth]: $%s", exchangeName, pair, data[3], data[4])
					} else {
						textMsg = fmt.Sprintf("Rule for %s-%s aleady exists", exchangeName, pair)
					}
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

				}

				if textMsg != "" {
					tb.SendMessage(update.Message.MessageID, utils.EscapeMarkdownV2(textMsg), nil)
				}
			}
		}
	}()
}

func (tb *TelegramBot) SendMessage(msgID int, textMsg string, replyMarkup *tgbotapi.InlineKeyboardMarkup) int {
	if tb.chatID == 0 {
		return -1
	}

	msg := tgbotapi.NewMessage(tb.chatID, textMsg)
	msg.ParseMode = "MarkdownV2"
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

func (tb *TelegramBot) DoMarketPairStatistics() {
	tb.logger.Infof("Start doing market pair statistics")

	textMsg := ""
	for i, token := range tb.tokens {
		originData, marketPairs, err := net.GetMarketPairs(token, tb.slugs[i])
		if err != nil {
			tb.logger.Errorf("Get %s market pairs error: [%s]", token, err.Error())
			return
		}

		tb.db.SaveMarketPairStatistics(token, originData, marketPairs)

		if time.Now().Minute() != 0 {
			continue
		}

		for _, marketPair := range marketPairs {
			rule := tb.db.GetMarketPairRuleByExchangeNameAndPair(marketPair.ExchangeName, marketPair.Pair)
			if rule.ID != 0 && strings.HasPrefix(marketPair.Pair, token) {
				msg, shouldReport := tb.buildMarketPairStatisticsMsg(marketPair, rule)
				if shouldReport {
					textMsg += fmt.Sprintf(">\\[%s\\] %s $%s\n%s\n",
						marketPair.ExchangeName, marketPair.Pair,
						utils.EscapeMarkdownV2(humanize.Ftoa(marketPair.Price)), msg)
				}
			}
		}

		time.Sleep(time.Second * 1)
	}

	if len(textMsg) > 0 {
		textMsg = "Rules Alarm\n" + textMsg
		tb.SendMessage(0, textMsg, nil)
	} else if time.Now().Minute() == 0 {
		tb.SendMessage(0, "All rules are OK", nil)
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

	allPairsMsg := ""
	for i, token := range tb.tokens {
		_, marketPairs, err := net.GetMarketPairs(token, tb.slugs[i])
		if err != nil {
			tb.logger.Errorf("Get %s market pairs error: [%s]", token, err.Error())
			return
		}

		for _, marketPair := range marketPairs {
			rule := tb.db.GetMarketPairRuleByExchangeNameAndPair(marketPair.ExchangeName, marketPair.Pair)
			if rule.ID != 0 && strings.HasPrefix(marketPair.Pair, token) {
				msg, _ := tb.buildMarketPairStatisticsMsg(marketPair, rule)
				allPairsMsg += fmt.Sprintf(">\\[%s\\] %s $%s\n%s\n",
					marketPair.ExchangeName, marketPair.Pair,
					utils.EscapeMarkdownV2(humanize.Ftoa(marketPair.Price)), msg)
			}
		}

		time.Sleep(time.Second * 1)
	}

	textMsg := "*Heartbeat*: System is running 🔥\n"
	if len(allPairsMsg) > 0 {
		textMsg += allPairsMsg
	}

	tb.SendMessage(0, textMsg, nil)

	tb.logger.Infof("Finish reporting market pair statistics")
}

func (tb *TelegramBot) buildMarketPairStatisticsMsg(marketPair *models.MarketPairStatistic, rule *models.Rule) (string, bool) {
	hasRuleBroken := false
	msg := ""

	msg += fmt.Sprintf(">\\- \\[24h Volume\\]: *$%s* \\(*$%s*\\)",
		utils.EscapeMarkdownV2(humanize.SIWithDigits(marketPair.Volume, 2, "")),
		humanize.SIWithDigits(rule.Volume, 0, ""))
	if marketPair.Volume < rule.Volume {
		hasRuleBroken = true
		msg += " 🚨\n"
	} else {
		msg += " ✅\n"
	}

	msg += fmt.Sprintf(">\\- \\[\\+2%% Depth\\]: *$%s* \\(*$%s*\\)",
		utils.EscapeMarkdownV2(humanize.SIWithDigits(marketPair.DepthUsdPositiveTwo, 2, "")),
		humanize.SIWithDigits(rule.DepthUsdPositiveTwo, 0, ""))
	if marketPair.DepthUsdPositiveTwo < rule.DepthUsdPositiveTwo {
		hasRuleBroken = true
		msg += " 🚨\n"
	} else {
		msg += " ✅\n"
	}

	msg += fmt.Sprintf(">\\- \\[\\-2%% Depth\\]: *$%s* \\(*$%s*\\)",
		utils.EscapeMarkdownV2(humanize.SIWithDigits(marketPair.DepthUsdNegativeTwo, 2, "")),
		humanize.SIWithDigits(rule.DepthUsdNegativeTwo, 0, ""))
	if marketPair.DepthUsdNegativeTwo < rule.DepthUsdNegativeTwo {
		hasRuleBroken = true
		msg += " 🚨\n"
	} else {
		msg += " ✅\n"
	}

	return msg, hasRuleBroken
}
