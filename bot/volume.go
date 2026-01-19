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
	"tron-tracker/net"

	"github.com/dustin/go-humanize"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/renderer"
	"github.com/olekukonko/tablewriter/tw"
)

type VolumeBot struct {
	*Bot

	tokens []string
	slugs  []string

	isAddingRules bool // Flag to indicate if the bot is currently adding rules
}

func NewVolumeBot(cfg *config.BotConfig, db *database.RawDB) *VolumeBot {
	volumeBot := &VolumeBot{
		Bot: NewBot("volume", cfg.VolumeBotToken, db.GetTelegramBotChatID(), db, cfg.ValidUsers),

		tokens: []string{"TRX", "STEEM", "SUN", "BTT", "JST", "WIN", "NFT", "HTX", "USDD", "sTRX"},
		slugs:  []string{"tron", "steem", "sun-token", "bittorrent-new", "just", "wink", "apenft", "htx", "usdd", "staked-trx"},
	}

	return volumeBot
}

func (vb *VolumeBot) Start() {
	vb.logger.Infof("Started telegram volume bot with chat ID [%d]", vb.chatID)

	// Start volume bot loop
	go func() {
		u := tgbotapi.NewUpdate(0)
		u.Timeout = 60

		updates := vb.botApi.GetUpdatesChan(u)
		for update := range updates {
			if update.Message == nil {
				continue
			}

			// Check if the user is valid
			if !vb.isAuthorizedUser(update.Message.From.UserName, update.Message.Chat.ID) {
				continue
			}

			textMsg := ""
			if update.Message.IsCommand() {
				switch update.Message.Command() {
				case "start":
					vb.chatID = update.Message.Chat.ID
					vb.db.SaveTelegramChatID(update.Message.Chat.ID)
					vb.logger.Infof("Started at chat %s-[%d]", update.Message.Chat.Title, update.Message.Chat.ID)
					textMsg = "Hi! I am a bot that can track and report volumes for tron tokens"
				case "listrules":
					rules := vb.db.GetAllMarketPairRules()
					if len(rules) == 0 {
						textMsg = "No rules found"
					} else {
						vb.reportRules(rules, false)
					}
				case "addrule":
					data := strings.Fields(update.Message.Text)
					if len(data) == 1 {
						vb.isAddingRules = true
						textMsg = "OK. Send me a list of rules. Please use this format:\n\n[exchange_name pair volume +/-2%depth]\n"
						break
					}

					if len(data) != 5 {
						textMsg = "You need to specify the exchange_name, pari, volume and +/-2% depth"
						break
					}

					_, textMsg = vb.addRule(data[1:])
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

					if rule, ok := vb.db.GetMarketPairRuleByID(ruleID); !ok {
						textMsg = fmt.Sprintf("Rule with ID %d not found", ruleID)
						break
					} else {
						rule.Volume = volume
						rule.DepthUsdPositiveTwo = depth
						rule.DepthUsdNegativeTwo = depth

						vb.db.SaveMarketPairRule(rule)
						textMsg = fmt.Sprintf("Edited rule [%2d] for %s-%s [volume]: $%s, [+/-2%% Depth]: $%s",
							rule.ID, rule.ExchangeName, rule.Pair,
							humanize.SIWithDigits(rule.Volume, 0, ""),
							humanize.SIWithDigits(rule.DepthUsdPositiveTwo, 0, ""))
					}
				case "report":
					data := strings.Fields(update.Message.Text)
					if len(data) == 1 {
						vb.ReportMarketPairStatistics(time.Now())
					} else if len(data) == 2 {
						reportDate, err := time.Parse("2006-01-02", data[1])
						if err != nil {
							textMsg = "Invalid date format. Please use YYYY-MM-DD format."
							break
						}

						vb.ReportMarketPairStatistics(reportDate)
					} else {
						textMsg = "Too many arguments. Please use /report [YYYY-MM-DD] format."
					}
				default:
					textMsg = "Unknown command. Available commands: /start, /listrules, /addrule, /editrule, /report"
				}
			} else if update.Message.Text != "" {
				if !vb.isAddingRules {
					continue
				}

				// Handle rules list to add
				vb.isAddingRules = false
				lines := strings.Split(update.Message.Text, "\n")
				addedCount := 0
				if len(lines) > 1 {
					for _, line := range lines {
						data := strings.Fields(line)
						if ok, _ := vb.addRule(data); ok {
							addedCount++
						}
					}
				}
				textMsg = fmt.Sprintf("Input rules: %d, total added: %d, invalid or duplicate: %d", len(lines), addedCount, len(lines)-addedCount)
			}

			if textMsg != "" {
				vb.sendMessage(vb.chatID, update.Message.MessageID, tgbotapi.ModeMarkdownV2, common.EscapeMarkdownV2(textMsg), nil)
			}
		}
	}()
}

func (vb *VolumeBot) reportRules(allRules []*models.Rule, byExchange bool) {
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

			vb.sendMessage(vb.chatID, 0, tgbotapi.ModeMarkdownV2, sb.String(), nil)
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

		for _, token := range vb.tokens {
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

			vb.sendMessage(vb.chatID, 0, tgbotapi.ModeMarkdownV2, sb.String(), nil)
		}
	}
}

func (vb *VolumeBot) addRule(data []string) (bool, string) {
	exchangeName := strings.ReplaceAll(data[0], "#", " ")
	pair := data[1]

	if !vb.db.ContainExchangeAndPairInMarketPairStatistics(exchangeName, pair) {
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

	if vb.db.GetMarketPairRuleByExchangePair(exchangeName, pair).ID == 0 {
		vb.db.SaveMarketPairRule(rule)
		return true, fmt.Sprintf("Added rule %s-%s [volume]: $%s, [+/-2%% Depth]: $%s", exchangeName, pair, data[2], data[3])
	}

	return false, fmt.Sprintf("Rule for %s-%s aleady exists", exchangeName, pair)
}

func (vb *VolumeBot) DoMarketPairStatistics() error {
	vb.logger.Infof("Start doing market pair statistics")

	for i, token := range vb.tokens {
		originData, marketPairs, err := net.GetMarketPairs(token, vb.slugs[i])
		if err != nil {
			vb.logger.Errorf("Get %s market pairs error: [%s]", token, err.Error())
			return err
		}

		vb.db.SaveMarketPairStatistics(token, originData, marketPairs)

		time.Sleep(time.Second * 1)
	}

	vb.logger.Infof("Finish doing market pair statistics")

	return nil
}

func (vb *VolumeBot) DoTokenListingStatistics() error {
	vb.logger.Infof("Start doing token listing statistics")

	originData, tokenListings, err := net.GetTokenListings()
	if err != nil {
		vb.logger.Errorf("Get token listing error: [%s]", err.Error())
		return err
	}

	vb.db.SaveTokenListingStatistics(originData, tokenListings)

	vb.logger.Infof("Finish doing token listing statistics")

	return nil
}

func (vb *VolumeBot) DoHoldingsStatistics() error {
	vb.logger.Infof("Start doing holdings statistics")

	user := "TEySEZLJf6rs2mCujGpDEsgoMVWKLAk9mT"
	sTRX := "TU3kjFuhtEo42tsCBtfYUAZxoqQ4yuSLQ5"
	originData, err := net.Trigger(sTRX, "balanceOf(address)", "00000000000000000000000036e3acd0ad0533e30ee61e194818344c9d2a09b0")
	if err != nil {
		vb.logger.Errorf("Get holdings error: [%s]", err.Error())
		return err
	}

	vb.db.SaveHoldingsStatistics(user, sTRX, common.ConvertHexToBigInt(originData).Text(10))

	vb.logger.Infof("Finish doing holdings statistics")

	return nil
}

func (vb *VolumeBot) CheckMarketPairs(remind bool) {
	vb.logger.Infof("Start checking market pairs")

	textMsg := "<strong>[Daily checks for the past 7 days]</strong>\n\n"
	vb.sendMessage(vb.chatID, 0, tgbotapi.ModeHTML, textMsg, nil)

	brokenRulesStats := make([]*models.Rule, 0)
	lastWeek := time.Now().AddDate(0, 0, -7)
	for _, token := range []string{"TRX", "STEEM", "JST", "WIN"} {
		rulesStats := vb.db.GetMarketPairRulesStatsByTokenDateDays(token, lastWeek, 7)
		if len(rulesStats) == 0 {
			continue
		}

		for _, ruleStat := range rulesStats {
			if ruleStat.HitsCount > 0 &&
				(100.0*ruleStat.DepthPositiveBrokenCount/ruleStat.HitsCount > 40 ||
					100.0*ruleStat.DepthNegativeBrokenCount/ruleStat.HitsCount > 40) {
				brokenRulesStats = append(brokenRulesStats, ruleStat)
			}
		}

		time.Sleep(time.Second * 1)
	}

	if len(brokenRulesStats) == 0 {
		vb.sendMessage(vb.chatID, 0, tgbotapi.ModeHTML, "All rules are complied. No issues found.\n", nil)
	} else {
		vb.sendMessage(vb.chatID, 0, tgbotapi.ModeHTML, formatTable("", brokenRulesStats, false), nil)

		if remind {
			reminders := vb.db.GetVolumeReminders()
			var mentions strings.Builder
			if len(reminders) > 0 {
				for _, reminder := range reminders {
					mentions.WriteString("@" + reminder + " ")
				}
			}

			vb.sendMessage(vb.chatID, 0, "", mentions.String()+" Broken rules found.\n", nil)
		}
	}

	vb.logger.Infof("Finish checking market pairs")
}

func (vb *VolumeBot) ReportMarketPairStatistics(date time.Time) {
	vb.logger.Infof("Start reporting market pair statistics")

	textMsg := "<strong>[Statistics for the past 7 days]</strong>\n\n"
	vb.sendMessage(vb.chatID, 0, tgbotapi.ModeHTML, textMsg, nil)

	lastWeek := date.AddDate(0, 0, -7)
	for _, token := range vb.tokens {
		rulesStats := vb.db.GetMarketPairRulesStatsByTokenDateDays(token, lastWeek, 7)
		if len(rulesStats) == 0 {
			continue
		}

		sortedRulesStats := make([]*models.Rule, 0, len(rulesStats))
		for _, ruleStat := range rulesStats {
			if ruleStat.HitsCount > 0 {
				sortedRulesStats = append(sortedRulesStats, ruleStat)
			}
		}

		vb.sendMessage(vb.chatID, 0, tgbotapi.ModeHTML, formatTable(token, sortedRulesStats, true), nil)

		time.Sleep(time.Second * 1)
	}

	vb.logger.Infof("Finish reporting market pair statistics")
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
