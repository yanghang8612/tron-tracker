package google

import (
	"context"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf16"

	"github.com/dustin/go-humanize"
	"github.com/goccy/go-json"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
	"google.golang.org/api/slides/v1"
	"tron-tracker/common"
	"tron-tracker/config"
	"tron-tracker/database"
	"tron-tracker/database/models"
	"tron-tracker/net"
)

type Updater struct {
	presentationId string
	volumeId       string
	revenueId      string
	slidesService  *slides.Service
	sheetsService  *sheets.Service

	db     *database.RawDB
	logger *zap.SugaredLogger
}

func NewUpdater(db *database.RawDB, cfg *config.PPTConfig) *Updater {
	ctx := context.Background()

	// Load OAuth2 client credentials
	b, err := os.ReadFile("credentials.json")
	if err != nil {
		log.Fatalf("Unable to read client secret file: %v", err)
	}

	// Parse client credentials
	googleConfig, err := google.ConfigFromJSON(b, slides.PresentationsScope, sheets.SpreadsheetsScope)
	if err != nil {
		log.Fatalf("Unable to parse client secret file to config: %v", err)
	}

	client := getClient(googleConfig)

	// Create Slides service
	slidesService, err := slides.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Unable to create Slides service: %v", err)
	}

	// Create Sheets service
	sheetsService, err := sheets.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Unable to create Sheets service: %v", err)
	}

	return &Updater{
		presentationId: cfg.SlideID,
		volumeId:       cfg.VolumeID,
		revenueId:      cfg.RevenueID,

		slidesService: slidesService,
		sheetsService: sheetsService,

		db:     db,
		logger: zap.S().Named("[updater]"),
	}
}

// getClient retrieves a token from a local file, automatically refreshes it if needed,
// and returns an authenticated HTTP client.
// If token is refreshed, it will be saved back to the local token file.
func getClient(config *oauth2.Config) *http.Client {
	const tokenFile = "token.json"

	// Load token from local file
	token, err := tokenFromFile(tokenFile)
	if err != nil {
		log.Fatalf("Cannot read token from file %s: %v", tokenFile, err)
	}

	// Create a TokenSource which handles automatic token refreshing
	tokenSource := config.TokenSource(context.Background(), token)

	// Try to refresh the token immediately
	newToken, err := tokenSource.Token()
	if err != nil {
		log.Fatalf("Unable to retrieve new token: %v", err)
	}

	// If the token has changed (i.e., refreshed), save it back to the file
	if newToken.AccessToken != token.AccessToken {
		fmt.Println("Token refreshed, saving new token...")
		SaveToken(tokenFile, newToken)
	}

	// Return an authenticated HTTP client using the token source
	return oauth2.NewClient(context.Background(), tokenSource)
}

// tokenFromFile reads the OAuth2 token from the specified file.
func tokenFromFile(file string) (*oauth2.Token, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var token oauth2.Token
	err = json.NewDecoder(f).Decode(&token)
	return &token, err
}

// GetTokenFromWeb starts a web-based authorization flow to obtain a new token.
// It prints the authorization URL, waits for the user to paste the authorization code,
// exchanges the code for an OAuth2 token, and returns it.
func GetTokenFromWeb(config *oauth2.Config) *oauth2.Token {
	// Generate the authorization URL
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)

	fmt.Println("Go to the following link in your browser then type the authorization code:")
	fmt.Println(authURL)

	// Prompt user for authorization code
	var authCode string
	fmt.Print("Enter the authorization code here: ")

	if _, err := fmt.Scan(&authCode); err != nil {
		log.Fatalf("Unable to read authorization code: %v", err)
	}

	// Exchange authorization code for an OAuth2 token
	tok, err := config.Exchange(context.TODO(), authCode)
	if err != nil {
		log.Fatalf("Unable to retrieve token from web: %v", err)
	}
	return tok
}

// SaveToken writes the OAuth2 token to the specified file.
func SaveToken(path string, token *oauth2.Token) {
	f, err := os.Create(path)
	if err != nil {
		log.Fatalf("Unable to save OAuth token to %s: %v", path, err)
	}
	defer f.Close()

	json.NewEncoder(f).Encode(token)
}

func (u *Updater) TraverseAllObjectsInPPT() string {
	// Get full presentation
	presentation, err := u.slidesService.Presentations.Get(u.presentationId).Do()
	if err != nil {
		log.Fatalf("Unable to retrieve presentation: %v", err)
	}

	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("Presentation title: %s\n", presentation.Title))

	// Traverse all slides
	for i, slide := range presentation.Slides {
		sb.WriteString(fmt.Sprintf("Processing Slide ID: [%d] - %s\n", i, slide.ObjectId))

		for j, element := range slide.PageElements {
			objectId := element.ObjectId
			sb.WriteString(fmt.Sprintf("Object ID: [%d] - %s\n", j, objectId))

			// Check if the element is a TEXT_BOX
			if element.Shape != nil && element.Shape.ShapeType == "TEXT_BOX" {
				textContent := extractText(element)
				sb.WriteString(fmt.Sprintf("Text: %s\n", textContent))
			} else if element.Shape != nil {
				sb.WriteString(fmt.Sprintf("Shape: %s\n", element.Shape.ShapeType))
			} else if element.Table != nil {
				sb.WriteString(fmt.Sprintf("Table: %s\n", objectId))
			} else if element.SheetsChart != nil {
				sb.WriteString(fmt.Sprintf("Sheets Chart: %s\n", element.ObjectId))
			} else {
				sb.WriteString(fmt.Sprintf("Unknown: %s\n", element.ObjectId))
			}
			sb.WriteString(fmt.Sprint("------------------------------\n"))
		}
		sb.WriteString(fmt.Sprint("##############################\n\n"))
	}

	return sb.String()
}

func (u *Updater) Update(date time.Time) {
	lastMonth := date.AddDate(0, 0, -30)

	// Update TRX volume sheet
	_, err := u.sheetsService.Spreadsheets.Values.Update(u.volumeId, "TRX!A2:D31",
		&sheets.ValueRange{
			Values: u.getVolumeData(lastMonth, "TRX", []string{"Kraken", "Binance", "Total"}),
		}).ValueInputOption("USER_ENTERED").Do()
	if err != nil {
		u.logger.Errorf("Unable to update TRX volume sheet: %v", err)
	}

	// Update STEEM volume sheet
	_, err = u.sheetsService.Spreadsheets.Values.Update(u.volumeId, "STEEM!A2:C31",
		&sheets.ValueRange{
			Values: u.getVolumeData(lastMonth, "STEEM", []string{"Binance", "Total"}),
		}).ValueInputOption("USER_ENTERED").Do()
	if err != nil {
		u.logger.Errorf("Unable to update STEEM volume sheet: %v", err)
	}

	// Update JST volume sheet
	_, err = u.sheetsService.Spreadsheets.Values.Update(u.volumeId, "JST!A2:E31",
		&sheets.ValueRange{
			Values: u.getVolumeData(lastMonth, "JST", []string{"Poloniex", "HTX", "Binance", "Total"}),
		}).ValueInputOption("USER_ENTERED").Do()
	if err != nil {
		u.logger.Errorf("Unable to update JST volume sheet: %v", err)
	}

	// Update WIN volume data
	_, err = u.sheetsService.Spreadsheets.Values.Update(u.volumeId, "WIN!A2:E31",
		&sheets.ValueRange{
			Values: u.getVolumeData(lastMonth, "WIN", []string{"Poloniex", "HTX", "Binance", "Total"}),
		}).ValueInputOption("USER_ENTERED").Do()
	if err != nil {
		u.logger.Errorf("Unable to update WIN volume sheet: %v", err)
	}

	// Update USDT Supply
	_, err = u.sheetsService.Spreadsheets.Values.Update(u.volumeId, "USDT!A2:D19",
		&sheets.ValueRange{
			Values: u.getUSDTSupplyData(time.Now()),
		}).ValueInputOption("USER_ENTERED").Do()
	if err != nil {
		u.logger.Errorf("Unable to update USDT supply sheet: %v", err)
	}

	// Update revenue sheet
	revenueData := make([][]interface{}, 0)
	for i := 0; i < 30; i++ {
		queryDate := lastMonth.AddDate(0, 0, i)
		trxPrice := u.db.GetTokenPriceByDate("TRX", queryDate.AddDate(0, 0, 1))
		totalStats := u.db.GetTotalStatisticsByDateDays(queryDate, 1)
		usdtStats := u.db.GetTokenStatisticsByDateDaysToken(queryDate, 1, "USDT")

		row := make([]interface{}, 0)
		row = append(row, queryDate.Format("2006-01-02"))
		row = append(row, trxPrice)
		row = append(row, totalStats.EnergyFee)
		row = append(row, totalStats.NetFee)
		row = append(row, totalStats.EnergyUsage+totalStats.EnergyOriginUsage)
		row = append(row, totalStats.NetUsage)
		row = append(row, usdtStats.EnergyFee)
		row = append(row, usdtStats.NetFee)
		row = append(row, usdtStats.EnergyUsage+usdtStats.EnergyOriginUsage)
		row = append(row, usdtStats.NetUsage)

		revenueData = append(revenueData, row)
	}

	_, err = u.sheetsService.Spreadsheets.Values.Update(u.revenueId, "Total!N37:W66",
		&sheets.ValueRange{
			Values: revenueData,
		}).ValueInputOption("USER_ENTERED").Do()
	if err != nil {
		u.logger.Errorf("Unable to update revenue sheet: %v", err)
	}

	ppt, err := u.slidesService.Presentations.Get(u.presentationId).Do()
	if err != nil {
		u.logger.Errorf("Unable to retrieve presentation: %v", err)
		return
	}

	// Update the first slide with Chain data
	u.updateChainData(ppt.Slides[0], date.AddDate(0, 0, -7))

	// Update the next four slides with CEX data
	u.updateCexData(ppt.Slides[1], date, "TRX", map[string]bool{"Binance": true, "Kraken": true},
		[]string{"Binance-TRX/USDT", "Binance-TRX/BTC", "Bybit-TRX/USDT", "OKX-TRX/USDT", "Upbit-TRX/KRW", "Bitget-TRX/USDT"})

	u.updateCexData(ppt.Slides[2], date, "STEEM", map[string]bool{"Binance-STEEM/USDT": true, "Binance-STEEM/USDC": true},
		[]string{"Binance-STEEM/USDT", "Binance-STEEM/USDC", "Binance-STEEM/BTC", "Binance-STEEM/ETH", "Upbit-STEEM/KRW"})

	u.updateCexData(ppt.Slides[3], date, "JST", map[string]bool{"Binance": true, "HTX": true, "Poloniex": true},
		[]string{"Binance-JST/USDT", "Binance-JST/BTC", "Bybit-JST/USDT", "OKX-JST/USDT", "Upbit-JST/KRW", "Bitget-JST/USDT"})

	u.updateCexData(ppt.Slides[4], date, "WIN", map[string]bool{"Binance": true, "HTX": true, "Poloniex": true},
		[]string{"Binance-WIN/USDT", "Binance-WIN/TRX", "OKX-WIN/USDT", "Bitget-WIN/USDT"})

	// Update Revenue data
	u.updateRevenueData(ppt.Slides[5], date)

	// Update Stock data
	u.updateStockData(ppt.Slides[6], date)
}

func (u *Updater) updateChainData(page *slides.Page, startDate time.Time) {
	reqs := make([]*slides.Request, 0)

	// Update USDT tranfer fee
	usdtStats := u.db.GetTokenStatisticsByDateDaysToken(startDate, 7, "USDT")
	burnPercent := 1.0 - (float64(usdtStats.EnergyUsage)+float64(usdtStats.EnergyOriginUsage))/float64(usdtStats.EnergyTotal)

	fees := make([]string, 0)
	allFees := net.GetFees(startDate, 7)
	fees = append(fees, fmt.Sprintf("$%.2f", allFees["tronLowPrice"]))
	fees = append(fees, fmt.Sprintf("$%.2f", allFees["tronHighPrice"]))
	fees = append(fees, fmt.Sprintf("$%.2f", allFees["tronLowPrice"]*burnPercent))
	fees = append(fees, fmt.Sprintf("$%.2f", allFees["tronHighPrice"]*burnPercent))
	fees = append(fees, fmt.Sprintf("$%.2f", allFees["ethLowPrice"]))
	fees = append(fees, fmt.Sprintf("$%.2f", allFees["ethHighPrice"]))

	feeText := extractText(page.PageElements[4])
	feeObjectId := page.PageElements[4].ObjectId

	matches := regexp.MustCompile(`\$[0-9]+\.[0-9]+`).FindAllStringIndex(feeText, -1)
	for i, match := range matches {
		start := utf16Len(feeText[:match[0]])
		end := utf16Len(feeText[:match[1]])

		reqs = append(reqs, buildUpdateTextRequests(feeObjectId, -1, -1, start, end, fees[i])...)
	}

	// Update USDT Statistics
	thisWeekAvgStat := u.db.GetAvgFungibleTokenStatisticsByDateDaysTokenType(startDate, 7, "USDT", "1e0")
	lastWeekAvgStat := u.db.GetAvgFungibleTokenStatisticsByDateDaysTokenType(startDate.AddDate(0, 0, -7), 7, "USDT", "1e0")
	usdtText := fmt.Sprintf("USDT Statistics\n"+
		" - Transfer Volume:  $%s/d (%s)\n"+
		" - Total        Supply:  $%s\n"+
		" - Transfer Account: %s/d (%s)\n"+
		" - Transfer        Txs: %s/d (%s)",
		common.FormatWithUnits(float64(thisWeekAvgStat.AmountSum.Int64())/1e6),
		common.FormatChangePercent(lastWeekAvgStat.AmountSum.Int64(), thisWeekAvgStat.AmountSum.Int64()),
		common.FormatWithUnits(u.db.GetUSDTSupplyByDateChain(time.Now(), "Tron").TotalAuthorized),
		common.FormatWithUnits(float64(thisWeekAvgStat.Count)),
		common.FormatChangePercent(lastWeekAvgStat.Count, thisWeekAvgStat.Count),
		common.FormatWithUnits(float64(thisWeekAvgStat.UniqueUser)),
		common.FormatChangePercent(lastWeekAvgStat.UniqueUser, thisWeekAvgStat.UniqueUser))
	usdtObjectId := page.PageElements[6].ObjectId

	reqs = append(reqs, buildUpdateTextRequests(usdtObjectId, -1, -1, 0, 0, usdtText)...)

	// Update Supply Sheet Chart
	supplyChartId := page.PageElements[8].ObjectId
	reqs = append(reqs, []*slides.Request{
		{
			RefreshSheetsChart: &slides.RefreshSheetsChartRequest{
				ObjectId: supplyChartId,
			},
		},
	}...)

	// Update note
	thisStorageStats := u.db.GetUSDTStorageStatisticsByDateDays(startDate, 7)
	lastStorageStats := u.db.GetUSDTStorageStatisticsByDateDays(startDate.AddDate(0, 0, -7), 7)
	dateNote := fmt.Sprintf("Updated on %s\n\n", startDate.AddDate(0, 0, 7).Format("2006-01-02"))
	priceNote := fmt.Sprintf("TRX price: $%f\n\n", u.db.GetTokenListingStatistic(startDate.AddDate(0, 0, 7), "TRX").Price)
	storageNote := common.FormatStorageDiffReport(thisStorageStats, lastStorageStats) + "\n"
	resp, _ := u.sheetsService.Spreadsheets.Values.BatchGet(u.volumeId).Ranges("USDT!F2:H8").Do()
	supplyNote := common.FormatUSDTSupplyReport(resp.ValueRanges[0].Values)

	note := fmt.Sprintf("%s%s%s%s", dateNote, priceNote, storageNote, supplyNote)
	noteObjectId := page.SlideProperties.NotesPage.PageElements[1].ObjectId
	reqs = append(reqs, buildUpdateTextRequests(noteObjectId, -1, -1, 0, 0, note)...)

	_, updateErr := u.slidesService.Presentations.BatchUpdate(u.presentationId,
		&slides.BatchUpdatePresentationRequest{
			Requests: reqs,
		}).Do()
	if updateErr != nil {
		u.logger.Error("Failed to update USDT transfer fee", zap.Error(updateErr))
	}
}

func (u *Updater) updateCexData(page *slides.Page, today time.Time, token string, exchanges map[string]bool, concernedPairs []string) {
	reqs := make([]*slides.Request, 0)

	oneWeekAgo := today.AddDate(0, 0, -7)

	todayTokenListing := u.db.GetTokenListingStatistic(today, token)
	oneWeekAgoTokenListing := u.db.GetTokenListingStatistic(oneWeekAgo, token)

	// Update date in the title
	reqs = append(reqs, buildUpdateTitleRequests(page.PageElements[1], today)...)

	// Update the token price
	price := fmt.Sprintf("$%.4f", todayTokenListing.Price)
	if token == "WIN" {
		price = fmt.Sprintf("$0.0₄%d ", int(todayTokenListing.Price*1e7))
	}
	priceChange := common.FormatFloatChangePercent(oneWeekAgoTokenListing.Price, todayTokenListing.Price)
	priceObjectId := page.PageElements[4].ObjectId
	reqs = append(reqs, buildTextAndChangeRequests(priceObjectId, -1, -1, price, priceChange, 12, 9, true)...)

	thisWeek := today.AddDate(0, 0, -7)
	lastWeek := oneWeekAgo.AddDate(0, 0, -7)

	thisLowPrice, thisHighPrice := u.db.GetTokenPriceRangeByStartDateAndDays(token, thisWeek, 7)
	lastLowPrice, lastHighPrice := u.db.GetTokenPriceRangeByStartDateAndDays(token, lastWeek, 7)

	// Update the token low price
	lowPrice := fmt.Sprintf("$%.4f", thisLowPrice)
	if token == "WIN" {
		lowPrice = fmt.Sprintf("$0.0₄%d ", int(thisLowPrice*1e7))
	}
	lowPriceChange := common.FormatFloatChangePercent(lastLowPrice, thisLowPrice)
	lowPriceObjectId := page.PageElements[8].ObjectId
	reqs = append(reqs, buildTextAndChangeRequests(lowPriceObjectId, -1, -1, lowPrice, lowPriceChange, 7, 5, false)...)

	// Update the token high price
	higPrice := fmt.Sprintf("$%.4f", thisHighPrice)
	if token == "WIN" {
		higPrice = fmt.Sprintf("$0.0₄%d ", int(thisHighPrice*1e7))
	}
	highPriceChange := common.FormatFloatChangePercent(lastHighPrice, thisHighPrice)
	highPriceObjectId := page.PageElements[9].ObjectId
	reqs = append(reqs, buildTextAndChangeRequests(highPriceObjectId, -1, -1, higPrice, highPriceChange, 7, 5, false)...)

	// Update the market cap
	marketCap := "$" + common.FormatWithUnits(todayTokenListing.MarketCap)
	marketCapChange := common.FormatFloatChangePercent(oneWeekAgoTokenListing.MarketCap, todayTokenListing.MarketCap)
	marketCapObjectId := page.PageElements[14].ObjectId
	reqs = append(reqs, buildTextAndChangeRequests(marketCapObjectId, -1, -1, marketCap, marketCapChange, 11, 7, true)...)

	groupByExchange := true
	if token == "STEEM" {
		groupByExchange = false
	}
	thisMarketPairStats := u.db.GetMergedMarketPairStatistics(thisWeek, 7, token, true, groupByExchange)
	lastMarketPairStats := u.db.GetMergedMarketPairStatistics(lastWeek, 7, token, true, groupByExchange)

	// Update the 24h volume
	volume24H := "$" + common.FormatWithUnits(thisMarketPairStats["Total"].Volume)
	volume24HChange := common.FormatFloatChangePercent(lastMarketPairStats["Total"].Volume, thisMarketPairStats["Total"].Volume)
	volumeObjectId := page.PageElements[12].ObjectId
	reqs = append(reqs, buildTextAndChangeRequests(volumeObjectId, -1, -1, volume24H, volume24HChange, 11, 7, true)...)

	// Update the 24h volume excluding HTX&Poloniex
	thisOtherVolume := thisMarketPairStats["Total"].Volume
	for key, stat := range thisMarketPairStats {
		if strings.HasPrefix(key, "HTX") || strings.HasPrefix(key, "Poloniex") {
			thisOtherVolume -= stat.Volume
		}
	}
	lastOtherVolume := lastMarketPairStats["Total"].Volume
	for key, stat := range lastMarketPairStats {
		if strings.HasPrefix(key, "HTX") || strings.HasPrefix(key, "Poloniex") {
			lastOtherVolume -= stat.Volume
		}
	}
	otherVolume24H := "$" + common.FormatWithUnits(thisOtherVolume)
	otherVolume24HChange := common.FormatFloatChangePercent(lastOtherVolume, thisOtherVolume)
	otherVolumeObjectId := page.PageElements[16].ObjectId
	reqs = append(reqs, buildTextAndChangeRequests(otherVolumeObjectId, -1, -1, otherVolume24H, otherVolume24HChange, 11, 7, true)...)

	// Update the 24h volume / market cap
	thisVolumeToMarketCapRatio := thisMarketPairStats["Total"].Volume / todayTokenListing.MarketCap
	lastVolumeToMarketCapRatio := lastMarketPairStats["Total"].Volume / oneWeekAgoTokenListing.MarketCap
	VolumeToMarketCapRatio := fmt.Sprintf("%.2f%%", thisVolumeToMarketCapRatio*100)
	VolumeToMarketCapRatioChange := common.FormatPercentWithSign(thisVolumeToMarketCapRatio - lastVolumeToMarketCapRatio)
	VolumeToMarketCapRatioObjectId := page.PageElements[18].ObjectId
	reqs = append(reqs, buildTextAndChangeRequests(VolumeToMarketCapRatioObjectId, -1, -1, VolumeToMarketCapRatio, VolumeToMarketCapRatioChange, 11, 7, true)...)

	statsToInsert := make([]*models.MarketPairStatistic, 0)
	for key, stat := range thisMarketPairStats {
		if exchanges[key] {
			// Use datetime to store the key
			stat.Datetime = key
			statsToInsert = append(statsToInsert, stat)
		}
	}

	sort.Slice(statsToInsert, func(i, j int) bool {
		return statsToInsert[i].Volume > statsToInsert[j].Volume
	})

	// Update Monitoring Table
	monitorTableObjectId := page.PageElements[22].ObjectId
	rowIndex := int64(1)
	ruleStats := u.db.GetMarketPairRulesStatsByTokenAndStartDateAndDays(token, thisWeek, 7)
	for _, pair := range concernedPairs {
		ruleStat := ruleStats[pair]
		var color string

		// Update +2% depth cell
		depthPositiveCell, color, shouldAppendStat := getComplianceRateEmojiAndColor(ruleStat.DepthPositiveBrokenCount, ruleStat.HitsCount)
		if shouldAppendStat {
			depthPositiveCell += fmt.Sprintf(" %s\n(Dp: %s / %s)",
				common.FormatOfPercent(int64(ruleStat.HitsCount), int64(ruleStat.HitsCount-ruleStat.DepthPositiveBrokenCount)),
				humanize.SIWithDigits(ruleStat.DepthUsdPositiveTwoSum/float64(ruleStat.HitsCount), 0, ""),
				humanize.SIWithDigits(ruleStat.DepthUsdPositiveTwo, 0, ""))
		}
		reqs = append(reqs, buildFullTextRequests(monitorTableObjectId, rowIndex, 2, depthPositiveCell, 8, color, false)...)

		// Update +2% depth cell
		depthNegativeCell, color, shouldAppendStat := getComplianceRateEmojiAndColor(ruleStat.DepthNegativeBrokenCount, ruleStat.HitsCount)
		if shouldAppendStat {
			depthNegativeCell += fmt.Sprintf(" %s\n(Dp: %s / %s)",
				common.FormatOfPercent(int64(ruleStat.HitsCount), int64(ruleStat.HitsCount-ruleStat.DepthNegativeBrokenCount)),
				humanize.SIWithDigits(ruleStat.DepthUsdNegativeTwoSum/float64(ruleStat.HitsCount), 0, ""),
				humanize.SIWithDigits(ruleStat.DepthUsdNegativeTwo, 0, ""))
		}
		reqs = append(reqs, buildFullTextRequests(monitorTableObjectId, rowIndex, 3, depthNegativeCell, 8, color, false)...)

		// Update +2% depth cell
		volumeCell, color, shouldAppendStat := getComplianceRateEmojiAndColor(ruleStat.VolumeBrokenCount, ruleStat.HitsCount)
		if shouldAppendStat {
			volumeCell += fmt.Sprintf(" %s\n(Vol: %s / %s)",
				common.FormatOfPercent(int64(ruleStat.HitsCount), int64(ruleStat.HitsCount-ruleStat.VolumeBrokenCount)),
				humanize.SIWithDigits(ruleStat.VolumeSum/float64(ruleStat.HitsCount), 0, ""),
				humanize.SIWithDigits(ruleStat.Volume, 0, ""))
		}
		reqs = append(reqs, buildFullTextRequests(monitorTableObjectId, rowIndex, 5, volumeCell, 8, color, false)...)

		rowIndex++
	}

	// Update Volume Table
	volumeTableObjectId := page.PageElements[19].ObjectId
	rowIndex = int64(1)
	for _, stat := range statsToInsert {
		// Key is stored in the datetime field
		key := stat.Datetime

		// Update name cell
		name := key
		if strings.Contains(key, "-") {
			name = fmt.Sprintf("%s\n%s", stat.Pair, stat.ExchangeName)
		}
		reqs = append(reqs, buildFullTextRequests(volumeTableObjectId, rowIndex, 0, name, 7, "white", false)...)

		// Update volume cell
		volume := "$" + common.FormatWithUnits(stat.Volume)
		volumeChange := common.FormatFloatChangePercent(lastMarketPairStats[key].Volume, stat.Volume)
		reqs = append(reqs, buildTextAndChangeRequests(volumeTableObjectId, rowIndex, 1, volume, volumeChange, 7, 5, false)...)

		// Update depth cell
		depth := fmt.Sprintf("%s / %s",
			common.FormatWithUnits(stat.DepthUsdPositiveTwo),
			common.FormatWithUnits(stat.DepthUsdNegativeTwo))
		reqs = append(reqs, buildFullTextRequests(volumeTableObjectId, rowIndex, 2, depth, 7, "white", false)...)

		// Update percent cell
		percent := fmt.Sprintf("%.2f%%", stat.Percent*100)
		percentChange := fmt.Sprintf("%s", common.FormatPercentWithSign((stat.Percent-lastMarketPairStats[key].Percent)*100))
		reqs = append(reqs, buildTextAndChangeRequests(volumeTableObjectId, rowIndex, 3, percent, percentChange, 7, 5, false)...)

		rowIndex++
	}

	// Update Volume Chart
	volumeChartId := page.PageElements[25].ObjectId
	reqs = append(reqs, []*slides.Request{
		{
			RefreshSheetsChart: &slides.RefreshSheetsChartRequest{
				ObjectId: volumeChartId,
			},
		},
	}...)

	// Send the batch update request
	_, updateErr := u.slidesService.Presentations.BatchUpdate(u.presentationId,
		&slides.BatchUpdatePresentationRequest{
			Requests: reqs,
		}).Do()
	if updateErr != nil {
		u.logger.Error("Failed to update presentation", zap.Error(updateErr))
	}
}

func (u *Updater) updateRevenueData(page *slides.Page, today time.Time) {
	reqs := make([]*slides.Request, 0)

	// Update today in the title
	reqs = append(reqs, buildUpdateTitleRequests(page.PageElements[1], today)...)

	var pf = func(percent string) string {
		if strings.HasPrefix(percent, "-") {
			return percent
		}
		return "+" + percent
	}

	// Update statistics on the left
	resp, _ := u.sheetsService.Spreadsheets.Values.BatchGet(u.revenueId).Ranges("Total!O2:Q4", "USDT!M2:O4", "USDT!M6:O8").Do()
	totalRevenue := resp.ValueRanges[0].Values
	usdtRevenue := resp.ValueRanges[1].Values
	otherRevenue := resp.ValueRanges[2].Values

	template :=
		"Total Protocol Revenue (7D Avg.):\n" +
			"    Total: \t$%s  (%s)\n" +
			"    - Energy: \t$%s  (%s, %s)\n" +
			"    #- Burning: \t$%s  (%s#, %s)\n\n\n" +
			"Total USDT Revenue (7D Avg.):\n" +
			"    Total: \t$%s  (%s, %s)\n" +
			"   #- Burning: \t$%s  (%s#, %s)\n" +
			"    - Staking: \t$%s  (%s, %s)\n\n\n" +
			"Other Revenue (7D Avg.):\n" +
			"    #Total: \t$%s (%s#, %s)\n" +
			"    - Burning: \t$%s   (%s, %s)\n" +
			"    - Staking: \t$%s (%s, %s)\n"

	textWithAnchor := fmt.Sprintf(template, totalRevenue[0][0], pf(totalRevenue[0][2].(string)),
		totalRevenue[1][0], pf(totalRevenue[1][2].(string)), totalRevenue[1][1],
		totalRevenue[2][0], pf(totalRevenue[2][2].(string)), totalRevenue[2][1],
		usdtRevenue[0][0], pf(usdtRevenue[0][2].(string)), usdtRevenue[0][1],
		usdtRevenue[1][0], pf(usdtRevenue[1][2].(string)), usdtRevenue[1][1],
		usdtRevenue[2][0], pf(usdtRevenue[2][2].(string)), usdtRevenue[2][1],
		otherRevenue[0][0], pf(otherRevenue[0][2].(string)), otherRevenue[0][1],
		otherRevenue[1][0], pf(otherRevenue[1][2].(string)), otherRevenue[1][1],
		otherRevenue[2][0], pf(otherRevenue[2][2].(string)), otherRevenue[2][1])

	parts := strings.Split(textWithAnchor, "#")
	indexes := make([][]int64, 0)

	revenueText := strings.Builder{}
	indexes = append(indexes, []int64{utf16Len(parts[0]), utf16Len(parts[0] + parts[1])})
	revenueText.WriteString(parts[0] + parts[1])
	indexes = append(indexes, []int64{utf16Len(revenueText.String() + parts[2]), utf16Len(revenueText.String() + parts[2] + parts[3])})
	revenueText.WriteString(parts[2] + parts[3])
	indexes = append(indexes, []int64{utf16Len(revenueText.String() + parts[4]), utf16Len(revenueText.String() + parts[4] + parts[5])})
	revenueText.WriteString(parts[4] + parts[5] + parts[6])

	revenueObjectId := page.PageElements[2].ObjectId
	reqs = append(reqs, buildUpdateTextRequests(revenueObjectId, -1, -1, 0, 0, revenueText.String())...)
	var getColor = func(percent string) string {
		if strings.HasPrefix(percent, "-") {
			return "green"
		}
		return "yellow"
	}
	reqs = append(reqs, buildUpdateStyleRequest(revenueObjectId, -1, -1, indexes[0][0], indexes[0][1], 11, getColor(totalRevenue[2][2].(string)), true))
	reqs = append(reqs, buildUpdateStyleRequest(revenueObjectId, -1, -1, indexes[1][0], indexes[1][1], 11, getColor(usdtRevenue[1][2].(string)), true))
	reqs = append(reqs, buildUpdateStyleRequest(revenueObjectId, -1, -1, indexes[2][0], indexes[2][1], 11, getColor(otherRevenue[0][2].(string)), true))

	revenueChartId := page.PageElements[3].ObjectId
	reqs = append(reqs, []*slides.Request{
		{
			RefreshSheetsChart: &slides.RefreshSheetsChartRequest{
				ObjectId: revenueChartId,
			},
		},
	}...)

	// Update note
	resp, _ = u.sheetsService.Spreadsheets.Values.BatchGet(u.revenueId).Ranges("Total!O14:R16", "USDT!M12:P14", "USDT!M16:P18").Do()
	totalNote := resp.ValueRanges[0].Values
	usdtNote := resp.ValueRanges[1].Values
	otherNote := resp.ValueRanges[2].Values

	noteTemplate :=
		"(TRX amount based revenue)\tCurrent Week\t\tPercent\tChange\tLast Week\n" +
			"Total Protocol Revenue\t\t%s\t\t%s \t%s\t%s\n" +
			"Total Energy Revenue\t\t%s\t\t%s\t%s \t%s\n" +
			"Total Burning Revenue\t\t%s\t\t%s\t%s \t%s\n\n" +
			"Total Revenue from USDT\t\t%s\t\t%s\t%s \t%s\n" +
			"Burnt Revenue from USDT\t\t%s\t\t%s\t%s \t%s\n" +
			"Staked Revenue from USDT\t%s\t\t%s\t%s \t%s\n\n" +
			"Total Revenue from Other\t\t%s\t\t%s\t\t%s \t%s\n" +
			"Burnt Revenue from other\t\t%s\t\t%s\t%s \t%s\n" +
			"Staked Revenue from other\t%s\t\t%s\t%s \t%s\n\n" +
			"TRX Avg Price: %f"

	revenueNote := fmt.Sprintf(noteTemplate,
		totalNote[0][0], totalNote[0][1], pf(totalNote[0][2].(string)), totalNote[0][3],
		totalNote[1][0], totalNote[1][1], pf(totalNote[1][2].(string)), totalNote[1][3],
		totalNote[2][0], totalNote[2][1], pf(totalNote[2][2].(string)), totalNote[2][3],
		usdtNote[0][0], usdtNote[0][1], pf(usdtNote[0][2].(string)), usdtNote[0][3],
		usdtNote[1][0], usdtNote[1][1], pf(usdtNote[1][2].(string)), usdtNote[1][3],
		usdtNote[2][0], usdtNote[2][1], pf(usdtNote[2][2].(string)), usdtNote[2][3],
		otherNote[0][0], otherNote[0][1], pf(otherNote[0][2].(string)), otherNote[0][3],
		otherNote[1][0], otherNote[1][1], pf(otherNote[1][2].(string)), otherNote[1][3],
		otherNote[2][0], otherNote[2][1], pf(otherNote[2][2].(string)), otherNote[2][3],
		u.db.GetAvgTokenPriceByStartDateAndDays("TRX", today.AddDate(0, 0, -7), 7))

	revenueNoteObjectId := page.SlideProperties.NotesPage.PageElements[0].ObjectId
	reqs = append(reqs, buildUpdateTextRequests(revenueNoteObjectId, -1, -1, 0, 0, revenueNote)...)

	_, updateErr := u.slidesService.Presentations.BatchUpdate(u.presentationId,
		&slides.BatchUpdatePresentationRequest{
			Requests: reqs,
		}).Do()
	if updateErr != nil {
		u.logger.Error("Failed to update presentation", zap.Error(updateErr))
	}
}

func (u *Updater) getVolumeData(startDate time.Time, token string, exchanges []string) [][]interface{} {
	result := make([][]interface{}, 0)
	for i := 0; i < 30; i++ {
		curDate := startDate.AddDate(0, 0, i)
		marketPairStats := u.db.GetMergedMarketPairStatistics(curDate, 1, token, false, true)

		row := make([]interface{}, 0)
		row = append(row, curDate.Format("2006-01-02"))

		for _, exchange := range exchanges {
			if marketPairStats[exchange] == nil {
				row = append(row, 0)
			} else {
				row = append(row, marketPairStats[exchange].Volume)
			}
		}

		result = append(result, row)
	}
	return result
}

func (u *Updater) getUSDTSupplyData(date time.Time) [][]interface{} {
	data := u.db.GetUSDTSupplyByDate(date)
	sort.Slice(data, func(i, j int) bool {
		return data[i].TotalAuthorized > data[j].TotalAuthorized
	})

	result := make([][]interface{}, 0)
	for _, item := range data {
		row := make([]interface{}, 0)
		row = append(row, item.Chain)
		row = append(row, item.TotalAuthorized)
		row = append(row, item.NotIssued)

		result = append(result, row)
	}

	return result
}

func (u *Updater) updateStockData(page *slides.Page, today time.Time) {
	// 0:date 1:open 2:high 3:low 4:close 5:volume
	stockData := net.GetStockData(today)

	// Update Stock sheet
	_, err := u.sheetsService.Spreadsheets.Values.Update(u.volumeId, "SRM!A2:F41",
		&sheets.ValueRange{
			Values: stockData,
		}).ValueInputOption("USER_ENTERED").Do()
	if err != nil {
		u.logger.Errorf("Unable to update Stock sheet: %v", err)
	}

	// Update Stock chart
	reqs := make([]*slides.Request, 0)

	// Update today in the title
	reqs = append(reqs, buildUpdateTitleRequests(page.PageElements[0], today)...)

	todayData := stockData[len(stockData)-1]
	oneWeekAgoData := stockData[len(stockData)-8]

	// Update the stock price
	price := fmt.Sprintf("$%.2f", todayData[4])
	priceChange := common.FormatFloatChangePercent(oneWeekAgoData[4].(float64), todayData[4].(float64))
	priceObjectId := page.PageElements[5].ObjectId
	reqs = append(reqs, buildTextAndChangeRequests(priceObjectId, -1, -1, price, priceChange, 12, 9, true)...)

	thisLowPrice, thisHighPrice := 0.0, 0.0
	lastLowPrice, lastHighPrice := 0.0, 0.0

	for i := 1; i <= 7; i++ {
		thisLowPrice = math.Min(thisLowPrice, stockData[len(stockData)-i][3].(float64))
		thisHighPrice = math.Max(thisHighPrice, stockData[len(stockData)-i][2].(float64))
		lastLowPrice = math.Min(lastLowPrice, stockData[len(stockData)-7-i][3].(float64))
		lastHighPrice = math.Max(lastHighPrice, stockData[len(stockData)-7-i][2].(float64))
	}

	// Update the stock low price
	lowPrice := fmt.Sprintf("$%.2f", thisLowPrice)
	lowPriceChange := common.FormatFloatChangePercent(lastLowPrice, thisLowPrice)
	lowPriceObjectId := page.PageElements[9].ObjectId
	reqs = append(reqs, buildTextAndChangeRequests(lowPriceObjectId, -1, -1, lowPrice, lowPriceChange, 7, 5, false)...)

	// Update the stock high price
	higPrice := fmt.Sprintf("$%.2f", thisHighPrice)
	highPriceChange := common.FormatFloatChangePercent(lastHighPrice, thisHighPrice)
	highPriceObjectId := page.PageElements[10].ObjectId
	reqs = append(reqs, buildTextAndChangeRequests(highPriceObjectId, -1, -1, higPrice, highPriceChange, 7, 5, false)...)

	// Update the market cap
	marketCap := "$" + common.FormatWithUnits(todayData[4].(float64)*1724.4e6)
	marketCapChange := common.FormatFloatChangePercent(oneWeekAgoData[4].(float64), todayData[4].(float64))
	marketCapObjectId := page.PageElements[13].ObjectId
	reqs = append(reqs, buildTextAndChangeRequests(marketCapObjectId, -1, -1, marketCap, marketCapChange, 11, 7, true)...)

	// Update the 24h volume
	volume24H := "$" + common.FormatWithUnits(todayData[5].(float64))
	volume24HChange := common.FormatFloatChangePercent(oneWeekAgoData[5].(float64), todayData[5].(float64))
	volumeObjectId := page.PageElements[15].ObjectId
	reqs = append(reqs, buildTextAndChangeRequests(volumeObjectId, -1, -1, volume24H, volume24HChange, 11, 7, true)...)

	// TODO: update held digital asset value

	// Update K-line Sheet Chart
	kLineChartId := page.PageElements[21].ObjectId
	reqs = append(reqs, []*slides.Request{
		{
			RefreshSheetsChart: &slides.RefreshSheetsChartRequest{
				ObjectId: kLineChartId,
			},
		},
	}...)

	// Update Volume Chart
	volumeChartId := page.PageElements[22].ObjectId
	reqs = append(reqs, []*slides.Request{
		{
			RefreshSheetsChart: &slides.RefreshSheetsChartRequest{
				ObjectId: volumeChartId,
			},
		},
	}...)

	_, updateErr := u.slidesService.Presentations.BatchUpdate(u.presentationId,
		&slides.BatchUpdatePresentationRequest{
			Requests: reqs,
		}).Do()
	if updateErr != nil {
		u.logger.Error("Failed to update presentation", zap.Error(updateErr))
	}
}

func utf16Len(s string) int64 {
	return int64(len(utf16.Encode([]rune(s))))
}

func extractText(element *slides.PageElement) string {
	if element.Shape == nil || element.Shape.Text == nil {
		return ""
	}

	var result string
	for _, para := range element.Shape.Text.TextElements {
		if para.TextRun != nil {
			result += para.TextRun.Content
		}
	}
	return result
}

func getComplianceRateEmojiAndColor(brokenCount, hitsCount int) (string, string, bool) {
	if brokenCount < hitsCount/100 {
		return "✅", "white", false
	}
	if brokenCount < hitsCount/2 {
		return "⚠️", "orange", true
	}
	return "❌", "red", true
}

func buildUpdateTitleRequests(titleElement *slides.PageElement, date time.Time) []*slides.Request {
	reqs := make([]*slides.Request, 0)

	title := extractText(titleElement)
	titleObjectId := titleElement.ObjectId
	matches := regexp.MustCompile(`[0-9]{2}-[0-9]{2}`).FindAllStringIndex(title, -1)
	if len(matches) > 0 {
		start := utf16Len(title[:matches[0][0]])
		end := utf16Len(title[:matches[0][1]])
		reqs = append(reqs, buildUpdateTextRequests(titleObjectId, -1, -1, start, end, date.Format("01-02"))...)
	}

	return reqs
}

func buildFullTextRequests(objectId string, i, j int64, text string, fontSize float64, color string, bold bool) []*slides.Request {
	reqs := make([]*slides.Request, 0)
	reqs = append(reqs, buildUpdateTextRequests(objectId, i, j, 0, 0, text)...)
	reqs = append(reqs, buildUpdateStyleRequest(objectId, i, j, 0, utf16Len(text), fontSize, color, bold))
	return reqs
}

func buildTextAndChangeRequests(objectId string, i, j int64, text string, changePercent string, textFontSize, percentFontSize float64, isTextBold bool) []*slides.Request {
	fullText := fmt.Sprintf("%s %s", text, changePercent)

	reqs := make([]*slides.Request, 0)
	reqs = append(reqs, buildUpdateTextRequests(objectId, i, j, 0, 0, fullText)...)
	reqs = append(reqs, buildUpdateStyleRequest(objectId, i, j, 0, utf16Len(text)+1, textFontSize, "white", isTextBold))

	color := "green"
	if strings.HasPrefix(changePercent, "-") {
		color = "red"
	}
	reqs = append(reqs, buildUpdateStyleRequest(objectId, i, j, utf16Len(text)+1, utf16Len(fullText), percentFontSize, color, false))

	return reqs
}

func buildUpdateTextRequests(objectId string, i, j, start, end int64, text string) []*slides.Request {
	var cell *slides.TableCellLocation
	if i < 0 && j < 0 {
		cell = nil
	} else {
		cell = &slides.TableCellLocation{
			RowIndex:    i,
			ColumnIndex: j,
		}
	}

	reqs := make([]*slides.Request, 0)

	reqs = append(reqs, &slides.Request{
		DeleteText: &slides.DeleteTextRequest{
			CellLocation: cell,
			ObjectId:     objectId,
			TextRange:    getTextRange(start, end),
		},
	})

	reqs = append(reqs, &slides.Request{
		InsertText: &slides.InsertTextRequest{
			CellLocation:   cell,
			ObjectId:       objectId,
			InsertionIndex: start,
			Text:           text,
		},
	})

	return reqs
}

func buildUpdateStyleRequest(objectId string, i, j, start, end int64, fontSize float64, color string, bold bool) *slides.Request {
	var rgbColor *slides.RgbColor
	switch color {
	case "white":
		rgbColor = &slides.RgbColor{
			Red:   1.0,
			Green: 1.0,
			Blue:  1.0,
		}
	case "red":
		rgbColor = &slides.RgbColor{
			Red: 1.0,
		}
	case "green":
		rgbColor = &slides.RgbColor{
			Green: 1.0,
		}
	case "yellow":
		rgbColor = &slides.RgbColor{
			Red:   1.0,
			Green: 1.0,
		}
	case "orange":
		rgbColor = &slides.RgbColor{
			Red:   1.0,
			Green: 0.5,
		}
	}

	styleToUpdate := &slides.UpdateTextStyleRequest{
		ObjectId: objectId,
		TextRange: &slides.Range{
			Type:       "FIXED_RANGE",
			StartIndex: &start,
			EndIndex:   &end,
		},
		Style: &slides.TextStyle{
			FontSize: &slides.Dimension{
				Magnitude: fontSize,
				Unit:      "PT",
			},
			ForegroundColor: &slides.OptionalColor{
				OpaqueColor: &slides.OpaqueColor{
					RgbColor: rgbColor,
				},
			},
			Bold: bold,
		},
		Fields: "fontSize, foregroundColor, bold",
	}

	if i >= 0 && j >= 0 {
		styleToUpdate.CellLocation = &slides.TableCellLocation{
			RowIndex:    i,
			ColumnIndex: j,
		}
	}

	return &slides.Request{
		UpdateTextStyle: styleToUpdate,
	}
}

func getTextRange(start, end int64) *slides.Range {
	var textRange *slides.Range

	if start == 0 && end == 0 {
		textRange = &slides.Range{
			Type: "ALL",
		}
	} else if end < start {
		textRange = &slides.Range{
			Type:       "FROM_START_INDEX",
			StartIndex: &start,
		}
	} else {
		textRange = &slides.Range{
			Type:       "FIXED_RANGE",
			StartIndex: &start,
			EndIndex:   &end,
		}
	}

	return textRange
}
