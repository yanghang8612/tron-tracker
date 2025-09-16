package google

import (
	"context"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode/utf16"

	"tron-tracker/common"
	"tron-tracker/config"
	"tron-tracker/database"
	"tron-tracker/database/models"
	"tron-tracker/net"

	"github.com/dustin/go-humanize"
	"github.com/goccy/go-json"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
	"google.golang.org/api/slides/v1"
)

const EMUPerPt = 12700.0

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

// emuToPt 将 EMU 单位转换为 pt
func emuToPt(emu float64) float64 {
	return emu / EMUPerPt
}

// RenderedRect 计算出一个元素在幻灯片上渲染后的矩形：
// 返回值依次为 x, y, width, height，均为 pt 单位。
// - origWEmu/origHEmu: 元素原始宽高（Dimension.Magnitude），单位 EMU。
// - xf: 对应元素的 AffineTransform。
func RenderedRect(origWEmu, origHEmu float64, xf *slides.AffineTransform) (x, y, w, h float64) {
	// 1. 先把 translate 转成 EMU
	var txEmu, tyEmu float64
	switch xf.Unit {
	case "PT":
		txEmu = xf.TranslateX * EMUPerPt
		tyEmu = xf.TranslateY * EMUPerPt
	default: // 包括 "EMU" 或者空
		txEmu = xf.TranslateX
		tyEmu = xf.TranslateY
	}

	// 2. 四个角点在 EMU 坐标系下的位置
	//    top-left     : (0,0)
	//    top-right    : (origW, 0)
	//    bottom-left  : (0, origH)
	//    bottom-right : (origW, origH)
	x0, y0 := txEmu, tyEmu
	x1 := xf.ScaleX*origWEmu + xf.ShearX*0 + txEmu
	y1 := xf.ShearY*origWEmu + xf.ScaleY*0 + tyEmu
	x2 := xf.ScaleX*0 + xf.ShearX*origHEmu + txEmu
	y2 := xf.ShearY*0 + xf.ScaleY*origHEmu + tyEmu
	x3 := xf.ScaleX*origWEmu + xf.ShearX*origHEmu + txEmu
	y3 := xf.ShearY*origWEmu + xf.ScaleY*origHEmu + tyEmu

	// 3. 找最小/最大坐标，得到包围盒
	minX := math.Min(math.Min(x0, x1), math.Min(x2, x3))
	maxX := math.Max(math.Max(x0, x1), math.Max(x2, x3))
	minY := math.Min(math.Min(y0, y1), math.Min(y2, y3))
	maxY := math.Max(math.Max(y0, y1), math.Max(y2, y3))

	// 4. 转成 pt 并返回
	return emuToPt(minX), emuToPt(minY), emuToPt(maxX - minX), emuToPt(maxY - minY)
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
		sb.WriteString(fmt.Sprint("------------------------------\n"))

		for j, element := range slide.PageElements {
			objectId := element.ObjectId
			sb.WriteString(fmt.Sprintf("Object ID: [%d] - %s\n", j, objectId))

			box := element.Transform
			size := element.Size

			if box != nil && size != nil {
				x, y, w, h := RenderedRect(size.Width.Magnitude, size.Height.Magnitude, box)
				sb.WriteString(fmt.Sprintf("\nPosition: X=%.2fpt, Y=%.2fpt, Width=%.2fpt, Height=%.2fpt\n\n", x, y, w, h))
			}

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

		for j, element := range slide.SlideProperties.NotesPage.PageElements {
			objectId := element.ObjectId
			sb.WriteString(fmt.Sprintf("Note Object ID: [%d] - %s\n", j, objectId))

			// Check if the note element is a TEXT_BOX
			if element.Shape != nil && element.Shape.ShapeType == "TEXT_BOX" {
				textContent := extractText(element)
				sb.WriteString(fmt.Sprintf("Note Text: %s\n", textContent))
			} else {
				sb.WriteString(fmt.Sprintf("Note Unknown: %s\n", objectId))
			}
			sb.WriteString(fmt.Sprint("------------------------------\n"))
		}
		sb.WriteString(fmt.Sprint("##############################\n\n"))
	}

	return sb.String()
}

func (u *Updater) Update(date time.Time) {
	// Update TRX volume sheet
	_, err := u.sheetsService.Spreadsheets.Values.Update(u.volumeId, "TRX!A1:E31",
		&sheets.ValueRange{
			Values: u.getVolumeData(date, "TRX", u.getTop3Exchanges(date, "TRX", true, true)),
		}).ValueInputOption("USER_ENTERED").Do()
	if err != nil {
		u.logger.Errorf("Unable to update TRX volume sheet: %v", err)
	}

	// Update STEEM volume sheet
	_, err = u.sheetsService.Spreadsheets.Values.Update(u.volumeId, "STEEM!A1:E31",
		&sheets.ValueRange{
			Values: u.getVolumeData(date, "STEEM", u.getTop3Exchanges(date, "STEEM", true, true)),
		}).ValueInputOption("USER_ENTERED").Do()
	if err != nil {
		u.logger.Errorf("Unable to update STEEM volume sheet: %v", err)
	}

	// Update JST volume sheet
	_, err = u.sheetsService.Spreadsheets.Values.Update(u.volumeId, "JST!A1:E31",
		&sheets.ValueRange{
			Values: u.getVolumeData(date, "JST", []string{"Poloniex", "HTX", "Binance", "Total"}),
		}).ValueInputOption("USER_ENTERED").Do()
	if err != nil {
		u.logger.Errorf("Unable to update JST volume sheet: %v", err)
	}

	// Update WIN volume data
	_, err = u.sheetsService.Spreadsheets.Values.Update(u.volumeId, "WIN!A1:E31",
		&sheets.ValueRange{
			Values: u.getVolumeData(date, "WIN", []string{"Poloniex", "HTX", "Binance", "Total"}),
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
	lastMonth := date.AddDate(0, 0, -30)
	revenueData := make([][]interface{}, 0)
	for i := 0; i < 30; i++ {
		queryDate := lastMonth.AddDate(0, 0, i)
		trxPrice := u.db.GetTokenPriceByDate("TRX", queryDate.AddDate(0, 0, 1))
		totalStats := u.db.GetTotalStatisticsByDateDays(queryDate, 1)
		usdtStats := u.db.GetTokenStatisticsByDateDaysToken(queryDate, 1, "USDT")

		row := make([]interface{}, 0)
		row = append(row, queryDate.Format("2006-01-02"))
		if queryDate.Format("060102") == "250829" {
			row = append(row, 156.6)
		} else {
			row = append(row, totalStats.EnergyFee/(totalStats.EnergyTotal-totalStats.EnergyUsage-totalStats.EnergyOriginUsage))
		}
		row = append(row, trxPrice)
		row = append(row, totalStats.EnergyFee)
		row = append(row, totalStats.NetFee)
		row = append(row, totalStats.Fee-totalStats.EnergyFee-totalStats.NetFee)
		row = append(row, totalStats.EnergyUsage+totalStats.EnergyOriginUsage)
		row = append(row, totalStats.NetUsage)
		row = append(row, usdtStats.EnergyFee)
		row = append(row, usdtStats.NetFee)
		row = append(row, usdtStats.Fee-usdtStats.EnergyFee-usdtStats.NetFee)
		row = append(row, usdtStats.EnergyUsage+usdtStats.EnergyOriginUsage)
		row = append(row, usdtStats.NetUsage)

		revenueData = append(revenueData, row)
	}

	_, err = u.sheetsService.Spreadsheets.Values.Update(u.revenueId, "Data!A2:W66",
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
	u.updateCexData(ppt.Slides[1], date, "TRX", nil,
		[]string{"Binance-TRX/USDT", "Binance-TRX/BTC", "Bybit-TRX/USDT", "OKX-TRX/USDT", "Upbit-TRX/KRW", "Bitget-TRX/USDT"})

	u.updateCexData(ppt.Slides[2], date, "STEEM", nil,
		[]string{"Binance-STEEM/USDT", "Binance-STEEM/USDC", "Binance-STEEM/BTC", "Binance-STEEM/ETH", "Upbit-STEEM/KRW"})

	u.updateCexData(ppt.Slides[3], date, "JST", map[string]bool{"Binance": true, "HTX": true, "Poloniex": true},
		[]string{"Binance-JST/USDT", "Binance-JST/BTC", "Bybit-JST/USDT", "Upbit-JST/KRW", "Bitget-JST/USDT"})

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
	reqs = append(reqs, buildUpdateTitleRequests(page.PageElements[0], today)...)

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

	// Move the cursor
	reqs = append(reqs, buildMoveByPercentageRequest(page.PageElements[5], page.PageElements[10], (todayTokenListing.Price-thisLowPrice)/(thisHighPrice-thisLowPrice)))

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

	thisMarketPairStats := u.db.GetMergedMarketPairStatistics(thisWeek, 7, token, true, true)
	lastMarketPairStats := u.db.GetMergedMarketPairStatistics(lastWeek, 7, token, true, true)

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
	thisSortedMarketPairStats := make([]*models.MarketPairStatistic, 0)
	for _, stat := range thisMarketPairStats {
		thisSortedMarketPairStats = append(thisSortedMarketPairStats, stat)
	}
	sort.Slice(thisSortedMarketPairStats, func(i, j int) bool {
		return thisSortedMarketPairStats[i].Volume > thisSortedMarketPairStats[j].Volume
	})

	statsToInsert := make([]*models.MarketPairStatistic, 0)
	if exchanges == nil {
		exchanges = make(map[string]bool)
		for _, exchange := range u.getTop3Exchanges(today, token, false, false) {
			exchanges[exchange] = true
		}
	}

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

	// Update note
	var volumeNote strings.Builder
	for i := 1; i <= 10 && i < len(thisSortedMarketPairStats); i++ {
		thisStat := thisSortedMarketPairStats[i]
		lastStat := lastMarketPairStats[thisStat.ExchangeName]
		if lastStat == nil {
			lastStat = &models.MarketPairStatistic{}
		}

		volumeNote.WriteString(fmt.Sprintf("%-12s\t%-24s\t%-35s\t%s(%s)\n",
			thisStat.ExchangeName,
			fmt.Sprintf("%s(%s)",
				"$"+common.FormatWithUnits(thisStat.Volume),
				common.FormatFloatChangePercent(lastStat.Volume, thisStat.Volume)),
			fmt.Sprintf("%s / %s",
				common.FormatWithUnits(thisStat.DepthUsdPositiveTwo),
				common.FormatWithUnits(thisStat.DepthUsdNegativeTwo)),
			fmt.Sprintf("%.2f%%", thisStat.Percent*100),
			fmt.Sprintf("%s", common.FormatPercentWithSign((thisStat.Percent-lastStat.Percent)*100))))
	}

	volumeNoteObjectId := page.SlideProperties.NotesPage.PageElements[1].ObjectId
	reqs = append(reqs, buildUpdateTextRequests(volumeNoteObjectId, -1, -1, 0, 0, volumeNote.String())...)

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
	resp, _ := u.sheetsService.Spreadsheets.Values.BatchGet(u.revenueId).Ranges("Total!L2:N4", "USDT!I2:K4", "USDT!I6:K8").Do()
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
	resp, _ = u.sheetsService.Spreadsheets.Values.BatchGet(u.revenueId).Ranges("Total!L15:O17", "Total!L19:O21", "USDT!I12:L14", "USDT!I16:L18").Do()
	totalNote := resp.ValueRanges[0].Values
	burningNote := resp.ValueRanges[1].Values
	usdtNote := resp.ValueRanges[2].Values
	otherNote := resp.ValueRanges[3].Values

	noteTemplate :=
		"(TRX amount based revenue)\tCurrent Week\tPercent \tChange  \tLast Week\n" +
			"Total Protocol Revenue\t\t%-12s\t\t%-8s\t%-8s\t%s\n" +
			"Total Energy Revenue\t\t%-12s\t\t%-8s\t%-8s\t%s\n" +
			"Total Burning Revenue\t\t%-12s\t\t%-8s\t%-8s\t%s\n\n" +
			"Energy Burning Revenue\t\t%-12s\t\t%-8s\t%-8s\t%s\n" +
			"Bandwidth Burning Revenue\t%-12s\t\t%-8s\t%-8s\t%s\n" +
			"Other Burning Revenue\t\t%-12s\t\t%-8s\t%-8s\t%s\n\n" +
			"Total Revenue from USDT\t\t%-12s\t\t%-8s\t%-8s\t%s\n" +
			"Burnt Revenue from USDT\t\t%-12s\t\t%-8s\t%-8s\t%s\n" +
			"Staked Revenue from USDT\t%-12s\t\t%-8s\t%-8s\t%s\n\n" +
			"Total Revenue from Other\t\t%-12s\t\t%-8s\t%-8s\t%s\n" +
			"Burnt Revenue from other\t\t%-12s\t\t%-8s\t%-8s\t%s\n" +
			"Staked Revenue from other\t\t%-12s\t\t%-8s\t%-8s\t%s\n\n" +
			"TRX Avg Price: %f"

	revenueNote := fmt.Sprintf(noteTemplate,
		totalNote[0][0], totalNote[0][1], pf(totalNote[0][2].(string)), totalNote[0][3],
		totalNote[1][0], totalNote[1][1], pf(totalNote[1][2].(string)), totalNote[1][3],
		totalNote[2][0], totalNote[2][1], pf(totalNote[2][2].(string)), totalNote[2][3],
		burningNote[0][0], burningNote[0][1], pf(burningNote[0][2].(string)), burningNote[0][3],
		burningNote[1][0], burningNote[1][1], pf(burningNote[1][2].(string)), burningNote[1][3],
		burningNote[2][0], burningNote[2][1], pf(burningNote[2][2].(string)), burningNote[2][3],
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

func (u *Updater) getTop3Exchanges(today time.Time, token string, appendTotal, ascending bool) []string {
	thisWeek := today.AddDate(0, 0, -7)
	thisMarketPairStats := u.db.GetMergedMarketPairStatistics(thisWeek, 7, token, true, true)

	thisSortedMarketPairStats := make([]*models.MarketPairStatistic, 0)
	for _, stat := range thisMarketPairStats {
		thisSortedMarketPairStats = append(thisSortedMarketPairStats, stat)
	}
	sort.Slice(thisSortedMarketPairStats, func(i, j int) bool {
		return thisSortedMarketPairStats[i].Volume > thisSortedMarketPairStats[j].Volume
	})

	topExchanges := make([]string, 0)
	for i := 1; i <= 3 && i < len(thisSortedMarketPairStats); i++ {
		topExchanges = append(topExchanges, thisSortedMarketPairStats[i].ExchangeName)
	}

	if appendTotal {
		topExchanges = append([]string{"Total"}, topExchanges...)
	}

	if ascending {
		slices.Reverse(topExchanges)
	}

	return topExchanges
}

func (u *Updater) getVolumeData(today time.Time, token string, exchanges []string) [][]interface{} {
	result := make([][]interface{}, 0)

	header := make([]interface{}, 0)
	header = append(header, "Date")
	for _, exchange := range exchanges {
		header = append(header, exchange)
	}
	result = append(result, header)

	startDate := today.AddDate(0, 0, -30)
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
	stockData := net.GetStockData(today.AddDate(0, 0, -1), 20)

	// Update Stock sheet
	// stockDataInSheet := make([][]interface{}, 0, len(stockData))
	// for i, row := range stockData {
	// 	sheetRow := make([]interface{}, 0, 6)
	// 	if i%3 == 1 {
	// 		sheetRow = append(sheetRow, row[0].(string)[5:]) // date
	// 	} else {
	// 		sheetRow = append(sheetRow, "") // empty date for other rows
	// 	}
	//
	// 	sheetRow = append(sheetRow, row[1:]...)
	//
	// 	stockDataInSheet = append(stockDataInSheet, sheetRow)
	// }

	_, err := u.sheetsService.Spreadsheets.Values.Update(u.volumeId, "SRM!A2:F45",
		&sheets.ValueRange{
			Values: stockData,
		}).ValueInputOption("USER_ENTERED").Do()
	if err != nil {
		u.logger.Errorf("Unable to update Stock sheet: %v", err)
	}

	reqs := make([]*slides.Request, 0)

	// Update today in the title
	reqs = append(reqs, buildUpdateTitleRequests(page.PageElements[0], today)...)

	todayData := stockData[len(stockData)-1]
	oneWeekAgoData := stockData[len(stockData)-6]

	// Update the stock price
	price := fmt.Sprintf("%.2f", todayData[4])
	// priceChange := common.FormatFloatChangePercent(oneWeekAgoData[4].(float64), todayData[4].(float64))
	priceObjectId := page.PageElements[5].ObjectId
	reqs = append(reqs, buildTextAndChangeRequests(priceObjectId, -1, -1, price, "", 18, 9, true)...)

	thisLowPrice, thisHighPrice := 1e6, 0.0
	lastLowPrice, lastHighPrice := 1e6, 0.0

	for i := 1; i <= 5; i++ {
		thisLowPrice = math.Min(thisLowPrice, stockData[len(stockData)-i][3].(float64))
		thisHighPrice = math.Max(thisHighPrice, stockData[len(stockData)-i][2].(float64))
		lastLowPrice = math.Min(lastLowPrice, stockData[len(stockData)-5-i][3].(float64))
		lastHighPrice = math.Max(lastHighPrice, stockData[len(stockData)-5-i][2].(float64))
	}

	// Move the cursor
	reqs = append(reqs, buildMoveByPercentageRequest(page.PageElements[6], page.PageElements[11], (todayData[4].(float64)-thisLowPrice)/(thisHighPrice-thisLowPrice)))

	// Update the stock low price
	lowPrice := fmt.Sprintf("%.2f", thisLowPrice)
	lowPriceChange := common.FormatFloatChangePercent(lastLowPrice, thisLowPrice)
	lowPriceObjectId := page.PageElements[9].ObjectId
	reqs = append(reqs, buildTextAndChangeRequests(lowPriceObjectId, -1, -1, lowPrice, lowPriceChange, 7, 5, false)...)

	// Update the stock high price
	higPrice := fmt.Sprintf("%.2f", thisHighPrice)
	highPriceChange := common.FormatFloatChangePercent(lastHighPrice, thisHighPrice)
	highPriceObjectId := page.PageElements[10].ObjectId
	reqs = append(reqs, buildTextAndChangeRequests(highPriceObjectId, -1, -1, higPrice, highPriceChange, 7, 5, false)...)

	// Update the avg daily volume
	thisDailyAvgVolume, lastDailyAvgVolume := 0.0, 0.0
	for i := 1; i <= 5; i++ {
		thisAvgPrice := (stockData[len(stockData)-i][3].(float64) + stockData[len(stockData)-i][2].(float64)) / 2
		thisDailyAvgVolume += stockData[len(stockData)-i][5].(float64) * thisAvgPrice
		lastAvgPrice := (stockData[len(stockData)-5-i][3].(float64) + stockData[len(stockData)-5-i][2].(float64)) / 2
		lastDailyAvgVolume += stockData[len(stockData)-5-i][5].(float64) * lastAvgPrice
	}
	thisDailyAvgVolume /= 5.0
	lastDailyAvgVolume /= 5.0

	avgDailyVolume := "$" + common.FormatWithUnits(thisDailyAvgVolume)
	avgDailyVolumeChange := common.FormatFloatChangePercent(lastDailyAvgVolume, thisDailyAvgVolume)
	volumeObjectId := page.PageElements[13].ObjectId
	reqs = append(reqs, buildTextAndChangeRequests(volumeObjectId, -1, -1, avgDailyVolume, avgDailyVolumeChange, 11, 7, true)...)

	// Update the market cap
	marketCap := "$" + common.FormatWithUnits(todayData[4].(float64)*3346.8e4)
	marketCapChange := common.FormatFloatChangePercent(oneWeekAgoData[4].(float64), todayData[4].(float64))
	marketCapObjectId := page.PageElements[15].ObjectId
	reqs = append(reqs, buildTextAndChangeRequests(marketCapObjectId, -1, -1, marketCap, marketCapChange, 11, 7, true)...)

	oneWeekAgo := today.AddDate(0, 0, -7)

	// Update the value of digital assets held
	thisSTRXPrice := u.db.GetTokenPriceByDate("sTRX", today)
	lastSTRXPrice := u.db.GetTokenPriceByDate("sTRX", oneWeekAgo)
	user := "TEySEZLJf6rs2mCujGpDEsgoMVWKLAk9mT"
	sTRX := "TU3kjFuhtEo42tsCBtfYUAZxoqQ4yuSLQ5"
	thisSTRXAmount := common.DropDecimal(common.ConvertDecToBigInt(u.db.GetHoldingsStatistic(today, user, sTRX).Balance), 18)
	lastSTRXAmount := common.DropDecimal(common.ConvertDecToBigInt(u.db.GetHoldingsStatistic(oneWeekAgo, user, sTRX).Balance), 18)
	thisHeldValue := thisSTRXPrice * float64(thisSTRXAmount.Int64())
	lastHeldValue := lastSTRXPrice * float64(lastSTRXAmount.Int64())
	heldAsset := fmt.Sprintf("%s   sTRX   /   $%s", common.FormatWithUnits(float64(thisSTRXAmount.Int64())), common.FormatWithUnits(thisHeldValue))
	heldAssetChange := common.FormatFloatChangePercent(lastHeldValue, thisHeldValue)
	heldAssetObjectId := page.PageElements[17].ObjectId
	reqs = append(reqs, buildTextAndChangeRequests(heldAssetObjectId, -1, -1, heldAsset, heldAssetChange, 11, 7, true)...)

	// Update the value of digital assets held in the Stock sheet
	valueData := make([][]interface{}, 1)
	valueData[0] = make([]interface{}, 2)
	valueData[0][0] = fmt.Sprintf("sTRX (%s)", common.FormatWithUnits(float64(thisSTRXAmount.Int64())))
	valueData[0][1] = thisHeldValue
	_, err = u.sheetsService.Spreadsheets.Values.Update(u.volumeId, "SRM!I2:J2",
		&sheets.ValueRange{
			Values: valueData,
		}).ValueInputOption("USER_ENTERED").Do()
	if err != nil {
		u.logger.Errorf("Unable to update Stock sheet: %v", err)
	}

	// Update held assets distribution chart
	distributionChartId := page.PageElements[20].ObjectId
	reqs = append(reqs, []*slides.Request{
		{
			RefreshSheetsChart: &slides.RefreshSheetsChartRequest{
				ObjectId: distributionChartId,
			},
		},
	}...)

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

	// Update note
	stockDataStr := strings.Builder{}
	stockDataStr.WriteString("date\topen\thigh\tlow\tclose\tvolume(shares)\n")
	for i := 1; i <= 10; i++ {
		if i >= len(stockData) {
			break
		}
		row := stockData[len(stockData)-i]
		stockDataStr.WriteString(fmt.Sprintf("%s\t%.2f\t%.2f\t%.2f\t%.2f\t%s\n",
			row[0], row[1], row[2], row[3], row[4], common.FormatWithUnits(row[5].(float64))))
	}

	note := fmt.Sprintf("[Tron Inc. (TRON) Price]为昨日的收盘价\n"+
		"[Low/High]分别为上周内的最低价与最高价\n"+
		"[Daily Avg Vol]为股票过去五个交易日内日均交易量（按美元计价）\n"+
		"[Market Cap]为股票以昨日的收盘价计算的总市值\n"+
		"[Value of digital assets held]为SRM的关联TRON地址持有的代币的总价值\n"+
		"链上地址目前只持有一种代币为sTRX，共持有%s枚sTRX\n\n"+
		"sunewikeSRM: TFZZx3HXBEGqA1hJnYmRvscjS48gihWXY6\n"+
		"SRMTroninc: TEySEZLJf6rs2mCujGpDEsgoMVWKLAk9mT\n\n%s\n", humanize.Commaf(float64(thisSTRXAmount.Int64())), stockDataStr.String())
	noteObjectId := page.SlideProperties.NotesPage.PageElements[0].ObjectId
	reqs = append(reqs, buildUpdateTextRequests(noteObjectId, -1, -1, 0, 0, note)...)

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

	if changePercent == "" {
		return reqs
	}

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

func buildMoveByPercentageRequest(obj1, obj2 *slides.PageElement, percent float64) *slides.Request {
	x1 := obj1.Transform.TranslateX
	w1 := obj1.Size.Width.Magnitude * obj1.Transform.ScaleX

	targetX := x1 + w1*percent

	xf := obj2.Transform

	return &slides.Request{
		UpdatePageElementTransform: &slides.UpdatePageElementTransformRequest{
			ObjectId:  obj2.ObjectId,
			ApplyMode: "ABSOLUTE",
			Transform: &slides.AffineTransform{
				ScaleX:     xf.ScaleX,
				ScaleY:     xf.ScaleY,
				ShearX:     xf.ShearX,
				ShearY:     xf.ShearY,
				TranslateX: targetX,
				TranslateY: xf.TranslateY,
				Unit:       xf.Unit,
			},
		},
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
