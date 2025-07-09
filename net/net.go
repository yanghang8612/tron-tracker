package net

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/go-resty/resty/v2"
	"go.uber.org/zap"
	"tron-tracker/config"
	"tron-tracker/database/models"
	"tron-tracker/tron/types"
)

type slackMessage struct {
	Text string `json:"text"`
}

const (
	GetBlockPath               = "/wallet/getblockbynum?num="
	GetNowBlockPath            = "/wallet/getnowblock"
	GetTransactionInfoListPath = "/wallet/gettransactioninfobyblocknum?num="
)

var (
	client  = resty.New()
	configs *config.NetConfig
)

func Init(cfg *config.NetConfig) {
	configs = cfg
}

func ReportToSlack(msg string) {
	resp, err := client.R().SetBody(&slackMessage{Text: msg}).Post(configs.SlackWebhook)
	if err != nil {
		zap.S().Error(err)
		return
	}

	zap.S().Infof("Report to slack: %s", resp)
}

func GetNowBlock() (*types.Block, error) {
	url := configs.FullNode + GetNowBlockPath
	var block types.Block
	_, err := client.R().SetResult(&block).Get(url)
	return &block, err
}

func GetNowBlockForUSDT() (*types.Block, error) {
	url := "http://127.0.0.1:9088" + GetNowBlockPath
	var block types.Block
	_, err := client.R().SetResult(&block).Get(url)
	return &block, err
}

func GetBlockByHeight(height uint) (*types.Block, error) {
	url := configs.FullNode + GetBlockPath + strconv.FormatInt(int64(height), 10)
	var block types.Block
	_, err := client.R().SetResult(&block).Get(url)
	return &block, err
}

func GetTransactionInfoList(height uint) ([]*types.TransactionInfo, error) {
	url := configs.FullNode + GetTransactionInfoListPath + strconv.FormatInt(int64(height), 10)
	var txInfoList = make([]*types.TransactionInfo, 0)
	_, err := client.R().SetResult(&txInfoList).Get(url)
	return txInfoList, err
}

func GetTransactionInfoListForUSDT(height uint) ([]*types.TransactionInfo, error) {
	url := "http://127.0.0.1:9088" + GetTransactionInfoListPath + strconv.FormatInt(int64(height), 10)
	var txInfoList = make([]*types.TransactionInfo, 0)
	_, err := client.R().SetResult(&txInfoList).Get(url)
	return txInfoList, err
}

func GetExchanges() *models.Exchanges {
	var exchanges = models.Exchanges{}
	resp, err := client.R().Get("https://apilist.tronscanapi.com/api/hot/exchanges")
	if err != nil {
		zap.S().Error(err)
	} else {
		err = json.Unmarshal(resp.Body(), &exchanges)
		if err != nil {
			zap.S().Error(err)
		}
	}

	for i := range exchanges.Val {
		// exchangeList.Exchanges[i].Name = common.TrimExchangeName(exchangeList.Exchanges[i].Name)

		if exchanges.Val[i].Name == "BtcTurk" {
			exchanges.Val[i].Address = "TCTYyc1w6rzqnqRBcAhuAJUyNWZ9Bw9hrW"
		}
	}
	return &exchanges
}

type MarketPairsResponse struct {
	Data struct {
		MarketPairs []struct {
			Rank                int     `json:"rank"`
			ExchangeName        string  `json:"exchangeName"`
			MarketPair          string  `json:"marketPair"`
			MarketReputation    float64 `json:"marketReputation"`
			Price               float64 `json:"price"`
			VolumeUsd           float64 `json:"volumeUsd"`
			EffectiveLiquidity  float64 `json:"effectiveLiquidity"`
			LastUpdated         string  `json:"lastUpdated"`
			Quote               float64 `json:"quote"`
			VolumeBase          float64 `json:"volumeBase"`
			VolumeQuote         float64 `json:"volumeQuote"`
			VolumePercent       float64 `json:"volumePercent"`
			DepthUsdPositiveTwo float64 `json:"depthUsdPositiveTwo"`
			DepthUsdNegativeTwo float64 `json:"depthUsdNegativeTwo"`
		} `json:"marketPairs"`
	} `json:"data"`
}

func GetMarketPairs(token, slug string) (string, []*models.MarketPairStatistic, error) {
	resp, err := client.R().Get(fmt.Sprintf("https://api.coinmarketcap.com/data-api/v3/cryptocurrency/market-pairs/latest?slug=%s&start=1&limit=1000&category=spot&centerType=all&sort=cmc_rank_advanced&direction=desc&spotUntracked=true", slug))
	if err != nil {
		return "", nil, err
	}

	var response MarketPairsResponse
	err = json.Unmarshal(resp.Body(), &response)
	if err != nil {
		return "", nil, err
	}

	var marketPairs = make([]*models.MarketPairStatistic, 0)

	for _, marketPair := range response.Data.MarketPairs {
		marketPairs = append(marketPairs, &models.MarketPairStatistic{
			Datetime:            time.Now().Format("021504"),
			Token:               token,
			ExchangeName:        marketPair.ExchangeName,
			Pair:                marketPair.MarketPair,
			Reputation:          marketPair.MarketReputation,
			Price:               marketPair.Price,
			Volume:              marketPair.VolumeUsd,
			Percent:             marketPair.VolumePercent,
			DepthUsdPositiveTwo: marketPair.DepthUsdPositiveTwo,
			DepthUsdNegativeTwo: marketPair.DepthUsdNegativeTwo,
		})
	}

	return string(resp.Body()), marketPairs, nil
}

type TokenListingsResponse struct {
	Data []Coin `json:"data"`
}

type Coin struct {
	ID     uint   `json:"id"`
	Symbol string `json:"symbol"`
	Quote  struct {
		USD struct {
			Price           float64 `json:"price"`
			Volume24h       float64 `json:"volume_24h"`
			VolumeChange24h float64 `json:"volume_change_24h"`
			MarketCap       float64 `json:"market_cap"`
		} `json:"USD"`
	} `json:"quote"`
}

func GetTokenListings() (string, []*models.TokenListingStatistic, error) {
	resp, err := client.R().
		SetHeader("X-CMC_PRO_API_KEY", configs.CMCApiKey).
		Get("https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?limit=1000")
	if err != nil {
		return "", nil, err
	}

	var response TokenListingsResponse
	err = json.Unmarshal(resp.Body(), &response)
	if err != nil {
		return "", nil, err
	}

	var tokenListings = make([]*models.TokenListingStatistic, 0)
	for _, tokenListing := range response.Data {
		tokenListings = append(tokenListings, &models.TokenListingStatistic{
			Datetime:        time.Now().Format("021504"),
			CMCID:           tokenListing.ID,
			Token:           tokenListing.Symbol,
			Price:           tokenListing.Quote.USD.Price,
			Volume24H:       tokenListing.Quote.USD.Volume24h,
			VolumeChange24H: tokenListing.Quote.USD.VolumeChange24h,
			MarketCap:       tokenListing.Quote.USD.MarketCap,
		})
	}

	return string(resp.Body()), tokenListings, nil
}

func GetFees(startDate time.Time, days int) map[string]float64 {
	resp, err := client.R().Get(fmt.Sprintf("%s/transfer_fee?start_date=%s&days=%d",
		configs.FeeNode, startDate.Format("060102"), days))

	if err != nil {
		zap.S().Error(err)
		return nil
	}

	var fees = make(map[string]float64)
	err = json.Unmarshal(resp.Body(), &fees)
	if err != nil {
		zap.S().Error(err)
		return nil
	}

	return fees
}

type USDTSupplyResponse struct {
	DataFormatted []struct {
		ID          uint        `json:"id"`
		ISO         string      `json:"iso"`
		Name        string      `json:"name"`
		BlockChains []ChainData `json:"blockChains"`
	} `json:"data_formatted"`
}

type ChainData struct {
	TotalAuthorized number `json:"totalAuthorized"`
	NotIssued       number `json:"notIssued"`
	Quarantined     number `json:"quarantined"`
	Name            string `json:"name"`
}

// Number is a custom type that can handle JSON values
// which are either numeric (e.g. 123, 45.67) or string-formatted numbers (e.g. "123", "45.67").
// Underlying storage is float64.
type number float64

// UnmarshalJSON implements the json.Unmarshaler interface.
// It tries to parse the raw data first as a JSON number; if that fails,
// it tries to parse it as a JSON string and then converts the string to float64.
func (n *number) UnmarshalJSON(data []byte) error {
	// Attempt to unmarshal as a JSON number.
	var num float64
	if err := json.Unmarshal(data, &num); err == nil {
		*n = number(num)
		return nil
	}

	// If parsing as number failed, attempt to unmarshal as a JSON string.
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return fmt.Errorf("Number.UnmarshalJSON: unable to parse %s as number or string", string(data))
	}

	// Convert the string to float64.
	parsed, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return fmt.Errorf("Number.UnmarshalJSON: failed to convert string %q to float64: %w", str, err)
	}
	*n = number(parsed)
	return nil
}

func GetUSDTSupply() ([]*models.USDTSupplyStatistic, error) {
	resp, err := client.R().Get("https://app.tether.to/transparency.json")
	if err != nil {
		return nil, err
	}

	var response USDTSupplyResponse
	err = json.Unmarshal(resp.Body(), &response)
	if err != nil {
		return nil, err
	}

	var USDTSupply = make([]*models.USDTSupplyStatistic, 0)
	for _, chainData := range response.DataFormatted {
		if chainData.ISO == "usdt" {
			for _, data := range chainData.BlockChains {
				USDTSupply = append(USDTSupply, &models.USDTSupplyStatistic{
					Datetime:        time.Now().Format("06010215"),
					Chain:           data.Name,
					TotalAuthorized: float64(data.TotalAuthorized),
					NotIssued:       float64(data.NotIssued),
					Quarantined:     float64(data.Quarantined),
				})
			}
		}
	}

	return USDTSupply, nil
}
