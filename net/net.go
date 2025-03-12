package net

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/go-resty/resty/v2"
	"go.uber.org/zap"
	"tron-tracker/database/models"
	"tron-tracker/types"
	"tron-tracker/utils"
)

type Config struct {
	FullNode     string `toml:"full_node"`
	SlackWebhook string `toml:"slack_webhook"`
	CMCApiKey    string `toml:"cmc_api_key"`
}

type slackMessage struct {
	Text string `json:"text"`
}

const (
	GetBlockPath               = "/wallet/getblockbynum?num="
	GetNowBlockPath            = "/wallet/getnowblock"
	GetTransactionInfoListPath = "/wallet/gettransactioninfobyblocknum?num="
)

var (
	client = resty.New()
	config *Config
)

func Init(cfg *Config) {
	config = cfg
}

func ReportToSlack(msg string) {
	resp, err := client.R().SetBody(&slackMessage{Text: msg}).Post(config.SlackWebhook)
	if err != nil {
		zap.S().Error(err)
		return
	}

	zap.S().Infof("Report to slack: %s", resp)
}

func GetNowBlock() (*types.Block, error) {
	url := config.FullNode + GetNowBlockPath
	var block types.Block
	_, err := client.R().SetResult(&block).Get(url)
	return &block, err
}

func GetBlockByHeight(height uint) (*types.Block, error) {
	url := config.FullNode + GetBlockPath + strconv.FormatInt(int64(height), 10)
	var block types.Block
	_, err := client.R().SetResult(&block).Get(url)
	return &block, err
}

func GetTransactionInfoList(height uint) ([]*types.TransactionInfo, error) {
	url := config.FullNode + GetTransactionInfoListPath + strconv.FormatInt(int64(height), 10)
	var txInfoList = make([]*types.TransactionInfo, 0)
	_, err := client.R().SetResult(&txInfoList).Get(url)
	return txInfoList, err
}

func GetExchanges() *types.ExchangeList {
	var exchangeList = types.ExchangeList{}
	resp, err := client.R().Get("https://apilist.tronscanapi.com/api/hot/exchanges")
	if err != nil {
		zap.S().Error(err)
	} else {
		err = json.Unmarshal(resp.Body(), &exchangeList)
		if err != nil {
			zap.S().Error(err)
		}
	}

	for i := range exchangeList.Exchanges {
		exchangeList.Exchanges[i].Name = utils.TrimExchangeName(exchangeList.Exchanges[i].Name)

		if exchangeList.Exchanges[i].Name == "BtcTurk" {
			exchangeList.Exchanges[i].Address = "TCTYyc1w6rzqnqRBcAhuAJUyNWZ9Bw9hrW"
		}
	}
	return &exchangeList
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

func GetMarketPairs(token string) (string, []*models.MarketPairStatistic, error) {
	resp, err := client.R().Get(fmt.Sprintf("https://api.coinmarketcap.com/data-api/v3/cryptocurrency/market-pairs/latest?slug=%s&start=1&limit=1000&category=spot&centerType=all&sort=cmc_rank_advanced&direction=desc&spotUntracked=true", token))
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
		SetHeader("X-CMC_PRO_API_KEY", config.CMCApiKey).
		Get("https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest")
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
