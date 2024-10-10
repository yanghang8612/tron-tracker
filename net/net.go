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
	SlackWebhook string `toml:"slack_webhook"`
}

type slackMessage struct {
	Text string `json:"text"`
}

const (
	BaseUrl                    = "http://localhost:8088/"
	GetBlockPath               = "wallet/getblockbynum?num="
	GetNowBlockPath            = "wallet/getnowblock"
	GetTransactionInfoListPath = "wallet/gettransactioninfobyblocknum?num="
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
	url := BaseUrl + GetNowBlockPath
	var block types.Block
	_, err := client.R().SetResult(&block).Get(url)
	return &block, err
}

func GetBlockByHeight(height uint) (*types.Block, error) {
	url := BaseUrl + GetBlockPath + strconv.FormatInt(int64(height), 10)
	var block types.Block
	_, err := client.R().SetResult(&block).Get(url)
	return &block, err
}

func GetTransactionInfoList(height uint) ([]*types.TransactionInfo, error) {
	url := BaseUrl + GetTransactionInfoListPath + strconv.FormatInt(int64(height), 10)
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
			Volume:              marketPair.VolumeUsd,
			Percent:             marketPair.VolumePercent,
			DepthUsdPositiveTwo: marketPair.DepthUsdPositiveTwo,
			DepthUsdNegativeTwo: marketPair.DepthUsdNegativeTwo,
		})
	}

	return string(resp.Body()), marketPairs, nil
}
