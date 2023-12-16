package net

import (
	"encoding/json"
	"strconv"

	"github.com/go-resty/resty/v2"
	"go.uber.org/zap"
	"tron-tracker/types"
)

const (
	BaseUrl                    = "http://localhost:8088/"
	GetBlockPath               = "wallet/getblockbynum?num="
	GetNowBlockPath            = "wallet/getnowblock"
	GetTransactionInfoListPath = "wallet/gettransactioninfobyblocknum?num="
)

var client = resty.New()

func GetNowBlocks() (*types.Block, error) {
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
	var exchanges = types.ExchangeList{}
	resp, err := client.R().Get("https://apilist.tronscanapi.com/api/hot/exchanges")
	if err != nil {
		zap.S().Panic(err)
	} else {
		err = json.Unmarshal(resp.Body(), &exchanges)
		if err != nil {
			zap.S().Panic(err)
		}
	}
	return &exchanges
}
