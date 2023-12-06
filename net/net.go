package net

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"tron-tracker/types"
)

const (
	BaseUrl                    = "https://api.trongrid.io/"
	GetBlockPath               = "wallet/getblockbynum?num="
	GetNowBlockPath            = "wallet/getnowblock"
	GetTransactionInfoListPath = "wallet/gettransactioninfobyblocknum?num="
)

func Get(url string) []byte {
	resp, err := http.DefaultClient.Get(url)
	if err == nil && resp.StatusCode == 200 {
		defer resp.Body.Close()
		if body, err := io.ReadAll(resp.Body); err == nil {
			return body
		}
	}
	return nil
}

func HighGet(url string, res interface{}) error {
	rspData := Get(url)
	err := json.Unmarshal(rspData, res)
	if err != nil {
		return err
	}
	return nil
}

func GetNowBlocks() (*types.Block, error) {
	url := BaseUrl + GetNowBlockPath
	var block types.Block
	return &block, HighGet(url, &block)
}

func GetBlockByHeight(height uint) (*types.Block, error) {
	url := BaseUrl + GetBlockPath + strconv.FormatInt(int64(height), 10)
	var block types.Block
	return &block, HighGet(url, &block)
}

func GetTransactionInfoList(height uint) ([]*types.TransactionInfo, error) {
	url := BaseUrl + GetTransactionInfoListPath + strconv.FormatInt(int64(height), 10)
	var transactionInfoList = make([]*types.TransactionInfo, 0)
	return transactionInfoList, HighGet(url, &transactionInfoList)
}
