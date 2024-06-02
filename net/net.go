package net

import (
	"context"
	"encoding/json"
	"math/big"
	"strconv"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/go-resty/resty/v2"
	jsoniter "github.com/json-iterator/go"
	"go.uber.org/zap"
	"tron-tracker/types"
	"tron-tracker/utils"
)

const (
	BaseUrl                    = "http://localhost:8088/"
	EthJsonRpcEndpoint         = "http://localhost:8545/"
	EthIPCEndpoint             = "/data/ethereum/execution/data/geth.ipc"
	EtherScan                  = "https://api.etherscan.io/"
	GetBlockPath               = "wallet/getblockbynum?num="
	GetNowBlockPath            = "wallet/getnowblock"
	GetTransactionInfoListPath = "wallet/gettransactioninfobyblocknum?num="
)

var (
	client    = resty.New()
	ethClient *ethclient.Client
)

func init() {
	var err error
	ethClient, err = ethclient.Dial(EthIPCEndpoint)
	if err != nil {
		zap.S().Error(err)
	}
}

func GetNowBlock() (*types.Block, error) {
	url := BaseUrl + GetNowBlockPath
	var block types.Block
	_, err := client.R().SetResult(&block).Get(url)
	return &block, err
}

func GetBlockByHeight(height uint64) (*types.Block, error) {
	url := BaseUrl + GetBlockPath + strconv.FormatUint(height, 10)
	var block types.Block
	_, err := client.R().SetResult(&block).Get(url)
	return &block, err
}

func GetTransactionInfoList(height uint64) ([]*types.TransactionInfo, error) {
	url := BaseUrl + GetTransactionInfoListPath + strconv.FormatUint(height, 10)
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
	}
	return &exchangeList
}

func EthBlockNumber() (uint64, error) {
	return ethClient.BlockNumber(context.Background())
}

func EthBlockNumberByTime(timestamp int64) (uint64, error) {
	resp, err := client.R().Get(EtherScan +
		"api?module=block&action=getblocknobytime&closest=after&timestamp=" +
		strconv.FormatInt(timestamp, 10) + "&apikey=82SMH9HIUESXN4IPSFA237VHIMHQB1AQSI")

	if err != nil {
		return 0, err
	} else {
		var respStruct struct {
			Status  string `json:"status"`
			Message string `json:"message"`
			Result  string `json:"result"`
		}

		err = json.Unmarshal(resp.Body(), &respStruct)
		if err != nil {
			return 0, err
		}

		if respStruct.Status == "1" {
			blockNumber, err := strconv.ParseUint(respStruct.Result, 10, 64)
			if err != nil {
				return 0, err
			}
			return blockNumber, nil
		} else {
			return 0, ethereum.NotFound
		}
	}
}

func EthGetBlockByNumber(blockNumber uint64) (*ethtypes.Block, error) {
	return ethClient.BlockByNumber(context.Background(), new(big.Int).SetUint64(blockNumber))
}

func EthGetHeaderByNumber(blockNumber uint64) (*ethtypes.Header, error) {
	return ethClient.HeaderByNumber(context.Background(), new(big.Int).SetUint64(blockNumber))
}

func EthGetLogs(fromBlock, toBlock uint64, address common.Address, topics [][]common.Hash) ([]ethtypes.Log, error) {
	payload := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_getLogs",
		"params": []interface{}{
			map[string]interface{}{
				"fromBlock": "0x" + strconv.FormatUint(fromBlock, 16),
				"toBlock":   "0x" + strconv.FormatUint(toBlock, 16),
				"address":   address,
				"topics":    topics,
			},
		},
		"id": 1,
	}

	resp, err := client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(payload).Post(EthJsonRpcEndpoint)

	if err != nil {
		return nil, err
	}

	var respStruct struct {
		Result []ethtypes.Log `json:"result"`
	}

	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	err = json.Unmarshal(resp.Body(), &respStruct)
	if err != nil {
		return nil, err
	}

	return respStruct.Result, nil
}
