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
	"go.uber.org/zap"
	"tron-tracker/types"
	"tron-tracker/utils"
)

const (
	BaseUrl                    = "http://localhost:8088/"
	ETHJsonRpcUrl              = "http://localhost:8545/"
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
	ethClient, err = ethclient.Dial(ETHJsonRpcUrl)
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

func EthGetBlockByNumber(blockNumber uint64) (*ethtypes.Block, error) {
	return ethClient.BlockByNumber(context.Background(), new(big.Int).SetUint64(blockNumber))
}

func EthGetHeaderByNumber(blockNumber uint64) (*ethtypes.Header, error) {
	return ethClient.HeaderByNumber(context.Background(), new(big.Int).SetUint64(blockNumber))
}

func EthGetLogs(fromBlock, toBlock uint64, address common.Address, topics [][]common.Hash) ([]ethtypes.Log, error) {
	return ethClient.FilterLogs(context.Background(), ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(fromBlock),
		ToBlock:   new(big.Int).SetUint64(toBlock),
		Addresses: []common.Address{address},
		Topics:    topics,
	})
}
