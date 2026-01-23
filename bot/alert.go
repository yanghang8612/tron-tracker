package bot

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	tracker_common "tron-tracker/common"
	"tron-tracker/config"
	"tron-tracker/database"
	"tron-tracker/net"
	"tron-tracker/tron"
	"tron-tracker/tron/types"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
)

const txABI = `[
  {
    "name": "transactions",
    "type": "function",
    "stateMutability": "view",
    "inputs": [{"name":"", "type":"uint256"}],
    "outputs": [
      {"name":"destination", "type":"address"},
      {"name":"value", "type":"uint256"},
      {"name":"data", "type":"bytes"},
      {"name":"executed", "type":"bool"}
    ]
  }
]`

var (
	EthHE  = "0xD00e0079B8CAB524F3fa20EA879a7736E512a5Fc"
	TronHE = "TKVnVyJiTzyCDgTkZRYc5LM4q8B7xXEbh5"

	EthUSDTAddress            = common.HexToAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7")
	EthUSDCAddress            = common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	EthUSDTMultiWalletAddress = common.HexToAddress("0xC6CDE7C39eB2f0F0095F41570af89eFC2C1Ea828")

	TronUSDTAddress            = "a614f803b6fd780986a42c78ec9c7f77e6ded13c"
	TronUSDCAddress            = "3487b63d30b5b2c87fb7ffa8bcfade38eaac1abe"
	TronUSDTMultiWalletAddress = "0fa695d6b065707cb4e0ef73b751c93347682bf2"

	BaseUSDCAddress = common.HexToAddress("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913")

	AddedBlackListTopic = crypto.Keccak256Hash([]byte("AddedBlackList(address)"))
	BlacklistedTopic    = crypto.Keccak256Hash([]byte("Blacklisted(address)"))
	SubmissionTopic     = crypto.Keccak256Hash([]byte("Submission(uint256)"))

	actionsMap = map[string]string{
		"0xf2fde38b": "transferOwnership(address)",
		"0x8456cb59": "pause()",
		"0x3f4ba83a": "unpause()",
		"0x0ecb93c0": "addBlackList(address)",
		"0xe4997dc5": "removeBlackList(address)",
		"0xf3bdc228": "destroyBlackFunds(address)",
		"0x0753c30c": "deprecate(address)",
		"0xcc872b66": "issue(uint256)",
		"0xdb006a75": "redeem(uint256)",
		"0xc0324c77": "setParams(uint256,uint256)",
	}
)

type transaction struct {
	Destination common.Address
	Value       *big.Int
	Data        []byte
	Executed    bool
}

type AlertBot struct {
	*Bot

	ethClient       *ethclient.Client
	lastEthBlockNum uint64

	baseClient       *ethclient.Client
	lastBaseBlockNum uint64

	parsedABI abi.ABI
	tronLogCh chan types.Log
}

func NewAlertBot(cfg *config.BotConfig, db *database.RawDB) *AlertBot {
	ethClient, err := ethclient.Dial("https://mainnet.infura.io/v3/" + cfg.InfuraToken)
	if err != nil {
		panic(err)
	}
	lastEthBlockNum, err := ethClient.BlockNumber(context.Background())
	if err != nil {
		panic(err)
	}

	baseClient, err := ethclient.Dial("https://base-mainnet.infura.io/v3/" + cfg.InfuraToken)
	if err != nil {
		panic(err)
	}
	lastBaseBlockNum, err := baseClient.BlockNumber(context.Background())
	if err != nil {
		panic(err)
	}

	parsedABI, err := abi.JSON(strings.NewReader(txABI))
	if err != nil {
		panic(err)
	}

	alertBot := &AlertBot{
		Bot: NewBot("alert", cfg.AlertBotToken, -1, db, cfg.ValidUsers),

		ethClient:       ethClient,
		lastEthBlockNum: lastEthBlockNum,

		baseClient:       baseClient,
		lastBaseBlockNum: lastBaseBlockNum,

		parsedABI: parsedABI,
		tronLogCh: make(chan types.Log, 1024),
	}

	return alertBot
}

func (ab *AlertBot) Start() {
	ab.logger.Infof("Started alert bot")

	go func() {
		for {
			select {
			case vLog := <-ab.tronLogCh:
				if AddedBlackListTopic.Cmp(common.HexToHash(vLog.Topics[0])) == 0 {
					usr := tracker_common.EncodeToBase58(common.HexToAddress(vLog.Topics[1]).Hex()[2:])
					ab.logger.Infof("Detected AddedBlackList event on TRON for address: %s, tx_hash: %s", usr, vLog.ID)

					slackMsg := fmt.Sprintf("Found `TRON` - :usdtlogo: blacklisted address: `%s`, %s", usr, formatTronTxUrl(vLog.ID))
					net.ReportNotificationToSlack(slackMsg, usr == TronHE)
				} else if BlacklistedTopic.Cmp(common.HexToHash(vLog.Topics[0])) == 0 {
					usr := tracker_common.EncodeToBase58(common.HexToAddress(vLog.Topics[1]).Hex()[2:])
					ab.logger.Infof("Detected Blacklisted event on TRON for address: %s, tx_hash: %s", usr, vLog.ID)

					slackMsg := fmt.Sprintf("Found `TRON` - :usdclogo: blacklisted address: `%s`, %s", usr, formatTronTxUrl(vLog.ID))
					net.ReportNotificationToSlack(slackMsg, usr == TronHE)
				} else {
					txID := new(big.Int).SetBytes(common.HexToHash(vLog.Topics[1]).Bytes()).Uint64()
					ab.logger.Infof("Detected Submission event on TRON for txID: %d, tx_hash: %s", txID, vLog.ID)

					// trigger constant
					originData, err := net.Trigger("TBPxhVAsuzoFnKyXtc1o2UySEydPHgATto",
						"transactions(uint256)", vLog.Topics[1])
					if err != nil {
					}

					var tx transaction
					if err = ab.parsedABI.UnpackIntoInterface(&tx, "transactions", hexutil.MustDecode("0x"+originData)); err != nil {
						ab.logger.Errorf("Failed to unpack ABI: %v", err)
					}

					txData := hexutil.Encode(tx.Data)
					action := "unknown"
					usr := ""
					if len(txData) >= 10 {
						if act, ok := actionsMap[txData[:10]]; ok {
							action = act
							if action == "addBlackList(address)" {
								usr = tracker_common.EncodeToBase58(common.HexToAddress("0x" + txData[10+24:10+64]).Hex()[2:])
							}
						}
					}
					slackMsg := fmt.Sprintf("Found `TRON` - :usdtlogo: multi-sig submission: %s\n"+
						"> TxId: `%d`\n"+
						"> Destination: %s\n"+
						"> Value: %s\n"+
						"> Data: %s\n"+
						"> Action: `%s`\n",
						formatEthTxUrl(vLog.ID), txID, tx.Destination.Hex(), tx.Value.String(), txData, action)
					net.ReportNotificationToSlack(slackMsg, usr == TronHE)
				}
			}
		}
	}()
}

func (ab *AlertBot) RegisterFilters(tracker *tron.Tracker) {
	tracker.AddFilter(TronUSDTAddress, AddedBlackListTopic.Hex(), ab.tronLogCh)
	tracker.AddFilter(TronUSDCAddress, BlacklistedTopic.Hex(), ab.tronLogCh)
	tracker.AddFilter(TronUSDTMultiWalletAddress, SubmissionTopic.Hex(), ab.tronLogCh)
}

func (ab *AlertBot) GetFilterLogs() {
	latestEthBlockNum, err := ab.ethClient.BlockNumber(context.Background())
	if err != nil {
		ab.logger.Errorf("Failed to get latest eth block number: %v", err)
		return
	}

	ethQ := ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(ab.lastEthBlockNum)),
		ToBlock:   big.NewInt(int64(latestEthBlockNum)),
		Addresses: []common.Address{
			EthUSDTAddress,
			EthUSDCAddress,
			EthUSDTMultiWalletAddress,
		},
		Topics: [][]common.Hash{
			{
				AddedBlackListTopic,
				BlacklistedTopic,
				SubmissionTopic,
			},
		},
	}

	logs, err := ab.ethClient.FilterLogs(context.Background(), ethQ)
	if err != nil {
		ab.logger.Errorf("Failed to filter logs: %v", err)
		return
	}

	for _, vLog := range logs {
		if vLog.Topics[0] == AddedBlackListTopic {
			usr := common.BytesToAddress(vLog.Data).Hex()
			ab.logger.Infof("Detected AddedBlackList event on ETH for address: %s, tx_hash: %s", usr, vLog.TxHash.Hex())

			slackMsg := fmt.Sprintf("Found `ETH` - :usdtlogo: blacklisted address: `%s`, %s", usr, formatEthTxUrl(vLog.TxHash.Hex()))
			net.ReportNotificationToSlack(slackMsg, usr == EthHE)
		} else if vLog.Topics[0] == BlacklistedTopic {
			usr := common.HexToAddress(vLog.Topics[1].Hex()).Hex()
			ab.logger.Infof("Detected Blacklisted event on ETH for address: %s, tx_hash: %s", usr, vLog.TxHash.Hex())

			slackMsg := fmt.Sprintf("Found `ETH` - :usdclogo: blacklisted address: `%s`, %s", usr, formatEthTxUrl(vLog.TxHash.Hex()))
			net.ReportNotificationToSlack(slackMsg, usr == EthHE)
		} else {
			txID := new(big.Int).SetBytes(vLog.Topics[1].Bytes()).Uint64()
			ab.logger.Infof("Detected Submission event on ETH for txID: %d, tx_hash: %s", txID, vLog.TxHash.Hex())

			var (
				data     []byte
				outBytes []byte
				tx       transaction
			)

			// calldata
			data, err = ab.parsedABI.Pack("transactions", new(big.Int).SetUint64(txID))
			if err != nil {
				ab.logger.Errorf("Failed to pack ABI: %v", err)
			}

			// eth_call
			outBytes, err = ab.ethClient.CallContract(context.Background(), ethereum.CallMsg{
				To:   &EthUSDTMultiWalletAddress,
				Data: data,
			}, nil)
			if err != nil {
				ab.logger.Errorf("Failed to call contract: %v", err)
				continue
			}

			// 解码返回值
			if err = ab.parsedABI.UnpackIntoInterface(&tx, "transactions", outBytes); err != nil {
				ab.logger.Errorf("Failed to unpack ABI: %v", err)
			}

			txData := hexutil.Encode(tx.Data)
			action := "unknown"
			if len(txData) >= 10 {
				if act, ok := actionsMap[txData[:10]]; ok {
					action = act
				}
			}
			slackMsg := fmt.Sprintf("Found `ETH` - :usdtlogo: multi-sig submission: %s\n"+
				"> TxId: `%d`\n"+
				"> Destination: %s\n"+
				"> Value: %s\n"+
				"> Data: %s\n"+
				"> Action: `%s`\n",
				formatEthTxUrl(vLog.TxHash.Hex()), txID, tx.Destination.Hex(), tx.Value.String(), txData, action)
			net.ReportNotificationToSlack(slackMsg, strings.Contains(txData, strings.ToLower(EthHE)[2:]))
		}
	}

	ab.lastEthBlockNum = latestEthBlockNum + 1

	ab.logger.Infof("Success fetch ETH logs from block %d to %d, found %d logs",
		ethQ.FromBlock.Uint64(), ethQ.ToBlock.Uint64(), len(logs))

	latestBaseBlockNum, err := ab.baseClient.BlockNumber(context.Background())
	if err != nil {
		ab.logger.Errorf("Failed to get latest base block number: %v", err)
		return
	}

	baseQ := ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(ab.lastBaseBlockNum)),
		ToBlock:   big.NewInt(int64(latestBaseBlockNum)),
		Addresses: []common.Address{
			BaseUSDCAddress,
		},
		Topics: [][]common.Hash{
			{
				BlacklistedTopic,
			},
		},
	}

	logs, err = ab.baseClient.FilterLogs(context.Background(), baseQ)
	if err != nil {
		ab.logger.Errorf("Failed to filter logs: %v", err)
		return
	}

	for _, vLog := range logs {
		usr := common.HexToAddress(vLog.Topics[1].Hex()).Hex()
		ab.logger.Infof("Detected Blacklisted event on Base for address: %s, tx_hash: %s", usr, vLog.TxHash.Hex())

		slackMsg := fmt.Sprintf("Found `Base` - :usdclogo: blacklisted address: `%s`, %s", usr, formatBaseTxUrl(vLog.TxHash.Hex()))
		net.ReportNotificationToSlack(slackMsg, usr == EthHE)
	}

	ab.lastBaseBlockNum = latestBaseBlockNum + 1

	ab.logger.Infof("Success fetch Base logs from block %d to %d, found %d logs",
		baseQ.FromBlock.Uint64(), baseQ.ToBlock.Uint64(), len(logs))
}

func formatEthTxUrl(txHash string) string {
	return fmt.Sprintf(":clippy:<https://etherscan.io/tx/%s|TxHash>", txHash)
}

func formatTronTxUrl(txHash string) string {
	return fmt.Sprintf(":clippy:<https://tronscan.io/#/transaction/%s|TxHash>", txHash)
}

func formatBaseTxUrl(txHash string) string {
	return fmt.Sprintf(":clippy:<https://basescan.io/tx/%s|TxHash>", txHash)
}
