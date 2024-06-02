package main

import (
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"go.uber.org/zap"
	"tron-tracker/database"
	"tron-tracker/database/models"
	"tron-tracker/net"
	"tron-tracker/types"
	"tron-tracker/utils"
)

type Tracker struct {
	db *database.RawDB

	isCatching  bool
	reporter    *utils.Reporter
	ethReporter *utils.Reporter

	loopWG sync.WaitGroup
	quitCh chan struct{}

	logger *zap.SugaredLogger
}

func New(db *database.RawDB) *Tracker {
	return &Tracker{
		db: db,

		isCatching: true,
		reporter: utils.NewReporter(1000, 60*time.Second, 0, func(rs utils.ReporterState) string {
			return fmt.Sprintf("Tracked [%d] TRON blocks in [%.2fs], speed [%.2fblks/s]", rs.CountInc, rs.ElapsedTime, float64(rs.CountInc)/rs.ElapsedTime)
		}),
		ethReporter: utils.NewReporter(10000, 60*time.Second, 0, func(rs utils.ReporterState) string {
			return fmt.Sprintf("Tracked [%d] ETH blocks in [%.2fs], speed [%.2fblks/s]", rs.CountInc, rs.ElapsedTime, float64(rs.CountInc)/rs.ElapsedTime)
		}),

		quitCh: make(chan struct{}),

		logger: zap.S().Named("[tracker]"),
	}
}

func (t *Tracker) Start() {
	t.db.Start()

	t.loopWG.Add(1)
	go t.loop()

	t.logger.Info("Tracker started")
}

func (t *Tracker) Stop() {
	close(t.quitCh)
	t.loopWG.Wait()
}

func (t *Tracker) Report() {
	if !t.isCatching {
		t.logger.Infof("Status report, latest tracked block [%d](%s)",
			t.db.GetLastTrackedBlockNum(),
			time.Unix(t.db.GetLastTrackedBlockTime(), 0).Format("2006-01-02 15:04:05"))
	}
}

func (t *Tracker) loop() {
	for {
		select {
		case <-t.quitCh:
			t.logger.Info("Tracker quit, start closing rawdb")
			t.db.Close()
			t.logger.Info("Tracker quit, rawdb closed")
			t.loopWG.Done()
			return
		default:
			t.doTrackBlock()
			t.doTrackEthUSDT()
		}
	}
}

func (t *Tracker) doTrackBlock() {
	block, _ := net.GetBlockByHeight(t.db.GetLastTrackedBlockNum() + 1)
	block.BlockHeader.RawData.Timestamp /= 1000

	if block.BlockHeader.RawData.Number == 0 {
		time.Sleep(1 * time.Second)
		return
	}

	if time.Now().Unix()-block.BlockHeader.RawData.Timestamp < 10 {
		// Catch up with the latest block
		if t.isCatching {
			t.isCatching = false
			t.logger.Infof("Catching up with the latest block [%d]", block.BlockHeader.RawData.Number)
		}
	} else if shouldReport, reportContent := t.reporter.Add(1); shouldReport {
		nowBlock, _ := net.GetNowBlock()
		nowBlockNumber := nowBlock.BlockHeader.RawData.Number
		trackedBlockNumber := block.BlockHeader.RawData.Number
		t.logger.Infof("%s, tracking progress [%d] => [%d], left blocks [%d]", reportContent, trackedBlockNumber, nowBlockNumber, nowBlockNumber-trackedBlockNumber)
	}

	txInfoList, err := net.GetTransactionInfoList(t.db.GetLastTrackedBlockNum() + 1)
	// This happens when fetching the block from FullNode succeeds,
	// but fetching the info of the transactions in the block fails.
	if err != nil {
		t.logger.Error(err)
		return
	}

	transactions := make([]*models.Transaction, 0)
	t.db.SetLastTrackedBlock(block)
	for idx, tx := range block.Transactions {
		var txToDB = &models.Transaction{
			Hash:      tx.TxID,
			OwnerAddr: utils.EncodeToBase58(tx.RawData.Contract[0].Parameter.Value["owner_address"].(string)),
			Height:    block.BlockHeader.RawData.Number,
			Timestamp: block.BlockHeader.RawData.Timestamp,
			Type:      types.ConvertType(tx.RawData.Contract[0].Type),
			Fee:       txInfoList[idx].Fee,
		}

		txToDB.NetUsage = txInfoList[idx].Receipt.NetUsage
		txToDB.NetFee = txInfoList[idx].Receipt.NetFee
		txToDB.Result = txInfoList[idx].Receipt.Result
		txToDB.SigCount = uint8(len(tx.Signature))
		if txToDB.Type == 1 {
			txToDB.Name = "TRX"
			txToDB.FromAddr = txToDB.OwnerAddr
			txToDB.ToAddr = utils.EncodeToBase58(tx.RawData.Contract[0].Parameter.Value["to_address"].(string))

			amount := int64(tx.RawData.Contract[0].Parameter.Value["amount"].(float64))
			txToDB.SetAmount(amount)
			// The TRX charger should charge at least 1 TRX
			if amount > 1_000_000 {
				// t.db.SaveCharger(txToDB.FromAddr, txToDB.ToAddr, txToDB.Name)
			}
		} else if txToDB.Type == 2 {
			name, _ := hex.DecodeString(tx.RawData.Contract[0].Parameter.Value["asset_name"].(string))
			txToDB.Name = string(name)
			txToDB.FromAddr = txToDB.OwnerAddr
			txToDB.ToAddr = utils.EncodeToBase58(tx.RawData.Contract[0].Parameter.Value["to_address"].(string))
			txToDB.SetAmount(int64(tx.RawData.Contract[0].Parameter.Value["amount"].(float64)))
		} else if txToDB.Type == 12 {
			txToDB.SetAmount(txInfoList[idx].UnfreezeAmount)
		} else if txToDB.Type == 13 {
			txToDB.SetAmount(txInfoList[idx].WithdrawAmount)
		} else if txToDB.Type == 30 || txToDB.Type == 31 {
			txToDB.Name = utils.EncodeToBase58(txInfoList[idx].ContractAddress)
			if value, ok := tx.RawData.Contract[0].Parameter.Value["data"]; ok && txToDB.Type == 31 {
				data := value.(string)
				if len(data) >= 8 {
					txToDB.Method = data[:8]
				}

				if txToDB.Method == "a9059cbb" && len(data) == 8+64*2 {
					txToDB.FromAddr = txToDB.OwnerAddr
					txToDB.ToAddr = utils.EncodeToBase58(data[8+24 : 8+64])
					txToDB.Amount = models.NewBigInt(utils.ConvertHexToBigInt(data[8+64:]))
				}

				if txToDB.Method == "23b872dd" && len(data) == 8+64*3 {
					txToDB.FromAddr = utils.EncodeToBase58(data[8+24 : 8+64])
					txToDB.ToAddr = utils.EncodeToBase58(data[8+24+64 : 8+64*2])
					txToDB.Amount = models.NewBigInt(utils.ConvertHexToBigInt(data[8+64*2:]))
				}
			}
			txToDB.EnergyTotal = txInfoList[idx].Receipt.EnergyUsageTotal
			txToDB.EnergyFee = txInfoList[idx].Receipt.EnergyFee
			txToDB.EnergyUsage = txInfoList[idx].Receipt.EnergyUsage
			txToDB.EnergyOriginUsage = txInfoList[idx].Receipt.OriginEnergyUsage
		} else if txToDB.Type == 54 {
			txToDB.SetAmount(int64(tx.RawData.Contract[0].Parameter.Value["frozen_balance"].(float64)))
		} else if txToDB.Type == 55 {
			txToDB.SetAmount(int64(tx.RawData.Contract[0].Parameter.Value["unfreeze_balance"].(float64)))
		} else if txToDB.Type == 56 {
			txToDB.SetAmount(txInfoList[idx].WithdrawExpireAmount)
		} else if txToDB.Type == 57 {
			txToDB.ToAddr = utils.EncodeToBase58(tx.RawData.Contract[0].Parameter.Value["receiver_address"].(string))
			txToDB.SetAmount(int64(tx.RawData.Contract[0].Parameter.Value["balance"].(float64)))
		} else if txToDB.Type == 58 {
			txToDB.ToAddr = utils.EncodeToBase58(tx.RawData.Contract[0].Parameter.Value["receiver_address"].(string))
			txToDB.SetAmount(int64(tx.RawData.Contract[0].Parameter.Value["balance"].(float64)))
		} else if txToDB.Type == 59 {
			amount := int64(0)
			for _, entry := range txInfoList[idx].CancelUnfreezeV2Amount {
				amount += entry.Value
			}
			txToDB.SetAmount(amount)
		}

		// If the resource is ENERGY, add 100 to the type
		if resource, ok := tx.RawData.Contract[0].Parameter.Value["resource"]; ok && resource.(string) == "ENERGY" {
			txToDB.Type += 100
		}

		transactions = append(transactions, txToDB)
		// t.db.UpdateStatistics(txToDB)

		// for _, log := range txInfoList[idx].Log {
		// 	if len(log.Topics) == 3 && log.Topics[0] == "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" {
		// 		fromAddr := utils.EncodeToBase58(log.Topics[1][24:])
		// 		toAddr := utils.EncodeToBase58(log.Topics[2][24:])
		//
		// 		// Filter zero value charger
		// 		if txToDB.FromAddr == fromAddr || utils.ConvertHexToBigInt(log.Data).Int64() > 0 {
		// 			t.db.SaveCharger(fromAddr, toAddr, utils.EncodeToBase58(log.Address))
		// 		}
		// 	}
		// }
	}
	t.db.SaveTransactions(transactions)
}

func (t *Tracker) doTrackEthUSDT() {
	nowBlockNumber, err := net.EthBlockNumber()
	if err != nil {
		time.Sleep(12 * time.Second)
		return
	}

	trackedBlockNum := t.db.GetLastTrackedEthBlockNum()

	n := uint64(1)
	if nowBlockNumber-trackedBlockNum > 100 {
		n = uint64(100)
	} else if nowBlockNumber == trackedBlockNum {
		time.Sleep(1 * time.Second)
		return
	}

	ethLogs, err := net.EthGetLogs(
		trackedBlockNum+1,
		trackedBlockNum+n,
		common.HexToAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7"),
		[][]common.Hash{{
			common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"), // Transfer(address,address,uint256)
			common.HexToHash("0xcb8241adb0c3fdb35b70c24ce35c5eb0c17af7431c99f827d44a445ca624176a"), // Issue(uint256)
			common.HexToHash("0x702d5967f45f6513a38ffc42d6ba9bf230bd40e8f53b16363c7eb4fd2deb9a44"), // Redeem(uint256)
			common.HexToHash("0x61e6e66b0d6339b2980aecc6ccc0039736791f0ccde9ed512e789a7fbdd698c6"), // DestroyedBlackFunds(address,uint256)
		}})

	if err != nil {
		time.Sleep(12 * time.Second)
		return
	}

	for _, log := range ethLogs {
		t.db.ProcessEthUSDTTransferLog(log)

		if log.Topics[0].Hex() != "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" {
			var logType string
			switch log.Topics[0].Hex() {
			case "0xcb8241adb0c3fdb35b70c24ce35c5eb0c17af7431c99f827d44a445ca624176a":
				logType = "Issue"
			case "0x702d5967f45f6513a38ffc42d6ba9bf230bd40e8f53b16363c7eb4fd2deb9a44":
				logType = "Redeem"
			case "0x61e6e66b0d6339b2980aecc6ccc0039736791f0ccde9ed512e789a7fbdd698c6":
				logType = "DestroyedBlackFunds"
			}
			t.logger.Infof("%s log found: %s", logType, log.TxHash.Hex())
		}
	}

	if len(ethLogs) == 0 {
		t.db.SetLastTrackedEthBlockNum(trackedBlockNum + n)
	}

	if shouldReport, reportContent := t.ethReporter.Add(int(n)); shouldReport {
		nowBlockNumber, _ := net.EthBlockNumber()
		trackedBlockNumber := t.db.GetLastTrackedEthBlockNum()
		t.logger.Infof("%s, tracking from [%d] to [%d], left [%d], current total/dirty users [%d/%d]",
			reportContent, trackedBlockNumber, nowBlockNumber, nowBlockNumber-trackedBlockNumber, len(t.db.GetUsers()), t.db.GetDirtyUsersCount())
	}
}
