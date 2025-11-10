package tron

import (
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	"tron-tracker/common"
	"tron-tracker/database"
	"tron-tracker/database/models"
	modeltypes "tron-tracker/database/models/types"
	"tron-tracker/net"
	"tron-tracker/tron/types"

	"github.com/dustin/go-humanize"
	"go.uber.org/zap"
)

const TransferTopic = "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

type Tracker struct {
	db *database.RawDB

	isCatching bool
	reporter   *common.Reporter
	loopWG     sync.WaitGroup
	quitCh     chan struct{}

	blockNumAtLastCheck uint

	logger *zap.SugaredLogger
}

type transferData struct {
	From   string
	To     string
	Amount modeltypes.BigInt
}

func NewTracker(db *database.RawDB) *Tracker {
	return &Tracker{
		db: db,

		isCatching: true,
		reporter: common.NewReporter(1000, 60*time.Second, 0, func(rs common.ReporterState) string {
			return fmt.Sprintf("Tracked [%d] blocks in [%.2fs], speed [%.2fblocks/sec]", rs.CountInc, rs.ElapsedTime, float64(rs.CountInc)/rs.ElapsedTime)
		}),
		quitCh: make(chan struct{}),

		blockNumAtLastCheck: db.GetLastTrackedBlockNum(),

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

func (t *Tracker) IsTracking() bool {
	defer func() {
		t.blockNumAtLastCheck = t.db.GetLastTrackedBlockNum()
	}()
	return t.blockNumAtLastCheck != t.db.GetLastTrackedBlockNum()
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
			Index:     uint16(idx),
			OwnerAddr: common.EncodeToBase58(tx.RawData.Contract[0].Parameter.Value["owner_address"].(string)),
			Height:    block.BlockHeader.RawData.Number,
			Type:      types.ConvertType(tx.RawData.Contract[0].Type),
			Fee:       txInfoList[idx].Fee,
		}

		if len(tx.RawData.Data) > 0 {
			txToDB.WithMemo = true
		}

		txToDB.NetUsage = txInfoList[idx].Receipt.NetUsage
		txToDB.NetFee = txInfoList[idx].Receipt.NetFee
		txToDB.Result = types.ConvertResult(txInfoList[idx].Receipt.Result)
		txToDB.SigCount = uint8(len(tx.Signature))
		if txToDB.Type == 1 {
			txToDB.Name = "TRX"
			txToDB.FromAddr = txToDB.OwnerAddr
			txToDB.ToAddr = common.EncodeToBase58(tx.RawData.Contract[0].Parameter.Value["to_address"].(string))

			amount := int64(tx.RawData.Contract[0].Parameter.Value["amount"].(float64))
			txToDB.SetAmount(amount)
			// The TRX charger should charge at least 1 TRX
			if amount > 1_000_000 {
				t.db.SaveCharger(txToDB.FromAddr, txToDB.ToAddr, txToDB.Name)
			}
		} else if txToDB.Type == 2 {
			name, _ := hex.DecodeString(tx.RawData.Contract[0].Parameter.Value["asset_name"].(string))
			txToDB.Name = string(name)
			txToDB.FromAddr = txToDB.OwnerAddr
			txToDB.ToAddr = common.EncodeToBase58(tx.RawData.Contract[0].Parameter.Value["to_address"].(string))
			txToDB.SetAmount(int64(tx.RawData.Contract[0].Parameter.Value["amount"].(float64)))
		} else if txToDB.Type == 12 {
			txToDB.SetAmount(txInfoList[idx].UnfreezeAmount)
		} else if txToDB.Type == 13 {
			txToDB.SetAmount(txInfoList[idx].WithdrawAmount)
		} else if txToDB.Type == 30 || txToDB.Type == 31 {
			txToDB.Name = common.EncodeToBase58(txInfoList[idx].ContractAddress)
			if value, ok := tx.RawData.Contract[0].Parameter.Value["data"]; ok && txToDB.Type == 31 {
				data := value.(string)
				if len(data) >= 8 {
					txToDB.Method = data[:8]
				}

				if td, isTransfer := convertTransferData(txToDB.OwnerAddr, data); isTransfer {
					txToDB.FromAddr = td.From
					txToDB.ToAddr = td.To
					txToDB.Amount = td.Amount
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
			txToDB.ToAddr = common.EncodeToBase58(tx.RawData.Contract[0].Parameter.Value["receiver_address"].(string))
			txToDB.SetAmount(int64(tx.RawData.Contract[0].Parameter.Value["balance"].(float64)))
		} else if txToDB.Type == 58 {
			txToDB.ToAddr = common.EncodeToBase58(tx.RawData.Contract[0].Parameter.Value["receiver_address"].(string))
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
		t.db.UpdateStatistics(block.BlockHeader.RawData.Timestamp, txToDB)

		for _, log := range txInfoList[idx].Logs {
			if len(log.Topics) == 3 && log.Topics[0] == TransferTopic {
				fromAddr := common.EncodeToBase58(log.Topics[1][24:])
				toAddr := common.EncodeToBase58(log.Topics[2][24:])

				// Filter zero value charger
				if txToDB.FromAddr == fromAddr || common.ConvertHexToBigInt(log.Data).Int64() > 0 {
					t.db.SaveCharger(fromAddr, toAddr, common.EncodeToBase58(log.Address))
				}
			}
		}

		for _, internalTx := range txInfoList[idx].InternalTxs {
			if !internalTx.Rejected && internalTx.Note == "63616c6c" &&
				common.EncodeToBase58(internalTx.To) == database.USDT {
				if td, isTransfer := convertTransferData(common.EncodeToBase58(internalTx.From), internalTx.Data); isTransfer {
					var transferTxToDB = &models.Transaction{
						Height: block.BlockHeader.RawData.Number,
						Index:  uint16(idx),
						Type:   models.TransferType,
						Result: 1,
						Name:   database.USDT,
					}

					transferTxToDB.FromAddr = td.From
					transferTxToDB.ToAddr = td.To
					transferTxToDB.Amount = td.Amount

					var percent = float64(internalTx.EnergyUsed) / float64(txToDB.EnergyTotal)
					transferTxToDB.Fee = int64(float64(txToDB.Fee) * percent)
					transferTxToDB.EnergyTotal = internalTx.EnergyUsed
					transferTxToDB.EnergyUsage = int64(float64(txToDB.EnergyUsage) * percent)
					transferTxToDB.EnergyOriginUsage = int64(float64(txToDB.EnergyOriginUsage) * percent)

					transactions = append(transactions, transferTxToDB)
				}
			}
		}

		// If there is concerned event, report to slack
		if !t.isCatching && (txToDB.Type == 154 || txToDB.Type == 157) && len(txToDB.Amount.String()) >= 8+7 {
			net.ReportWarningToSlack(fmt.Sprintf("[block - %d] - index %d: [%s] -> [%s], amount [%s]",
				txToDB.Height, txToDB.Index, txToDB.OwnerAddr, txToDB.ToAddr, humanize.Comma(txToDB.Amount.Int64()/1e6)))
		}
	}
	t.db.SaveTransactions(transactions)
}

func convertTransferData(owner, data string) (*transferData, bool) {
	var t transferData

	if len(data) >= 8+64*2 && strings.HasPrefix(data, "a9059cbb") {
		t.From = owner
		t.To = common.EncodeToBase58(data[8+24 : 8+64])
		t.Amount = modeltypes.NewBigInt(common.ConvertHexToBigInt(data[8+64 : 8+64*2]))
		return &t, true
	} else if len(data) >= 8+64*3 && strings.HasPrefix(data, "23b872dd") {
		t.From = common.EncodeToBase58(data[8+24 : 8+64])
		t.To = common.EncodeToBase58(data[8+24+64 : 8+64*2])
		t.Amount = modeltypes.NewBigInt(common.ConvertHexToBigInt(data[8+64*2 : 8+64*3]))
		return &t, true
	}
	return nil, false
}
