package main

import (
	"encoding/hex"
	"sync"
	"time"

	"go.uber.org/zap"
	"tron-tracker/database"
	"tron-tracker/database/models"
	_ "tron-tracker/log"
	"tron-tracker/net"
	"tron-tracker/types"
	"tron-tracker/utils"
)

type Tracker struct {
	db *database.RawDB
	el *types.ExchangeList

	reporter *utils.Reporter
	loopWG   sync.WaitGroup
	quitCh   chan struct{}
}

func New(db *database.RawDB) *Tracker {
	return &Tracker{
		db: db,
		el: net.GetExchanges(),

		reporter: utils.NewReporter(1000, 60*time.Second, "Tracker report, tracked [%d] blocks in [%.2fs], speed [%.2fblocks/sec]"),
		quitCh:   make(chan struct{}),
	}
}

func (t *Tracker) Start() {
	t.db.Start()

	t.loopWG.Add(1)
	go t.loop()

	zap.L().Info("Tracker started")
}

func (t *Tracker) Stop() {
	close(t.quitCh)
	t.loopWG.Wait()
}

func (t *Tracker) loop() {
	for {
		select {
		case <-t.quitCh:
			zap.L().Info("Tracker quit, start closing rawdb")
			t.db.Close()
			zap.L().Info("Tracker quit, rawdb closed")
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

	if block.BlockHeader.RawData.Number%100 == 0 {
		t.el = net.GetExchanges()
	}

	if shouldReport, reportContent := t.reporter.Add(1); shouldReport {
		zap.L().Info(reportContent)
	}

	txInfoList, _ := net.GetTransactionInfoList(t.db.GetLastTrackedBlockNum() + 1)
	transactions := make([]models.Transaction, 0)
	transfers := make([]models.TRC20Transfer, 0)
	t.db.SetLastTrackedBlock(block)
	for idx, tx := range block.Transactions {
		var txToDB = models.Transaction{
			Hash:      tx.TxID,
			FromAddr:  utils.EncodeToBase58(tx.RawData.Contract[0].Parameter.Value["owner_address"].(string)),
			Height:    block.BlockHeader.RawData.Number,
			Timestamp: block.BlockHeader.RawData.Timestamp,
			Type:      types.ConvertType(tx.RawData.Contract[0].Type),
			Fee:       txInfoList[idx].Fee,
		}

		txToDB.NetUsage = txInfoList[idx].Receipt.NetUsage
		txToDB.NetFee = txInfoList[idx].Receipt.NetFee
		txToDB.Result = txInfoList[idx].Receipt.Result
		txToDB.SigCount = uint(len(tx.Signature))
		if txToDB.Type == 1 {
			txToDB.Name = "_"
			txToDB.ToAddr = utils.EncodeToBase58(tx.RawData.Contract[0].Parameter.Value["to_address"].(string))
			txToDB.Amount = int64(tx.RawData.Contract[0].Parameter.Value["amount"].(float64))

			// Filter exchange charger and small value TRX charger
			if t.el.Contains(txToDB.ToAddr) && !t.el.Contains(txToDB.FromAddr) && txToDB.Amount > 1000000 {
				t.db.SaveCharger(txToDB.FromAddr, t.el.Get(txToDB.ToAddr))
			}

			// Charger should only interact with its own exchange
			if txToDB.Amount > 1000000 {
				t.db.CheckCharger(txToDB.FromAddr, t.el.Get(txToDB.ToAddr))
			}

			t.db.UpdateToStatistic(txToDB.ToAddr, &txToDB)
		} else if txToDB.Type == 2 {
			name, _ := hex.DecodeString(tx.RawData.Contract[0].Parameter.Value["asset_name"].(string))
			txToDB.Name = string(name)
			txToDB.Amount = int64(tx.RawData.Contract[0].Parameter.Value["amount"].(float64))
		} else if txToDB.Type == 12 {
			txToDB.Amount = int64(txInfoList[idx].UnfreezeAmount)
		} else if txToDB.Type == 13 {
			txToDB.Amount = int64(txInfoList[idx].WithdrawAmount)
		} else if txToDB.Type == 30 || txToDB.Type == 31 {
			txToDB.Name = utils.EncodeToBase58(txInfoList[idx].ContractAddress)
			txToDB.EnergyTotal = txInfoList[idx].Receipt.EnergyUsageTotal
			txToDB.EnergyFee = txInfoList[idx].Receipt.EnergyFee
			txToDB.EnergyUsage = txInfoList[idx].Receipt.EnergyUsage
			txToDB.EnergyOriginUsage = txInfoList[idx].Receipt.OriginEnergyUsage
		} else if txToDB.Type == 54 {
			txToDB.Amount = int64(tx.RawData.Contract[0].Parameter.Value["frozen_balance"].(float64))
		} else if txToDB.Type == 55 {
			txToDB.Amount = int64(tx.RawData.Contract[0].Parameter.Value["unfreeze_balance"].(float64))
		} else if txToDB.Type == 56 {
			txToDB.Amount = int64(txInfoList[idx].WithdrawExpireAmount)
		} else if txToDB.Type == 57 {
			txToDB.ToAddr = utils.EncodeToBase58(txInfoList[idx].ContractAddress)
			txToDB.Amount = int64(tx.RawData.Contract[0].Parameter.Value["balance"].(float64))
		} else if txToDB.Type == 58 {
			txToDB.ToAddr = utils.EncodeToBase58(txInfoList[idx].ContractAddress)
			txToDB.Amount = int64(tx.RawData.Contract[0].Parameter.Value["balance"].(float64))
		} else if txToDB.Type == 59 {
			for _, entry := range txInfoList[idx].CancelUnfreezeV2Amount {
				txToDB.Amount += int64(entry.Value)
			}
		}
		if resource, ok := tx.RawData.Contract[0].Parameter.Value["resource"]; ok && resource.(string) == "BANDWIDTH" {
			txToDB.Amount = -txToDB.Amount
		}
		transactions = append(transactions, txToDB)

		recorded := make(map[string]bool)
		for _, log := range txInfoList[idx].Log {
			if len(log.Topics) == 3 && log.Topics[0] == "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" {
				var transferToDB = models.TRC20Transfer{
					Hash:      tx.TxID,
					Token:     utils.EncodeToBase58(log.Address),
					FromAddr:  utils.EncodeToBase58(log.Topics[1][24:]),
					ToAddr:    utils.EncodeToBase58(log.Topics[2][24:]),
					Timestamp: block.BlockHeader.RawData.Timestamp,
					Amount:    models.NewBigInt(utils.ConvertHexToBigInt(log.Data)),
				}
				transfers = append(transfers, transferToDB)

				// Filter exchange charger
				if t.el.Contains(transferToDB.ToAddr) && !t.el.Contains(txToDB.FromAddr) {
					// Filter small value USDT charger
					if transferToDB.Token != "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t" || utils.ConvertHexToBigInt(log.Data).Int64() > 500000 {
						t.db.SaveCharger(transferToDB.FromAddr, t.el.Get(transferToDB.ToAddr))
					}
				}

				// Charger should only interact with its own exchange
				if transferToDB.Token != "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t" || utils.ConvertHexToBigInt(log.Data).Int64() > 500000 {
					t.db.CheckCharger(transferToDB.FromAddr, t.el.Get(transferToDB.ToAddr))
				}

				if _, ok := recorded[transferToDB.ToAddr]; !ok {
					recorded[transferToDB.ToAddr] = true
					t.db.UpdateToStatistic(transferToDB.ToAddr, &txToDB)
				}
			}
		}
		t.db.UpdateFromStatistic(&txToDB)
	}
	t.db.SaveTransactions(&transactions)
	t.db.SaveTransfers(&transfers)
}
