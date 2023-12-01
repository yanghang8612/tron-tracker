package main

import (
	"encoding/hex"

	"go.uber.org/zap"
	"tron-tracker/database"
	"tron-tracker/database/models"
	"tron-tracker/net"
	"tron-tracker/types"
	"tron-tracker/utils"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	db := database.New()
	for i := 0; i < 86400/3; i++ {
		block, _ := net.GetBlockByHeight(56084338 + i)
		txInfoList, _ := net.GetTransactionInfoList(56084338 + i)

		var txs = make([]models.Transaction, 0)
		for idx, tx := range block.Transactions {
			var txToDB = models.Transaction{
				Hash:      tx.TxID,
				Owner:     utils.EncodeToBase58(tx.RawData.Contract[0].Parameter.Value["owner_address"].(string)),
				Height:    block.BlockHeader.RawData.Number,
				Timestamp: block.BlockHeader.RawData.Timestamp / 1000,
				Type:      types.ConvertType(tx.RawData.Contract[0].Type),
				Fee:       txInfoList[idx].Fee,
			}

			txToDB.NetUsage = txInfoList[idx].Receipt.NetUsage
			txToDB.NetFee = txInfoList[idx].Receipt.NetFee
			txToDB.Result = txInfoList[idx].Receipt.Result
			if txToDB.Type == 1 {
				txToDB.Name = "_"
				txToDB.To = utils.EncodeToBase58(tx.RawData.Contract[0].Parameter.Value["to_address"].(string))
				txToDB.Amount = int64(tx.RawData.Contract[0].Parameter.Value["amount"].(float64))
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
				txToDB.To = utils.EncodeToBase58(tx.RawData.Contract[0].Parameter.Value["receiver_address"].(string))
				txToDB.Amount = int64(tx.RawData.Contract[0].Parameter.Value["balance"].(float64))
			} else if txToDB.Type == 58 {
				txToDB.To = utils.EncodeToBase58(tx.RawData.Contract[0].Parameter.Value["receiver_address"].(string))
				txToDB.Amount = int64(tx.RawData.Contract[0].Parameter.Value["balance"].(float64))
			} else if txToDB.Type == 59 {
				for _, entry := range txInfoList[idx].CancelUnfreezeV2Amount {
					txToDB.Amount += int64(entry.Value)
				}
			}
			if resource, ok := tx.RawData.Contract[0].Parameter.Value["resource"]; ok && resource.(string) == "BANDWIDTH" {
				txToDB.Amount = -txToDB.Amount
			}
			txs = append(txs, txToDB)

			transfers := make([]models.TRC20Transfer, 0)
			for _, log := range txInfoList[idx].Log {
				if len(log.Topics) == 3 && log.Topics[0] == "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" {
					var transferToDB = models.TRC20Transfer{
						Hash:      tx.TxID,
						Token:     utils.EncodeToBase58(log.Address),
						From:      utils.EncodeToBase58(log.Topics[1][24:]),
						To:        utils.EncodeToBase58(log.Topics[2][24:]),
						Timestamp: block.BlockHeader.RawData.Timestamp / 1000,
						Amount:    models.NewBigInt(utils.ConvertHexToBigInt(log.Data)),
					}

					transfers = append(transfers, transferToDB)
				}
			}
			db.SaveTransfers(&transfers)
			db.UpdateStats(&txToDB)
		}
		db.SetLastTrackedBlockNum(block.BlockHeader.RawData.Number)
		db.SaveTransactions(&txs)

		if i%100 == 0 {
			logger.Info("track stats report", zap.Int("count", i))
		}
	}
}
