package models

import (
	"math/big"

	"tron-tracker/database/models/types"
)

type TransferStatistic struct {
	ID        uint                `gorm:"primaryKey" json:"-"`
	Address   string              `gorm:"size:34;uniqueIndex" json:"address,omitempty"`
	Fee       int64               `json:"fee"`
	TxTotal   int64               `json:"tx_total"`
	TRXStats  types.TransferStats `json:"trx_transfer_stats"`
	USDTStats types.TransferStats `json:"usdt_transfer_stats"`
}

func NewTransferStatistic(address string) *TransferStatistic {
	return &TransferStatistic{
		Address:   address,
		TRXStats:  types.NewTransferStats(),
		USDTStats: types.NewTransferStats(),
	}
}

func (o *TransferStatistic) Merge(other *UserStatistic) {
	if other == nil {
		return
	}

	o.Fee += other.Fee
	o.TxTotal += other.TxTotal
	o.TRXStats.Merge(&other.TRXStats)
	o.USDTStats.Merge(&other.USDTStats)
}

type UserStatistic struct {
	ID                uint                `gorm:"primaryKey" json:"-"`
	Address           string              `gorm:"size:34;uniqueIndex" json:"address,omitempty"`
	Fee               int64               `json:"fee"`
	EnergyTotal       int64               `json:"energy_total"`
	EnergyFee         int64               `json:"energy_fee"`
	EnergyUsage       int64               `json:"energy_usage"`
	EnergyOriginUsage int64               `json:"energy_origin_usage"`
	NetUsage          int64               `json:"net_usage"`
	NetFee            int64               `json:"net_fee"`
	TxTotal           int64               `json:"tx_total"`
	TRXTotal          int64               `json:"trx_total"`
	SmallTRXTotal     int64               `json:"small_trx_total"`
	TRC10Total        int64               `json:"trc10_total"`
	TRC20Total        int64               `json:"trc20_total"`
	SCTotal           int64               `json:"sc_total"`
	USDTTotal         int64               `json:"usdt_total"`
	SmallUSDTTotal    int64               `json:"small_usdt_total"`
	StakeTotal        int64               `json:"stake_total"`
	DelegateTotal     int64               `json:"delegate_total"`
	VoteTotal         int64               `json:"vote_total"`
	MultiSigTotal     int64               `json:"multi_sig_total"`
	TRXStats          types.TransferStats `json:"trx_transfer_stats"`
	USDTStats         types.TransferStats `json:"usdt_transfer_stats"`
}

func NewUserStatistic(address string) *UserStatistic {
	return &UserStatistic{
		Address:   address,
		TRXStats:  types.NewTransferStats(),
		USDTStats: types.NewTransferStats(),
	}
}

func (o *UserStatistic) Merge(other *UserStatistic) {
	if other == nil {
		return
	}

	o.Fee += other.Fee
	o.EnergyTotal += other.EnergyTotal
	o.EnergyFee += other.EnergyFee
	o.EnergyUsage += other.EnergyUsage
	o.EnergyOriginUsage += other.EnergyOriginUsage
	o.NetUsage += other.NetUsage
	o.NetFee += other.NetFee
	o.TxTotal += other.TxTotal
	o.TRXTotal += other.TRXTotal
	o.SmallTRXTotal += other.SmallTRXTotal
	o.TRC10Total += other.TRC10Total
	o.TRC20Total += other.TRC20Total
	o.SCTotal += other.SCTotal
	o.USDTTotal += other.USDTTotal
	o.SmallUSDTTotal += other.SmallUSDTTotal
	o.StakeTotal += other.StakeTotal
	o.DelegateTotal += other.DelegateTotal
	o.VoteTotal += other.VoteTotal
	o.MultiSigTotal += other.MultiSigTotal
	o.TRXStats.Merge(&other.TRXStats)
	o.USDTStats.Merge(&other.USDTStats)
}

func (o *UserStatistic) Add(tx *Transaction) {
	if tx == nil {
		return
	}

	o.Fee += tx.Fee
	o.EnergyTotal += tx.EnergyTotal
	o.EnergyFee += tx.EnergyFee
	o.EnergyUsage += tx.EnergyUsage
	o.EnergyOriginUsage += tx.EnergyOriginUsage
	o.NetUsage += tx.NetUsage
	o.NetFee += tx.NetFee
	o.TxTotal += 1

	txType := tx.Type
	if txType > 100 {
		txType -= 100
	}

	switch txType {
	case 1:
		o.TRXTotal++
		o.TRXStats.Add(tx.Amount.Length(), 1)
		if tx.Amount.Int64() < 100000 {
			o.SmallTRXTotal++
		}
	case 2:
		o.TRC10Total++
	case 4:
		o.VoteTotal++
	case 11, 12, 54, 55, 59:
		o.StakeTotal++
	case 30, 31:
		o.SCTotal++
		if len(tx.ToAddr) > 0 {
			o.TRC20Total++
			if tx.Name == "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t" {
				o.USDTTotal++
				if tx.Result == 1 {
					o.USDTStats.Add(tx.Amount.Length(), 1)
				}
				if tx.Amount.Int64() < 500000 {
					o.SmallUSDTTotal++
				}
			}
		}
	case 57, 58:
		o.DelegateTotal++
	}

	if tx.SigCount > 1 {
		o.MultiSigTotal++
	}
}

type UserTokenStatistic struct {
	ID                    uint   `gorm:"primaryKey" json:"-"`
	User                  string `gorm:"size:34;index" json:"address"`
	Token                 string `gorm:"size:34;index" json:"token,omitempty"`
	FromTXCount           int64  `json:"from_tx_count"`
	FromFee               int64  `json:"from_fee"`
	FromEnergyTotal       int64  `json:"from_energy_total"`
	FromEnergyFee         int64  `json:"from_energy_fee"`
	FromEnergyUsage       int64  `json:"from_energy_usage"`
	FromEnergyOriginUsage int64  `json:"from_energy_origin_usage"`
	FromNetUsage          int64  `json:"from_net_usage"`
	FromNetFee            int64  `json:"from_net_fee"`
	ToTXCount             int64  `json:"to_tx_count"`
	ToFee                 int64  `json:"to_fee"`
	ToEnergyTotal         int64  `json:"to_energy_total"`
	ToEnergyFee           int64  `json:"to_energy_fee"`
	ToEnergyUsage         int64  `json:"to_energy_usage"`
	ToEnergyOriginUsage   int64  `json:"to_energy_origin_usage"`
	ToNetUsage            int64  `json:"to_net_usage"`
	ToNetFee              int64  `json:"to_net_fee"`
}

func (o *UserTokenStatistic) Merge(other *UserTokenStatistic) {
	if other == nil {
		return
	}

	o.FromTXCount += other.FromTXCount
	o.FromFee += other.FromFee
	o.FromEnergyTotal += other.FromEnergyTotal
	o.FromEnergyFee += other.FromEnergyFee
	o.FromEnergyUsage += other.FromEnergyUsage
	o.FromEnergyOriginUsage += other.FromEnergyOriginUsage
	o.FromNetUsage += other.FromNetUsage
	o.FromNetFee += other.FromNetFee

	o.ToTXCount += other.ToTXCount
	o.ToFee += other.ToFee
	o.ToEnergyTotal += other.ToEnergyTotal
	o.ToEnergyFee += other.ToEnergyFee
	o.ToEnergyUsage += other.ToEnergyUsage
	o.ToEnergyOriginUsage += other.ToEnergyOriginUsage
	o.ToNetUsage += other.ToNetUsage
	o.ToNetFee += other.ToNetFee
}

func (o *UserTokenStatistic) AddFrom(tx *Transaction) {
	o.FromTXCount++
	o.FromFee += tx.Fee
	o.FromEnergyTotal += tx.EnergyTotal
	o.FromEnergyFee += tx.EnergyFee
	o.FromEnergyUsage += tx.EnergyUsage
	o.FromEnergyOriginUsage += tx.EnergyOriginUsage
	o.FromNetUsage += tx.NetUsage
	o.FromNetFee += tx.NetFee
}

func (o *UserTokenStatistic) AddTo(tx *Transaction) {
	o.ToTXCount++
	o.ToFee += tx.Fee
	o.ToEnergyTotal += tx.EnergyTotal
	o.ToEnergyFee += tx.EnergyFee
	o.ToEnergyUsage += tx.EnergyUsage
	o.ToEnergyOriginUsage += tx.EnergyOriginUsage
	o.ToNetUsage += tx.NetUsage
	o.ToNetFee += tx.NetFee
}

type TokenStatistic struct {
	ID                uint   `gorm:"primaryKey" json:"-"`
	Address           string `gorm:"size:34;uniqueIndex" json:"address,omitempty"`
	Fee               int64  `json:"fee"`
	EnergyTotal       int64  `json:"energy_total"`
	EnergyFee         int64  `json:"energy_fee"`
	EnergyUsage       int64  `json:"energy_usage"`
	EnergyOriginUsage int64  `json:"energy_origin_usage"`
	NetUsage          int64  `json:"net_usage"`
	NetFee            int64  `json:"net_fee"`
	TxTotal           int64  `json:"tx_total"`
}

func (o *TokenStatistic) Merge(other *TokenStatistic) {
	if other == nil {
		return
	}

	o.Fee += other.Fee
	o.EnergyTotal += other.EnergyTotal
	o.EnergyFee += other.EnergyFee
	o.EnergyUsage += other.EnergyUsage
	o.EnergyOriginUsage += other.EnergyOriginUsage
	o.NetUsage += other.NetUsage
	o.NetFee += other.NetFee
	o.TxTotal += other.TxTotal
}

func (o *TokenStatistic) Add(tx *Transaction) {
	if tx == nil {
		return
	}

	o.Fee += tx.Fee
	o.EnergyTotal += tx.EnergyTotal
	o.EnergyFee += tx.EnergyFee
	o.EnergyUsage += tx.EnergyUsage
	o.EnergyOriginUsage += tx.EnergyOriginUsage
	o.NetUsage += tx.NetUsage
	o.NetFee += tx.NetFee
	o.TxTotal++
}

type ExchangeStatistic struct {
	ID                  uint   `gorm:"primaryKey" json:"-"`
	Date                string `gorm:"size:6;index" json:"date,omitempty"`
	Name                string `json:"name,omitempty"`
	Token               string `gorm:"index;" json:"token,omitempty"`
	TotalFee            int64  `json:"total_fee"`
	ChargeTxCount       int64  `json:"charge_tx_count"`
	ChargeFee           int64  `json:"charge_fee"`
	ChargeNetFee        int64  `json:"charge_net_fee"`
	ChargeNetUsage      int64  `json:"charge_net_usage"`
	ChargeEnergyTotal   int64  `json:"charge_energy_total"`
	ChargeEnergyFee     int64  `json:"charge_energy_fee"`
	ChargeEnergyUsage   int64  `json:"charge_energy_usage"`
	CollectTxCount      int64  `json:"collect_tx_count"`
	CollectFee          int64  `json:"collect_fee"`
	CollectNetFee       int64  `json:"collect_net_fee"`
	CollectNetUsage     int64  `json:"collect_net_usage"`
	CollectEnergyTotal  int64  `json:"collect_energy_total"`
	CollectEnergyFee    int64  `json:"collect_energy_fee"`
	CollectEnergyUsage  int64  `json:"collect_energy_usage"`
	WithdrawTxCount     int64  `json:"withdraw_tx_count"`
	WithdrawFee         int64  `json:"withdraw_fee"`
	WithdrawNetFee      int64  `json:"withdraw_net_fee"`
	WithdrawNetUsage    int64  `json:"withdraw_net_usage"`
	WithdrawEnergyTotal int64  `json:"withdraw_energy_total"`
	WithdrawEnergyFee   int64  `json:"withdraw_energy_fee"`
	WithdrawEnergyUsage int64  `json:"withdraw_energy_usage"`
}

func (o *ExchangeStatistic) Merge(other *ExchangeStatistic) {
	if other == nil {
		return
	}

	o.TotalFee += other.TotalFee
	o.ChargeTxCount += other.ChargeTxCount
	o.ChargeFee += other.ChargeFee
	o.ChargeNetFee += other.ChargeNetFee
	o.ChargeNetUsage += other.ChargeNetUsage
	o.ChargeEnergyTotal += other.ChargeEnergyTotal
	o.ChargeEnergyFee += other.ChargeEnergyFee
	o.ChargeEnergyUsage += other.ChargeEnergyUsage
	o.CollectTxCount += other.CollectTxCount
	o.CollectFee += other.CollectFee
	o.CollectNetFee += other.CollectNetFee
	o.CollectNetUsage += other.CollectNetUsage
	o.CollectEnergyTotal += other.CollectEnergyTotal
	o.CollectEnergyFee += other.CollectEnergyFee
	o.CollectEnergyUsage += other.CollectEnergyUsage
	o.WithdrawTxCount += other.WithdrawTxCount
	o.WithdrawFee += other.WithdrawFee
	o.WithdrawNetFee += other.WithdrawNetFee
	o.WithdrawNetUsage += other.WithdrawNetUsage
	o.WithdrawEnergyTotal += other.WithdrawEnergyTotal
	o.WithdrawEnergyFee += other.WithdrawEnergyFee
	o.WithdrawEnergyUsage += other.WithdrawEnergyUsage
}

func (o *ExchangeStatistic) AddCharge(stat *UserTokenStatistic) {
	o.TotalFee += stat.ToFee
	o.ChargeTxCount += stat.ToTXCount
	o.ChargeFee += stat.ToFee
	o.ChargeNetFee += stat.ToNetFee
	o.ChargeNetUsage += stat.ToNetUsage
	o.ChargeEnergyTotal += stat.ToEnergyTotal
	o.ChargeEnergyFee += stat.ToEnergyFee
	o.ChargeEnergyUsage += stat.ToEnergyUsage
}

func (o *ExchangeStatistic) AddCollect(stat *UserTokenStatistic) {
	o.TotalFee += stat.ToFee
	o.CollectTxCount += stat.ToTXCount
	o.CollectFee += stat.ToFee
	o.CollectNetFee += stat.ToNetFee
	o.CollectNetUsage += stat.ToNetUsage
	o.CollectEnergyTotal += stat.ToEnergyTotal
	o.CollectEnergyFee += stat.ToEnergyFee
	o.CollectEnergyUsage += stat.ToEnergyUsage
}

func (o *ExchangeStatistic) AddWithdraw(stat *UserTokenStatistic) {
	o.TotalFee += stat.FromFee
	o.WithdrawTxCount += stat.FromTXCount
	o.WithdrawFee += stat.FromFee
	o.WithdrawNetFee += stat.FromNetFee
	o.WithdrawNetUsage += stat.FromNetUsage
	o.WithdrawEnergyTotal += stat.FromEnergyTotal
	o.WithdrawEnergyFee += stat.FromEnergyFee
	o.WithdrawEnergyUsage += stat.FromEnergyUsage
}

func (o *ExchangeStatistic) AddWithdrawFromTx(tx *Transaction) {
	o.TotalFee += tx.Fee
	o.WithdrawTxCount++
	o.WithdrawFee += tx.Fee
	o.WithdrawNetFee += tx.NetFee
	o.WithdrawNetUsage += tx.NetUsage
	o.WithdrawEnergyTotal += tx.EnergyTotal
	o.WithdrawEnergyFee += tx.EnergyFee
	o.WithdrawEnergyUsage += tx.EnergyUsage
}

type FungibleTokenStatistic struct {
	ID            uint            `gorm:"primaryKey" json:"-"`
	Date          string          `gorm:"size:6;index" json:"date,omitempty"`
	Address       string          `gorm:"index" json:"address"`
	Type          string          `json:"type"`
	Count         int64           `json:"count"`
	AmountSum     types.BigInt    `json:"amount_sum"`
	UniqueFrom    int64           `json:"unique_from"`
	UniqueFromMap map[string]bool `gorm:"-" json:"-"`
	UniqueTo      int64           `json:"unique_to"`
	UniqueToMap   map[string]bool `gorm:"-" json:"-"`
}

func NewFungibleTokenStatistic(date, address, token string, tx *Transaction) *FungibleTokenStatistic {
	var stat = &FungibleTokenStatistic{
		Date:      date,
		Address:   address,
		Type:      token,
		Count:     1,
		AmountSum: types.NewBigInt(big.NewInt(0)),
	}
	stat.AmountSum.Add(tx.Amount)
	stat.UniqueFrom = 1
	stat.UniqueFromMap = make(map[string]bool)
	stat.UniqueFromMap[tx.FromAddr] = true
	stat.UniqueTo = 1
	stat.UniqueToMap = make(map[string]bool)
	stat.UniqueToMap[tx.ToAddr] = true

	return stat
}

func (o *FungibleTokenStatistic) Add(tx *Transaction) {
	o.Count++
	o.AmountSum.Add(tx.Amount)
	if _, ok := o.UniqueFromMap[tx.FromAddr]; !ok {
		o.UniqueFrom++
		o.UniqueFromMap[tx.FromAddr] = true
	}
	if _, ok := o.UniqueToMap[tx.ToAddr]; !ok {
		o.UniqueTo++
		o.UniqueToMap[tx.ToAddr] = true
	}
}

type MarketPairStatistic struct {
	ID                  uint    `gorm:"primaryKey" json:"-"`
	Datetime            string  `gorm:"size:6;index" json:"date,omitempty"`
	Token               string  `gorm:"index" json:"token,omitempty"`
	ExchangeName        string  `gorm:"index" json:"exchange_name,omitempty"`
	Pair                string  `json:"pair,omitempty"`
	Reputation          float64 `json:"reputation,omitempty"`
	Volume              float64 `json:"volume,omitempty"`
	Percent             float64 `json:"percent,omitempty"`
	DepthUsdPositiveTwo float64 `json:"depth_usd_positive_two,omitempty"`
	DepthUsdNegativeTwo float64 `json:"depth_usd_negative_two,omitempty"`
}

type PhishingStatistic struct {
	ID                          uint   `gorm:"primaryKey" json:"-"`
	Date                        string `gorm:"size:6;index" json:"date,omitempty"`
	TotalTxWithTRX              uint   `json:"total_tx_with_trx"`
	TotalCostWithTRX            uint64 `json:"total_cost_with_trx"`
	TotalTxFeeWithTRX           uint64 `json:"total_tx_fee_with_trx"`
	TRXSuccessTxWithTRX         uint   `json:"trx_success_tx_with_trx"`
	TRXSuccessAmountWithTRX     uint64 `json:"trx_success_amount_with_trx"`
	USDTSuccessTXWithTRX        uint   `json:"usdt_success_tx_with_trx"`
	USDTSuccessAmountWithTRX    uint64 `json:"usdt_success_amount_with_trx"`
	TotalTxWithUSDT             uint   `json:"total_tx_with_usdt"`
	TotalCostWithUSDT           uint64 `json:"total_cost_with_usdt"`
	TotalTxFeeWithUSDT          uint64 `json:"total_tx_fee_with_usdt"`
	TotalTxFrozenEnergyWithUSDT uint64 `json:"total_tx_frozen_energy_with_usdt"`
	TRXSuccessTxWithUSDT        uint   `json:"trx_success_tx_with_usdt"`
	TRXSuccessAmountWithUSDT    uint64 `json:"trx_success_amount_with_usdt"`
	USDTSuccessTXWithUSDT       uint   `json:"usdt_success_tx_with_usdt"`
	USDTSuccessAmountWithUSDT   uint64 `json:"usdt_success_amount_with_usdt"`
}

func (o *PhishingStatistic) Merge(other *PhishingStatistic) {
	if other == nil {
		return
	}

	o.TotalTxWithTRX += other.TotalTxWithTRX
	o.TotalCostWithTRX += other.TotalCostWithTRX
	o.TotalTxFeeWithTRX += other.TotalTxFeeWithTRX
	o.TRXSuccessTxWithTRX += other.TRXSuccessTxWithTRX
	o.TRXSuccessAmountWithTRX += other.TRXSuccessAmountWithTRX
	o.USDTSuccessTXWithTRX += other.USDTSuccessTXWithTRX
	o.USDTSuccessAmountWithTRX += other.USDTSuccessAmountWithTRX
	o.TotalTxWithUSDT += other.TotalTxWithUSDT
	o.TotalCostWithUSDT += other.TotalCostWithUSDT
	o.TotalTxFeeWithUSDT += other.TotalTxFeeWithUSDT
	o.TotalTxFrozenEnergyWithUSDT += other.TotalTxFrozenEnergyWithUSDT
	o.TRXSuccessTxWithUSDT += other.TRXSuccessTxWithUSDT
	o.TRXSuccessAmountWithUSDT += other.TRXSuccessAmountWithUSDT
	o.USDTSuccessTXWithUSDT += other.USDTSuccessTXWithUSDT
	o.USDTSuccessAmountWithUSDT += other.USDTSuccessAmountWithUSDT
}

type USDTStorageStatistic struct {
	ID                     uint   `gorm:"primaryKey" json:"-"`
	Date                   string `gorm:"size:6;index" json:"date,omitempty"`
	SetTxCount             uint   `json:"set_tx_count"`
	SetEnergyTotal         uint64 `json:"set_energy_total"`
	SetEnergyFee           uint64 `json:"set_energy_fee"`
	SetEnergyUsage         uint64 `json:"set_energy_usage"`
	SetEnergyOriginUsage   uint64 `json:"set_energy_origin_usage"`
	SetNetUsage            uint64 `json:"set_net_usage"`
	SetNetFee              uint64 `json:"set_net_fee"`
	ResetTxCount           uint   `json:"reset_tx_count"`
	ResetEnergyTotal       uint64 `json:"reset_energy_total"`
	ResetEnergyFee         uint64 `json:"reset_energy_fee"`
	ResetEnergyUsage       uint64 `json:"reset_energy_usage"`
	ResetEnergyOriginUsage uint64 `json:"reset_energy_origin_usage"`
	ResetNetUsage          uint64 `json:"reset_net_usage"`
	ResetNetFee            uint64 `json:"reset_net_fee"`
}

func (o *USDTStorageStatistic) Add(tx *Transaction) {
	if tx.EnergyTotal < 100_000 {
		o.SetTxCount++
		o.SetEnergyTotal += uint64(tx.EnergyTotal)
		o.SetEnergyFee += uint64(tx.EnergyFee)
		o.SetEnergyUsage += uint64(tx.EnergyUsage)
		o.SetEnergyOriginUsage += uint64(tx.EnergyOriginUsage)
		o.SetNetUsage += uint64(tx.NetUsage)
		o.SetNetFee += uint64(tx.NetFee)
	} else {
		o.ResetTxCount++
		o.ResetEnergyTotal += uint64(tx.EnergyTotal)
		o.ResetEnergyFee += uint64(tx.EnergyFee)
		o.ResetEnergyUsage += uint64(tx.EnergyUsage)
		o.ResetEnergyOriginUsage += uint64(tx.EnergyOriginUsage)
		o.ResetNetUsage += uint64(tx.NetUsage)
		o.ResetNetFee += uint64(tx.NetFee)
	}
}

func (o *USDTStorageStatistic) Merge(other *USDTStorageStatistic) {
	if other == nil {
		return
	}

	o.SetTxCount += other.SetTxCount
	o.SetEnergyTotal += other.SetEnergyTotal
	o.SetEnergyFee += other.SetEnergyFee
	o.SetEnergyUsage += other.SetEnergyUsage
	o.SetEnergyOriginUsage += other.SetEnergyOriginUsage
	o.SetNetUsage += other.SetNetUsage
	o.SetNetFee += other.SetNetFee
	o.ResetTxCount += other.ResetTxCount
	o.ResetEnergyTotal += other.ResetEnergyTotal
	o.ResetEnergyFee += other.ResetEnergyFee
	o.ResetEnergyUsage += other.ResetEnergyUsage
	o.ResetEnergyOriginUsage += other.ResetEnergyOriginUsage
	o.ResetNetUsage += other.ResetNetUsage
	o.ResetNetFee += other.ResetNetFee
}
