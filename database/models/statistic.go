package models

import (
	"fmt"
	"math/big"

	"tron-tracker/common"
	"tron-tracker/database/models/types"

	"github.com/dustin/go-humanize"
)

const (
	seenFrom = 1 << iota // 0001
	seenTo               // 0010
)

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
	ID                    uint         `gorm:"primaryKey" json:"-"`
	User                  string       `gorm:"size:34;index" json:"address"`
	Token                 string       `gorm:"size:34;index" json:"token,omitempty"`
	FromTXCount           int64        `json:"from_tx_count"`
	FromAmount            types.BigInt `json:"from_amount"`
	FromFee               int64        `json:"from_fee"`
	FromEnergyTotal       int64        `json:"from_energy_total"`
	FromEnergyFee         int64        `json:"from_energy_fee"`
	FromEnergyUsage       int64        `json:"from_energy_usage"`
	FromEnergyOriginUsage int64        `json:"from_energy_origin_usage"`
	FromNetUsage          int64        `json:"from_net_usage"`
	FromNetFee            int64        `json:"from_net_fee"`
	ToTXCount             int64        `json:"to_tx_count"`
	ToAmount              types.BigInt `json:"to_amount"`
	ToFee                 int64        `json:"to_fee"`
	ToEnergyTotal         int64        `json:"to_energy_total"`
	ToEnergyFee           int64        `json:"to_energy_fee"`
	ToEnergyUsage         int64        `json:"to_energy_usage"`
	ToEnergyOriginUsage   int64        `json:"to_energy_origin_usage"`
	ToNetUsage            int64        `json:"to_net_usage"`
	ToNetFee              int64        `json:"to_net_fee"`
}

func NewUserTokenStatistic(user, token string) *UserTokenStatistic {
	return &UserTokenStatistic{
		User:       user,
		Token:      token,
		FromAmount: types.NewBigInt(big.NewInt(0)),
		ToAmount:   types.NewBigInt(big.NewInt(0)),
	}
}

func (o *UserTokenStatistic) Merge(other *UserTokenStatistic) {
	if other == nil {
		return
	}

	o.FromTXCount += other.FromTXCount
	o.FromAmount.Add(other.FromAmount)
	o.FromFee += other.FromFee
	o.FromEnergyTotal += other.FromEnergyTotal
	o.FromEnergyFee += other.FromEnergyFee
	o.FromEnergyUsage += other.FromEnergyUsage
	o.FromEnergyOriginUsage += other.FromEnergyOriginUsage
	o.FromNetUsage += other.FromNetUsage
	o.FromNetFee += other.FromNetFee

	o.ToTXCount += other.ToTXCount
	o.ToAmount.Add(other.ToAmount)
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
	o.FromAmount.Add(tx.Amount)
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
	o.ToAmount.Add(tx.Amount)
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
	BurnPercent       string `gorm:"-" json:"burn_percent,omitempty"`
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

func (o *TokenStatistic) Fill() *TokenStatistic {
	if o.EnergyTotal > 0 {
		o.BurnPercent = common.FormatOfPercent(o.EnergyTotal, o.EnergyTotal-o.EnergyUsage-o.EnergyOriginUsage)
	}
	return o
}

type FeeStatistic struct {
	ID         uint   `gorm:"primaryKey" json:"-"`
	Date       string `gorm:"size:6;index" json:"date,omitempty"`
	Energy     int64  `json:"energy"`
	Net        int64  `json:"net"`
	NewAcc     int64  `json:"new_acc"`
	UpdateAuth int64  `json:"update_auth"`
	MultiSig   int64  `json:"multi_sig"`
	Memo       int64  `json:"memo"`
	SR         int64  `json:"sr"`
	IssueTRC10 int64  `json:"issue_trc10"`
	NewBancor  int64  `json:"new_bancor"`
}

func NewFeeStatistic(date string) *FeeStatistic {
	return &FeeStatistic{
		Date: date,
	}
}

func (o *FeeStatistic) Merge(other *FeeStatistic) {
	if other == nil {
		return
	}

	o.Energy += other.Energy
	o.Net += other.Net
	o.NewAcc += other.NewAcc
	o.UpdateAuth += other.UpdateAuth
	o.MultiSig += other.MultiSig
	o.Memo += other.Memo
	o.SR += other.SR
	o.IssueTRC10 += other.IssueTRC10
	o.NewBancor += other.NewBancor
}

func (o *FeeStatistic) Add(tx *Transaction) {
	if tx == nil {
		return
	}

	o.Energy += tx.EnergyFee
	o.Net += tx.NetFee

	multiSigFee := int64(0)
	if tx.SigCount > 1 {
		// TODO: should depend on proposal
		multiSigFee = 1e6
		o.MultiSig += 1e6
	}

	memoFee := int64(0)
	if tx.WithMemo {
		// TODO: should depend on proposal
		memoFee = 1e6
		o.Memo += 1e6
	}

	specialFee := tx.Fee - tx.NetFee - multiSigFee - memoFee
	switch tx.Type {
	case 0, 1, 2:
		o.NewAcc += specialFee
	case 5:
		o.SR += specialFee
	case 6:
		o.IssueTRC10 += specialFee
	case 41:
		o.NewBancor += specialFee
	case 46:
		o.UpdateAuth += specialFee
	}
}

type ExchangeStatistic struct {
	ID                  uint         `gorm:"primaryKey" json:"-"`
	Date                string       `gorm:"size:6;index" json:"date,omitempty"`
	Name                string       `json:"name,omitempty"`
	Token               string       `gorm:"index;" json:"token,omitempty"`
	TotalFee            int64        `json:"total_fee"`
	ChargeTxCount       int64        `json:"charge_tx_count"`
	ChargeAmount        types.BigInt `json:"charge_amount,omitempty"`
	ChargeFee           int64        `json:"charge_fee"`
	ChargeNetFee        int64        `json:"charge_net_fee"`
	ChargeNetUsage      int64        `json:"charge_net_usage"`
	ChargeEnergyTotal   int64        `json:"charge_energy_total"`
	ChargeEnergyFee     int64        `json:"charge_energy_fee"`
	ChargeEnergyUsage   int64        `json:"charge_energy_usage"`
	CollectTxCount      int64        `json:"collect_tx_count"`
	CollectAmount       types.BigInt `json:"collect_amount,omitempty"`
	CollectFee          int64        `json:"collect_fee"`
	CollectNetFee       int64        `json:"collect_net_fee"`
	CollectNetUsage     int64        `json:"collect_net_usage"`
	CollectEnergyTotal  int64        `json:"collect_energy_total"`
	CollectEnergyFee    int64        `json:"collect_energy_fee"`
	CollectEnergyUsage  int64        `json:"collect_energy_usage"`
	WithdrawTxCount     int64        `json:"withdraw_tx_count"`
	WithdrawAmount      types.BigInt `json:"withdraw_amount,omitempty"`
	WithdrawFee         int64        `json:"withdraw_fee"`
	WithdrawNetFee      int64        `json:"withdraw_net_fee"`
	WithdrawNetUsage    int64        `json:"withdraw_net_usage"`
	WithdrawEnergyTotal int64        `json:"withdraw_energy_total"`
	WithdrawEnergyFee   int64        `json:"withdraw_energy_fee"`
	WithdrawEnergyUsage int64        `json:"withdraw_energy_usage"`
}

func NewExchangeStatistic(date, name, token string) *ExchangeStatistic {
	return &ExchangeStatistic{
		Date:           date,
		Name:           name,
		Token:          token,
		ChargeAmount:   types.NewBigInt(big.NewInt(0)),
		CollectAmount:  types.NewBigInt(big.NewInt(0)),
		WithdrawAmount: types.NewBigInt(big.NewInt(0)),
	}
}

func (o *ExchangeStatistic) Merge(other *ExchangeStatistic) {
	if other == nil {
		return
	}

	o.TotalFee += other.TotalFee
	o.ChargeTxCount += other.ChargeTxCount
	o.ChargeAmount.Add(other.ChargeAmount)
	o.ChargeFee += other.ChargeFee
	o.ChargeNetFee += other.ChargeNetFee
	o.ChargeNetUsage += other.ChargeNetUsage
	o.ChargeEnergyTotal += other.ChargeEnergyTotal
	o.ChargeEnergyFee += other.ChargeEnergyFee
	o.ChargeEnergyUsage += other.ChargeEnergyUsage
	o.CollectTxCount += other.CollectTxCount
	o.CollectAmount.Add(other.CollectAmount)
	o.CollectFee += other.CollectFee
	o.CollectNetFee += other.CollectNetFee
	o.CollectNetUsage += other.CollectNetUsage
	o.CollectEnergyTotal += other.CollectEnergyTotal
	o.CollectEnergyFee += other.CollectEnergyFee
	o.CollectEnergyUsage += other.CollectEnergyUsage
	o.WithdrawTxCount += other.WithdrawTxCount
	o.WithdrawAmount.Add(other.WithdrawAmount)
	o.WithdrawFee += other.WithdrawFee
	o.WithdrawNetFee += other.WithdrawNetFee
	o.WithdrawNetUsage += other.WithdrawNetUsage
	o.WithdrawEnergyTotal += other.WithdrawEnergyTotal
	o.WithdrawEnergyFee += other.WithdrawEnergyFee
	o.WithdrawEnergyUsage += other.WithdrawEnergyUsage
}

func (o *ExchangeStatistic) AddCharge(tx *Transaction) {
	o.TotalFee += tx.Fee
	o.ChargeTxCount += 1
	o.ChargeAmount.Add(tx.Amount)
	o.ChargeFee += tx.Fee
	o.ChargeNetFee += tx.NetFee
	o.ChargeNetUsage += tx.NetUsage
	o.ChargeEnergyTotal += tx.EnergyTotal
	o.ChargeEnergyFee += tx.EnergyFee
	o.ChargeEnergyUsage += tx.EnergyUsage + tx.EnergyOriginUsage
}

func (o *ExchangeStatistic) AddCollect(tx *Transaction) {
	o.TotalFee += tx.Fee
	o.CollectTxCount++
	o.CollectAmount.Add(tx.Amount)
	o.CollectFee += tx.Fee
	o.CollectNetFee += tx.NetFee
	o.CollectNetUsage += tx.NetUsage
	o.CollectEnergyTotal += tx.EnergyTotal
	o.CollectEnergyFee += tx.EnergyFee
	o.CollectEnergyUsage += tx.EnergyUsage
}

func (o *ExchangeStatistic) AddWithdraw(tx *Transaction) {
	o.TotalFee += tx.Fee
	o.WithdrawTxCount++
	o.WithdrawAmount.Add(tx.Amount)
	o.WithdrawFee += tx.Fee
	o.WithdrawNetFee += tx.NetFee
	o.WithdrawNetUsage += tx.NetUsage
	o.WithdrawEnergyTotal += tx.EnergyTotal
	o.WithdrawEnergyFee += tx.EnergyFee
	o.WithdrawEnergyUsage += tx.EnergyUsage
}

func (o *ExchangeStatistic) ClearAmountFields() {
	o.ChargeAmount = types.NewBigInt(nil)
	o.CollectAmount = types.NewBigInt(nil)
	o.WithdrawAmount = types.NewBigInt(nil)
}

// FungibleTokenStatistic Data is not repaired, please pay attention to the date field
type FungibleTokenStatistic struct {
	ID         uint         `gorm:"primaryKey" json:"-"`
	Date       string       `gorm:"size:6;index" json:"date,omitempty"`
	Address    string       `gorm:"index" json:"address"`
	Type       string       `json:"type"`
	Count      int64        `json:"count"`
	AmountSum  types.BigInt `json:"amount_sum"`
	UniqueFrom int64        `json:"unique_from"`
	UniqueTo   int64        `json:"unique_to"`
	UniqueUser int64        `json:"unique_user"`

	seenMap map[string]uint8 `gorm:"-"`
}

func NewFungibleTokenStatistic(date, address, typeName string, tx *Transaction) *FungibleTokenStatistic {
	var stat = &FungibleTokenStatistic{
		Date:      date,
		Address:   address,
		Type:      typeName,
		AmountSum: types.NewBigInt(big.NewInt(0)),

		seenMap: make(map[string]uint8),
	}

	stat.Add(tx)

	return stat
}

func (o *FungibleTokenStatistic) Add(tx *Transaction) {
	if tx == nil {
		return
	}
	o.Count++
	o.AmountSum.Add(tx.Amount)

	flags := o.seenMap[tx.FromAddr]
	if flags == 0 {
		o.UniqueUser++
	}
	if flags&seenFrom == 0 {
		o.UniqueFrom++
		o.seenMap[tx.FromAddr] |= seenFrom
	}

	flags = o.seenMap[tx.ToAddr]
	if flags == 0 {
		o.UniqueUser++
	}
	if flags&seenTo == 0 {
		o.UniqueTo++
		o.seenMap[tx.ToAddr] |= seenTo
	}
}

type MarketPairStatistic struct {
	ID                  uint    `gorm:"primaryKey" json:"-"`
	Datetime            string  `gorm:"size:6;index" json:"date,omitempty"`
	Token               string  `gorm:"size:16;index" json:"token,omitempty"`
	ExchangeName        string  `gorm:"size:32;index:idx_exchange_pair" json:"exchange_name,omitempty"`
	Pair                string  `gorm:"size:16;index:idx_exchange_pair" json:"pair,omitempty"`
	Reputation          float64 `json:"reputation,omitempty"`
	Price               float64 `json:"price,omitempty"`
	Volume              float64 `json:"volume,omitempty"`
	Percent             float64 `gorm:"index" json:"percent,omitempty"`
	DepthUsdPositiveTwo float64 `json:"depth_usd_positive_two,omitempty"`
	DepthUsdNegativeTwo float64 `json:"depth_usd_negative_two,omitempty"`
}

type TokenListingStatistic struct {
	ID              uint    `gorm:"primaryKey" json:"-"`
	Datetime        string  `gorm:"size:6;index" json:"date,omitempty"`
	CMCID           uint    `gorm:"index" json:"cmcid,omitempty"`
	Token           string  `gorm:"size:16;index" json:"token,omitempty"`
	Price           float64 `json:"price,omitempty"`
	Volume24H       float64 `json:"volume_24h,omitempty"`
	VolumeChange24H float64 `json:"volume_change_24h,omitempty"`
	MarketCap       float64 `json:"market_cap,omitempty"`
}

type USDTSupplyStatistic struct {
	ID              uint    `gorm:"primaryKey" json:"-"`
	Datetime        string  `gorm:"size:8;index" json:"date,omitempty"`
	Chain           string  `json:"name"`
	TotalAuthorized float64 `json:"total_authorized,omitempty"`
	NotIssued       float64 `json:"not_issued,omitempty"`
	Quarantined     float64 `json:"quarantined,omitempty"`
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
	// TODO: should depend on USDT Phishing Factor
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

func (o *USDTStorageStatistic) Diff(other *USDTStorageStatistic) string {
	var (
		curStats  = o
		lastStats = other
	)

	return fmt.Sprintf("SetStorage:\n"+
		"\tAverage Fee Per Tx: %.2f TRX (%s)\n"+
		"\tDaily transactions: %s (%s)\n"+
		"\tDaily total energy: %s (%s)\n"+
		"\tDaily energy with staking: %s (%s)\n"+
		"\tDaily energy fee: %s TRX (%s)\n"+
		"\tBurn energy: %.2f%%\n"+
		"ResetStorage:\n"+
		"\tAverage Fee Per Tx: %.2f TRX (%s)\n"+
		"\tDaily transactions: %s (%s)\n"+
		"\tDaily total energy: %s (%s)\n"+
		"\tDaily energy with staking: %s (%s)\n"+
		"\tDaily energy fee: %s TRX (%s)\n"+
		"\tBurn energy: %.2f%%\n",
		float64(curStats.SetEnergyFee)/float64(curStats.SetTxCount)/1e6,
		common.FormatChangePercent(int64(lastStats.SetEnergyFee/uint64(lastStats.SetTxCount)), int64(curStats.SetEnergyFee/uint64(curStats.SetTxCount))),
		humanize.Comma(int64(curStats.SetTxCount/7)),
		common.FormatChangePercent(int64(lastStats.SetTxCount), int64(curStats.SetTxCount)),
		humanize.Comma(int64(curStats.SetEnergyTotal/7)),
		common.FormatChangePercent(int64(lastStats.SetEnergyTotal), int64(curStats.SetEnergyTotal)),
		humanize.Comma(int64(curStats.SetEnergyUsage+curStats.SetEnergyOriginUsage)/7),
		common.FormatChangePercent(int64(lastStats.SetEnergyUsage+lastStats.SetEnergyOriginUsage), int64(curStats.SetEnergyUsage+curStats.SetEnergyOriginUsage)),
		humanize.Comma(int64(curStats.SetEnergyFee/7_000_000)),
		common.FormatChangePercent(int64(lastStats.SetEnergyFee), int64(curStats.SetEnergyFee)),
		100.0-float64(curStats.SetEnergyUsage+curStats.SetEnergyOriginUsage)/float64(curStats.SetEnergyTotal)*100,
		float64(curStats.ResetEnergyFee)/float64(curStats.ResetTxCount)/1e6,
		common.FormatChangePercent(int64(lastStats.ResetEnergyFee/uint64(lastStats.ResetTxCount)), int64(curStats.ResetEnergyFee/uint64(curStats.ResetTxCount))),
		humanize.Comma(int64(curStats.ResetTxCount/7)),
		common.FormatChangePercent(int64(lastStats.ResetTxCount), int64(curStats.ResetTxCount)),
		humanize.Comma(int64(curStats.ResetEnergyTotal/7)),
		common.FormatChangePercent(int64(lastStats.ResetEnergyTotal), int64(curStats.ResetEnergyTotal)),
		humanize.Comma(int64(curStats.ResetEnergyUsage+curStats.ResetEnergyOriginUsage)/7),
		common.FormatChangePercent(int64(lastStats.ResetEnergyUsage+lastStats.ResetEnergyOriginUsage), int64(curStats.ResetEnergyUsage+curStats.ResetEnergyOriginUsage)),
		humanize.Comma(int64(curStats.ResetEnergyFee/7_000_000)),
		common.FormatChangePercent(int64(lastStats.ResetEnergyFee), int64(curStats.ResetEnergyFee)),
		100.0-float64(curStats.ResetEnergyUsage+curStats.ResetEnergyOriginUsage)/float64(curStats.ResetEnergyTotal)*100)
}

type HoldingsStatistic struct {
	ID      uint   `gorm:"primaryKey" json:"-"`
	Date    string `gorm:"size:6;index" json:"date,omitempty"`
	User    string `gorm:"size:34" json:"user,omitempty"`
	Token   string `gorm:"size:34" json:"token,omitempty"`
	Balance string `json:"balance,omitempty"`
}
