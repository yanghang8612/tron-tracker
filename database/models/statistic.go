package models

type UserStatistic struct {
	ID                uint   `gorm:"primaryKey" json:"-"`
	Address           string `gorm:"size:34;uniqueIndex" json:"address"`
	Fee               int64  `json:"fee"`
	EnergyTotal       int64  `json:"energy_total"`
	EnergyFee         int64  `json:"energy_fee"`
	EnergyUsage       int64  `json:"energy_usage"`
	EnergyOriginUsage int64  `json:"energy_origin_usage"`
	NetUsage          int64  `json:"net_usage"`
	NetFee            int64  `json:"net_fee"`
	TXTotal           int64  `json:"tx_total"`
	TRXTotal          int64  `json:"trx_total"`
	SmallTRXTotal     int64  `json:"small_trx_total"`
	TRC10Total        int64  `json:"trc10_total"`
	TRC20Total        int64  `json:"trc20_total"`
	SCTotal           int64  `json:"sc_total"`
	USDTTotal         int64  `json:"usdt_total"`
	SmallUSDTTotal    int64  `json:"small_usdt_total"`
	StakeTotal        int64  `json:"stake_total"`
	DelegateTotal     int64  `json:"delegate_total"`
	VoteTotal         int64  `json:"vote_total"`
	MultiSigTotal     int64  `json:"multi_sig_total"`
}

func NewUserStatistic(address string, tx *Transaction) *UserStatistic {
	var stats = &UserStatistic{
		Address:           address,
		Fee:               tx.Fee,
		EnergyTotal:       tx.EnergyTotal,
		EnergyFee:         tx.EnergyFee,
		EnergyUsage:       tx.EnergyUsage,
		EnergyOriginUsage: tx.EnergyOriginUsage,
		NetUsage:          tx.NetUsage,
		NetFee:            tx.NetFee,
		TXTotal:           1,
	}

	switch tx.Type {
	case 1:
		stats.TRXTotal = 1
		if tx.Amount.Int64() < 500000 {
			stats.SmallTRXTotal = 1
		}
	case 2:
		stats.TRC10Total = 1
	case 3:
		stats.VoteTotal = 1
	case 11, 12, 54, 55, 59:
		stats.StakeTotal = 1
	case 30, 31:
		stats.SCTotal = 1
		if len(tx.ToAddr) > 0 {
			stats.TRC20Total = 1
			if tx.Name == "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t" {
				stats.USDTTotal = 1
				if tx.Amount.Int64() < 500000 {
					stats.SmallUSDTTotal = 1
				}
			}
		}

	case 57, 58:
		stats.DelegateTotal = 1
	}

	if tx.SigCount > 1 {
		stats.MultiSigTotal = 1
	}

	return stats
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
	o.TXTotal += other.TXTotal
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

	o.TXTotal++
	switch tx.Type {
	case 1:
		o.TRXTotal++
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
	Token                 string `gorm:"size:34;index" json:"token"`
	FromTXCount           int64  `gorm:"index" json:"from_tx_count"`
	FromFee               int64  `gorm:"index" json:"from_fee"`
	FromEnergyTotal       int64  `json:"from_energy_total"`
	FromEnergyFee         int64  `json:"from_energy_fee"`
	FromEnergyUsage       int64  `json:"from_energy_usage"`
	FromEnergyOriginUsage int64  `json:"from_energy_origin_usage"`
	FromNetUsage          int64  `json:"from_net_usage"`
	FromNetFee            int64  `json:"from_net_fee"`
	ToTXCount             int64  `gorm:"index" json:"to_tx_count"`
	ToFee                 int64  `gorm:"index" json:"to_fee"`
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
	Address           string `gorm:"size:34;uniqueIndex" json:"address"`
	Fee               int64  `json:"fee"`
	EnergyTotal       int64  `json:"energy_total"`
	EnergyFee         int64  `json:"energy_fee"`
	EnergyUsage       int64  `json:"energy_usage"`
	EnergyOriginUsage int64  `json:"energy_origin_usage"`
	NetUsage          int64  `json:"net_usage"`
	NetFee            int64  `json:"net_fee"`
	TXTotal           int64  `json:"tx_total"`
}

func NewTokenStatistic(address string, tx *Transaction) *TokenStatistic {
	var stats = &TokenStatistic{
		Address:           address,
		Fee:               tx.Fee,
		EnergyTotal:       tx.EnergyTotal,
		EnergyFee:         tx.EnergyFee,
		EnergyUsage:       tx.EnergyUsage,
		EnergyOriginUsage: tx.EnergyOriginUsage,
		NetUsage:          tx.NetUsage,
		NetFee:            tx.NetFee,
		TXTotal:           1,
	}

	return stats
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
	o.TXTotal += other.TXTotal
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
	o.TXTotal++
}

type ExchangeStatistic struct {
	ID                  uint   `gorm:"primaryKey" json:"-"`
	Date                string `gorm:"index;size:6" json:"date"`
	Name                string `json:"name,omitempty"`
	Address             string `gorm:"index;size:34" json:"address,omitempty"`
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

func (o *ExchangeStatistic) AddCharge(stats *UserTokenStatistic) {
	o.TotalFee += stats.ToFee
	o.ChargeTxCount += stats.ToTXCount
	o.ChargeFee += stats.ToFee
	o.ChargeNetFee += stats.ToNetFee
	o.ChargeNetUsage += stats.ToNetUsage
	o.ChargeEnergyTotal += stats.ToEnergyTotal
	o.ChargeEnergyFee += stats.ToEnergyFee
	o.ChargeEnergyUsage += stats.ToEnergyUsage
}

func (o *ExchangeStatistic) AddCollect(stats *UserTokenStatistic) {
	o.TotalFee += stats.ToFee
	o.CollectTxCount += stats.ToTXCount
	o.CollectFee += stats.ToFee
	o.CollectNetFee += stats.ToNetFee
	o.CollectNetUsage += stats.ToNetUsage
	o.CollectEnergyTotal += stats.ToEnergyTotal
	o.CollectEnergyFee += stats.ToEnergyFee
	o.CollectEnergyUsage += stats.ToEnergyUsage
}

func (o *ExchangeStatistic) AddWithdraw(stats *UserTokenStatistic) {
	o.TotalFee += stats.FromFee
	o.WithdrawTxCount += stats.FromTXCount
	o.WithdrawFee += stats.FromFee
	o.WithdrawNetFee += stats.FromNetFee
	o.WithdrawNetUsage += stats.FromNetUsage
	o.WithdrawEnergyTotal += stats.FromEnergyTotal
	o.WithdrawEnergyFee += stats.FromEnergyFee
	o.WithdrawEnergyUsage += stats.FromEnergyUsage
}
