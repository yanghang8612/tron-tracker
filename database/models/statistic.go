package models

type UserStatistic struct {
	ID                uint   `gorm:"primaryKey" json:"-"`
	Address           string `gorm:"size:34;uniqueIndex"`
	Fee               int64
	EnergyTotal       int64
	EnergyFee         int64
	EnergyUsage       int64
	EnergyOriginUsage int64
	NetUsage          int64
	NetFee            int64
	TXTotal           int64
	TRXTotal          int64
	SmallTRXTotal     int64
	TRC10Total        int64
	TRC20Total        int64
	SCTotal           int64
	USDTTotal         int64
	SmallUSDTTotal    int64
	StakeTotal        int64
	DelegateTotal     int64
	VoteTotal         int64
	MultiSigTotal     int64
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
		if tx.Amount.Int64() < 100000 {
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
	case 3:
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

type TokenStatistic struct {
	ID                uint   `gorm:"primaryKey" json:"-"`
	Address           string `gorm:"size:34;uniqueIndex"`
	Fee               int64
	EnergyTotal       int64
	EnergyFee         int64
	EnergyUsage       int64
	EnergyOriginUsage int64
	NetUsage          int64
	NetFee            int64
	TXTotal           int64
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
	Date                string `gorm:"index;size:6"`
	Name                string
	Address             string `gorm:"size:34" json:"address,omitempty"`
	TotalFee            int64  `gorm:"-:all"`
	ChargeTxCount       int64
	ChargeFee           int64
	ChargeNetFee        int64
	ChargeNetUsage      int64
	ChargeEnergyTotal   int64
	ChargeEnergyFee     int64
	ChargeEnergyUsage   int64
	CollectTxCount      int64
	CollectFee          int64
	CollectNetFee       int64
	CollectNetUsage     int64
	CollectEnergyTotal  int64
	CollectEnergyFee    int64
	CollectEnergyUsage  int64
	WithdrawTxCount     int64
	WithdrawFee         int64
	WithdrawNetFee      int64
	WithdrawNetUsage    int64
	WithdrawEnergyTotal int64
	WithdrawEnergyFee   int64
	WithdrawEnergyUsage int64
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
