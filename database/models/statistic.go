package models

type UserStatistic struct {
	ID                uint   `gorm:"primaryKey"`
	Address           string `gorm:"size:34;uniqueIndex"`
	Fee               uint64
	EnergyTotal       uint64
	EnergyFee         uint64
	EnergyUsage       uint64
	EnergyOriginUsage uint64
	NetUsage          uint64
	NetFee            uint64
	TXTotal           uint64
	TRXTotal          uint64
	SmallTRXTotal     uint64
	TRC10Total        uint64
	TRC20Total        uint64
	SCTotal           uint64
	USDTTotal         uint64
	SmallUSDTTotal    uint64
	StakeTotal        uint64
	DelegateTotal     uint64
	VoteTotal         uint64
	MultiSigTotal     uint64
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
	o.TRC10Total += other.TRC10Total
	o.SCTotal += other.SCTotal
	o.USDTTotal += other.USDTTotal
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

type ExchangeStatistic struct {
	ID                  uint   `gorm:"primaryKey" json:"-"`
	Date                string `gorm:"index;size:6"`
	Name                string
	Address             string `gorm:"size:34" json:"address,omitempty"`
	TotalFee            uint64 `gorm:"-:all"`
	ChargeTxCount       uint64
	ChargeFee           uint64
	ChargeNetFee        uint64
	ChargeNetUsage      uint64
	ChargeEnergyFee     uint64
	ChargeEnergyUsage   uint64
	CollectTxCount      uint64
	CollectFee          uint64
	CollectNetFee       uint64
	CollectNetUsage     uint64
	CollectEnergyFee    uint64
	CollectEnergyUsage  uint64
	WithdrawTxCount     uint64
	WithdrawFee         uint64
	WithdrawNetFee      uint64
	WithdrawNetUsage    uint64
	WithdrawEnergyFee   uint64
	WithdrawEnergyUsage uint64
}

func (o *ExchangeStatistic) Merge(other *ExchangeStatistic) {
	if other == nil {
		return
	}

	o.ChargeTxCount += other.ChargeTxCount
	o.ChargeFee += other.ChargeFee
	o.ChargeNetFee += other.ChargeNetFee
	o.ChargeNetUsage += other.ChargeNetUsage
	o.ChargeEnergyFee += other.ChargeEnergyFee
	o.ChargeEnergyUsage += other.ChargeEnergyUsage
	o.CollectTxCount += other.CollectTxCount
	o.CollectFee += other.CollectFee
	o.CollectNetFee += other.CollectNetFee
	o.CollectNetUsage += other.CollectNetUsage
	o.CollectEnergyFee += other.CollectEnergyFee
	o.CollectEnergyUsage += other.CollectEnergyUsage
	o.WithdrawTxCount += other.WithdrawTxCount
	o.WithdrawFee += other.WithdrawFee
	o.WithdrawNetFee += other.WithdrawNetFee
	o.WithdrawNetUsage += other.WithdrawNetUsage
	o.WithdrawEnergyFee += other.WithdrawEnergyFee
	o.WithdrawEnergyUsage += other.WithdrawEnergyUsage
}
