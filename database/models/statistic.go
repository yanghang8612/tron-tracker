package models

type UserStatistic struct {
	ID                uint   `gorm:"primaryKey"`
	Address           string `gorm:"size:34;uniqueIndex"`
	Fee               uint
	EnergyTotal       uint
	EnergyFee         uint
	EnergyUsage       uint
	EnergyOriginUsage uint
	NetUsage          uint
	NetFee            uint
	TXTotal           uint
	TRXTotal          uint
	TRC10Total        uint
	TRC20Total        uint
	SCTotal           uint
	StakeTotal        uint
	DelegateTotal     uint
	VoteTotal         uint
	MultiSigTotal     uint
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
	case 2:
		stats.TRC10Total = 1
	case 3:
		stats.VoteTotal = 1
	case 11, 12, 54, 55, 59:
		stats.StakeTotal = 1
	case 30, 31:
		stats.SCTotal = 1
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
	o.TRC20Total += other.TRC20Total
	o.SCTotal += other.SCTotal
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
	case 2:
		o.TRC10Total++
	case 3:
		o.VoteTotal++
	case 11, 12, 54, 55, 59:
		o.StakeTotal++
	case 30, 31:
		o.SCTotal++
	case 57, 58:
		o.DelegateTotal++
	}
	if tx.SigCount > 1 {
		o.MultiSigTotal++
	}
}

type ExchangeStatistic struct {
	ID                  uint   `gorm:"primaryKey"`
	Date                string `gorm:"index"`
	Name                string
	Address             string
	ChargeTxCount       uint
	ChargeFee           uint
	ChargeNetFee        uint
	ChargeNetUsage      uint
	ChargeEnergyFee     uint
	ChargeEnergyUsage   uint
	CollectTxCount      uint
	CollectFee          uint
	CollectNetFee       uint
	CollectNetUsage     uint
	CollectEnergyFee    uint
	CollectEnergyUsage  uint
	WithdrawTxCount     uint
	WithdrawFee         uint
	WithdrawNetFee      uint
	WithdrawNetUsage    uint
	WithdrawEnergyFee   uint
	WithdrawEnergyUsage uint
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
