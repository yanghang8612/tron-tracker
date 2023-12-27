package models

type UserStatistic struct {
	ID                uint   `gorm:"primaryKey"`
	Address           string `gorm:"size:34;uniqueIndex"`
	EnergyTotal       uint
	EnergyFee         uint
	EnergyUsage       uint
	EnergyOriginUsage uint
	NetUsage          uint
	NetFee            uint
	OtherFee          uint
	TransactionTotal  uint
	TRXTotal          uint
	TRC10Total        uint
	SCTotal           uint
}

func NewUserStatistic(address string, tx *Transaction) *UserStatistic {
	var stats = &UserStatistic{
		Address:           address,
		EnergyTotal:       tx.EnergyTotal,
		EnergyFee:         tx.EnergyFee,
		EnergyUsage:       tx.EnergyUsage,
		EnergyOriginUsage: tx.EnergyOriginUsage,
		NetUsage:          tx.NetUsage,
		NetFee:            tx.NetFee,
		OtherFee:          tx.Fee - tx.EnergyFee - tx.NetFee,
		TransactionTotal:  1,
	}
	switch tx.Type {
	case 1:
		stats.TRXTotal = 1
	case 2:
		stats.TRC10Total = 1
	case 30, 31:
		stats.SCTotal = 1
	}
	return stats
}

func (o *UserStatistic) Merge(other *UserStatistic) {
	o.EnergyTotal += other.EnergyTotal
	o.EnergyFee += other.EnergyFee
	o.EnergyUsage += other.EnergyUsage
	o.EnergyOriginUsage += other.EnergyOriginUsage
	o.NetUsage += other.NetUsage
	o.NetFee += other.NetFee
	o.OtherFee += other.OtherFee
	o.TransactionTotal += other.TransactionTotal
	o.TRXTotal += other.TRXTotal
	o.TRC10Total += other.TRC10Total
	o.SCTotal += other.SCTotal
}

func (o *UserStatistic) Add(tx *Transaction) {
	o.EnergyTotal += tx.EnergyTotal
	o.EnergyFee += tx.EnergyFee
	o.EnergyUsage += tx.EnergyUsage
	o.EnergyOriginUsage += tx.EnergyOriginUsage
	o.NetUsage += tx.NetUsage
	o.NetFee += tx.NetFee
	o.OtherFee += tx.Fee - tx.EnergyFee - tx.NetFee
	o.TransactionTotal++
	switch tx.Type {
	case 1:
		o.TRXTotal++
	case 2:
		o.TRC10Total++
	case 30, 31:
		o.SCTotal++
	}
}

type ExchangeStatistic struct {
	ID                  uint   `gorm:"primaryKey"`
	Date                string `gorm:"index"`
	Name                string
	Address             string
	ChargeTxCount       uint
	ChargeNetFee        uint
	ChargeNetUsage      uint
	ChargeEnergyFee     uint
	ChargeEnergyUsage   uint
	CollectTxCount      uint
	CollectNetFee       uint
	CollectNetUsage     uint
	CollectEnergyFee    uint
	CollectEnergyUsage  uint
	WithdrawTxCount     uint
	WithdrawNetFee      uint
	WithdrawNetUsage    uint
	WithdrawEnergyFee   uint
	WithdrawEnergyUsage uint
}

func (o *ExchangeStatistic) Merge(other *ExchangeStatistic) {
	o.ChargeTxCount += other.ChargeTxCount
	o.ChargeNetFee += other.ChargeNetFee
	o.ChargeNetUsage += other.ChargeNetUsage
	o.ChargeEnergyFee += other.ChargeEnergyFee
	o.ChargeEnergyUsage += other.ChargeEnergyUsage
	o.CollectTxCount += other.CollectTxCount
	o.CollectNetFee += other.CollectNetFee
	o.CollectNetUsage += other.CollectNetUsage
	o.CollectEnergyFee += other.CollectEnergyFee
	o.CollectEnergyUsage += other.CollectEnergyUsage
	o.WithdrawTxCount += other.WithdrawTxCount
	o.WithdrawNetFee += other.WithdrawNetFee
	o.WithdrawNetUsage += other.WithdrawNetUsage
	o.WithdrawEnergyFee += other.WithdrawEnergyFee
	o.WithdrawEnergyUsage += other.WithdrawEnergyUsage
}
