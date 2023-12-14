package models

type Statistic struct {
	ID                uint   `gorm:"primaryKey"`
	Owner             string `gorm:"char(21),uniqueIndex"`
	EnergyTotal       uint
	EnergyFee         uint
	EnergyUsage       uint
	EnergyOriginUsage uint
	NetUsage          uint
	NetFee            uint
	TransactionTotal  uint
	TRXTotal          uint
	TRC10Total        uint
	SCTotal           uint
}

func NewStats(owner string, tx *Transaction) *Statistic {
	var stats = &Statistic{
		Owner:             owner,
		EnergyTotal:       tx.EnergyTotal,
		EnergyFee:         tx.EnergyFee,
		EnergyUsage:       tx.EnergyUsage,
		EnergyOriginUsage: tx.EnergyOriginUsage,
		NetUsage:          tx.NetUsage,
		NetFee:            tx.NetFee,
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

func (o *Statistic) Merge(other *Statistic) {
	o.EnergyTotal += other.EnergyTotal
	o.EnergyFee += other.EnergyFee
	o.EnergyUsage += other.EnergyUsage
	o.EnergyOriginUsage += other.EnergyOriginUsage
	o.NetUsage += other.NetUsage
	o.NetFee += other.NetFee
	o.TransactionTotal += other.TransactionTotal
	o.TRXTotal += other.TRXTotal
	o.TRC10Total += other.TRC10Total
	o.SCTotal += other.SCTotal
}

func (o *Statistic) Add(tx *Transaction) {
	o.EnergyTotal += tx.EnergyTotal
	o.EnergyFee += tx.EnergyFee
	o.EnergyUsage += tx.EnergyUsage
	o.EnergyOriginUsage += tx.EnergyOriginUsage
	o.NetUsage += tx.NetUsage
	o.NetFee += tx.NetFee
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
