package models

type Charger struct {
	ID            uint   `gorm:"primaryKey"`
	Address       string `gorm:"size:34;index"`
	ExchangeName  string
	BackupAddress string `gorm:"size:34"`
	IsFake        bool
}

type Phisher struct {
	ID      uint   `gorm:"primaryKey"`
	Address string `gorm:"size:34;index"`
}

type EthUSDTUser struct {
	ID          uint   `gorm:"primaryKey"`
	Address     string `gorm:"size:42;index"`
	Amount      uint64 `gorm:"index"`
	TransferIn  uint
	TransferOut uint
	Dirty       bool `gorm:"-:all"`
}

func (e *EthUSDTUser) Add(o *EthUSDTUser) {
	e.Amount += o.Amount
	e.TransferIn += o.TransferIn
	e.TransferOut += o.TransferOut
}
