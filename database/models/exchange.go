package models

type Exchange struct {
	ID         uint   `gorm:"primaryKey"  json:"-"`
	Address    string `gorm:"size:34;index" json:"address"`
	Name       string
	OriginName string `json:"name"`
	FromAsuka  bool   `json:"-"`
}

type Exchanges struct {
	Val []*Exchange `json:"exchanges"`
}
