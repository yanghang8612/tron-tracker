package models

type Exchange struct {
	ID         uint   `gorm:"primaryKey"  json:"-"`
	Address    string `gorm:"size:34;index" json:"address"`
	Name       string `json:"tag"`
	OriginName string `json:"origin_name"`
	FromAsuka  bool   `json:"from_asuka"`
}

type Exchanges struct {
	Val []*Exchange `json:"exchanges"`
}
