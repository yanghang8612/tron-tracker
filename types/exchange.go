package types

type Exchange struct {
	Address string `json:"address"`
	Name    string `json:"name"`
}

type ExchangeList struct {
	Exchanges []Exchange `json:"exchanges"`
}

func (el *ExchangeList) Contains(address string) bool {
	for _, exchange := range el.Exchanges {
		if exchange.Address == address {
			return true
		}
	}
	return false
}

func (el *ExchangeList) Get(address string) Exchange {
	for _, exchange := range el.Exchanges {
		if exchange.Address == address {
			return exchange
		}
	}
	return Exchange{}
}
