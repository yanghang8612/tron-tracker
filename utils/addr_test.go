package utils

import (
	"testing"

	"github.com/go-resty/resty/v2"
	"github.com/goccy/go-json"
	"go.uber.org/zap"
	"tron-tracker/types"
)

func TestTrimExchangeName(t *testing.T) {
	var exchangeList = types.ExchangeList{}
	resp, err := resty.New().R().Get("https://apilist.tronscanapi.com/api/hot/exchanges")
	if err != nil {
		zap.S().Error(err)
	} else {
		err = json.Unmarshal(resp.Body(), &exchangeList)
		if err != nil {
			zap.S().Error(err)
		}
	}

	names := make(map[string]bool)
	for _, exchange := range exchangeList.Exchanges {
		names[TrimExchangeName(exchange.Name)] = true
	}

	for name := range names {
		t.Log(name)
	}
}
