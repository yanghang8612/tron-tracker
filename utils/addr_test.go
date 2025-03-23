package utils

import (
	"testing"

	"github.com/go-resty/resty/v2"
	"github.com/goccy/go-json"
	"go.uber.org/zap"
	"tron-tracker/database/models"
)

func TestTrimExchangeName(t *testing.T) {
	var exchanges = models.Exchanges{}
	resp, err := resty.New().R().Get("https://apilist.tronscanapi.com/api/hot/exchanges")
	if err != nil {
		zap.S().Error(err)
	} else {
		err = json.Unmarshal(resp.Body(), &exchanges)
		if err != nil {
			zap.S().Error(err)
		}
	}

	names := make(map[string]bool)
	for _, exchange := range exchanges.Val {
		names[TrimExchangeName(exchange.OriginName)] = true
	}

	for name := range names {
		t.Log(name)
	}
}
