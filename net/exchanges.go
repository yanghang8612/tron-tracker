package net

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"tron-tracker/database/models"
)

func GetExchanges() (*models.Exchanges, error) {
	return getExchanges("https://apilist.tronscanapi.com/api/hot/exchanges")
}

// Return errors instead of disguising failed/invalid responses as empty lists.
// The caller owns logging/backoff; never log the whole upstream response.
func getExchanges(url string) (*models.Exchanges, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	resp, err := client.R().SetContext(ctx).Get(url)
	if err != nil {
		return nil, fmt.Errorf("fetch exchanges: %w", err)
	}
	if resp.IsError() {
		return nil, fmt.Errorf("fetch exchanges: HTTP %d", resp.StatusCode())
	}
	var response struct {
		Exchanges []*struct {
			Address    string `json:"address"`
			Tag        string `json:"tag"`
			Name       string `json:"name"`
			OriginName string `json:"origin_name"`
			FromAsuka  bool   `json:"from_asuka"`
		} `json:"exchanges"`
	}
	if err := json.Unmarshal(resp.Body(), &response); err != nil {
		return nil, fmt.Errorf("decode exchanges: %w", err)
	}
	if len(response.Exchanges) == 0 {
		return nil, fmt.Errorf("fetch exchanges: empty exchange list")
	}
	exchanges := &models.Exchanges{Val: make([]*models.Exchange, 0, len(response.Exchanges))}
	for i, item := range response.Exchanges {
		if item == nil || strings.TrimSpace(item.Address) == "" {
			return nil, fmt.Errorf("fetch exchanges: invalid entry at index %d", i)
		}
		// New responses have address/name; older ones used tag/origin_name.
		// Preserve the absence of origin_name so short tags cannot overwrite
		// the caller's existing descriptive names.
		name := strings.TrimSpace(item.Tag)
		if name == "" {
			name = strings.TrimSpace(item.Name)
		}
		origin := strings.TrimSpace(item.OriginName)
		if name == "" {
			name = origin
		}
		if name == "" {
			return nil, fmt.Errorf("fetch exchanges: entry %d has no name", i)
		}
		address := item.Address
		if name == "BtcTurk" {
			address = "TCTYyc1w6rzqnqRBcAhuAJUyNWZ9Bw9hrW"
		}
		exchanges.Val = append(exchanges.Val, &models.Exchange{
			Address: address, Name: name, OriginName: origin, FromAsuka: item.FromAsuka,
		})
	}
	return exchanges, nil
}
