package net

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// NetworkStakeTotals are the network-wide staking weights in TRX, not sun
// (unlike ExchangeResourceStatistic). They include both staking versions.
type NetworkStakeTotals struct {
	BandwidthTRX int64
	EnergyTRX    int64
}

// GetNetworkStakeTotals queries current network weights. The account is only
// needed by getaccountresource; the two total weights are network-wide.
func GetNetworkStakeTotals() (NetworkStakeTotals, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	resp, err := gridClient.R().SetContext(ctx).SetBody(map[string]interface{}{
		"address": "T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb",
		"visible": true,
	}).Post(configs.FullNode + "/wallet/getaccountresource")
	if err != nil {
		return NetworkStakeTotals{}, fmt.Errorf("get network staking weights: %w", err)
	}
	if resp.IsError() {
		return NetworkStakeTotals{}, fmt.Errorf("get network staking weights: HTTP %d", resp.StatusCode())
	}
	var result struct {
		Bandwidth json.Number `json:"TotalNetWeight"`
		Energy    json.Number `json:"TotalEnergyWeight"`
		Error     string      `json:"Error"`
	}
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return NetworkStakeTotals{}, fmt.Errorf("decode network staking weights: %w", err)
	}
	if result.Error != "" {
		return NetworkStakeTotals{}, fmt.Errorf("get network staking weights: %s", result.Error)
	}
	bw, bwErr := result.Bandwidth.Int64()
	en, enErr := result.Energy.Int64()
	if bwErr != nil || enErr != nil || bw <= 0 || en <= 0 {
		return NetworkStakeTotals{}, fmt.Errorf("invalid network staking weights: bandwidth=%q energy=%q", result.Bandwidth, result.Energy)
	}
	return NetworkStakeTotals{BandwidthTRX: bw, EnergyTRX: en}, nil
}
