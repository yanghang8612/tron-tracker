package net

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Resty's SetResult does not report non-2xx responses as Go errors. Do not
// mistake an HTTP failure or malformed body for an empty transaction array.
func getTrackerJSON(url string, result interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	resp, err := gridClient.R().SetContext(ctx).Get(url)
	if err != nil {
		return err
	}
	if resp.IsError() {
		return fmt.Errorf("fullnode request returned HTTP %d", resp.StatusCode())
	}
	if strings.TrimSpace(string(resp.Body())) == "null" {
		return fmt.Errorf("fullnode returned null instead of query data")
	}
	var failure struct {
		Error string `json:"Error"`
	}
	if json.Unmarshal(resp.Body(), &failure) == nil && failure.Error != "" {
		// Keep diagnostics bounded even if upstream returns a large error body.
		message := failure.Error
		if len(message) > 256 {
			message = message[:256] + "..."
		}
		return fmt.Errorf("fullnode API error: %s", message)
	}
	if err := json.Unmarshal(resp.Body(), result); err != nil {
		return fmt.Errorf("decode fullnode response: %w", err)
	}
	return nil
}
