package net

import (
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"time"

	"tron-tracker/config"
	"tron-tracker/database/models"
	"tron-tracker/tron/types"

	"github.com/btcsuite/btcd/btcutil/base58"
	"github.com/go-resty/resty/v2"
	"go.uber.org/zap"
)

type slackMessage struct {
	Text string `json:"text"`
}

const (
	GetBlockPath                           = "/wallet/getblockbynum?num="
	GetNowBlockPath                        = "/wallet/getnowblock"
	GetAccountPath                         = "/wallet/getaccount"
	GetTransactionInfoListPath             = "/wallet/gettransactioninfobyblocknum?num="
	TriggerPath                            = "/wallet/triggerconstantcontract"
	GetDelegatedResourceAccountIndexV2Path = "/wallet/getdelegatedresourceaccountindexv2"
	GetDelegatedResourceV2Path             = "/wallet/getdelegatedresourcev2"
)

var (
	client  = resty.New()
	configs *config.NetConfig
)

func Init(cfg *config.NetConfig) {
	configs = cfg
	client.
		SetRetryCount(3).
		SetRetryWaitTime(500 * time.Millisecond).
		SetRetryMaxWaitTime(3 * time.Second)
}

func ReportTronlinkStatsToSlack(msg string) {
	resp, err := client.R().SetBody(&slackMessage{Text: msg}).Post(configs.TronlinkWebhook)
	if err != nil {
		zap.S().Error(err)
		return
	}

	zap.S().Infof("Report to slack: %s", resp)
}

func ReportWarningToSlack(msg string, atMe bool) {
	if atMe {
		msg += " <@U01DFGWQ2JK>"
	}
	resp, err := client.R().SetBody(&slackMessage{Text: msg}).Post(configs.WarningWebhook)
	if err != nil {
		zap.S().Error(err)
		return
	}

	zap.S().Infof("Report to slack: %s", resp)
}

func ReportNotificationToSlack(msg string, isWarning bool) {
	if !isWarning {
		ReportWarningToSlack(msg, false)
		return
	}

	resp, err := client.R().SetBody(&slackMessage{Text: msg + "\nPlease check this! <!channel>"}).Post(configs.NotifierWebhook)
	if err != nil {
		zap.S().Error(err)
		return
	}

	zap.S().Infof("Report to slack: %s", resp)
}

func GetNowBlock() (*types.Block, error) {
	url := configs.FullNode + GetNowBlockPath
	var block types.Block
	_, err := client.R().SetResult(&block).Get(url)
	return &block, err
}

func GetBlockByHeight(height uint) (*types.Block, error) {
	url := configs.FullNode + GetBlockPath + strconv.FormatInt(int64(height), 10)
	var block types.Block
	_, err := client.R().SetResult(&block).Get(url)
	return &block, err
}

func GetTransactionInfoList(height uint) ([]*types.TransactionInfo, error) {
	url := configs.FullNode + GetTransactionInfoListPath + strconv.FormatInt(int64(height), 10)
	var txInfoList = make([]*types.TransactionInfo, 0)
	_, err := client.R().SetResult(&txInfoList).Get(url)
	return txInfoList, err
}

func GetTransactionIdByBlockNumAndIndex(height uint, index uint16) (string, error) {
	txInfoList, err := GetTransactionInfoList(height)
	if err != nil {
		return "", err
	}

	if int(index) >= len(txInfoList) {
		return "", fmt.Errorf("index %d out of range for block height %d", index, height)
	}

	return txInfoList[index].ID, nil
}

type triggerRequest struct {
	OwnerAddress     string `json:"owner_address"`
	ContractAddress  string `json:"contract_address"`
	FunctionSelector string `json:"function_selector"`
	Parameter        string `json:"parameter"`
	Visible          bool   `json:"visible"`
}

type triggerResponse struct {
	Result    []string `json:"constant_result"`
	RpcResult struct {
		TriggerResult bool `json:"result"`
	} `json:"result"`
}

func Trigger(addr, selector, param string) (string, error) {
	resData, err := client.R().SetBody(&triggerRequest{
		OwnerAddress:     "T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb",
		ContractAddress:  addr,
		FunctionSelector: selector,
		Parameter:        param,
		Visible:          true,
	}).Post(configs.FullNode + TriggerPath)
	if err != nil {
		zap.S().Error(err)
		return "", err
	}

	var queryRes triggerResponse
	_ = json.Unmarshal(resData.Body(), &queryRes)

	if !queryRes.RpcResult.TriggerResult {
		return "", fmt.Errorf("query failed")
	}

	if len(queryRes.Result) > 0 {
		return queryRes.Result[0], nil
	}

	return "", fmt.Errorf("no return value")
}

func GetAccountBalance(address string) (*big.Int, error) {
	resData, err := client.R().SetBody(map[string]interface{}{
		"address": address,
		"visible": true,
	}).Post(configs.FullNode + GetAccountPath)
	if err != nil {
		return nil, err
	}

	var res struct {
		Balance int64 `json:"balance"`
	}
	_ = json.Unmarshal(resData.Body(), &res)
	return big.NewInt(res.Balance), nil
}

// FrozenResources holds the six raw components of an account's frozen TRX
// (in sun) needed for precise per-exchange stake accounting:
//   - Self*:         v1 self-frozen + v2 self-frozen still on this account
//   - DelegatedOut*: v1 + v2 stake this account has delegated out to others
//   - Acquired*:     v1 + v2 stake delegated to this account by others
//
// "Owned stake" of this account = Self + DelegatedOut.
// "Available stake" (java-tron getAllFrozenBalanceFor*) = Self + Acquired.
type FrozenResources struct {
	SelfBandwidth         int64
	DelegatedOutBandwidth int64
	AcquiredBandwidth     int64
	SelfEnergy            int64
	DelegatedOutEnergy    int64
	AcquiredEnergy        int64
}

// GetAccountFrozenResources returns the 6 frozen-TRX components (in sun)
// for the given address, derived from /wallet/getaccount.
func GetAccountFrozenResources(address string) (FrozenResources, error) {
	resData, err := client.R().SetBody(map[string]interface{}{
		"address": address,
		"visible": true,
	}).Post(configs.FullNode + GetAccountPath)
	if err != nil {
		return FrozenResources{}, err
	}

	var res struct {
		AcquiredDelegatedFrozenBalanceForBandwidth   int64 `json:"acquired_delegated_frozen_balance_for_bandwidth"`
		AcquiredDelegatedFrozenV2BalanceForBandwidth int64 `json:"acquired_delegated_frozenV2_balance_for_bandwidth"`
		DelegatedFrozenBalanceForBandwidth           int64 `json:"delegated_frozen_balance_for_bandwidth"`
		DelegatedFrozenV2BalanceForBandwidth         int64 `json:"delegated_frozenV2_balance_for_bandwidth"`
		Frozen                                       []struct {
			FrozenBalance int64 `json:"frozen_balance"`
		} `json:"frozen"`
		FrozenV2 []struct {
			Type   string `json:"type,omitempty"`
			Amount int64  `json:"amount"`
		} `json:"frozenV2"`
		AccountResource struct {
			FrozenBalanceForEnergy struct {
				FrozenBalance int64 `json:"frozen_balance"`
			} `json:"frozen_balance_for_energy"`
			AcquiredDelegatedFrozenBalanceForEnergy   int64 `json:"acquired_delegated_frozen_balance_for_energy"`
			AcquiredDelegatedFrozenV2BalanceForEnergy int64 `json:"acquired_delegated_frozenV2_balance_for_energy"`
			DelegatedFrozenBalanceForEnergy           int64 `json:"delegated_frozen_balance_for_energy"`
			DelegatedFrozenV2BalanceForEnergy         int64 `json:"delegated_frozenV2_balance_for_energy"`
		} `json:"account_resource"`
	}
	if err := json.Unmarshal(resData.Body(), &res); err != nil {
		return FrozenResources{}, err
	}

	out := FrozenResources{
		AcquiredBandwidth: res.AcquiredDelegatedFrozenBalanceForBandwidth +
			res.AcquiredDelegatedFrozenV2BalanceForBandwidth,
		DelegatedOutBandwidth: res.DelegatedFrozenBalanceForBandwidth +
			res.DelegatedFrozenV2BalanceForBandwidth,
		SelfEnergy: res.AccountResource.FrozenBalanceForEnergy.FrozenBalance,
		AcquiredEnergy: res.AccountResource.AcquiredDelegatedFrozenBalanceForEnergy +
			res.AccountResource.AcquiredDelegatedFrozenV2BalanceForEnergy,
		DelegatedOutEnergy: res.AccountResource.DelegatedFrozenBalanceForEnergy +
			res.AccountResource.DelegatedFrozenV2BalanceForEnergy,
	}
	for _, f := range res.Frozen {
		out.SelfBandwidth += f.FrozenBalance
	}
	for _, f := range res.FrozenV2 {
		switch f.Type {
		case "", "BANDWIDTH":
			out.SelfBandwidth += f.Amount
		case "ENERGY":
			out.SelfEnergy += f.Amount
		}
	}
	return out, nil
}

// GetAccountDelegatedTo returns the list of addresses to which `address` has
// outstanding stake-2.0 delegations (the `toAccounts` field of
// /wallet/getdelegatedresourceaccountindexv2). Returns nil (not error) when
// the account has no outgoing delegations.
func GetAccountDelegatedTo(address string) ([]string, error) {
	resData, err := client.R().SetBody(map[string]interface{}{
		"value":   address,
		"visible": true,
	}).Post(configs.FullNode + GetDelegatedResourceAccountIndexV2Path)
	if err != nil {
		return nil, err
	}

	var res struct {
		ToAccounts []string `json:"toAccounts"`
	}
	if err := json.Unmarshal(resData.Body(), &res); err != nil {
		return nil, err
	}
	return res.ToAccounts, nil
}

func GetTRC20Balance(contractAddr, ownerAddr string) (*big.Int, error) {
	decoded, _, err := base58.CheckDecode(ownerAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid address %s: %w", ownerAddr, err)
	}
	param := fmt.Sprintf("%064x", new(big.Int).SetBytes(decoded))

	result, err := Trigger(contractAddr, "balanceOf(address)", param)
	if err != nil {
		return nil, err
	}

	balance, ok := new(big.Int).SetString(result, 16)
	if !ok {
		return big.NewInt(0), nil
	}
	return balance, nil
}

func GetExchanges() *models.Exchanges {
	var exchanges = models.Exchanges{}
	resp, err := client.R().Get("https://apilist.tronscanapi.com/api/hot/exchanges")
	if err != nil {
		zap.S().Error(err)
	} else {
		err = json.Unmarshal(resp.Body(), &exchanges)
		if err != nil {
			zap.S().Error(err)
		}
	}

	for i := range exchanges.Val {
		// exchangeList.Exchanges[i].Name = common.TrimExchangeName(exchangeList.Exchanges[i].Name)

		if exchanges.Val[i].Name == "BtcTurk" {
			exchanges.Val[i].Address = "TCTYyc1w6rzqnqRBcAhuAJUyNWZ9Bw9hrW"
		}
	}
	return &exchanges
}

type MarketPairsResponse struct {
	Data struct {
		MarketPairs []struct {
			Rank                int     `json:"rank"`
			ExchangeName        string  `json:"exchangeName"`
			MarketPair          string  `json:"marketPair"`
			MarketReputation    float64 `json:"marketReputation"`
			Price               float64 `json:"price"`
			VolumeUsd           float64 `json:"volumeUsd"`
			EffectiveLiquidity  float64 `json:"effectiveLiquidity"`
			LastUpdated         string  `json:"lastUpdated"`
			Quote               float64 `json:"quote"`
			VolumeBase          float64 `json:"volumeBase"`
			VolumeQuote         float64 `json:"volumeQuote"`
			VolumePercent       float64 `json:"volumePercent"`
			DepthUsdPositiveTwo float64 `json:"depthUsdPositiveTwo"`
			DepthUsdNegativeTwo float64 `json:"depthUsdNegativeTwo"`
		} `json:"marketPairs"`
	} `json:"data"`
}

func GetMarketPairs(token, slug string) (string, []*models.MarketPairStatistic, error) {
	resp, err := client.R().Get(fmt.Sprintf("https://api.coinmarketcap.com/data-api/v3/cryptocurrency/market-pairs/latest?slug=%s&start=1&limit=1000&category=spot&centerType=all&sort=cmc_rank_advanced&direction=desc&spotUntracked=true", slug))
	if err != nil {
		return "", nil, err
	}

	if resp.StatusCode() != 200 {
		return "", nil, fmt.Errorf("failed to get market pairs for %s: status code %d", token, resp.StatusCode())
	}

	var response MarketPairsResponse
	err = json.Unmarshal(resp.Body(), &response)
	if err != nil {
		return "", nil, err
	}

	var marketPairs = make([]*models.MarketPairStatistic, 0)

	for _, marketPair := range response.Data.MarketPairs {
		marketPairs = append(marketPairs, &models.MarketPairStatistic{
			Datetime:            time.Now().Format("021504"),
			Token:               token,
			ExchangeName:        marketPair.ExchangeName,
			Pair:                marketPair.MarketPair,
			Reputation:          marketPair.MarketReputation,
			Price:               marketPair.Price,
			Volume:              marketPair.VolumeUsd,
			Percent:             marketPair.VolumePercent,
			DepthUsdPositiveTwo: marketPair.DepthUsdPositiveTwo,
			DepthUsdNegativeTwo: marketPair.DepthUsdNegativeTwo,
		})
	}

	return string(resp.Body()), marketPairs, nil
}

type TokenListingsResponse struct {
	Data []Coin `json:"data"`
}

type Coin struct {
	ID     uint   `json:"id"`
	Symbol string `json:"symbol"`
	Quote  struct {
		USD struct {
			Price           float64 `json:"price"`
			Volume24h       float64 `json:"volume_24h"`
			VolumeChange24h float64 `json:"volume_change_24h"`
			MarketCap       float64 `json:"market_cap"`
		} `json:"USD"`
	} `json:"quote"`
}

func GetTokenListings() (string, []*models.TokenListingStatistic, error) {
	resp, err := client.R().
		SetHeader("X-CMC_PRO_API_KEY", configs.CMCApiKey).
		Get("https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?limit=1000")
	if err != nil {
		return "", nil, err
	}

	var response TokenListingsResponse
	err = json.Unmarshal(resp.Body(), &response)
	if err != nil {
		return "", nil, err
	}

	var tokenListings = make([]*models.TokenListingStatistic, 0)
	for _, tokenListing := range response.Data {
		tokenListings = append(tokenListings, &models.TokenListingStatistic{
			Datetime:        time.Now().Format("021504"),
			CMCID:           tokenListing.ID,
			Token:           tokenListing.Symbol,
			Price:           tokenListing.Quote.USD.Price,
			Volume24H:       tokenListing.Quote.USD.Volume24h,
			VolumeChange24H: tokenListing.Quote.USD.VolumeChange24h,
			MarketCap:       tokenListing.Quote.USD.MarketCap,
		})
	}

	return string(resp.Body()), tokenListings, nil
}

func GetFees(startDate time.Time, days int) map[string]float64 {
	resp, err := client.R().Get(fmt.Sprintf("%s/transfer_fee?start_date=%s&days=%d",
		configs.FeeNode, startDate.Format("060102"), days))

	if err != nil {
		zap.S().Error(err)
		return nil
	}

	var fees = make(map[string]float64)
	err = json.Unmarshal(resp.Body(), &fees)
	if err != nil {
		zap.S().Error(err)
		return nil
	}

	return fees
}

type USDTSupplyResponse struct {
	DataFormatted []struct {
		ID          uint        `json:"id"`
		ISO         string      `json:"iso"`
		Name        string      `json:"name"`
		BlockChains []ChainData `json:"blockChains"`
	} `json:"data_formatted"`
}

type ChainData struct {
	TotalAuthorized number `json:"totalAuthorized"`
	NotIssued       number `json:"notIssued"`
	Quarantined     number `json:"quarantined"`
	Name            string `json:"name"`
}

// Number is a custom type that can handle JSON values
// which are either numeric (e.g. 123, 45.67) or string-formatted numbers (e.g. "123", "45.67").
// Underlying storage is float64.
type number float64

// UnmarshalJSON implements the json.Unmarshaler interface.
// It tries to parse the raw data first as a JSON number; if that fails,
// it tries to parse it as a JSON string and then converts the string to float64.
func (n *number) UnmarshalJSON(data []byte) error {
	// Attempt to unmarshal as a JSON number.
	var num float64
	if err := json.Unmarshal(data, &num); err == nil {
		*n = number(num)
		return nil
	}

	// If parsing as number failed, attempt to unmarshal as a JSON string.
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return fmt.Errorf("Number.UnmarshalJSON: unable to parse %s as number or string", string(data))
	}

	// Convert the string to float64.
	parsed, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return fmt.Errorf("Number.UnmarshalJSON: failed to convert string %q to float64: %w", str, err)
	}
	*n = number(parsed)
	return nil
}

func GetUSDTSupply() ([]*models.USDTSupplyStatistic, error) {
	resp, err := client.R().Get("https://app.tether.to/transparency.json")
	if err != nil {
		return nil, err
	}

	var response USDTSupplyResponse
	err = json.Unmarshal(resp.Body(), &response)
	if err != nil {
		return nil, err
	}

	var USDTSupply = make([]*models.USDTSupplyStatistic, 0)
	for _, chainData := range response.DataFormatted {
		if chainData.ISO == "usdt" {
			for _, data := range chainData.BlockChains {
				USDTSupply = append(USDTSupply, &models.USDTSupplyStatistic{
					Datetime:        time.Now().Format("06010215"),
					Chain:           data.Name,
					TotalAuthorized: float64(data.TotalAuthorized),
					NotIssued:       float64(data.NotIssued),
					Quarantined:     float64(data.Quarantined),
				})
			}
		}
	}

	return USDTSupply, nil
}

type AlphaVantageResponse struct {
	TimeSeries map[string]struct {
		Open   string `json:"1. open"`
		High   string `json:"2. high"`
		Low    string `json:"3. low"`
		Close  string `json:"4. close"`
		Volume string `json:"5. volume"`
	} `json:"Time Series (Daily)"`
}

func GetStockData(date time.Time, days int) [][]interface{} {
	symbol := "TRON"
	url := fmt.Sprintf(
		"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=%s&outputsize=compact&apikey=%s",
		symbol, configs.AlphaVantageApiKey,
	)

	resp, err := client.R().Get(url)
	if err != nil {
		zap.S().Errorf("Failed to fetch stock data for %s: %v", symbol, err)
		return nil
	}

	var response AlphaVantageResponse
	if err = json.Unmarshal(resp.Body(), &response); err != nil {
		zap.S().Errorf("Failed to parse stock data for %s: %v", symbol, err)
		return nil
	}

	// Collect and sort dates
	dates := make([]string, 0, len(response.TimeSeries))
	for d := range response.TimeSeries {
		dates = append(dates, d)
	}
	sort.Strings(dates)

	// Filter dates up to the given date and take the last `days` entries
	endDate := date.Format("2006-01-02")
	var filtered []string
	for _, d := range dates {
		if d <= endDate {
			filtered = append(filtered, d)
		}
	}
	if len(filtered) > days {
		filtered = filtered[len(filtered)-days:]
	}

	var stockData [][]interface{}
	for _, d := range filtered {
		ts := response.TimeSeries[d]
		open, _ := strconv.ParseFloat(ts.Open, 64)
		high, _ := strconv.ParseFloat(ts.High, 64)
		low, _ := strconv.ParseFloat(ts.Low, 64)
		close, _ := strconv.ParseFloat(ts.Close, 64)
		volume, _ := strconv.ParseFloat(ts.Volume, 64)

		stockData = append(stockData, []interface{}{d[5:], open, high, low, close, volume})
	}

	return stockData
}

// subtractBusinessDays days from the given date, skipping weekends. Start and end dates are inclusive.
func subtractBusinessDays(date time.Time, days int) time.Time {
	d := date
	count := 0
	for {
		wd := d.Weekday()
		if wd != time.Saturday && wd != time.Sunday {
			count++

			if count >= days {
				break
			}
		}
		d = d.AddDate(0, 0, -1)
	}
	return d
}
