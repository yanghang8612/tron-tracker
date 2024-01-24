package types

type Transaction struct {
	RawData struct {
		Contract []struct {
			Parameter struct {
				Value   map[string]interface{} `json:"value"`
				TypeUrl string                 `json:"type_url"`
			} `json:"parameter"`
			Type string `json:"type"`
		} `json:"contract"`
		RefBlockBytes string `json:"ref_block_bytes"`
		RefBlockHash  string `json:"ref_block_hash"`
		Expiration    uint   `json:"expiration"`
		Timestamp     uint   `json:"timestamp"`
	} `json:"raw_data"`
	Signature  []string `json:"signature"`
	TxID       string   `json:"txID"`
	RawDataHex string   `json:"raw_data_hex"`
}

func ConvertType(contractType string) uint8 {
	switch contractType {
	case "AccountCreateContract":
		return 0
	case "TransferContract":
		return 1
	case "TransferAssetContract":
		return 2
	case "VoteAssetContract":
		return 3
	case "VoteWitnessContract":
		return 4
	case "WitnessCreateContract":
		return 5
	case "AssetIssueContract":
		return 6
	case "WitnessUpdateContract":
		return 8
	case "ParticipateAssetIssueContract":
		return 9
	case "AccountUpdateContract":
		return 10
	case "FreezeBalanceContract":
		return 11
	case "UnfreezeBalanceContract":
		return 12
	case "WithdrawBalanceContract":
		return 13
	case "UnfreezeAssetContract":
		return 14
	case "UpdateAssetContract":
		return 15
	case "ProposalCreateContract":
		return 16
	case "ProposalApproveContract":
		return 17
	case "ProposalDeleteContract":
		return 18
	case "SetAccountIdContract":
		return 19
	case "CustomContract":
		return 20
	case "CreateSmartContract":
		return 30
	case "TriggerSmartContract":
		return 31
	case "GetContract":
		return 32
	case "UpdateSettingContract":
		return 33
	case "ExchangeCreateContract":
		return 41
	case "ExchangeInjectContract":
		return 42
	case "ExchangeWithdrawContract":
		return 43
	case "ExchangeTransactionContract":
		return 44
	case "UpdateEnergyLimitContract":
		return 45
	case "AccountPermissionUpdateContract":
		return 46
	case "ClearABIContract":
		return 48
	case "UpdateBrokerageContract":
		return 49
	case "ShieldedTransferContract":
		return 51
	case "MarketSellAssetContract":
		return 52
	case "MarketCancelOrderContract":
		return 53
	case "FreezeBalanceV2Contract":
		return 54
	case "UnfreezeBalanceV2Contract":
		return 55
	case "WithdrawExpireUnfreezeContract":
		return 56
	case "DelegateResourceContract":
		return 57
	case "UnDelegateResourceContract":
		return 58
	case "CancelAllUnfreezeV2Contract":
		return 59
	default:
		return 255
	}
}

type Receipt struct {
	EnergyUsage       uint64 `json:"energy_usage"`
	EnergyFee         uint64 `json:"energy_fee"`
	OriginEnergyUsage uint64 `json:"origin_energy_usage"`
	EnergyUsageTotal  uint64 `json:"energy_usage_total"`
	NetUsage          uint64 `json:"net_usage"`
	NetFee            uint64 `json:"net_fee"`
	Result            string `json:"result"`
}

type Log struct {
	Address string   `json:"address"`
	Topics  []string `json:"topics"`
	Data    string   `json:"data"`
}

type TransactionInfo struct {
	ID                     string   `json:"id"`
	Fee                    uint64   `json:"fee"`
	BlockNumber            uint     `json:"blockNumber"`
	BlockTimeStamp         int64    `json:"blockTimeStamp"`
	ContractResult         []string `json:"contractResult"`
	ContractAddress        string   `json:"contract_address"`
	Receipt                Receipt  `json:"receipt"`
	Log                    []Log    `json:"log"`
	Result                 string   `json:"result"`
	WithdrawAmount         uint64   `json:"withdraw_amount"`
	UnfreezeAmount         uint64   `json:"unfreeze_amount"`
	WithdrawExpireAmount   uint64   `json:"withdraw_expire_amount"`
	CancelUnfreezeV2Amount []struct {
		Key   string `json:"key"`
		Value uint64 `json:"value"`
	} `json:"cancel_unfreezeV2_amount"`
}
