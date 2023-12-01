package types

type BlockHeader struct {
	RawData struct {
		Number         uint   `json:"number"`
		TxTrieRoot     string `json:"txTrieRoot"`
		WitnessAddress string `json:"witness_address"`
		ParentHash     string `json:"parentHash"`
		Version        uint   `json:"version"`
		Timestamp      int64  `json:"timestamp"`
	} `json:"raw_data"`
	WitnessSignature string `json:"witness_signature"`
}

type Block struct {
	BlockID      string        `json:"blockID"`
	BlockHeader  BlockHeader   `json:"block_header"`
	Transactions []Transaction `json:"transactions"`
}
