package config

import (
	"fmt"

	"github.com/BurntSushi/toml"
)

type BotConfig struct {
	InfuraToken     string   `toml:"infura_token"`
	AlertBotToken   string   `toml:"alert_bot_token"`
	TrackerBotToken string   `toml:"tracker_bot_token"`
	VolumeBotToken  string   `toml:"volume_bot_token"`
	ValidUsers      []string `toml:"valid_users"`
}

type PPTConfig struct {
	SlideID   string `toml:"slide_id"`
	VolumeID  string `toml:"volume_id"`
	RevenueID string `toml:"revenue_id"`
}

type ServerConfig struct {
	HttpPort int `toml:"http_port"`
}

type NetConfig struct {
	FullNode        string `toml:"full_node"`
	FeeNode         string `toml:"fee_node"`
	TronlinkWebhook string `toml:"tronlink_webhook"`
	WarningWebhook  string `toml:"warning_webhook"`
	NotifierWebhook string `toml:"notifier_webhook"`
	CMCApiKey       string `toml:"cmc_api_key"`
}

type LogConfig struct {
	Path  string `toml:"log_path"`
	File  string `toml:"log_file"`
	Level string `toml:"log_level"`
}

type DeFiConfig struct {
	SunSwapV1  []string `toml:"sunswap_v1"`
	SunSwapV2  []string `toml:"sunswap_v2"`
	SunSwapV3  []string `toml:"sunswap_v3"`
	SunPump    []string `toml:"sunpump"`
	JustLend   []string `toml:"justlend"`
	BTTC       []string `toml:"bttc"`
	USDTCasino []string `toml:"usdtcasino"`
}

type DBConfig struct {
	Host        string     `toml:"host"`
	DB          string     `toml:"db"`
	User        string     `toml:"user"`
	Password    string     `toml:"password"`
	StartDate   string     `toml:"start_date"`
	StartNum    int        `toml:"start_num"`
	ValidTokens [][]string `toml:"valid_tokens"`
}

type Config struct {
	Bot    BotConfig    `toml:"bot"`
	PPT    PPTConfig    `toml:"ppt"`
	Server ServerConfig `toml:"server"`
	Net    NetConfig    `toml:"net"`
	Log    LogConfig    `toml:"log"`
	DB     DBConfig     `toml:"database"`
	DeFi   DeFiConfig   `toml:"defi"`
}

func LoadConfig() *Config {
	var config Config
	data, err := toml.DecodeFile("./config.toml", &config)
	if err != nil {
		fmt.Println(data, err)
	}
	return &config
}
