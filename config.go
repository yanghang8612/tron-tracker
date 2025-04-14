package main

import (
	"fmt"

	"github.com/BurntSushi/toml"
	"tron-tracker/api"
	"tron-tracker/database"
	"tron-tracker/log"
	"tron-tracker/net"
)

type Config struct {
	BotToken string           `toml:"bot_token"`
	Server   api.ServerConfig `toml:"server"`
	Net      net.Config       `toml:"net"`
	Log      log.Config       `toml:"log"`
	DB       database.Config  `toml:"database"`
	DeFi     api.DeFiConfig   `toml:"defi"`
}

func loadConfig() *Config {
	var config Config
	data, err := toml.DecodeFile("./config.toml", &config)
	if err != nil {
		fmt.Println(data, err)
	}
	return &config
}
