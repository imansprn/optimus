package config

import (
    "log"
    "strings"

    "github.com/spf13/viper"
)

type Config struct {
    UpstreamHost    string `mapstructure:"upstream_host"`
    UpstreamPort    int    `mapstructure:"upstream_port"`
    UpstreamSender  string `mapstructure:"upstream_sender"`
    UpstreamTarget  string `mapstructure:"upstream_target"`
    UpstreamUser    string `mapstructure:"upstream_user"`
    UpstreamPass    string `mapstructure:"upstream_pass"`

    ListenAddr      string `mapstructure:"listen_addr"`
    AcceptorSender  string `mapstructure:"acceptor_sender"`

    LogLevel        string `mapstructure:"log_level"`
    LogFormat       string `mapstructure:"log_format"`
}

func Load() *Config {
    v := viper.New()

    // Default values
    v.SetDefault("upstream_host", "pricing.primexm.com")
    v.SetDefault("upstream_port", 9879)
    v.SetDefault("upstream_sender", "GW01")
    v.SetDefault("upstream_target", "PXMD")
    v.SetDefault("listen_addr", "0.0.0.0:9878")
    v.SetDefault("acceptor_sender", "GW_GATEWAY")
    v.SetDefault("log_level", "info")
    v.SetDefault("log_format", "console")

    // Config file
    v.SetConfigName("config")
    v.SetConfigType("yaml")
    v.AddConfigPath(".")
    v.AddConfigPath("./config")

    // Environment variables
    v.SetEnvPrefix("OPTIMUS")
    v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
    v.AutomaticEnv()

    if err := v.ReadInConfig(); err != nil {
        if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
            log.Printf("Error reading config file: %v", err)
        }
    }

    var cfg Config
    if err := v.Unmarshal(&cfg); err != nil {
        log.Fatalf("Unable to decode into struct, %v", err)
    }

    return &cfg
}
