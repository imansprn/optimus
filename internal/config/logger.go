package config

import (
    "io"
    "os"
    "time"

    "github.com/rs/zerolog"
    "github.com/rs/zerolog/log"
)

// InitLogger initializes the global zerolog logger based on config.
func InitLogger(cfg *Config) {
    // 1. Set log level
    level, err := zerolog.ParseLevel(cfg.LogLevel)
    if err != nil {
        level = zerolog.InfoLevel
    }
    zerolog.SetGlobalLevel(level)

    // 2. Set time format
    zerolog.TimeFieldFormat = time.RFC3339

    // 3. Set output format
    var out io.Writer = os.Stderr
    if cfg.LogFormat == "console" {
        out = zerolog.ConsoleWriter{
            Out:        os.Stderr,
            TimeFormat: "15:04:05",
        }
    }

    log.Logger = zerolog.New(out).With().Timestamp().Caller().Logger()
}
