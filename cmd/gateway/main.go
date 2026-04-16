package main

import (
    "context"
    "github.com/imansprn/optimus/internal/config"
    "github.com/imansprn/optimus/internal/router"
    "github.com/imansprn/optimus/internal/session"
    "os"
    "os/signal"
    "syscall"

    "github.com/prometheus/client_golang/prometheus/promhttp"
    "net/http"

    "github.com/rs/zerolog/log"
)

func main() {
    cfg := config.Load()
    config.InitLogger(cfg)
    
    log.Info().Msg("Starting PrimeXM FIX Gateway...")

    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()

    // 1. Create Router (The heart)
    r := router.NewRouter()

    // 2. Create Upstream Initiator
    upstream := session.NewUpstreamSession(
        cfg.UpstreamHost,
        cfg.UpstreamPort,
        cfg.UpstreamSender,
        cfg.UpstreamTarget,
        cfg.UpstreamUser,
        cfg.UpstreamPass,
        30, // HeartBtInt
        r.OnUpstreamMessage,
    )

    // Wire upstream into router
    r.SetUpstream(upstream)

    // 3. Create Downstream Acceptor
    acceptor := session.NewAcceptor(
        cfg.ListenAddr,
        cfg.AcceptorSender,
        nil, // onMarketData (unused for now)
        r.OnClientSubscribe,
    )

    // 4. Start services
    upstream.Start(ctx)
    
    go func() {
        if err := acceptor.Start(ctx); err != nil {
            log.Fatal().Err(err).Msg("Acceptor failed")
        }
    }()
    
    // 5. Start Metrics Server
    go func() {
        http.Handle("/metrics", promhttp.Handler())
        log.Info().Msg("Metrics server listening on :9090")
        if err := http.ListenAndServe(":9090", nil); err != nil {
            log.Error().Err(err).Msg("Metrics server failed")
        }
    }()

    log.Info().Msg("Gateway is running. Press Ctrl+C to stop.")

    <-ctx.Done()
    log.Info().Msg("Shutting down...")
}
