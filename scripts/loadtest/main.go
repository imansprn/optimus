package main

import (
    "context"
    "flag"
    "fmt"
    "net"
    "github.com/imansprn/optimus/internal/fix"
    "os"
    "os/signal"
    "sync"
    "sync/atomic"
    "time"

    "github.com/rs/zerolog"
    "github.com/rs/zerolog/log"
)

var (
    target      = flag.String("target", "127.0.0.1:9878", "Gateway address")
    connections = flag.Int("c", 100, "Number of concurrent connections")
    symbol      = flag.String("symbol", "EUR/USD", "Symbol to subscribe to")
)

func main() {
    flag.Parse()
    zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
    log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
    defer stop()

    log.Info().Int("connections", *connections).Str("target", *target).Msg("Starting load test...")

    var wg sync.WaitGroup
    var activeConns int64
    var totalTicks int64

    start := time.Now()

    for i := 0; i < *connections; i++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            
            // Stagger connections
            time.Sleep(time.Duration(id%100) * 10 * time.Millisecond)
            
            if err := runClient(ctx, id, &activeConns, &totalTicks); err != nil {
                log.Error().Err(err).Int("id", id).Msg("Client failed")
            }
        }(i)
    }

    // Reporter loop
    ticker := time.NewTicker(2 * time.Second)
    go func() {
        for {
            select {
            case <-ctx.Done():
                return
            case <-ticker.C:
                active := atomic.LoadInt64(&activeConns)
                ticks := atomic.LoadInt64(&totalTicks)
                log.Info().Int64("active", active).Int64("total_ticks", ticks).Msg("Progress")
            }
        }
    }()

    <-ctx.Done()
    log.Info().Msg("Waiting for clients to exit...")
    wg.Wait()

    elapsed := time.Since(start)
    log.Info().
        Int("target_conns", *connections).
        Int64("final_active", atomic.LoadInt64(&activeConns)).
        Int64("total_ticks", atomic.LoadInt64(&totalTicks)).
        Dur("elapsed", elapsed).
        Msg("Load test complete")
}

func runClient(ctx context.Context, id int, activeCounter, tickCounter *int64) error {
    dialer := net.Dialer{Timeout: 5 * time.Second}
    conn, err := dialer.DialContext(ctx, "tcp", *target)
    if err != nil {
        return err
    }
    defer conn.Close()

    atomic.AddInt64(activeCounter, 1)
    defer atomic.AddInt64(activeCounter, -1)

    sender := fmt.Sprintf("CLIENT_%d", id)

    // 1. Logon
    logon := fix.NewMessage(fix.MsgTypeLogon)
    logon.AddField(fix.TagSenderCompID, sender)
    logon.AddField(fix.TagTargetCompID, "GW_GATEWAY")
    logon.AddField(fix.TagMsgSeqNum, "1")
    logon.AddField(fix.TagSendingTime, time.Now().UTC().Format("20060102-15:04:05.000"))
    logon.AddField(fix.TagEncryptMethod, "0")
    logon.AddField(fix.TagHeartBtInt, "30")
    if _, err := conn.Write(fix.Serialize(logon)); err != nil {
        return err
    }

    // 2. Subscribe
    sub := fix.NewMessage(fix.MsgTypeMarketDataRequest)
    sub.AddField(fix.TagMDReqID, fmt.Sprintf("REQ_%d", id))
    sub.AddField(fix.TagSubscriptionRequestType, "1")
    sub.AddField(fix.TagSymbol, *symbol)
    if _, err := conn.Write(fix.Serialize(sub)); err != nil {
        return err
    }

    // 3. Read loop
    buf := make([]byte, 4096)
    for {
        select {
        case <-ctx.Done():
            return nil
        default:
            if err := conn.SetReadDeadline(time.Now().Add(1 * time.Second)); err != nil {
                return err
            }
            n, err := conn.Read(buf)
            if err != nil {
                if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
                    continue
                }
                return err
            }
            if n > 0 {
                atomic.AddInt64(tickCounter, 1)
            }
        }
    }
}
