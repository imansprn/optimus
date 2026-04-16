package testutil

import (
    "bufio"
    "context"
    "fmt"
    "net"
    "github.com/imansprn/optimus/internal/fix"
    "strconv"
    "sync"
    "sync/atomic"
    "time"

    "github.com/rs/zerolog"
    "github.com/rs/zerolog/log"
)

type MockPrimeXM struct {
    addr         string
    symbols      sync.Map // string (reqID) -> string (symbol)
    sessions     sync.Map // string (connID) -> net.Conn
    outSeq       int64
    tickDelay    time.Duration
    logger       zerolog.Logger
}

func NewMockPrimeXM(addr string) *MockPrimeXM {
    return &MockPrimeXM{
        addr:      addr,
        tickDelay: 1 * time.Second,
        logger:    log.With().Str("component", "mock_primexm").Logger(),
    }
}

func (m *MockPrimeXM) Start(ctx context.Context) error {
    ln, err := net.Listen("tcp", m.addr)
    if err != nil {
        return err
    }
    m.logger.Info().Str("addr", m.addr).Msg("Mock PrimeXM server started")

    go func() {
        <-ctx.Done()
        ln.Close()
    }()

    go m.streamLoop(ctx)

    for {
        conn, err := ln.Accept()
        if err != nil {
            select {
            case <-ctx.Done():
                return nil
            default:
                m.logger.Error().Err(err).Msg("Accept error")
                continue
            }
        }
        go m.handleConnection(ctx, conn)
    }
}

func (m *MockPrimeXM) handleConnection(ctx context.Context, conn net.Conn) {
    connID := conn.RemoteAddr().String()
    m.sessions.Store(connID, conn)
    defer func() {
        m.sessions.Delete(connID)
        conn.Close()
    }()
    
    scanner := bufio.NewScanner(conn)
    scanner.Split(fix.SplitFixMessage)

    for scanner.Scan() {
        msg, err := fix.Parse(scanner.Bytes())
        if err != nil {
            m.logger.Error().Err(err).Msg("Parse error")
            return
        }

        m.logger.Debug().Str("type", msg.MsgType).Msg("Received message")

        switch msg.MsgType {
        case fix.MsgTypeLogon:
            m.sendAck(conn, fix.MsgTypeLogon)
        case fix.MsgTypeMarketDataRequest:
            reqID, _ := msg.GetField(fix.TagMDReqID)
            symbol, _ := msg.GetField(fix.TagSymbol)
            m.symbols.Store(reqID, symbol)
            m.logger.Info().Str("symbol", symbol).Str("req_id", reqID).Msg("New subscription received")
        }
    }
}

func (m *MockPrimeXM) sendAck(conn net.Conn, msgType string) {
    msg := fix.NewMessage(msgType)
    msg.AddField(fix.TagMsgSeqNum, strconv.FormatInt(atomic.AddInt64(&m.outSeq, 1), 10))
    msg.AddField(fix.TagSendingTime, time.Now().UTC().Format("20060102-15:04:05.000"))
    conn.Write(fix.Serialize(msg))
}

func (m *MockPrimeXM) streamLoop(ctx context.Context) {
    ticker := time.NewTicker(m.tickDelay)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            m.symbols.Range(func(key, value interface{}) bool {
                reqID := key.(string)
                symbol := value.(string)
                
                // Build a Mass Quote (35=i)
                msg := fix.NewMessage(fix.MsgTypeMassQuote)
                msg.AddField(fix.TagMsgSeqNum, strconv.FormatInt(atomic.AddInt64(&m.outSeq, 1), 10))
                msg.AddField(fix.TagSendingTime, time.Now().UTC().Format("20060102-15:04:05.000"))
                msg.AddField(fix.TagQuoteID, fmt.Sprintf("Q%d", time.Now().UnixNano()))
                msg.AddField(fix.TagNoQuoteSets, "1")
                msg.AddField(fix.TagQuoteSetID, reqID)
                msg.AddField(fix.TagNoQuoteEntries, "1")
                msg.AddField(fix.TagQuoteEntryID, "1")
                msg.AddField(fix.TagSymbol, symbol)
                msg.AddField(fix.TagBidSpotRate, "1.1000")
                msg.AddField(fix.TagOfferSpotRate, "1.1005")
                msg.AddField(fix.TagBidSize, "1000000")
                msg.AddField(fix.TagOfferSize, "1000000")

                data := fix.Serialize(msg)
                m.sessions.Range(func(key, value interface{}) bool {
                    conn := value.(net.Conn)
                    conn.Write(data)
                    return true
                })
                return true
            })
        }
    }
}
