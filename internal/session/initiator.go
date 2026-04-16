package session

import (
    "bufio"
    "bytes"
    "context"
    "fmt"
    "net"
    "github.com/imansprn/optimus/internal/fix"
    "github.com/imansprn/optimus/internal/metrics"
    "strconv"
    "sync"
    "sync/atomic"
    "time"

    "github.com/rs/zerolog/log"
)

type UpstreamSession struct {
    conn         net.Conn
    host         string
    port         int
    senderCompID string
    targetCompID string
    username     string
    password     string
    heartBtInt   int
    inSeqNum     int64
    outSeqNum    int64
    state        SessionState
    reconnectCh  chan struct{}
    writeMu      sync.Mutex
    
    onMsg        func(*fix.Message)
    store        *SequenceStore
}

type SessionState int

const (
    StateConnecting SessionState = iota
    StateActive
    StateDisconnecting
    StateClosed
)

func NewUpstreamSession(host string, port int, sender, target, user, pass string, heartBt int, onMsg func(*fix.Message)) *UpstreamSession {
    return &UpstreamSession{
        host:         host,
        port:         port,
        senderCompID: sender,
        targetCompID: target,
        username:     user,
        password:     pass,
        heartBtInt:   heartBt,
        reconnectCh:  make(chan struct{}, 1),
        onMsg:        onMsg,
        store:        NewSequenceStore("./data"),
    }
}

func (s *UpstreamSession) Start(ctx context.Context) {
    s.reconnectCh <- struct{}{}
    go s.reconnectLoop(ctx)
}

func (s *UpstreamSession) reconnectLoop(ctx context.Context) {
    backoffs := []time.Duration{1 * time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second, 30 * time.Second, 60 * time.Second}
    attempt := 0
    
    for {
        select {
        case <-ctx.Done():
            return
        case <-s.reconnectCh:
        }

        if attempt > 0 {
            idx := attempt - 1
            if idx >= len(backoffs) {
                idx = len(backoffs) - 1
            }
            delay := backoffs[idx]
            log.Info().Dur("delay", delay).Msg("Waiting to reconnect...")
            time.Sleep(delay)
        }

        log.Info().Str("host", s.host).Int("port", s.port).Msg("Connecting to PrimeXM...")
        err := s.connect()
        if err != nil {
            log.Error().Err(err).Msg("Failed to connect upstream")
            attempt++
            s.reconnectCh <- struct{}{}
            continue
        }

        attempt = 0
        s.readLoop(ctx)
        s.reconnectCh <- struct{}{}
    }
}

func (s *UpstreamSession) connect() error {
    addr := fmt.Sprintf("%s:%d", s.host, s.port)
    conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
    if err != nil {
        return err
    }
    s.conn = conn
    s.state = StateConnecting
    log.Info().Str("host", s.host).Int("port", s.port).Msg("Connected to PrimeXM. Sending Logon...")
    
    // Load sequences from store
    in, out, err := s.store.Load(s.senderCompID, s.targetCompID)
    if err == nil && in > 0 {
        atomic.StoreInt64(&s.inSeqNum, in)
        atomic.StoreInt64(&s.outSeqNum, out)
        log.Info().Int64("in", in).Int64("out", out).Msg("Restored session sequences")
    } else {
        // New session or reset
        atomic.StoreInt64(&s.inSeqNum, 0)
        atomic.StoreInt64(&s.outSeqNum, 0)
    }

    logon := fix.NewMessage(fix.MsgTypeLogon)
    // ... (rest of message setup)
    logon.AddField(fix.TagSenderCompID, s.senderCompID)
    logon.AddField(fix.TagTargetCompID, s.targetCompID)
    logon.AddField(fix.TagMsgSeqNum, strconv.FormatInt(atomic.AddInt64(&s.outSeqNum, 1), 10))
    logon.AddField(fix.TagSendingTime, time.Now().UTC().Format("20060102-15:04:05.000"))
    logon.AddField(fix.TagEncryptMethod, "0")
    logon.AddField(fix.TagHeartBtInt, strconv.Itoa(s.heartBtInt))
    
    if in == 0 {
        logon.AddField(fix.TagResetSeqNumFlag, "Y")
    } else {
        logon.AddField(fix.TagResetSeqNumFlag, "N")
    }
    
    logon.AddField(fix.TagUsername, s.username)
    logon.AddField(fix.TagPassword, s.password)

    log.Debug().Msg("Logon request sent")
    return s.Send(logon)
}

func (s *UpstreamSession) Send(msg *fix.Message) error {
    s.writeMu.Lock()
    defer s.writeMu.Unlock()
    
    if s.conn == nil {
        return fmt.Errorf("connection closed")
    }

    data := fix.Serialize(msg)
    
    // Increment and save outSeq
    out := atomic.LoadInt64(&s.outSeqNum)
    in := atomic.LoadInt64(&s.inSeqNum)
    s.store.Save(s.senderCompID, s.targetCompID, in, out)

    // Redact sensitive tags for logging
    logStr := string(data)
    if idx := bytes.Index(data, []byte("\x01554=")); idx != -1 {
        // Found tag 554, find next SOH
        start := idx + 5
        end := bytes.IndexByte(data[start:], fix.SOH)
        if end != -1 {
            logStr = string(data[:start]) + "*****" + string(data[start+end:])
        }
    }
    
    log.Trace().Str("raw", logStr).Msg("Sending raw FIX message upstream")
    _, err := s.conn.Write(data)
    return err
}

func (s *UpstreamSession) readLoop(ctx context.Context) {
    scanner := bufio.NewScanner(s.conn)
    scanner.Split(fix.SplitFixMessage)
    
    for scanner.Scan() {
        s.conn.SetReadDeadline(time.Now().Add(time.Duration(s.heartBtInt) * 2 * time.Second))
        data := scanner.Bytes()
        
        msg, err := fix.Parse(data)
        if err != nil {
            log.Error().Err(err).Msg("Upstream parse error")
            continue
        }

        s.handleMessage(msg)
    }

    if err := scanner.Err(); err != nil {
        log.Error().Err(err).Msg("Upstream read error")
    } else {
        log.Warn().Msg("Upstream connection closed by peer")
    }
    s.conn.Close()
}

func (s *UpstreamSession) handleMessage(msg *fix.Message) {
    in := atomic.AddInt64(&s.inSeqNum, 1)
    out := atomic.LoadInt64(&s.outSeqNum)
    s.store.Save(s.senderCompID, s.targetCompID, in, out)

    log.Trace().Str("msg_type", msg.MsgType).Int64("seq", in).Msg("Upstream message received")
    
    switch msg.MsgType {
    case fix.MsgTypeLogon:
        log.Info().Msg("Upstream logon successful")
        s.state = StateActive
        metrics.UpstreamSessionState.Set(1)
        go s.heartbeatLoop()
    case fix.MsgTypeHeartbeat:
        // Reset timeout handled by read deadline
    case fix.MsgTypeTestRequest:
        testReqID, _ := msg.GetField(fix.TagTestReqID)
        hb := fix.NewMessage(fix.MsgTypeHeartbeat)
        hb.AddField(fix.TagTestReqID, testReqID)
        s.Send(hb)
    case fix.MsgTypeMassQuote:
        if quoteID, ok := msg.GetField(fix.TagQuoteID); ok {
            ack := fix.NewMessage(fix.MsgTypeMassQuoteAck)
            ack.AddField(fix.TagSenderCompID, s.senderCompID)
            ack.AddField(fix.TagTargetCompID, s.targetCompID)
            ack.AddField(fix.TagMsgSeqNum, strconv.FormatInt(atomic.AddInt64(&s.outSeqNum, 1), 10))
            ack.AddField(fix.TagSendingTime, time.Now().UTC().Format("20060102-15:04:05.000"))
            ack.AddField(fix.TagQuoteID, quoteID)
            s.Send(ack)
        }
        if s.onMsg != nil {
            s.onMsg(msg)
        }
    case fix.MsgTypeLogout:
        log.Info().Msg("Upstream logout received")
        s.state = StateClosed
        metrics.UpstreamSessionState.Set(0)
        s.conn.Close()
    default:
        if s.onMsg != nil {
            s.onMsg(msg)
        }
    }
}

func (s *UpstreamSession) heartbeatLoop() {
    ticker := time.NewTicker(time.Duration(s.heartBtInt) * time.Second)
    defer ticker.Stop()
    
    for range ticker.C {
        if s.state != StateActive {
            return
        }
        hb := fix.NewMessage(fix.MsgTypeHeartbeat)
        hb.AddField(fix.TagSenderCompID, s.senderCompID)
        hb.AddField(fix.TagTargetCompID, s.targetCompID)
        hb.AddField(fix.TagMsgSeqNum, strconv.FormatInt(atomic.AddInt64(&s.outSeqNum, 1), 10))
        hb.AddField(fix.TagSendingTime, time.Now().UTC().Format("20060102-15:04:05.000"))
        if err := s.Send(hb); err != nil {
            log.Error().Err(err).Msg("Failed to send upstream heartbeat")
            return
        }
        log.Trace().Msg("Upstream heartbeat sent")
    }
}

func min(a, b time.Duration) time.Duration {
    if a < b {
        return a
    }
    return b
}
