package session

import (
    "bufio"
    "context"
    "net"
    "github.com/imansprn/optimus/internal/fix"
    "github.com/imansprn/optimus/internal/metrics"
    "strconv"
    "sync"
    "sync/atomic"
    "time"

    "github.com/google/uuid"
    "github.com/rs/zerolog"
    "github.com/rs/zerolog/log"
)

type ClientSession struct {
    ID            string
    SenderCompID  string
    TargetCompID  string
    conn          net.Conn
    outboundCh    chan []byte
    outSeqNum     int64
    heartBtInt    int
    lastRecv      time.Time
    state         SessionState
    cancel        context.CancelFunc
    logger        zerolog.Logger
}

type Acceptor struct {
    listenAddr    string
    senderCompID  string
    sessions      sync.Map // string -> *ClientSession
    onMarketData  func(*ClientSession, *fix.Message)
    onSubscribe   func(*ClientSession, *fix.Message)
}

func NewAcceptor(addr, sender string, onMD, onSub func(*ClientSession, *fix.Message)) *Acceptor {
    return &Acceptor{
        listenAddr:   addr,
        senderCompID: sender,
        onMarketData: onMD,
        onSubscribe:  onSub,
    }
}

func (a *Acceptor) Start(ctx context.Context) error {
    ln, err := net.Listen("tcp", a.listenAddr)
    if err != nil {
        return err
    }
    log.Info().Str("addr", a.listenAddr).Msg("Acceptor listening")

    go func() {
        <-ctx.Done()
        ln.Close()
    }()

    for {
        conn, err := ln.Accept()
        if err != nil {
            select {
            case <-ctx.Done():
                return nil
            default:
                log.Error().Err(err).Msg("Accept error")
                continue
            }
        }
        go a.handleConnection(ctx, conn)
    }
}

func (a *Acceptor) handleConnection(ctx context.Context, conn net.Conn) {
    sessionCtx, cancel := context.WithCancel(ctx)
    defer cancel()

    s := &ClientSession{
        ID:           uuid.New().String(),
        conn:         conn,
        outboundCh:   make(chan []byte, 8192),
        state:        StateConnecting,
        cancel:       cancel,
        logger:       log.With().Str("session_id", "").Logger(), // Will update after logon
    }

    go s.writeLoop(sessionCtx)

    scanner := bufio.NewScanner(conn)
    scanner.Split(fix.SplitFixMessage)

    // Read first message (Logon)
    if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
        log.Error().Err(err).Msg("Failed to set read deadline")
        conn.Close()
        return
    }
    if !scanner.Scan() {
        if err := scanner.Err(); err != nil {
            log.Error().Err(err).Msg("Initial read error")
        } else {
            log.Warn().Msg("Initial read: client disconnected before logon")
        }
        conn.Close()
        return
    }

    msg, err := fix.Parse(scanner.Bytes())
    if err != nil {
        log.Error().Err(err).Msg("Initial parse error")
        conn.Close()
        return
    }

    if msg.MsgType != fix.MsgTypeLogon {
        log.Warn().Str("type", msg.MsgType).Msg("First message must be Logon")
        conn.Close()
        return
    }

    // Basic auth (in v1 we accept all)
    s.SenderCompID, _ = msg.GetField(fix.TagSenderCompID)
    s.TargetCompID, _ = msg.GetField(fix.TagTargetCompID)
    hbt, _ := msg.GetField(fix.TagHeartBtInt)
    s.heartBtInt, _ = strconv.Atoi(hbt)
    if s.heartBtInt == 0 {
        s.heartBtInt = 30
    }
    resetSeqNum, _ := msg.GetField(fix.TagResetSeqNumFlag)

    s.logger = log.With().Str("client", s.SenderCompID).Str("id", s.ID).Logger()
    s.state = StateActive
    a.sessions.Store(s.ID, s)
    metrics.DownstreamSessions.Inc()

    defer func() {
        a.sessions.Delete(s.ID)
        metrics.DownstreamSessions.Dec()
    }()

    s.logger.Info().Msg("Client logon successful")

    // Respond with Logon
    ack := fix.NewMessage(fix.MsgTypeLogon)
    ack.AddField(fix.TagSenderCompID, a.senderCompID)
    ack.AddField(fix.TagTargetCompID, s.SenderCompID)
    ack.AddField(fix.TagMsgSeqNum, strconv.FormatInt(atomic.AddInt64(&s.outSeqNum, 1), 10))
    ack.AddField(fix.TagSendingTime, time.Now().UTC().Format("20060102-15:04:05.000"))
    ack.AddField(fix.TagEncryptMethod, "0")
    ack.AddField(fix.TagHeartBtInt, strconv.Itoa(s.heartBtInt))
    if resetSeqNum == "Y" {
        ack.AddField(fix.TagResetSeqNumFlag, "Y")
    }
    s.Send(ack)

    go s.heartbeatLoop(sessionCtx)

    // Main read loop
    for scanner.Scan() {
        if err := conn.SetReadDeadline(time.Now().Add(time.Duration(s.heartBtInt) * 2 * time.Second)); err != nil {
            s.logger.Error().Err(err).Msg("Failed to set read deadline")
            s.cancel()
            return
        }
        data := scanner.Bytes()
        s.logger.Trace().Int("len", len(data)).Msg("Raw data received from client")

        msg, err := fix.Parse(data)
        if err != nil {
            s.logger.Error().Err(err).Msg("Client parse error")
            continue
        }

        s.lastRecv = time.Now()
        a.handleClientMessage(s, msg)
    }

    if err := scanner.Err(); err != nil {
        s.logger.Error().Err(err).Msg("Client read error")
    }
}

func (s *ClientSession) Send(msg *fix.Message) {
	// If the message was built without session header fields (e.g. MassQuote
	// from the engine), inject them here so downstream clients receive a valid
	// FIX message with 49/56/34/52.
	hasSender := false
	for _, f := range msg.Fields {
		if f.Tag == fix.TagSenderCompID {
			hasSender = true
			break
		}
	}
	if !hasSender {
		seq := atomic.AddInt64(&s.outSeqNum, 1)
		header := []fix.Field{
			{Tag: fix.TagSenderCompID, Value: s.TargetCompID}, // acceptor CompID
			{Tag: fix.TagTargetCompID, Value: s.SenderCompID}, // client CompID
			{Tag: fix.TagMsgSeqNum, Value: strconv.FormatInt(seq, 10)},
			{Tag: fix.TagSendingTime, Value: time.Now().UTC().Format("20060102-15:04:05.000")},
		}
		newFields := make([]fix.Field, 0, len(msg.Fields)+len(header))
		newFields = append(newFields, msg.Fields[0]) // 35=MsgType
		newFields = append(newFields, header...)
		newFields = append(newFields, msg.Fields[1:]...)
		msg.Fields = newFields
	}

	data := fix.Serialize(msg)
	s.SendRaw(data)
}

func (s *ClientSession) SendRaw(data []byte) {
    select {
    case s.outboundCh <- data:
        s.logger.Trace().Int("len", len(data)).Msg("Message queued for client")
    default:
        s.logger.Warn().Msg("Outbound channel full, dropping message")
        metrics.SlowClientDrops.WithLabelValues(s.ID).Inc()
    }
}

func (s *ClientSession) writeLoop(ctx context.Context) {
    for {
        select {
        case <-ctx.Done():
            s.conn.Close()
            return
        case data := <-s.outboundCh:
            if err := s.conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
                s.logger.Error().Err(err).Msg("Failed to set write deadline")
                s.cancel()
                return
            }
            _, err := s.conn.Write(data)
            if err != nil {
                s.logger.Error().Err(err).Msg("Client write error")
                s.cancel()
                return
            }
        }
    }
}

func (s *ClientSession) heartbeatLoop(ctx context.Context) {
    ticker := time.NewTicker(time.Duration(s.heartBtInt) * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            hb := fix.NewMessage(fix.MsgTypeHeartbeat)
            hb.AddField(fix.TagSenderCompID, s.TargetCompID)
            hb.AddField(fix.TagTargetCompID, s.SenderCompID)
            hb.AddField(fix.TagMsgSeqNum, strconv.FormatInt(atomic.AddInt64(&s.outSeqNum, 1), 10))
            hb.AddField(fix.TagSendingTime, time.Now().UTC().Format("20060102-15:04:05.000"))
            s.Send(hb)
        }
    }
}

func (a *Acceptor) handleClientMessage(s *ClientSession, msg *fix.Message) {
    switch msg.MsgType {
    case fix.MsgTypeHeartbeat:
        // OK
    case fix.MsgTypeTestRequest:
        tid, _ := msg.GetField(fix.TagTestReqID)
        ack := fix.NewMessage(fix.MsgTypeHeartbeat)
        ack.AddField(fix.TagTestReqID, tid)
        s.Send(ack)
    case fix.MsgTypeMarketDataRequest:
        if a.onSubscribe != nil {
            a.onSubscribe(s, msg)
        }
    case fix.MsgTypeLogout:
        s.logger.Info().Msg("Client logout")
        s.cancel()
    default:
        s.logger.Debug().Str("type", msg.MsgType).Msg("Unhandled client message")
    }
}
