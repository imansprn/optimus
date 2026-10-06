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
    dialTimeout  time.Duration
    senderCompID string
    targetCompID string
    username     string
    password     string
    heartBtInt   int
    inSeqNum     int64
    outSeqNum    int64
    pendingMessages map[int64]*fix.Message
    resendUntil int64
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

func NewUpstreamSession(host string, port int, sender, target, user, pass string, heartBt int, dialTimeout time.Duration, onMsg func(*fix.Message)) *UpstreamSession {
	if heartBt <= 0 {
		log.Warn().Int("heart_bt_int", heartBt).Msg("Invalid upstream heartbeat interval; defaulting to 30 seconds")
		heartBt = 30
	}
    return &UpstreamSession{
        host:         host,
        port:         port,
        dialTimeout:  dialTimeout,
        senderCompID: sender,
        targetCompID: target,
        username:     user,
        password:     pass,
        heartBtInt:   heartBt,
        reconnectCh:  make(chan struct{}, 1),
        pendingMessages: make(map[int64]*fix.Message),
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
            timer := time.NewTimer(delay)
            select {
            case <-ctx.Done():
                timer.Stop()
                return
            case <-timer.C:
            }
        }

        log.Info().Str("host", s.host).Int("port", s.port).Msg("Connecting to PrimeXM...")
        err := s.connect(ctx)
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

func (s *UpstreamSession) connect(ctx context.Context) error {
    addr := fmt.Sprintf("%s:%d", s.host, s.port)
    dt := s.dialTimeout
    if dt <= 0 {
        dt = 10 * time.Second
    }
    dialer := net.Dialer{Timeout: dt}
    conn, err := dialer.DialContext(ctx, "tcp", addr)
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
    clear(s.pendingMessages)
    s.resendUntil = 0

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
    if err := s.Send(logon); err != nil {
        _ = conn.Close()
        return err
    }
    return nil
}

func (s *UpstreamSession) Send(msg *fix.Message) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if s.conn == nil {
		return fmt.Errorf("connection closed")
	}

	// If the message was built without session header fields (e.g. MarketDataRequest
	// from the router), inject them here in the correct FIX field order:
	// 35 (already first) → 49, 56, 34, 52 → rest of body.
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
			{Tag: fix.TagSenderCompID, Value: s.senderCompID},
			{Tag: fix.TagTargetCompID, Value: s.targetCompID},
			{Tag: fix.TagMsgSeqNum, Value: strconv.FormatInt(seq, 10)},
			{Tag: fix.TagSendingTime, Value: time.Now().UTC().Format("20060102-15:04:05.000")},
		}
		// Fields[0] is always 35=MsgType; insert header right after it
		newFields := make([]fix.Field, 0, len(msg.Fields)+len(header))
		newFields = append(newFields, msg.Fields[0])
		newFields = append(newFields, header...)
		newFields = append(newFields, msg.Fields[1:]...)
		msg.Fields = newFields
	}

	data := fix.Serialize(msg)

	// Save sequence state
	out := atomic.LoadInt64(&s.outSeqNum)
	in := atomic.LoadInt64(&s.inSeqNum)
	if err := s.store.Save(s.senderCompID, s.targetCompID, in, out); err != nil {
		log.Error().Err(err).Msg("Failed to save sequence")
	}

	// Redact password (tag 554) before logging
	logStr := string(data)
	if idx := bytes.Index(data, []byte("\x01554=")); idx != -1 {
		start := idx + 5
		end := bytes.IndexByte(data[start:], fix.SOH)
		if end != -1 {
			logStr = string(data[:start]) + "*****" + string(data[start+end:])
		}
	}

	log.Trace().Str("raw", logStr).Msg("Sending raw FIX message upstream")
	if err := s.conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	return writeFull(s.conn, data)
}

const maxPendingUpstreamMessages = 16

func (s *UpstreamSession) readLoop(ctx context.Context) {
    conn := s.conn
    connCtx, cancelConn := context.WithCancel(ctx)
    defer cancelConn()
    stopClose := make(chan struct{})
    go func() {
        select {
        case <-ctx.Done():
            _ = conn.Close()
        case <-stopClose:
        }
    }()
    defer close(stopClose)
    defer func() {
        s.state = StateClosed
        metrics.UpstreamSessionState.Set(0)
        _ = conn.Close()
    }()

    scanner := bufio.NewScanner(conn)
    scanner.Split(fix.SplitFixMessage)
    scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
    
    for {
        if err := conn.SetReadDeadline(time.Now().Add(time.Duration(s.heartBtInt) * 2 * time.Second)); err != nil {
            log.Error().Err(err).Msg("Failed to set read deadline")
            return
        }
        if !scanner.Scan() {
            break
        }
        data := scanner.Bytes()
        
        msg, err := fix.Parse(data)
        if err != nil {
            log.Error().Err(err).Msg("Upstream parse error")
            continue
        }

        s.handleMessage(connCtx, msg)
        if s.state == StateClosed {
            break
        }
    }

    if err := scanner.Err(); err != nil {
        log.Error().Err(err).Msg("Upstream read error")
    } else if s.state != StateClosed {
        log.Warn().Msg("Upstream connection closed by peer")
    }
    s.conn.Close()
}

func (s *UpstreamSession) handleMessage(ctx context.Context, msg *fix.Message) {
    seqText, ok := msg.GetField(fix.TagMsgSeqNum)
    seq, err := strconv.ParseInt(seqText, 10, 64)
    if !ok || err != nil || seq < 1 {
        log.Error().Str("msg_type", msg.MsgType).Msg("Upstream message has invalid MsgSeqNum")
        s.state = StateClosed
        return
    }
    previous := atomic.LoadInt64(&s.inSeqNum)
    expected := previous + 1
    if seq < expected {
        log.Warn().Int64("expected", expected).Int64("received", seq).Msg("Ignoring duplicate or stale upstream FIX message")
        return
    }
    if seq > expected {
        if s.pendingMessages == nil {
            s.pendingMessages = make(map[int64]*fix.Message)
        }
        if _, exists := s.pendingMessages[seq]; !exists && len(s.pendingMessages) >= maxPendingUpstreamMessages {
            log.Error().Int("pending_messages", len(s.pendingMessages)).Msg("Too many out-of-order upstream FIX messages; closing session")
            s.state = StateClosed
            if s.conn != nil { _ = s.conn.Close() }
            return
        }
        s.pendingMessages[seq] = msg
        if seq-1 > s.resendUntil {
            request := fix.NewMessage(fix.MsgTypeResendRequest)
            request.AddField(fix.TagBeginSeqNo, strconv.FormatInt(expected, 10))
            request.AddField(fix.TagEndSeqNo, strconv.FormatInt(seq-1, 10))
            if err := s.Send(request); err != nil {
                log.Error().Err(err).Msg("Failed to request upstream FIX resend")
                s.state = StateClosed
                if s.conn != nil { _ = s.conn.Close() }
                return
            }
            s.resendUntil = seq - 1
            log.Warn().Int64("begin_seq_no", expected).Int64("end_seq_no", seq-1).Msg("Requested missing upstream FIX messages")
        }
        return
    }
    s.processOrderedMessage(ctx, msg, seq)
    for {
        next := atomic.LoadInt64(&s.inSeqNum) + 1
        pending, ok := s.pendingMessages[next]
        if !ok { break }
        delete(s.pendingMessages, next)
        s.processOrderedMessage(ctx, pending, next)
        if s.state == StateClosed { return }
    }
    if atomic.LoadInt64(&s.inSeqNum) >= s.resendUntil { s.resendUntil = 0 }
}

func (s *UpstreamSession) processOrderedMessage(ctx context.Context, msg *fix.Message, seq int64) {
    if msg.MsgType == fix.MsgTypeSequenceReset {
        newSeqText, ok := msg.GetField(fix.TagNewSeqNo)
        newSeq, err := strconv.ParseInt(newSeqText, 10, 64)
        if !ok || err != nil || newSeq <= seq {
            log.Error().Int64("seq", seq).Str("new_seq_no", newSeqText).Msg("Invalid upstream SequenceReset")
            s.state = StateClosed
            if s.conn != nil { _ = s.conn.Close() }
            return
        }
        atomic.StoreInt64(&s.inSeqNum, newSeq-1)
        for pendingSeq := range s.pendingMessages {
            if pendingSeq < newSeq { delete(s.pendingMessages, pendingSeq) }
        }
    } else {
        atomic.StoreInt64(&s.inSeqNum, seq)
    }
    in := atomic.LoadInt64(&s.inSeqNum)
    out := atomic.LoadInt64(&s.outSeqNum)
    if err := s.store.Save(s.senderCompID, s.targetCompID, in, out); err != nil {
        log.Error().Err(err).Msg("Failed to save sequence")
    }

    log.Trace().Str("msg_type", msg.MsgType).Int64("seq", in).Msg("Upstream message received")
    
    switch msg.MsgType {
    case fix.MsgTypeLogon:
        log.Info().Msg("Upstream logon successful")
        s.state = StateActive
        metrics.UpstreamSessionState.Set(1)
        go s.heartbeatLoop(ctx)
        if s.onMsg != nil {
            s.onMsg(msg)
        }
    case fix.MsgTypeHeartbeat:
        // Reset timeout handled by read deadline
    case fix.MsgTypeTestRequest:
        testReqID, _ := msg.GetField(fix.TagTestReqID)
        hb := fix.NewMessage(fix.MsgTypeHeartbeat)
        hb.AddField(fix.TagTestReqID, testReqID)
        if err := s.Send(hb); err != nil {
            log.Error().Err(err).Msg("Failed to send test request response")
        }
    case fix.MsgTypeMassQuote:
        if quoteID, ok := msg.GetField(fix.TagQuoteID); ok {
            ack := fix.NewMessage(fix.MsgTypeMassQuoteAck)
            ack.AddField(fix.TagSenderCompID, s.senderCompID)
            ack.AddField(fix.TagTargetCompID, s.targetCompID)
            ack.AddField(fix.TagMsgSeqNum, strconv.FormatInt(atomic.AddInt64(&s.outSeqNum, 1), 10))
            ack.AddField(fix.TagSendingTime, time.Now().UTC().Format("20060102-15:04:05.000"))
            ack.AddField(fix.TagQuoteID, quoteID)
            if err := s.Send(ack); err != nil {
                log.Error().Err(err).Msg("Failed to send quote ack")
            }
        }
        if s.onMsg != nil {
            s.onMsg(msg)
        }
    case fix.MsgTypeLogout:
        log.Info().Msg("Upstream logout received")
        s.state = StateClosed
        metrics.UpstreamSessionState.Set(0)
        // Don't close conn here — readLoop will close it cleanly after this returns
    default:
        if s.onMsg != nil {
            s.onMsg(msg)
        }
    }
}

func (s *UpstreamSession) heartbeatLoop(ctx context.Context) {
    ticker := time.NewTicker(time.Duration(s.heartBtInt) * time.Second)
    defer ticker.Stop()
    
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
        }
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
