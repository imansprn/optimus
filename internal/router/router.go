package router

import (
    "github.com/imansprn/optimus/internal/fix"
    "github.com/imansprn/optimus/internal/metrics"
    "github.com/imansprn/optimus/internal/quote"
    "github.com/imansprn/optimus/internal/session"
    "strconv"
    "sync"
    "sync/atomic"
    "time"

    "github.com/rs/zerolog/log"
)

type ClientSubscription struct {
    Session     *session.ClientSession
    ClientReqID string // Client's original 262 (MDReqID)
}

type SymbolSubscription struct {
    Symbol        string
    UpstreamReqID string
    Subscribers   map[string]ClientSubscription // sessionID -> ClientSubscription
    mu            sync.RWMutex
}

type Router struct {
    symbols     sync.Map // string (symbol) -> *SymbolSubscription
    reqIDToSym  sync.Map // string (upstreamReqID) -> string (symbol)
    sessions    sync.Map // string (sessionID) -> *session.ClientSession
    upstream    *session.UpstreamSession
    
    books       sync.Map // string (symbol) -> *quote.QuoteBook
    
    upstreamSeq int64
    
    unsubDelay  time.Duration
    pendingUnsub sync.Map // string (symbol) -> *time.Timer
}

func NewRouter() *Router {
    r := &Router{
        unsubDelay: 5 * time.Second,
    }
    go r.snapshotLoop()
    return r
}

func (r *Router) snapshotLoop() {
    ticker := time.NewTicker(5 * time.Minute)
    defer ticker.Stop()

    for range ticker.C {
        r.books.Range(func(key, value interface{}) bool {
            symbol := key.(string)
            book := value.(*quote.QuoteBook)
            r.emitSnapshot(symbol, book)
            return true
        })
    }
}

func (r *Router) emitSnapshot(symbol string, book *quote.QuoteBook) {
    if subVal, ok := r.symbols.Load(symbol); ok {
        ss := subVal.(*SymbolSubscription)
        ss.mu.RLock()
        subscribers := make([]quote.ClientSub, 0, len(ss.Subscribers))
        for _, cs := range ss.Subscribers {
            subscribers = append(subscribers, quote.ClientSub{
                Session:     cs.Session,
                ClientReqID: cs.ClientReqID,
            })
        }
        ss.mu.RUnlock()

        quote.EmitSnapshot(symbol, book, subscribers)
    }
}

func (r *Router) SetUpstream(u *session.UpstreamSession) {
    r.upstream = u
}

func (r *Router) OnUpstreamMessage(msg *fix.Message) {
    switch msg.MsgType {
    case fix.MsgTypeMassQuote:
        r.handleMassQuote(msg)
    case fix.MsgTypeMarketDataSnapshot:
        r.handleSnapshot(msg)
    default:
        log.Debug().Str("type", msg.MsgType).Msg("Unhandled upstream message in router")
    }
}

func (r *Router) handleMassQuote(msg *fix.Message) {
    // PrimeXM Mass Quote can have multiple QuoteSets
    // Each 302 corresponds to an upstream MDReqID
    
    // For simplicity, we'll iterate and find 302
    var currentSymbol string
    
    // Note: Real parsing of repeating groups is complex. 
    // This is a simplified version.
    for i := 0; i < len(msg.Fields); i++ {
        f := msg.Fields[i]
        if f.Tag == fix.TagQuoteSetID {
            if sym, ok := r.reqIDToSym.Load(f.Value); ok {
                currentSymbol = sym.(string)
            }
        }
        
        if currentSymbol != "" && f.Tag == fix.TagQuoteEntryID {
            // Start of a quote entry
            entry := quote.QuoteLevel{}
            entry.QuoteEntryID, _ = strconv.Atoi(f.Value)
            
            // Scan ahead for other fields in this entry
            for j := i + 1; j < len(msg.Fields); j++ {
                f2 := msg.Fields[j]
                if f2.Tag == fix.TagQuoteEntryID || f2.Tag == fix.TagQuoteSetID {
                    break
                }
                switch f2.Tag {
                case fix.TagIssuer: entry.Issuer = f2.Value
                case fix.TagBidSpotRate: entry.BidSpotRate, _ = strconv.ParseFloat(f2.Value, 64)
                case fix.TagOfferSpotRate: entry.OfferSpotRate, _ = strconv.ParseFloat(f2.Value, 64)
                case fix.TagBidSize: entry.BidSize, _ = strconv.ParseFloat(f2.Value, 64)
                case fix.TagOfferSize: entry.OfferSize, _ = strconv.ParseFloat(f2.Value, 64)
                }
            }
            
            r.updateAndFanOut(currentSymbol, entry)
        }
    }
    if currentSymbol == "" {
        log.Trace().Msg("Mass Quote received but no matching subscribers found")
    }
}

func (r *Router) handleSnapshot(msg *fix.Message) {
    // symbol, _ := msg.GetField(fix.TagSymbol)
    // Process snapshot and replace quote book
    // ... (implementation similar to mass quote)
}

func (r *Router) updateAndFanOut(symbol string, entry quote.QuoteLevel) {
    val, ok := r.books.Load(symbol)
    if !ok {
        val, _ = r.books.LoadOrStore(symbol, quote.NewQuoteBook(symbol))
    }
    book := val.(*quote.QuoteBook)
    book.Update(entry)
    metrics.TicksTotal.WithLabelValues(symbol).Inc()
    
    // Get subscribers
    if subVal, ok := r.symbols.Load(symbol); ok {
        ss := subVal.(*SymbolSubscription)
        ss.mu.RLock()
        subscribers := make([]quote.ClientSub, 0, len(ss.Subscribers))
        for _, cs := range ss.Subscribers {
            subscribers = append(subscribers, quote.ClientSub{
                Session:     cs.Session,
                ClientReqID: cs.ClientReqID,
            })
        }
        ss.mu.RUnlock()
        
        log.Debug().Str("symbol", symbol).Int("subscribers", len(subscribers)).Msg("Routing update to clients")
        quote.FanOut(symbol, book, subscribers)
    }
}

func (r *Router) OnClientSubscribe(s *session.ClientSession, msg *fix.Message) {
    symbol, _ := msg.GetField(fix.TagSymbol)
    reqID, _ := msg.GetField(fix.TagMDReqID)
    subType, _ := msg.GetField(fix.TagSubscriptionRequestType)

    if symbol == "" || reqID == "" {
        log.Warn().Str("client", s.SenderCompID).Msg("Invalid subscription request: missing Symbol or MDReqID")
        return
    }

    if subType == "1" { // Subscribe
        r.subscribe(s, symbol, reqID)
    } else if subType == "2" { // Unsubscribe
        r.unsubscribe(s, symbol, reqID)
    }
}

func (r *Router) subscribe(s *session.ClientSession, symbol, reqID string) {
    val, loaded := r.symbols.LoadOrStore(symbol, &SymbolSubscription{
        Symbol:      symbol,
        Subscribers: make(map[string]ClientSubscription),
    })
    ss := val.(*SymbolSubscription)

    ss.mu.Lock()
    defer ss.mu.Unlock()

    firstSub := len(ss.Subscribers) == 0
    ss.Subscribers[s.ID] = ClientSubscription{
        Session:     s,
        ClientReqID: reqID,
    }

    if firstSub || !loaded {
        // If there's a pending unsubscribe, cancel it
        if val, ok := r.pendingUnsub.Load(symbol); ok {
            timer := val.(*time.Timer)
            if timer.Stop() {
                log.Info().Str("symbol", symbol).Msg("Resuming previous symbol stream (unsubscription cancelled)")
                r.pendingUnsub.Delete(symbol)
                return
            }
        }
        r.sendUpstreamSubscribe(ss)
    }

    log.Info().Str("symbol", symbol).Str("client", s.SenderCompID).Msg("Client subscribed")
}

func (r *Router) unsubscribe(s *session.ClientSession, symbol, reqID string) {
    val, ok := r.symbols.Load(symbol)
    if !ok {
        return
    }
    ss := val.(*SymbolSubscription)

    ss.mu.Lock()
    defer ss.mu.Unlock()

    delete(ss.Subscribers, s.ID)

    if len(ss.Subscribers) == 0 {
        log.Info().Str("symbol", symbol).Dur("grace_period", r.unsubDelay).Msg("Last client unsubscribed. Starting unsubscription grace period...")
        
        timer := time.AfterFunc(r.unsubDelay, func() {
            log.Info().Str("symbol", symbol).Msg("Unsubscription grace period expired. Sending upstream unsubscribe.")
            r.sendUpstreamUnsubscribe(ss)
            r.symbols.Delete(symbol)
            r.pendingUnsub.Delete(symbol)
        })
        r.pendingUnsub.Store(symbol, timer)
    }

    log.Info().Str("symbol", symbol).Str("client", s.SenderCompID).Msg("Client unsubscribed")
}

func (r *Router) sendUpstreamSubscribe(ss *SymbolSubscription) {
    seq := atomic.AddInt64(&r.upstreamSeq, 1)
    reqID := encodeID(seq)
    ss.UpstreamReqID = reqID
    r.reqIDToSym.Store(reqID, ss.Symbol)

    msg := fix.NewMessage(fix.MsgTypeMarketDataRequest)
    msg.AddField(fix.TagMDReqID, reqID)
    msg.AddField(fix.TagSubscriptionRequestType, "1") // Snapshot + Updates
    msg.AddField(fix.TagMarketDepth, "0")              // Full Book
    msg.AddField(fix.TagMDUpdateType, "1")             // Incremental
    msg.AddField(fix.TagNoRelatedSym, "1")
    msg.AddField(fix.TagSymbol, ss.Symbol)

    log.Info().Str("symbol", ss.Symbol).Str("upstream_req_id", reqID).Msg("Sending upstream subscription")
    r.upstream.Send(msg)
}

func (r *Router) sendUpstreamUnsubscribe(ss *SymbolSubscription) {
    if ss.UpstreamReqID == "" {
        return
    }

    msg := fix.NewMessage(fix.MsgTypeMarketDataRequest)
    msg.AddField(fix.TagMDReqID, ss.UpstreamReqID)
    msg.AddField(fix.TagSubscriptionRequestType, "2") // Unsubscribe
    msg.AddField(fix.TagNoRelatedSym, "1")
    msg.AddField(fix.TagSymbol, ss.Symbol)

    log.Info().Str("symbol", ss.Symbol).Str("upstream_req_id", ss.UpstreamReqID).Msg("Sending upstream unsubscription")
    r.upstream.Send(msg)
    ss.UpstreamReqID = ""
}

// encodeID encodes a sequence number into a 3-character string (Base36).
func encodeID(seq int64) string {
    const chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    res := make([]byte, 3)
    for i := 2; i >= 0; i-- {
        res[i] = chars[seq%36]
        seq /= 36
    }
    return string(res)
}
