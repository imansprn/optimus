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
	subscriptionMu sync.Mutex // serializes subscription changes with grace-period callbacks
	symbols     sync.Map // string (symbol) -> *SymbolSubscription
	reqIDToSym  sync.Map // string (upstreamReqID) -> string (symbol)
	upstream   *session.UpstreamSession

	books       sync.Map // string (symbol) -> *quote.QuoteBook
	engine      *quote.Engine

	upstreamSeq int64

	unsubDelay  time.Duration
	pendingUnsub sync.Map // string (symbol) -> *time.Timer
}

func NewRouter() *Router {
	r := &Router{
		unsubDelay: 5 * time.Second,
		engine:     quote.NewEngine(),
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

		r.engine.EmitSnapshot(symbol, book, subscribers)
	}
}

func (r *Router) SetUpstream(u *session.UpstreamSession) {
    r.upstream = u
}

// OnClientDisconnect removes every subscription owned by a closed FIX session.
func (r *Router) OnClientDisconnect(s *session.ClientSession) {
    r.symbols.Range(func(key, _ interface{}) bool {
        r.unsubscribe(s, key.(string), "")
        return true
    })
}

func (r *Router) OnUpstreamMessage(msg *fix.Message) {
    switch msg.MsgType {
    case fix.MsgTypeLogon:
        r.resubscribeAll()
    case fix.MsgTypeMassQuote:
        r.handleMassQuote(msg)
    case fix.MsgTypeMarketDataSnapshot:
        r.handleSnapshot(msg)
    default:
        log.Debug().Str("type", msg.MsgType).Msg("Unhandled upstream message in router")
    }
}

func (r *Router) resubscribeAll() {
	r.subscriptionMu.Lock()
	defer r.subscriptionMu.Unlock()
	r.symbols.Range(func(key, value interface{}) bool {
        ss := value.(*SymbolSubscription)
        ss.mu.Lock()
        defer ss.mu.Unlock()
        if len(ss.Subscribers) == 0 {
            return true
        }
        if ss.UpstreamReqID != "" {
            r.reqIDToSym.Delete(ss.UpstreamReqID)
        }
        r.sendUpstreamSubscribe(ss)
        return true
    })
}

func (r *Router) handleMassQuote(msg *fix.Message) {
    var currentSymbol string
    for i := 0; i < len(msg.Fields); i++ {
        f := msg.Fields[i]
        if f.Tag == fix.TagQuoteSetID {
            // A QuoteSetID starts a new group; never inherit the previous symbol.
            currentSymbol = ""
            if sym, ok := r.reqIDToSym.Load(f.Value); ok {
                currentSymbol = sym.(string)
            }
            continue
        }
        if currentSymbol == "" || f.Tag != fix.TagQuoteEntryID {
            continue
        }
        entry := quote.QuoteLevel{}
        entry.QuoteEntryID, _ = strconv.Atoi(f.Value)
        for j := i + 1; j < len(msg.Fields); j++ {
            f2 := msg.Fields[j]
            if f2.Tag == fix.TagQuoteEntryID || f2.Tag == fix.TagQuoteSetID {
                break
            }
            switch f2.Tag {
            case fix.TagIssuer:
                entry.Issuer = f2.Value
            case fix.TagBidSpotRate:
                entry.BidSpotRate, _ = strconv.ParseFloat(f2.Value, 64)
            case fix.TagOfferSpotRate:
                entry.OfferSpotRate, _ = strconv.ParseFloat(f2.Value, 64)
            case fix.TagBidSize:
                entry.BidSize, _ = strconv.ParseFloat(f2.Value, 64)
            case fix.TagOfferSize:
                entry.OfferSize, _ = strconv.ParseFloat(f2.Value, 64)
            }
        }
        r.updateAndFanOut(currentSymbol, entry)
    }
}

func (r *Router) handleSnapshot(msg *fix.Message) {
    symbol, ok := msg.GetField(fix.TagSymbol)
    if !ok || symbol == "" {
        return
    }
    entries := parseSnapshotEntries(msg.Fields)
    val, _ := r.books.LoadOrStore(symbol, quote.NewQuoteBook(symbol))
    book := val.(*quote.QuoteBook)
    book.Replace(entries)
    metrics.TicksTotal.WithLabelValues(symbol).Add(float64(len(entries)))
    r.fanOutToSubscribers(symbol, book)
}

// parseSnapshotEntries extracts QuoteLevels from a MarketDataSnapshot (35=W) by
// scanning fields linearly and pairing Bid (269=0) and Offer (269=1) by QuoteEntryID (299).
func parseSnapshotEntries(fields []fix.Field) []quote.QuoteLevel {
    levels := make(map[int]*quote.QuoteLevel)
    entryType := ""
    entryID := -1
    var price, size float64
    var issuer string
    flush := func() {
        if entryID < 0 {
            return
        }
        level := levels[entryID]
        if level == nil {
            level = &quote.QuoteLevel{QuoteEntryID: entryID}
            levels[entryID] = level
        }
        if entryType == "0" {
            level.BidSpotRate = price
            level.BidSize = size
        } else if entryType == "1" {
            level.OfferSpotRate = price
            level.OfferSize = size
        }
        if issuer != "" {
            level.Issuer = issuer
        }
    }
    for _, f := range fields {
        switch f.Tag {
        case fix.TagMDEntryType:
            flush()
            entryType = f.Value
            entryID = -1
            price, size, issuer = 0, 0, ""
        case fix.TagMDEntryPx:
            price, _ = strconv.ParseFloat(f.Value, 64)
        case fix.TagMDEntrySize:
            size, _ = strconv.ParseFloat(f.Value, 64)
        case fix.TagQuoteEntryID:
            entryID, _ = strconv.Atoi(f.Value)
        case fix.TagIssuer:
            issuer = f.Value
        }
    }
    flush()
    result := make([]quote.QuoteLevel, 0, len(levels))
    for _, level := range levels {
        result = append(result, *level)
    }
    return result
}

func (r *Router) updateAndFanOut(symbol string, entry quote.QuoteLevel) {
	val, ok := r.books.Load(symbol)
	if !ok {
		val, _ = r.books.LoadOrStore(symbol, quote.NewQuoteBook(symbol))
	}
	book := val.(*quote.QuoteBook)
	book.Update(entry)
	metrics.TicksTotal.WithLabelValues(symbol).Inc()
	r.fanOutToSubscribers(symbol, book)
}

func (r *Router) fanOutToSubscribers(symbol string, book *quote.QuoteBook) {
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
		r.engine.FanOut(symbol, book, subscribers)
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
    r.subscriptionMu.Lock()
    defer r.subscriptionMu.Unlock()
    val, loaded := r.symbols.LoadOrStore(symbol, &SymbolSubscription{
        Symbol:      symbol,
        Subscribers: make(map[string]ClientSubscription),
    })
    ss := val.(*SymbolSubscription)

    ss.mu.Lock()
    defer ss.mu.Unlock()

    firstSub := len(ss.Subscribers) == 0
    subscriptionKey := s.ID + "\x00" + reqID
    ss.Subscribers[subscriptionKey] = ClientSubscription{
        Session:     s,
        ClientReqID: reqID,
    }

    if firstSub || !loaded {
        // If there's a pending unsubscribe, cancel it
        if val, ok := r.pendingUnsub.Load(symbol); ok {
            timer := val.(*time.Timer)
            timer.Stop()
            r.pendingUnsub.Delete(symbol)
            log.Info().Str("symbol", symbol).Msg("Resuming previous symbol stream (unsubscription cancelled)")
            // The global subscription lock prevents a fired callback from running
            // before this cancellation is visible; the existing upstream request remains active.
            return
        }
        r.sendUpstreamSubscribe(ss)
    }

    log.Info().Str("symbol", symbol).Str("client", s.SenderCompID).Msg("Client subscribed")
}

func (r *Router) unsubscribe(s *session.ClientSession, symbol, reqID string) {
    r.subscriptionMu.Lock()
    defer r.subscriptionMu.Unlock()
    val, ok := r.symbols.Load(symbol)
    if !ok {
        return
    }
    ss := val.(*SymbolSubscription)

    ss.mu.Lock()
    defer ss.mu.Unlock()

    removed := false
    if reqID != "" {
        key := s.ID + "\x00" + reqID
        if _, subscribed := ss.Subscribers[key]; subscribed {
            delete(ss.Subscribers, key)
            removed = true
        }
    } else {
        for key, sub := range ss.Subscribers {
            if sub.Session.ID == s.ID {
                delete(ss.Subscribers, key)
                removed = true
            }
        }
    }
    if !removed {
        return
    }

    if len(ss.Subscribers) == 0 {
        log.Info().Str("symbol", symbol).Dur("grace_period", r.unsubDelay).Msg("Last client unsubscribed. Starting unsubscription grace period...")
        
        var timer *time.Timer
        timer = time.AfterFunc(r.unsubDelay, func() {
            r.subscriptionMu.Lock()
            defer r.subscriptionMu.Unlock()
            pending, ok := r.pendingUnsub.Load(symbol)
            current, exists := r.symbols.Load(symbol)
            if !ok || pending != timer || !exists || current != ss {
                return
            }
            ss.mu.Lock()
            defer ss.mu.Unlock()
            if len(ss.Subscribers) != 0 {
                r.pendingUnsub.Delete(symbol)
                return
            }
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
    if err := r.upstream.Send(msg); err != nil {
        log.Error().Err(err).Msg("Failed to send upstream subscription")
    }
}

func (r *Router) sendUpstreamUnsubscribe(ss *SymbolSubscription) {
    if ss.UpstreamReqID == "" {
        return
    }

    reqID := ss.UpstreamReqID
    msg := fix.NewMessage(fix.MsgTypeMarketDataRequest)
    msg.AddField(fix.TagMDReqID, reqID)
    msg.AddField(fix.TagSubscriptionRequestType, "2") // Unsubscribe
    msg.AddField(fix.TagNoRelatedSym, "1")
    msg.AddField(fix.TagSymbol, ss.Symbol)

    log.Info().Str("symbol", ss.Symbol).Str("upstream_req_id", ss.UpstreamReqID).Msg("Sending upstream unsubscription")
    if err := r.upstream.Send(msg); err != nil {
        log.Error().Err(err).Msg("Failed to send upstream unsubscription")
    }
    r.reqIDToSym.Delete(reqID)
    ss.UpstreamReqID = ""
}

// encodeID encodes a sequence number into a 3-character string (Base36).
func encodeID(seq int64) string {
    return strconv.FormatInt(seq, 36)
}
