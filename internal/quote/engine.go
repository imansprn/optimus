package quote

import (
    "github.com/imansprn/optimus/internal/fix"
    "github.com/imansprn/optimus/internal/metrics"
    "github.com/imansprn/optimus/internal/session"
    "strconv"

    "github.com/rs/zerolog/log"
)

type Engine struct {
    books map[string]*QuoteBook
}

func NewEngine() *Engine {
    return &Engine{
        books: make(map[string]*QuoteBook),
    }
}

// ProcessUpstreamQuote handles MsgType=i (Mass Quote) from PrimeXM.
func (e *Engine) ProcessUpstreamQuote(msg *fix.Message, getClients func(string) []ClientSub) {
    // 302 represents the upstream MDReqID. 
    // We need to map it back to the symbol.
    // But in Mass Quote, 302 is inside QuoteSets.
    
    // For simplicity in this engine, we assume the router has already mapped 302 -> symbol.
    // Actually, the engine shouldn't know about Router specifics.
    // Let's assume the msg passed here is already identified by Symbol.
}

type ClientSub struct {
    Session     *session.ClientSession
    ClientReqID string
}

// FanOut sends a Mass Quote to all interested clients.
func FanOut(symbol string, book *QuoteBook, subscribers []ClientSub) {
    if len(subscribers) == 0 {
        return
    }

    // Reconstruct Mass Quote from book
    // Note: PrimeXM spec says 35=i for updates.
    // We'll build a 35=i template.
    
    base := fix.NewMessage(fix.MsgTypeMassQuote)
    base.AddField(fix.TagNoQuoteSets, "1")
    base.AddField(fix.TagSymbol, symbol)
    // Use a 20-character placeholder to ensure enough space for any ReqID
    base.AddField(fix.TagQuoteSetID, "____________________") 

    entries := book.GetEntries()
    base.AddField(fix.TagNoQuoteEntries, strconv.Itoa(len(entries)))
    for _, entry := range entries {
        base.AddField(fix.TagQuoteEntryID, strconv.Itoa(entry.QuoteEntryID))
        base.AddField(fix.TagIssuer, entry.Issuer)
        base.AddField(fix.TagBidSize, strconv.FormatFloat(entry.BidSize, 'f', 2, 64))
        base.AddField(fix.TagOfferSize, strconv.FormatFloat(entry.OfferSize, 'f', 2, 64))
        base.AddField(fix.TagBidSpotRate, strconv.FormatFloat(entry.BidSpotRate, 'f', 5, 64))
        base.AddField(fix.TagOfferSpotRate, strconv.FormatFloat(entry.OfferSpotRate, 'f', 5, 64))
    }

    tmpl := fix.NewTemplate(base, []int{fix.TagQuoteSetID})
    
    for _, sub := range subscribers {
        metrics.FanoutTotal.WithLabelValues(symbol).Inc()
        data := tmpl.Patch(fix.TagQuoteSetID, sub.ClientReqID)
        
        log.Trace().Str("symbol", symbol).Str("client", sub.Session.SenderCompID).Msg("Fanning out Mass Quote")
        sub.Session.SendRaw(data)
    }
}

// EmitSnapshot sends a Full Refresh (MsgType=W) to all subscribers.
func EmitSnapshot(symbol string, book *QuoteBook, subscribers []ClientSub) {
    if len(subscribers) == 0 {
        return
    }

    entries := book.GetEntries()
    base := fix.NewMessage(fix.MsgTypeMarketDataSnapshot)
    base.AddField(fix.TagSymbol, symbol)
    // Placeholder for client MDReqID
    base.AddField(fix.TagMDReqID, "____________________") 
    base.AddField(fix.TagNoMDEntries, strconv.Itoa(len(entries)*2))
    
    for _, entry := range entries {
        // Bid
        base.AddField(fix.TagMDEntryType, "0")
        base.AddField(fix.TagMDEntryPx, strconv.FormatFloat(entry.BidSpotRate, 'f', 5, 64))
        base.AddField(fix.TagMDEntrySize, strconv.FormatFloat(entry.BidSize, 'f', 2, 64))
        base.AddField(fix.TagQuoteEntryID, strconv.Itoa(entry.QuoteEntryID))
        base.AddField(fix.TagIssuer, entry.Issuer)
        
        // Offer
        base.AddField(fix.TagMDEntryType, "1")
        base.AddField(fix.TagMDEntryPx, strconv.FormatFloat(entry.OfferSpotRate, 'f', 5, 64))
        base.AddField(fix.TagMDEntrySize, strconv.FormatFloat(entry.OfferSize, 'f', 2, 64))
        base.AddField(fix.TagQuoteEntryID, strconv.Itoa(entry.QuoteEntryID))
        base.AddField(fix.TagIssuer, entry.Issuer)
    }

    tmpl := fix.NewTemplate(base, []int{fix.TagMDReqID})
    
    for _, sub := range subscribers {
        data := tmpl.Patch(fix.TagMDReqID, sub.ClientReqID)
        log.Trace().Str("symbol", symbol).Str("client", sub.Session.SenderCompID).Msg("Emitting Snapshot")
        sub.Session.SendRaw(data)
    }
}
