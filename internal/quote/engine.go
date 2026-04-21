package quote

import (
	"strconv"

	"github.com/imansprn/optimus/internal/fix"
	"github.com/imansprn/optimus/internal/metrics"
	"github.com/imansprn/optimus/internal/session"
	"github.com/rs/zerolog/log"
)

// Engine handles fan-out of market data to downstream clients.
type Engine struct{}

func NewEngine() *Engine { return &Engine{} }

// ClientSub represents a downstream client subscription.
type ClientSub struct {
	Session     *session.ClientSession
	ClientReqID string
}

// FanOut sends a MassQuote (35=i) to all subscribers, serializing once per client
// so that each client receives their own MDReqID without padding artifacts.
func (e *Engine) FanOut(symbol string, book *QuoteBook, subscribers []ClientSub) {
	if len(subscribers) == 0 {
		return
	}

	entries := book.GetEntries()

	for _, sub := range subscribers {
		msg := fix.NewMessage(fix.MsgTypeMassQuote)
		msg.AddField(fix.TagQuoteID, sub.ClientReqID)
		msg.AddField(fix.TagNoQuoteSets, "1")
		// 296 group: 302, 295, then per-entry: 299, 188, 190, 134, 135
		msg.AddField(fix.TagQuoteSetID, sub.ClientReqID)
		msg.AddField(fix.TagNoQuoteEntries, strconv.Itoa(len(entries)))
		for _, entry := range entries {
			msg.AddField(fix.TagQuoteEntryID, strconv.Itoa(entry.QuoteEntryID))
			msg.AddField(fix.TagBidSpotRate, strconv.FormatFloat(entry.BidSpotRate, 'f', 5, 64))
			msg.AddField(fix.TagOfferSpotRate, strconv.FormatFloat(entry.OfferSpotRate, 'f', 5, 64))
		}

		metrics.FanoutTotal.WithLabelValues(symbol).Inc()
		log.Trace().Str("symbol", symbol).Str("client", sub.Session.SenderCompID).Msg("Fanning out Mass Quote")
		sub.Session.Send(msg)
	}
}

// EmitSnapshot sends a MarketDataSnapshot (35=W) full refresh to all subscribers.
func (e *Engine) EmitSnapshot(symbol string, book *QuoteBook, subscribers []ClientSub) {
	if len(subscribers) == 0 {
		return
	}

	entries := book.GetEntries()

	for _, sub := range subscribers {
		msg := fix.NewMessage(fix.MsgTypeMarketDataSnapshot)
		msg.AddField(fix.TagSymbol, symbol)
		msg.AddField(fix.TagMDReqID, sub.ClientReqID)
		msg.AddField(fix.TagNoMDEntries, strconv.Itoa(len(entries)*2))
		for _, entry := range entries {
			msg.AddField(fix.TagMDEntryType, "0")
			msg.AddField(fix.TagMDEntryPx, strconv.FormatFloat(entry.BidSpotRate, 'f', 5, 64))
			msg.AddField(fix.TagMDEntrySize, strconv.FormatFloat(entry.BidSize, 'f', 2, 64))
			msg.AddField(fix.TagQuoteEntryID, strconv.Itoa(entry.QuoteEntryID))
			if entry.Issuer != "" {
				msg.AddField(fix.TagIssuer, entry.Issuer)
			}

			msg.AddField(fix.TagMDEntryType, "1")
			msg.AddField(fix.TagMDEntryPx, strconv.FormatFloat(entry.OfferSpotRate, 'f', 5, 64))
			msg.AddField(fix.TagMDEntrySize, strconv.FormatFloat(entry.OfferSize, 'f', 2, 64))
			msg.AddField(fix.TagQuoteEntryID, strconv.Itoa(entry.QuoteEntryID))
			if entry.Issuer != "" {
				msg.AddField(fix.TagIssuer, entry.Issuer)
			}
		}

		log.Trace().Str("symbol", symbol).Str("client", sub.Session.SenderCompID).Msg("Emitting Snapshot")
		sub.Session.Send(msg)
	}
}
