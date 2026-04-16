package quote

import (
    "sync"
    "time"
)

type QuoteLevel struct {
    QuoteEntryID int     // 299
    Issuer       string  // 106
    BidSize      float64 // 134
    OfferSize    float64 // 135
    BidSpotRate  float64 // 188
    OfferSpotRate float64 // 190
}

type QuoteBook struct {
    Symbol    string
    Entries   map[int]QuoteLevel
    UpdatedAt time.Time
    mu        sync.RWMutex
}

func NewQuoteBook(symbol string) *QuoteBook {
    return &QuoteBook{
        Symbol:  symbol,
        Entries: make(map[int]QuoteLevel),
    }
}

func (b *QuoteBook) Update(entry QuoteLevel) {
    b.mu.Lock()
    defer b.mu.Unlock()
    b.Entries[entry.QuoteEntryID] = entry
    b.UpdatedAt = time.Now()
}

func (b *QuoteBook) Replace(entries []QuoteLevel) {
    b.mu.Lock()
    defer b.mu.Unlock()
    newEntries := make(map[int]QuoteLevel)
    for _, e := range entries {
        newEntries[e.QuoteEntryID] = e
    }
    b.Entries = newEntries
    b.UpdatedAt = time.Now()
}

func (b *QuoteBook) GetEntries() []QuoteLevel {
    b.mu.RLock()
    defer b.mu.RUnlock()
    res := make([]QuoteLevel, 0, len(b.Entries))
    for _, e := range b.Entries {
        res = append(res, e)
    }
    return res
}
