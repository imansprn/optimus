package fix

import "fmt"

// Message types
const (
    MsgTypeLogon                = "A"
    MsgTypeHeartbeat            = "0"
    MsgTypeTestRequest          = "1"
    MsgTypeResendRequest        = "2"
    MsgTypeReject               = "3"
    MsgTypeSequenceReset        = "4"
    MsgTypeLogout               = "5"
    MsgTypeMarketDataRequest    = "V"
    MsgTypeMarketDataSnapshot   = "W"
    MsgTypeMarketDataReject     = "Y"
    MsgTypeMassQuote            = "i"
    MsgTypeMassQuoteAck         = "b"
)

// Tags
const (
    TagBeginString              = 8
    TagBodyLength               = 9
    TagMsgType                  = 35
    TagSenderCompID             = 49
    TagTargetCompID             = 56
    TagMsgSeqNum                = 34
    TagSendingTime              = 52
    TagPossDupFlag              = 43
    TagOrigSendingTime          = 122
    TagEncryptMethod            = 98
    TagHeartBtInt               = 108
    TagUsername                 = 553
    TagPassword                 = 554
    TagResetSeqNumFlag          = 141
    TagTestReqID                = 112
    TagBeginSeqNo               = 7
    TagEndSeqNo                 = 16
    TagNewSeqNo                 = 36
    TagGapFillFlag              = 123
    TagText                     = 58
    TagRefMsgType               = 372
    TagRefSeqNum                = 45
    TagSessionRejectReason      = 373
    TagMDReqID                  = 262
    TagSubscriptionRequestType  = 263
    TagMarketDepth              = 264
    TagMDUpdateType             = 265
    TagNoRelatedSym             = 146
    TagSymbol                   = 55
    TagNoMDEntries              = 268
    TagMDEntryType              = 269
    TagMDEntryPx                = 270
    TagMDEntrySize              = 271
    TagQuoteEntryID             = 299
    TagIssuer                   = 106
    TagQuoteID                  = 117
    TagNoQuoteSets              = 296
    TagQuoteSetID               = 302
    TagNoQuoteEntries           = 295
    TagBidSpotRate              = 188
    TagOfferSpotRate            = 190
    TagBidSize                  = 134
    TagOfferSize                = 135
    TagCheckSum                 = 10
)

const SOH = 0x01

// Message represents a parsed FIX message.
type Message struct {
    MsgType    string
    Fields     []Field
    fastFields [1024]string // O(1) lookup for tags 0-1023
}

// Field represents a single tag-value pair.
type Field struct {
    Tag   int
    Value string
}

func (m *Message) GetField(tag int) (string, bool) {
    // First check fast-lookup index
    if tag >= 0 && tag < 1024 {
        val := m.fastFields[tag]
        if val != "" {
            return val, true
        }
    }

    // Double check in Fields slice (for tags > 1024 or duplicates)
    for _, f := range m.Fields {
        if f.Tag == tag {
            return f.Value, true
        }
    }
    return "", false
}

func (m *Message) String() string {
    res := ""
    for _, f := range m.Fields {
        res += fmt.Sprintf("%d=%s|", f.Tag, f.Value)
    }
    return res
}
