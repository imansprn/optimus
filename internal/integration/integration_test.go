package integration

import (
    "context"
    "net"
    "github.com/imansprn/optimus/internal/fix"
    "github.com/imansprn/optimus/internal/router"
    "github.com/imansprn/optimus/internal/session"
    "github.com/imansprn/optimus/internal/testutil"
    "testing"
    "time"

    "github.com/rs/zerolog"
)

func TestEndToEnd(t *testing.T) {
    zerolog.SetGlobalLevel(zerolog.DebugLevel)
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    mockAddr := "127.0.0.1:9999"
    gwAddr := "127.0.0.1:9998"

    // 1. Start Mock PrimeXM
    mock := testutil.NewMockPrimeXM(mockAddr)
    if err := mock.Start(ctx); err != nil {
        t.Fatalf("Failed to start mock: %v", err)
    }
    time.Sleep(100 * time.Millisecond) // Wait for start

    // 2. Start Gateway
    r := router.NewRouter()
    upstream := session.NewUpstreamSession("127.0.0.1", 9999, "GW01", "PXMD", "user", "pass", 30, r.OnUpstreamMessage)
    r.SetUpstream(upstream)
    
    acceptor := session.NewAcceptor(gwAddr, "GW_GATEWAY", nil, r.OnClientSubscribe)
    
    upstream.Start(ctx)
    if err := acceptor.Start(ctx); err != nil {
        t.Fatalf("Failed to start acceptor: %v", err)
    }
    time.Sleep(500 * time.Millisecond) // Wait for logon

    // 3. Connect Client
    conn, err := net.Dial("tcp", gwAddr)
    if err != nil {
        t.Fatalf("Failed to connect client: %v", err)
    }
    defer conn.Close()

    // 4. Client Logon
    logon := fix.NewMessage(fix.MsgTypeLogon)
    logon.AddField(fix.TagSenderCompID, "CLIENT1")
    logon.AddField(fix.TagTargetCompID, "GW_GATEWAY")
    logon.AddField(fix.TagMsgSeqNum, "1")
    logon.AddField(fix.TagSendingTime, time.Now().UTC().Format("20060102-15:04:05.000"))
    logon.AddField(fix.TagEncryptMethod, "0")
    logon.AddField(fix.TagHeartBtInt, "30")
    if _, err := conn.Write(fix.Serialize(logon)); err != nil {
		t.Fatalf("Failed to send logon: %v", err)
	}

    // Wait for logon ack
    buf := make([]byte, 1024)
    n, _ := conn.Read(buf)
    if n == 0 {
        t.Fatal("No logon ack received")
    }

    // 5. Client Subscribe
    sub := fix.NewMessage(fix.MsgTypeMarketDataRequest)
    sub.AddField(fix.TagMDReqID, "SUB01")
    sub.AddField(fix.TagSubscriptionRequestType, "1")
    sub.AddField(fix.TagSymbol, "EUR/USD")
    if _, err := conn.Write(fix.Serialize(sub)); err != nil {
		t.Fatalf("Failed to send subscribe: %v", err)
	}

    // 6. Verify Quote Received
    // Mock streams every 1s
    if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("Failed to set read deadline: %v", err)
	}
    n, err = conn.Read(buf)
    if err != nil {
        t.Fatalf("Failed to read quote: %v", err)
    }

    msg, err := fix.Parse(buf[:n])
    if err != nil {
        t.Fatalf("Failed to parse quote: %v", err)
    }

    if msg.MsgType != fix.MsgTypeMassQuote {
        t.Errorf("Expected MassQuote (i), got %s", msg.MsgType)
    }
    
    sym, _ := msg.GetField(fix.TagSymbol)
    if sym != "EUR/USD" {
        t.Errorf("Expected EUR/USD, got %s", sym)
    }
}
