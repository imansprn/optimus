package session

import (
	"bufio"
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/imansprn/optimus/internal/fix"
)

func TestPricingLogonAlwaysResetsSequenceNumbers(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	addr := listener.Addr().(*net.TCPAddr)
	sender := "TEST_PRICING_RESET"
	target := "PXMD"
	stateFile := filepath.Join("data", sender+"_"+target+".json")
	t.Cleanup(func() { _ = os.Remove(stateFile) })

	store := NewSequenceStore("data")
	if err = store.Save(sender, target, 3339, 104); err != nil {
		t.Fatalf("seed sequence store: %v", err)
	}

	msgCh := make(chan *fix.Message, 1)
	errCh := make(chan error, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			errCh <- acceptErr
			return
		}
		defer conn.Close()

		scanner := bufio.NewScanner(conn)
		scanner.Split(fix.SplitFixMessage)
		if !scanner.Scan() {
			errCh <- scanner.Err()
			return
		}
		msg, parseErr := fix.Parse(scanner.Bytes())
		if parseErr != nil {
			errCh <- parseErr
			return
		}
		msgCh <- msg
	}()

	s := NewUpstreamSession("127.0.0.1", addr.Port, sender, target, "user", "pass", 30, time.Second, nil)
	if err = s.connect(context.Background()); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer s.conn.Close()

	select {
	case err = <-errCh:
		if err != nil {
			t.Fatalf("server: %v", err)
		}
	case msg := <-msgCh:
		seq, _ := msg.GetField(fix.TagMsgSeqNum)
		if seq != "1" {
			t.Fatalf("expected MsgSeqNum 1, got %s", seq)
		}
		reset, _ := msg.GetField(fix.TagResetSeqNumFlag)
		if reset != "Y" {
			t.Fatalf("expected ResetSeqNumFlag Y, got %s", reset)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for pricing logon")
	}
}
