package fix

import (
    "bufio"
    "bytes"
    "testing"
)

func TestSplitFixMessage(t *testing.T) {
    // A sample logon message (BodyLength=59, CheckSum=046)
    msg1 := []byte("8=FIX.4.4\x019=59\x0135=A\x0149=SNDR\x0156=TGT\x0134=1\x0152=20240101-00:00:00\x0198=0\x01108=30\x0110=046\x01")
    // A sample heartbeat (BodyLength=47, CheckSum=113)
    msg2 := []byte("8=FIX.4.4\x019=47\x0135=0\x0149=SNDR\x0156=TGT\x0134=2\x0152=20240101-00:00:01\x0110=113\x01")
    
    combined := append(msg1, msg2...)
    
    scanner := bufio.NewScanner(bytes.NewReader(combined))
    scanner.Split(SplitFixMessage)
    
    if !scanner.Scan() {
        t.Fatal("Failed to scan first message")
    }
    if !bytes.Equal(scanner.Bytes(), msg1) {
        t.Errorf("First message mismatch.\nExpected: %q\nGot:      %q", msg1, scanner.Bytes())
    }
    
    if !scanner.Scan() {
        t.Fatal("Failed to scan second message")
    }
    if !bytes.Equal(scanner.Bytes(), msg2) {
        t.Errorf("Second message mismatch.\nExpected: %q\nGot:      %q", msg2, scanner.Bytes())
    }
}

func TestParse(t *testing.T) {
    original := NewMessage(MsgTypeLogon)
    original.AddField(TagSenderCompID, "SNDR")
    raw := Serialize(original)

    msg, err := Parse(raw)
    if err != nil {
        t.Fatalf("Parse error: %v", err)
    }
    
    if msg.MsgType != MsgTypeLogon {
        t.Errorf("Expected MsgType A, got %s", msg.MsgType)
    }
    
    val, _ := msg.GetField(TagSenderCompID)
    if val != "SNDR" {
        t.Errorf("Expected SNDR, got %s", val)
    }
}

func TestParseRejectsIncorrectBodyLength(t *testing.T) {
    original := NewMessage(MsgTypeLogon)
    original.AddField(TagSenderCompID, "SNDR")
    raw := Serialize(original)
    raw = bytes.Replace(raw, []byte("\x019=13\x01"), []byte("\x019=12\x01"), 1)

    if _, err := Parse(raw); err == nil {
        t.Fatal("Expected invalid body length to be rejected")
    }
}
