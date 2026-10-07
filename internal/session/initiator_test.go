package session

import (
    "context"
    "testing"

    "github.com/imansprn/optimus/internal/fix"
)

func TestResetLogonReplacesStoredSequences(t *testing.T) {
    ctx, cancel := context.WithCancel(context.Background())
    cancel()
    s := &UpstreamSession{
        state: StateConnecting,
        inSeqNum: 3339,
        outSeqNum: 224,
        heartBtInt: 30,
        store: NewSequenceStore(t.TempDir()),
        pendingMessages: map[int64]*fix.Message{3341: fix.NewMessage(fix.MsgTypeHeartbeat)},
        resendUntil: 3341,
    }
    msg := fix.NewMessage(fix.MsgTypeLogon)
    msg.AddField(fix.TagMsgSeqNum, "1")
    msg.AddField(fix.TagResetSeqNumFlag, "Y")
    s.handleMessage(ctx, msg)
    if s.state != StateActive || s.inSeqNum != 1 || s.outSeqNum != 1 {
        t.Fatalf("reset failed: state=%v in=%d out=%d", s.state, s.inSeqNum, s.outSeqNum)
    }
    if len(s.pendingMessages) != 0 || s.resendUntil != 0 {
        t.Fatal("reset retained pending messages")
    }
    in, out, err := s.store.Load(s.senderCompID, s.targetCompID)
    if err != nil || in != 1 || out != 1 {
        t.Fatalf("reset not persisted: in=%d out=%d err=%v", in, out, err)
    }
}
