package fix

import (
    "bytes"
    "fmt"
    "strconv"
)

// Serialize a Message into raw FIX bytes.
func Serialize(m *Message) []byte {
    var body bytes.Buffer
    
    // Skip 8 and 9, they are calculated later.
    // But 35 must be first after 8 and 9.
    
    // Add other fields
    for _, f := range m.Fields {
        if f.Tag == TagBeginString || f.Tag == TagBodyLength || f.Tag == TagCheckSum {
            continue
        }
        body.WriteString(strconv.Itoa(f.Tag))
        body.WriteByte('=')
        body.WriteString(f.Value)
        body.WriteByte(SOH)
    }
    
    bodyBytes := body.Bytes()
    bodyLength := len(bodyBytes)
    
    beginString, _ := m.GetField(TagBeginString)
    if beginString == "" {
        beginString = "FIX.4.4"
    }
    
    var full bytes.Buffer
    full.WriteString("8=")
    full.WriteString(beginString)
    full.WriteByte(SOH)
    
    full.WriteString("9=")
    full.WriteString(strconv.Itoa(bodyLength))
    full.WriteByte(SOH)
    
    full.Write(bodyBytes)
    
    // Checksum
    checksum := CalculateChecksum(full.Bytes())
    full.WriteString("10=")
    full.WriteString(checksum)
    full.WriteByte(SOH)
    
    return full.Bytes()
}

// NewMessage creates a new FIX message with the given MsgType.
func NewMessage(msgType string) *Message {
    return &Message{
        MsgType: msgType,
        Fields: []Field{
            {Tag: TagMsgType, Value: msgType},
        },
    }
}

func (m *Message) AddField(tag int, value string) {
    m.Fields = append(m.Fields, Field{Tag: tag, Value: value})
}

func (m *Message) SetField(tag int, value string) {
    for i, f := range m.Fields {
        if f.Tag == tag {
            m.Fields[i].Value = value
            return
        }
    }
    m.AddField(tag, value)
}
// Template represents a pre-serialized message with placeholders for fast patching.
type Template struct {
    Data    []byte
    Patches map[int]PatchInfo
}

type PatchInfo struct {
    Offset int
    Length int
}

// NewTemplate creates a new Template and identifies the offsets of tags to be patched.
func NewTemplate(m *Message, patchTags []int) *Template {
    // We'll use a unique placeholder for each patch tag to find its offset.
    // For simplicity, we assume the tags already exist in the message.
    
    // Create a copy of the message to avoid mutating the original
    tmplMsg := &Message{
        MsgType: m.MsgType,
        Fields:  make([]Field, len(m.Fields)),
    }
    copy(tmplMsg.Fields, m.Fields)
    
    patches := make(map[int]PatchInfo)
    for _, tag := range patchTags {
        // Set a distinctive placeholder
        placeholder := fmt.Sprintf("__%d__", tag)
        tmplMsg.SetField(tag, placeholder)
    }
    
    data := Serialize(tmplMsg)
    
    for _, tag := range patchTags {
        placeholder := []byte(fmt.Sprintf("__%d__", tag))
        offset := bytes.Index(data, placeholder)
        if offset != -1 {
            patches[tag] = PatchInfo{
                Offset: offset,
                Length: len(placeholder),
            }
        }
    }
    
    return &Template{
        Data:    data,
        Patches: patches,
    }
}

// Patch creates a copy of the template data and overwrites the specified tag value.
// Note: This only works if the new value has the SAME length as the placeholder, 
// OR if the template was designed with a large enough padding.
// For our use case (IDs), we'll ensure the placeholder is long enough.
func (t *Template) Patch(tag int, newValue string) []byte {
    info, ok := t.Patches[tag]
    if !ok {
        return t.Data
    }
    
    res := make([]byte, len(t.Data))
    copy(res, t.Data)
    
    // Overwrite at offset. Pad with spaces if newValue is shorter.
    valBytes := []byte(newValue)
    for i := 0; i < info.Length; i++ {
        if i < len(valBytes) {
            res[info.Offset+i] = valBytes[i]
        } else {
            res[info.Offset+i] = ' ' // Padding
        }
    }
    
    // Recalculate checksum (last 7 bytes: 10=XXX|)
    checksum := CalculateChecksum(res[:len(res)-7])
    copy(res[len(res)-4:len(res)-1], []byte(checksum))
    
    return res
}
