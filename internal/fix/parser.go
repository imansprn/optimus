package fix

import (
    "bytes"
    "errors"
    "fmt"
    "strconv"
)

var (
    ErrInvalidFormat = errors.New("invalid FIX message format")
    ErrChecksum      = errors.New("checksum mismatch")
)

// Parse raw bytes into a Message struct.
// Expects a full FIX message including checksum.
func Parse(data []byte) (*Message, error) {
    if len(data) < 7 { // 8=FIX.4.4|...10=XXX|
        return nil, ErrInvalidFormat
    }

    msg := &Message{
        Fields: make([]Field, 0, 16),
    }

    pos := 0
    for pos < len(data) {
        eqPos := bytes.IndexByte(data[pos:], '=')
        if eqPos == -1 {
            break
        }
        eqPos += pos

        tagBytes := data[pos:eqPos]
        tag, err := strconv.Atoi(string(tagBytes))
        if err != nil {
            return nil, fmt.Errorf("invalid tag at pos %d: %w", pos, err)
        }

        sohPos := bytes.IndexByte(data[eqPos:], SOH)
        if sohPos == -1 {
            // Some FIX variants use | as separator in logs, and PrimeXM might too.
            // But the spec says SOH (0x01).
            return nil, ErrInvalidFormat
        }
        sohPos += eqPos

        val := string(bytes.TrimRight(data[eqPos+1:sohPos], " "))
        msg.Fields = append(msg.Fields, Field{Tag: tag, Value: val})

        // Fill fast-lookup index
        if tag >= 0 && tag < 1024 {
            msg.fastFields[tag] = val
        }

        if tag == TagMsgType {
            msg.MsgType = val
        }

        pos = sohPos + 1
    }

    // Basic validation: must have 8, 9, 35, and 10.
    if len(msg.Fields) < 4 {
        return nil, ErrInvalidFormat
    }
    if msg.Fields[0].Tag != TagBeginString {
        return nil, errors.New("missing or misplaced Tag 8")
    }
    if msg.Fields[1].Tag != TagBodyLength {
        return nil, errors.New("missing or misplaced Tag 9")
    }

    // Checksum validation
    // Data to checksum is everything from start to the index of tag 10
    idx10 := bytes.LastIndex(data, []byte("\x0110="))
    if idx10 != -1 {
        calculated := CalculateChecksum(data[:idx10+1])
        provided, _ := msg.GetField(TagCheckSum)
        if calculated != provided {
            return nil, fmt.Errorf("checksum mismatch: expected %s, got %s", provided, calculated)
        }
    }

    return msg, nil
}

// CalculateChecksum calculates the FIX checksum for the given data.
func CalculateChecksum(data []byte) string {
    var sum uint32
    for _, b := range data {
        sum += uint32(b)
    }
    return fmt.Sprintf("%03d", sum%256)
}

// SplitFixMessage is a bufio.SplitFunc for splitting a FIX message stream.
func SplitFixMessage(data []byte, atEOF bool) (advance int, token []byte, err error) {
    if atEOF && len(data) == 0 {
        return 0, nil, nil
    }

    // Find the start of the message (8=)
    start := bytes.Index(data, []byte("8="))
    if start == -1 {
        if atEOF {
            return len(data), nil, nil
        }
        return 0, nil, nil
    }

    // We need at least the BodyLength tag (9=) to know the full length
    // 8=FIX.4.4|9=
    idx9 := bytes.Index(data[start:], []byte("\x019="))
    if idx9 == -1 {
        if atEOF {
            return len(data), nil, nil
        }
        return 0, nil, nil
    }
    idx9 += start + 1 // Start of "9="

    // Find the SOH after the length value
    idxSoh := bytes.IndexByte(data[idx9+2:], SOH)
    if idxSoh == -1 {
        if atEOF {
            return len(data), nil, nil
        }
        return 0, nil, nil
    }
    idxSoh += idx9 + 2

    // Parse the body length
    bodyLengthStr := string(data[idx9+2 : idxSoh])
    bodyLength, err := strconv.Atoi(bodyLengthStr)
    if err != nil {
        return 0, nil, err
    }

    // The body starts after the SOH of the 9= tag.
    // The checksum tag (10=) starts after the body.
    bodyStart := idxSoh + 1
    
    // Total length = bodyStart + bodyLength + checksumTagLength
    // We'll search for the SOH after 10= to be safe.
    checksumSearchStart := bodyStart + bodyLength
    if len(data) < checksumSearchStart+5 {
        if atEOF {
            return len(data), nil, nil
        }
        return 0, nil, nil
    }

    // Find the SOH after Tag 10. It should be within a reasonable distance.
    // Usually at index 6 or 7 from checksumSearchStart.
    nextSoh := bytes.IndexByte(data[checksumSearchStart:], SOH)
    if nextSoh == -1 {
        if atEOF {
            return len(data), nil, nil
        }
        return 0, nil, nil
    }
    
    totalLength := checksumSearchStart + nextSoh + 1
    return totalLength, data[start:totalLength], nil
}
