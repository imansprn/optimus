package session

import (
    "encoding/json"
    "fmt"
    "os"
    "path/filepath"
    "sync"

    "github.com/rs/zerolog/log"
)

type SequenceStore struct {
    path string
    mu   sync.Mutex
}

type SessionStateData struct {
    InSeq  int64 `json:"in_seq"`
    OutSeq int64 `json:"out_seq"`
}

func NewSequenceStore(dir string) *SequenceStore {
    if err := os.MkdirAll(dir, 0755); err != nil {
        log.Error().Err(err).Msg("Failed to create sequence store directory")
    }
    return &SequenceStore{path: dir}
}

func (s *SequenceStore) Load(sender, target string) (int64, int64, error) {
    s.mu.Lock()
    defer s.mu.Unlock()

    filename := filepath.Join(s.path, fmt.Sprintf("%s_%s.json", sender, target))
    data, err := os.ReadFile(filename)
    if err != nil {
        if os.IsNotExist(err) {
            return 0, 0, nil
        }
        return 0, 0, err
    }

    var state SessionStateData
    if err := json.Unmarshal(data, &state); err != nil {
        return 0, 0, err
    }
    return state.InSeq, state.OutSeq, nil
}

func (s *SequenceStore) Save(sender, target string, inSeq, outSeq int64) error {
    s.mu.Lock()
    defer s.mu.Unlock()

    state := SessionStateData{
        InSeq:  inSeq,
        OutSeq: outSeq,
    }
    data, err := json.Marshal(state)
    if err != nil {
        return err
    }

    filename := filepath.Join(s.path, fmt.Sprintf("%s_%s.json", sender, target))
    return os.WriteFile(filename, data, 0644)
}
