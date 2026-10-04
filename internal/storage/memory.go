package storage

import (
	"sync"

	"raftdemo/internal/raft"
)

type MemoryStore struct {
	mu    sync.Mutex
	state raft.PersistentState
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{}
}

func (s *MemoryStore) Load() (raft.PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state, nil
}

func (s *MemoryStore) Save(state raft.PersistentState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = state
	return nil
}
