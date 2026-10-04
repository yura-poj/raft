package storage

import (
	"os"
	"path/filepath"
	"testing"

	"raftdemo/internal/raft"
)

func TestFileStoreRoundTrip(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "nested", "node1.json")
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if initial != (raft.PersistentState{}) {
		t.Fatalf("initial state = %+v", initial)
	}

	want := raft.PersistentState{CurrentTerm: 7, VotedFor: "node2"}
	if err := store.Save(want); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := reopened.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("Load() = %+v, want %+v", got, want)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("state file mode = %o, want 600", info.Mode().Perm())
	}
}

func TestFileStoreRejectsCorruptedState(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "node1.json")
	if err := os.WriteFile(path, []byte(`{"current_term":3,"unknown":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(); err == nil {
		t.Fatal("Load() error = nil for corrupted state")
	}
}
