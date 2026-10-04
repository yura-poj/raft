package integration

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"raftdemo/internal/raft"
	"raftdemo/internal/storage"
)

type offlineClient struct{}

func (offlineClient) RequestVote(context.Context, raft.Peer, raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	return raft.RequestVoteResponse{}, errors.New("offline")
}

func (offlineClient) AppendEntries(context.Context, raft.Peer, raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	return raft.AppendEntriesResponse{}, errors.New("offline")
}

func TestVoteAndTermSurviveRestartWithFileStore(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "node1.json")
	peers := []raft.Peer{
		{ID: "node1", URL: "http://127.0.0.1:9001"},
		{ID: "node2", URL: "http://127.0.0.1:9002"},
		{ID: "node3", URL: "http://127.0.0.1:9003"},
	}
	cfg := raft.Config{
		ID:                 "node1",
		Address:            peers[0].URL,
		Peers:              peers,
		HeartbeatInterval:  10 * time.Millisecond,
		ElectionTimeoutMin: 100 * time.Millisecond,
		ElectionTimeoutMax: 200 * time.Millisecond,
		RPCTimeout:         20 * time.Millisecond,
	}

	firstStore, err := storage.NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	first, err := raft.NewNode(cfg, offlineClient{}, firstStore)
	if err != nil {
		t.Fatal(err)
	}
	vote, err := first.HandleRequestVote(raft.RequestVoteRequest{Term: 5, CandidateID: "node2"})
	if err != nil || !vote.VoteGranted {
		t.Fatalf("first vote = %+v, err = %v", vote, err)
	}

	reopenedStore, err := storage.NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := raft.NewNode(cfg, offlineClient{}, reopenedStore)
	if err != nil {
		t.Fatal(err)
	}
	secondVote, err := restarted.HandleRequestVote(raft.RequestVoteRequest{Term: 5, CandidateID: "node3"})
	if err != nil {
		t.Fatal(err)
	}
	if secondVote.VoteGranted {
		t.Fatal("restarted node granted a second vote in term 5")
	}
	status := restarted.Status()
	if status.Term != 5 || status.VotedFor == nil || *status.VotedFor != "node2" {
		t.Fatalf("restored status = %+v", status)
	}
}
