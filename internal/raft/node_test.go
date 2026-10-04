package raft

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeClient struct {
	requestVote   func(context.Context, Peer, RequestVoteRequest) (RequestVoteResponse, error)
	appendEntries func(context.Context, Peer, AppendEntriesRequest) (AppendEntriesResponse, error)
}

type testStore struct {
	mu    sync.Mutex
	state PersistentState
	err   error
}

func (s *testStore) Load() (PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state, s.err
}

func (s *testStore) Save(state PersistentState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.state = state
	return nil
}

func (f fakeClient) RequestVote(ctx context.Context, peer Peer, req RequestVoteRequest) (RequestVoteResponse, error) {
	if f.requestVote == nil {
		return RequestVoteResponse{}, errors.New("unreachable")
	}
	return f.requestVote(ctx, peer, req)
}

func (f fakeClient) AppendEntries(ctx context.Context, peer Peer, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	if f.appendEntries == nil {
		return AppendEntriesResponse{}, errors.New("unreachable")
	}
	return f.appendEntries(ctx, peer, req)
}

func TestVoteOnceAndPreserveVoteOnSameTermHeartbeat(t *testing.T) {
	t.Parallel()
	node := newTestNode(t, "node1", fakeClient{}, 200*time.Millisecond)

	vote, err := node.HandleRequestVote(RequestVoteRequest{Term: 1, CandidateID: "node2"})
	if err != nil || !vote.VoteGranted {
		t.Fatalf("first vote = %+v, err = %v", vote, err)
	}
	heartbeat, err := node.HandleAppendEntries(AppendEntriesRequest{Term: 1, LeaderID: "node3"})
	if err != nil || !heartbeat.Success {
		t.Fatalf("heartbeat = %+v, err = %v", heartbeat, err)
	}
	secondVote, err := node.HandleRequestVote(RequestVoteRequest{Term: 1, CandidateID: "node3"})
	if err != nil {
		t.Fatalf("second vote error = %v", err)
	}
	if secondVote.VoteGranted {
		t.Fatal("node granted a second vote in the same term")
	}
	status := node.Status()
	if status.VotedFor == nil || *status.VotedFor != "node2" {
		t.Fatalf("voted_for = %v, want node2", status.VotedFor)
	}
}

func TestUnknownMemberDoesNotChangeState(t *testing.T) {
	t.Parallel()
	node := newTestNode(t, "node1", fakeClient{}, 200*time.Millisecond)
	before := node.Status()
	_, err := node.HandleRequestVote(RequestVoteRequest{Term: 9, CandidateID: "intruder"})
	if !errors.Is(err, ErrUnknownMember) {
		t.Fatalf("error = %v, want ErrUnknownMember", err)
	}
	after := node.Status()
	if after.Term != before.Term || after.Role != before.Role || after.VotedFor != nil {
		t.Fatalf("state changed after invalid member: before=%+v after=%+v", before, after)
	}
}

func TestHigherTermHeartbeatClearsOldVote(t *testing.T) {
	t.Parallel()
	node := newTestNode(t, "node1", fakeClient{}, 200*time.Millisecond)
	if _, err := node.HandleRequestVote(RequestVoteRequest{Term: 1, CandidateID: "node2"}); err != nil {
		t.Fatal(err)
	}
	if _, err := node.HandleAppendEntries(AppendEntriesRequest{Term: 2, LeaderID: "node3"}); err != nil {
		t.Fatal(err)
	}
	status := node.Status()
	if status.Term != 2 || status.Role != RoleFollower || status.VotedFor != nil || status.LeaderID == nil || *status.LeaderID != "node3" {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestSingleNodeElectsItself(t *testing.T) {
	t.Parallel()
	cfg := testConfig("node1", []Peer{{ID: "node1", URL: "http://127.0.0.1:9001"}})
	node, err := NewNode(cfg, fakeClient{}, &testStore{}, WithElectionTimeoutSource(func() time.Duration { return 15 * time.Millisecond }))
	if err != nil {
		t.Fatal(err)
	}
	node.Start()
	defer stopTestNode(t, node)
	waitFor(t, time.Second, func() bool { return node.Status().Role == RoleLeader })
}

func TestElectionPersistsTermAndVoteBeforeRequestVote(t *testing.T) {
	t.Parallel()
	store := &testStore{}
	client := fakeClient{requestVote: func(_ context.Context, _ Peer, req RequestVoteRequest) (RequestVoteResponse, error) {
		state, err := store.Load()
		if err != nil {
			return RequestVoteResponse{}, err
		}
		if state.CurrentTerm != req.Term || state.VotedFor != "node1" {
			return RequestVoteResponse{}, errors.New("election state was not persisted before RequestVote")
		}
		return RequestVoteResponse{Term: req.Term, VoteGranted: true}, nil
	}}
	peers := []Peer{
		{ID: "node1", URL: "http://127.0.0.1:9001"},
		{ID: "node2", URL: "http://127.0.0.1:9002"},
	}
	node, err := NewNode(testConfig("node1", peers), client, store, WithElectionTimeoutSource(func() time.Duration { return 15 * time.Millisecond }))
	if err != nil {
		t.Fatal(err)
	}
	node.Start()
	defer stopTestNode(t, node)
	waitFor(t, time.Second, func() bool { return node.Status().Role == RoleLeader })
}

func TestFailedElectionRetriesAndWinsNextTerm(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	client := fakeClient{
		requestVote: func(_ context.Context, _ Peer, req RequestVoteRequest) (RequestVoteResponse, error) {
			mu.Lock()
			defer mu.Unlock()
			return RequestVoteResponse{Term: req.Term, VoteGranted: req.Term >= 2}, nil
		},
		appendEntries: func(_ context.Context, _ Peer, req AppendEntriesRequest) (AppendEntriesResponse, error) {
			return AppendEntriesResponse{Term: req.Term, Success: true}, nil
		},
	}
	node := newTestNode(t, "node1", client, 25*time.Millisecond)
	node.Start()
	defer stopTestNode(t, node)
	waitFor(t, time.Second, func() bool {
		status := node.Status()
		return status.Role == RoleLeader && status.Term >= 2
	})
}

func TestMinorityCannotElectLeader(t *testing.T) {
	t.Parallel()
	node := newTestNode(t, "node1", fakeClient{}, 20*time.Millisecond)
	node.Start()
	defer stopTestNode(t, node)
	time.Sleep(100 * time.Millisecond)
	status := node.Status()
	if status.Role == RoleLeader {
		t.Fatalf("minority elected a leader: %+v", status)
	}
	if status.Term < 2 {
		t.Fatalf("expected repeated election rounds, got term %d", status.Term)
	}
}

func TestHeartbeatsKeepFollowerStable(t *testing.T) {
	t.Parallel()
	node := newTestNode(t, "node1", fakeClient{}, 35*time.Millisecond)
	node.Start()
	defer stopTestNode(t, node)
	deadline := time.Now().Add(140 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, err := node.HandleAppendEntries(AppendEntriesRequest{Term: 1, LeaderID: "node2"}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	status := node.Status()
	if status.Role != RoleFollower || status.Term != 1 {
		t.Fatalf("heartbeat did not keep follower stable: %+v", status)
	}
}

func TestHigherTermVoteResponseStepsCandidateDown(t *testing.T) {
	t.Parallel()
	client := fakeClient{requestVote: func(_ context.Context, _ Peer, _ RequestVoteRequest) (RequestVoteResponse, error) {
		return RequestVoteResponse{Term: 7, VoteGranted: false}, nil
	}}
	node := newTestNode(t, "node1", client, 15*time.Millisecond)
	node.Start()
	defer stopTestNode(t, node)
	waitFor(t, time.Second, func() bool {
		status := node.Status()
		return status.Role == RoleFollower && status.Term == 7
	})
}

func TestElectionStateSurvivesNodeRecreation(t *testing.T) {
	t.Parallel()
	store := &testStore{}
	cfg := testConfig("node1", []Peer{
		{ID: "node1", URL: "http://127.0.0.1:9001"},
		{ID: "node2", URL: "http://127.0.0.1:9002"},
		{ID: "node3", URL: "http://127.0.0.1:9003"},
	})
	first, err := NewNode(cfg, fakeClient{}, store)
	if err != nil {
		t.Fatal(err)
	}
	vote, err := first.HandleRequestVote(RequestVoteRequest{Term: 5, CandidateID: "node2"})
	if err != nil || !vote.VoteGranted {
		t.Fatalf("first vote = %+v, err = %v", vote, err)
	}

	restarted, err := NewNode(cfg, fakeClient{}, store)
	if err != nil {
		t.Fatal(err)
	}
	status := restarted.Status()
	if status.Term != 5 || status.VotedFor == nil || *status.VotedFor != "node2" {
		t.Fatalf("restored status = %+v", status)
	}
	secondVote, err := restarted.HandleRequestVote(RequestVoteRequest{Term: 5, CandidateID: "node3"})
	if err != nil {
		t.Fatal(err)
	}
	if secondVote.VoteGranted {
		t.Fatal("restarted node granted a second vote in persisted term")
	}
}

func TestStorageFailureRejectsVoteWithoutChangingTerm(t *testing.T) {
	t.Parallel()
	store := &testStore{}
	node, err := NewNode(testConfig("node1", []Peer{
		{ID: "node1", URL: "http://127.0.0.1:9001"},
		{ID: "node2", URL: "http://127.0.0.1:9002"},
	}), fakeClient{}, store)
	if err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.err = errors.New("disk unavailable")
	store.mu.Unlock()

	_, err = node.HandleRequestVote(RequestVoteRequest{Term: 1, CandidateID: "node2"})
	if !errors.Is(err, ErrStorage) {
		t.Fatalf("HandleRequestVote() error = %v, want ErrStorage", err)
	}
	status := node.Status()
	if status.Term != 0 || status.VotedFor != nil || status.Role != RoleFollower {
		t.Fatalf("state changed after failed durable write: %+v", status)
	}
}

func TestHeartbeatRoundsDoNotOverlap(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	current := 0
	maximum := 0
	calls := 0
	client := fakeClient{
		requestVote: func(_ context.Context, _ Peer, req RequestVoteRequest) (RequestVoteResponse, error) {
			return RequestVoteResponse{Term: req.Term, VoteGranted: true}, nil
		},
		appendEntries: func(ctx context.Context, _ Peer, req AppendEntriesRequest) (AppendEntriesResponse, error) {
			mu.Lock()
			current++
			calls++
			if current > maximum {
				maximum = current
			}
			mu.Unlock()
			select {
			case <-ctx.Done():
			case <-time.After(35 * time.Millisecond):
			}
			mu.Lock()
			current--
			mu.Unlock()
			return AppendEntriesResponse{Term: req.Term, Success: true}, nil
		},
	}
	peers := []Peer{
		{ID: "node1", URL: "http://127.0.0.1:9001"},
		{ID: "node2", URL: "http://127.0.0.1:9002"},
	}
	cfg := testConfig("node1", peers)
	cfg.HeartbeatInterval = 5 * time.Millisecond
	cfg.ElectionTimeoutMin = 10 * time.Millisecond
	node, err := NewNode(cfg, client, &testStore{}, WithElectionTimeoutSource(func() time.Duration { return 12 * time.Millisecond }))
	if err != nil {
		t.Fatal(err)
	}
	node.Start()
	waitFor(t, time.Second, func() bool { return node.Status().Role == RoleLeader })
	time.Sleep(100 * time.Millisecond)
	stopTestNode(t, node)

	mu.Lock()
	defer mu.Unlock()
	if calls < 2 {
		t.Fatalf("heartbeat calls = %d, want at least 2", calls)
	}
	if maximum != 1 {
		t.Fatalf("maximum concurrent heartbeat calls = %d, want 1", maximum)
	}
}

func newTestNode(t *testing.T, id string, client PeerClient, electionTimeout time.Duration) *Node {
	t.Helper()
	peers := []Peer{
		{ID: "node1", URL: "http://127.0.0.1:9001"},
		{ID: "node2", URL: "http://127.0.0.1:9002"},
		{ID: "node3", URL: "http://127.0.0.1:9003"},
	}
	node, err := NewNode(testConfig(id, peers), client, &testStore{}, WithElectionTimeoutSource(func() time.Duration { return electionTimeout }))
	if err != nil {
		t.Fatalf("NewNode() error = %v", err)
	}
	return node
}

func testConfig(id string, peers []Peer) Config {
	address := ""
	for _, peer := range peers {
		if peer.ID == id {
			address = peer.URL
		}
	}
	return Config{
		ID:                 id,
		Address:            address,
		Peers:              peers,
		HeartbeatInterval:  5 * time.Millisecond,
		ElectionTimeoutMin: 10 * time.Millisecond,
		ElectionTimeoutMax: 500 * time.Millisecond,
		RPCTimeout:         20 * time.Millisecond,
	}
}

func stopTestNode(t *testing.T, node *Node) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := node.Stop(ctx); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition was not satisfied before timeout")
}
