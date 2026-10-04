package raft

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Option func(*Node)

func WithElectionTimeoutSource(source func() time.Duration) Option {
	return func(n *Node) {
		if source != nil {
			n.electionTimeout = source
		}
	}
}

type Node struct {
	mu sync.Mutex

	id      string
	address string
	peers   []Peer
	members map[string]struct{}
	client  PeerClient
	store   StableStore

	heartbeatInterval time.Duration
	rpcTimeout        time.Duration
	electionTimeout   func() time.Duration

	role               Role
	currentTerm        uint64
	votedFor           string
	leaderID           string
	lastLeaderContact  time.Time
	electionDeadline   time.Time
	electionGeneration uint64
	electionCancel     context.CancelFunc
	heartbeatInFlight  bool
	storageErr         error

	rootCtx    context.Context
	rootCancel context.CancelFunc
	timerWake  chan struct{}
	stopCh     chan struct{}
	startOnce  sync.Once
	stopOnce   sync.Once
	wg         sync.WaitGroup
}

func NewNode(cfg Config, client PeerClient, store StableStore, options ...Option) (*Node, error) {
	if cfg.ID == "" || cfg.Address == "" || len(cfg.Peers) == 0 {
		return nil, errors.New("id, address and peers are required")
	}
	if client == nil {
		return nil, errors.New("peer client is required")
	}
	if store == nil {
		return nil, errors.New("stable store is required")
	}
	if cfg.HeartbeatInterval <= 0 || cfg.RPCTimeout <= 0 || cfg.ElectionTimeoutMin <= cfg.HeartbeatInterval || cfg.ElectionTimeoutMax <= cfg.ElectionTimeoutMin {
		return nil, errors.New("invalid raft timing configuration")
	}

	peers := append([]Peer(nil), cfg.Peers...)
	members := make(map[string]struct{}, len(peers))
	selfCount := 0
	for _, peer := range peers {
		if peer.ID == "" || peer.URL == "" {
			return nil, errors.New("peer id and address are required")
		}
		if _, exists := members[peer.ID]; exists {
			return nil, errors.New("peer ids must be unique")
		}
		members[peer.ID] = struct{}{}
		if peer.ID == cfg.ID {
			selfCount++
		}
	}
	if selfCount != 1 {
		return nil, errors.New("current node must appear in peers exactly once")
	}
	persisted, err := store.Load()
	if err != nil {
		return nil, fmt.Errorf("%w: load state: %v", ErrStorage, err)
	}
	if persisted.CurrentTerm == 0 && persisted.VotedFor != "" {
		return nil, errors.New("persisted vote requires a non-zero term")
	}
	if persisted.VotedFor != "" {
		if _, exists := members[persisted.VotedFor]; !exists {
			return nil, fmt.Errorf("persisted vote references unknown member %q", persisted.VotedFor)
		}
	}

	rootCtx, rootCancel := context.WithCancel(context.Background())
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	n := &Node{
		id:                cfg.ID,
		address:           cfg.Address,
		peers:             peers,
		members:           members,
		client:            client,
		store:             store,
		heartbeatInterval: cfg.HeartbeatInterval,
		rpcTimeout:        cfg.RPCTimeout,
		role:              RoleFollower,
		currentTerm:       persisted.CurrentTerm,
		votedFor:          persisted.VotedFor,
		rootCtx:           rootCtx,
		rootCancel:        rootCancel,
		timerWake:         make(chan struct{}, 1),
		stopCh:            make(chan struct{}),
	}
	span := cfg.ElectionTimeoutMax - cfg.ElectionTimeoutMin
	n.electionTimeout = func() time.Duration {
		return cfg.ElectionTimeoutMin + time.Duration(rng.Int63n(int64(span)))
	}
	for _, option := range options {
		option(n)
	}
	n.resetElectionDeadlineLocked(time.Now())
	return n, nil
}

func (n *Node) Start() {
	n.startOnce.Do(func() {
		n.wg.Add(1)
		go n.run()
	})
}

func (n *Node) Stop(ctx context.Context) error {
	n.stopOnce.Do(func() {
		close(n.stopCh)
		n.rootCancel()
		n.mu.Lock()
		cancel := n.electionCancel
		n.electionCancel = nil
		n.mu.Unlock()
		if cancel != nil {
			cancel()
		}
	})

	done := make(chan struct{})
	go func() {
		n.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()

	status := Status{
		ObservedAt: time.Now().UTC(),
		NodeID:     n.id,
		Address:    n.address,
		Role:       n.role,
		Term:       n.currentTerm,
		Peers:      append([]Peer(nil), n.peers...),
	}
	if n.leaderID != "" {
		leaderID := n.leaderID
		status.LeaderID = &leaderID
	}
	if n.votedFor != "" {
		votedFor := n.votedFor
		status.VotedFor = &votedFor
	}
	if !n.lastLeaderContact.IsZero() {
		contact := n.lastLeaderContact.UTC()
		status.LastLeaderContact = &contact
	}
	return status
}

func (n *Node) Peers() []Peer {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]Peer(nil), n.peers...)
}

func (n *Node) HandleRequestVote(req RequestVoteRequest) (RequestVoteResponse, error) {
	if req.Term == 0 || req.CandidateID == "" {
		return RequestVoteResponse{}, &ValidationError{Cause: ErrInvalidRPC, Field: "request_vote"}
	}
	if _, exists := n.members[req.CandidateID]; !exists {
		return RequestVoteResponse{}, &ValidationError{Cause: ErrUnknownMember, Field: "candidate_id", Value: req.CandidateID}
	}
	if req.LastLogIndex != 0 || req.LastLogTerm != 0 {
		return RequestVoteResponse{}, &ValidationError{Cause: ErrUnsupportedLog, Field: "request_vote"}
	}

	n.mu.Lock()
	if n.storageErr != nil {
		err := n.storageErr
		n.mu.Unlock()
		return RequestVoteResponse{}, err
	}
	var cancel context.CancelFunc
	if req.Term < n.currentTerm {
		response := RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}
		n.mu.Unlock()
		return response, nil
	}
	if req.Term > n.currentTerm {
		var err error
		cancel, err = n.observeHigherTermLocked(req.Term)
		if err != nil {
			failureCancel, failure := n.failStorageLocked(err)
			n.mu.Unlock()
			cancelFunctions(cancel, failureCancel)
			n.rootCancel()
			return RequestVoteResponse{}, failure
		}
	}

	granted := n.votedFor == "" || n.votedFor == req.CandidateID
	if granted {
		if n.votedFor != req.CandidateID {
			if err := n.persistLocked(PersistentState{CurrentTerm: n.currentTerm, VotedFor: req.CandidateID}); err != nil {
				failureCancel, failure := n.failStorageLocked(err)
				n.mu.Unlock()
				cancelFunctions(cancel, failureCancel)
				n.rootCancel()
				return RequestVoteResponse{}, failure
			}
			n.votedFor = req.CandidateID
		}
		n.resetElectionDeadlineLocked(time.Now())
	}
	response := RequestVoteResponse{Term: n.currentTerm, VoteGranted: granted}
	n.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if granted {
		n.signalTimerWake()
	}
	return response, nil
}

func (n *Node) HandleAppendEntries(req AppendEntriesRequest) (AppendEntriesResponse, error) {
	if req.Term == 0 || req.LeaderID == "" {
		return AppendEntriesResponse{}, &ValidationError{Cause: ErrInvalidRPC, Field: "append_entries"}
	}
	if _, exists := n.members[req.LeaderID]; !exists {
		return AppendEntriesResponse{}, &ValidationError{Cause: ErrUnknownMember, Field: "leader_id", Value: req.LeaderID}
	}
	if req.PrevLogIndex != 0 || req.PrevLogTerm != 0 || req.LeaderCommit != 0 || len(req.Entries) != 0 {
		return AppendEntriesResponse{}, &ValidationError{Cause: ErrUnsupportedLog, Field: "append_entries"}
	}

	n.mu.Lock()
	if n.storageErr != nil {
		err := n.storageErr
		n.mu.Unlock()
		return AppendEntriesResponse{}, err
	}
	if req.Term < n.currentTerm {
		response := AppendEntriesResponse{Term: n.currentTerm, Success: false}
		n.mu.Unlock()
		return response, nil
	}
	cancel, err := n.acceptLeaderLocked(req.Term, req.LeaderID, time.Now())
	if err != nil {
		failureCancel, failure := n.failStorageLocked(err)
		n.mu.Unlock()
		cancelFunctions(cancel, failureCancel)
		n.rootCancel()
		return AppendEntriesResponse{}, failure
	}
	response := AppendEntriesResponse{Term: n.currentTerm, Success: true}
	n.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	n.signalTimerWake()
	return response, nil
}

func (n *Node) observeHigherTermLocked(term uint64) (context.CancelFunc, error) {
	if term <= n.currentTerm {
		return nil, nil
	}
	if err := n.persistLocked(PersistentState{CurrentTerm: term}); err != nil {
		return nil, err
	}
	cancel := n.electionCancel
	n.electionCancel = nil
	n.currentTerm = term
	n.role = RoleFollower
	n.votedFor = ""
	n.leaderID = ""
	n.lastLeaderContact = time.Time{}
	n.resetElectionDeadlineLocked(time.Now())
	return cancel, nil
}

func (n *Node) acceptLeaderLocked(term uint64, leaderID string, now time.Time) (context.CancelFunc, error) {
	var cancel context.CancelFunc
	if term > n.currentTerm {
		var err error
		cancel, err = n.observeHigherTermLocked(term)
		if err != nil {
			return nil, err
		}
	} else if n.role != RoleFollower {
		cancel = n.electionCancel
		n.electionCancel = nil
		n.role = RoleFollower
	}
	n.leaderID = leaderID
	n.lastLeaderContact = now
	n.resetElectionDeadlineLocked(now)
	return cancel, nil
}

func (n *Node) persistLocked(state PersistentState) error {
	return n.store.Save(state)
}

func (n *Node) failStorageLocked(err error) (context.CancelFunc, error) {
	if n.storageErr == nil {
		n.storageErr = fmt.Errorf("%w: %v", ErrStorage, err)
	}
	cancel := n.electionCancel
	n.electionCancel = nil
	n.role = RoleFollower
	n.leaderID = ""
	return cancel, n.storageErr
}

func cancelFunctions(cancels ...context.CancelFunc) {
	for _, cancel := range cancels {
		if cancel != nil {
			cancel()
		}
	}
}

func (n *Node) resetElectionDeadlineLocked(now time.Time) {
	n.electionGeneration++
	n.electionDeadline = now.Add(n.electionTimeout())
}

func (n *Node) signalTimerWake() {
	select {
	case n.timerWake <- struct{}{}:
	default:
	}
}

func (n *Node) majority() int {
	return len(n.peers)/2 + 1
}
