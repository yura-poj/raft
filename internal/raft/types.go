package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type Role string

const (
	RoleFollower  Role = "follower"
	RoleCandidate Role = "candidate"
	RoleLeader    Role = "leader"
)

var (
	ErrUnknownMember  = errors.New("unknown cluster member")
	ErrUnsupportedLog = errors.New("log replication is not supported")
	ErrInvalidRPC     = errors.New("invalid raft rpc")
	ErrStorage        = errors.New("stable storage failure")
)

type Peer struct {
	ID  string `json:"node_id"`
	URL string `json:"address"`
}

type Config struct {
	ID                 string
	Address            string
	Peers              []Peer
	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	RPCTimeout         time.Duration
}

type PersistentState struct {
	CurrentTerm uint64 `json:"current_term"`
	VotedFor    string `json:"voted_for"`
}

type StableStore interface {
	Load() (PersistentState, error)
	Save(PersistentState) error
}

type RequestVoteRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

type RequestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

type AppendEntriesRequest struct {
	Term         uint64            `json:"term"`
	LeaderID     string            `json:"leader_id"`
	PrevLogIndex uint64            `json:"prev_log_index"`
	PrevLogTerm  uint64            `json:"prev_log_term"`
	Entries      []json.RawMessage `json:"entries,omitempty"`
	LeaderCommit uint64            `json:"leader_commit"`
}

type AppendEntriesResponse struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
}

type Status struct {
	ObservedAt        time.Time  `json:"observed_at"`
	NodeID            string     `json:"node_id"`
	Address           string     `json:"address"`
	Role              Role       `json:"role"`
	Term              uint64     `json:"term"`
	LeaderID          *string    `json:"leader_id"`
	VotedFor          *string    `json:"voted_for"`
	LastLeaderContact *time.Time `json:"last_leader_contact"`
	Peers             []Peer     `json:"peers"`
}

type PeerClient interface {
	RequestVote(ctx context.Context, peer Peer, req RequestVoteRequest) (RequestVoteResponse, error)
	AppendEntries(ctx context.Context, peer Peer, req AppendEntriesRequest) (AppendEntriesResponse, error)
}

type ValidationError struct {
	Cause error
	Field string
	Value string
}

func (e *ValidationError) Error() string {
	if e.Value == "" {
		return fmt.Sprintf("%s: %v", e.Field, e.Cause)
	}
	return fmt.Sprintf("%s %q: %v", e.Field, e.Value, e.Cause)
}

func (e *ValidationError) Unwrap() error {
	return e.Cause
}
