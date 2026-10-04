package raft

import (
	"context"
	"sync"
	"time"
)

type electionRound struct {
	ctx        context.Context
	cancel     context.CancelFunc
	term       uint64
	generation uint64
	peers      []Peer
}

type voteResult struct {
	peerID   string
	response RequestVoteResponse
	err      error
}

func (n *Node) beginElection(expectedGeneration uint64) {
	n.mu.Lock()
	now := time.Now()
	if n.storageErr != nil || n.role == RoleLeader || n.electionGeneration != expectedGeneration || now.Before(n.electionDeadline) {
		n.mu.Unlock()
		return
	}
	nextState := PersistentState{CurrentTerm: n.currentTerm + 1, VotedFor: n.id}
	if err := n.persistLocked(nextState); err != nil {
		cancel, _ := n.failStorageLocked(err)
		n.mu.Unlock()
		cancelFunctions(cancel)
		n.rootCancel()
		return
	}

	previousCancel := n.electionCancel
	n.currentTerm = nextState.CurrentTerm
	n.role = RoleCandidate
	n.votedFor = n.id
	n.leaderID = ""
	n.lastLeaderContact = time.Time{}
	n.resetElectionDeadlineLocked(now)
	roundCtx, roundCancel := context.WithCancel(n.rootCtx)
	n.electionCancel = roundCancel
	round := electionRound{
		ctx:        roundCtx,
		cancel:     roundCancel,
		term:       n.currentTerm,
		generation: n.electionGeneration,
		peers:      n.otherPeersLocked(),
	}
	n.mu.Unlock()

	if previousCancel != nil {
		previousCancel()
	}
	n.signalTimerWake()
	n.launchTask(func() { n.runElectionRound(round) })
}

func (n *Node) runElectionRound(round electionRound) {
	var workers sync.WaitGroup
	defer func() {
		round.cancel()
		workers.Wait()
	}()

	votes := map[string]struct{}{n.id: {}}
	if len(votes) >= n.majority() {
		n.promoteToLeader(round.term, round.generation)
		return
	}

	results := make(chan voteResult, len(round.peers))
	for _, peer := range round.peers {
		peer := peer
		workers.Add(1)
		go func() {
			defer workers.Done()
			ctx, cancel := context.WithTimeout(round.ctx, n.rpcTimeout)
			defer cancel()
			response, err := n.client.RequestVote(ctx, peer, RequestVoteRequest{Term: round.term, CandidateID: n.id})
			results <- voteResult{peerID: peer.ID, response: response, err: err}
		}()
	}

	for range round.peers {
		select {
		case <-round.ctx.Done():
			return
		case result := <-results:
			if result.err != nil {
				continue
			}
			if result.response.Term > round.term {
				n.observeHigherTerm(result.response.Term)
				return
			}
			if result.response.Term != round.term || !result.response.VoteGranted {
				continue
			}
			if !n.isCurrentCandidate(round.term, round.generation) {
				return
			}
			votes[result.peerID] = struct{}{}
			if len(votes) >= n.majority() {
				n.promoteToLeader(round.term, round.generation)
				return
			}
		}
	}
}

func (n *Node) promoteToLeader(term uint64, generation uint64) {
	n.mu.Lock()
	if n.role != RoleCandidate || n.currentTerm != term || n.electionGeneration != generation {
		n.mu.Unlock()
		return
	}
	cancel := n.electionCancel
	n.electionCancel = nil
	n.role = RoleLeader
	n.leaderID = n.id
	n.lastLeaderContact = time.Time{}
	n.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	n.signalTimerWake()
	n.launchHeartbeat()
}

func (n *Node) isCurrentCandidate(term uint64, generation uint64) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == RoleCandidate && n.currentTerm == term && n.electionGeneration == generation
}

func (n *Node) observeHigherTerm(term uint64) {
	n.mu.Lock()
	if n.storageErr != nil {
		n.mu.Unlock()
		return
	}
	changed := term > n.currentTerm
	cancel, err := n.observeHigherTermLocked(term)
	if err != nil {
		failureCancel, _ := n.failStorageLocked(err)
		n.mu.Unlock()
		cancelFunctions(cancel, failureCancel)
		n.rootCancel()
		return
	}
	n.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if changed {
		n.signalTimerWake()
	}
}

func (n *Node) otherPeersLocked() []Peer {
	peers := make([]Peer, 0, len(n.peers)-1)
	for _, peer := range n.peers {
		if peer.ID != n.id {
			peers = append(peers, peer)
		}
	}
	return peers
}
