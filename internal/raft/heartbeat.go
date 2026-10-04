package raft

import (
	"context"
	"sync"
)

type heartbeatResult struct {
	response AppendEntriesResponse
	err      error
}

func (n *Node) launchHeartbeat() {
	n.mu.Lock()
	if n.storageErr != nil || n.role != RoleLeader || n.heartbeatInFlight {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	peers := n.otherPeersLocked()
	n.heartbeatInFlight = true
	n.mu.Unlock()

	if !n.launchTask(func() {
		defer n.finishHeartbeat()
		n.broadcastHeartbeat(term, peers)
	}) {
		n.finishHeartbeat()
	}
}

func (n *Node) finishHeartbeat() {
	n.mu.Lock()
	n.heartbeatInFlight = false
	n.mu.Unlock()
}

func (n *Node) broadcastHeartbeat(term uint64, peers []Peer) {
	if len(peers) == 0 {
		return
	}
	ctx, cancel := context.WithCancel(n.rootCtx)
	var workers sync.WaitGroup
	defer func() {
		cancel()
		workers.Wait()
	}()
	results := make(chan heartbeatResult, len(peers))
	for _, peer := range peers {
		peer := peer
		workers.Add(1)
		go func() {
			defer workers.Done()
			requestCtx, requestCancel := context.WithTimeout(ctx, n.rpcTimeout)
			defer requestCancel()
			response, err := n.client.AppendEntries(requestCtx, peer, AppendEntriesRequest{Term: term, LeaderID: n.id})
			results <- heartbeatResult{response: response, err: err}
		}()
	}

	for range peers {
		select {
		case <-ctx.Done():
			return
		case result := <-results:
			if result.err == nil && result.response.Term > term {
				n.observeHigherTerm(result.response.Term)
				return
			}
		}
	}
}
