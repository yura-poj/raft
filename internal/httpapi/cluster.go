package httpapi

import (
	"context"
	"time"

	"raftdemo/internal/raft"
)

type StatusClient interface {
	Status(ctx context.Context, peer raft.Peer) (raft.Status, error)
}

type ClusterStatus struct {
	ObserverID string              `json:"observer_id"`
	ObservedAt time.Time           `json:"observed_at"`
	Nodes      []ClusterNodeStatus `json:"nodes"`
}

type ClusterNodeStatus struct {
	NodeID    string       `json:"node_id"`
	Address   string       `json:"address"`
	Reachable bool         `json:"reachable"`
	Status    *raft.Status `json:"status"`
	Error     *string      `json:"error"`
}

type clusterResult struct {
	index  int
	status raft.Status
	err    error
}

func collectClusterStatus(ctx context.Context, node *raft.Node, client StatusClient, timeout time.Duration) ClusterStatus {
	local := node.Status()
	peers := node.Peers()
	result := ClusterStatus{
		ObserverID: local.NodeID,
		ObservedAt: time.Now().UTC(),
		Nodes:      make([]ClusterNodeStatus, len(peers)),
	}

	responses := make(chan clusterResult, len(peers))
	pending := 0
	for index, peer := range peers {
		result.Nodes[index] = ClusterNodeStatus{NodeID: peer.ID, Address: peer.URL}
		if peer.ID == local.NodeID {
			status := local
			result.Nodes[index].Reachable = true
			result.Nodes[index].Status = &status
			continue
		}
		pending++
		go func(index int, peer raft.Peer) {
			requestCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			status, err := client.Status(requestCtx, peer)
			responses <- clusterResult{index: index, status: status, err: err}
		}(index, peer)
	}

	for range pending {
		response := <-responses
		if response.err != nil {
			message := "unreachable"
			result.Nodes[response.index].Error = &message
			continue
		}
		status := response.status
		result.Nodes[response.index].Reachable = true
		result.Nodes[response.index].Status = &status
	}
	return result
}
