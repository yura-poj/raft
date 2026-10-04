package integration

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"testing"
	"time"

	"raftdemo/internal/httpapi"
	"raftdemo/internal/raft"
	"raftdemo/internal/storage"
	"raftdemo/internal/transport"
)

type runningNode struct {
	id       string
	url      string
	node     *raft.Node
	server   *http.Server
	listener net.Listener
	stopped  bool
}

func TestThreeNodeElectionAndFailover(t *testing.T) {
	cluster := startCluster(t)
	defer func() {
		for _, member := range cluster {
			member.stop(t)
		}
	}()

	leader := waitForSingleLeader(t, cluster, 3*time.Second)
	initialTerm := leader.node.Status().Term
	time.Sleep(180 * time.Millisecond)
	stableLeader := waitForSingleLeader(t, cluster, time.Second)
	if stableLeader.id != leader.id {
		t.Fatalf("leader changed while heartbeats were active: %s -> %s", leader.id, stableLeader.id)
	}

	leader.stop(t)
	survivors := make([]*runningNode, 0, 2)
	for _, member := range cluster {
		if !member.stopped {
			survivors = append(survivors, member)
		}
	}
	newLeader := waitForSingleLeader(t, survivors, 3*time.Second)
	if newLeader.node.Status().Term <= initialTerm {
		t.Fatalf("new leader term = %d, want > %d", newLeader.node.Status().Term, initialTerm)
	}

	response, err := http.Get(newLeader.url + "/cluster/status")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var status httpapi.ClusterStatus
	if err := json.NewDecoder(response.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	for _, member := range status.Nodes {
		if member.NodeID == leader.id && member.Reachable {
			t.Fatalf("stopped leader is still reachable: %+v", member)
		}
	}
}

func startCluster(t *testing.T) []*runningNode {
	t.Helper()
	const count = 3
	listeners := make([]net.Listener, count)
	peers := make([]raft.Peer, count)
	for index := range count {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[index] = listener
		peers[index] = raft.Peer{ID: "node" + string(rune('1'+index)), URL: "http://" + listener.Addr().String()}
	}

	timeouts := []time.Duration{80 * time.Millisecond, 160 * time.Millisecond, 240 * time.Millisecond}
	cluster := make([]*runningNode, count)
	for index := range count {
		client := transport.NewClient()
		timeout := timeouts[index]
		node, err := raft.NewNode(raft.Config{
			ID:                 peers[index].ID,
			Address:            peers[index].URL,
			Peers:              peers,
			HeartbeatInterval:  25 * time.Millisecond,
			ElectionTimeoutMin: 60 * time.Millisecond,
			ElectionTimeoutMax: 300 * time.Millisecond,
			RPCTimeout:         50 * time.Millisecond,
		}, client, storage.NewMemoryStore(), raft.WithElectionTimeoutSource(func() time.Duration { return timeout }))
		if err != nil {
			t.Fatal(err)
		}
		handler, err := httpapi.New(node, client, 50*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		server := &http.Server{Handler: handler, ReadHeaderTimeout: time.Second}
		member := &runningNode{id: peers[index].ID, url: peers[index].URL, node: node, server: server, listener: listeners[index]}
		cluster[index] = member
		go func() { _ = server.Serve(member.listener) }()
	}
	for _, member := range cluster {
		member.node.Start()
	}
	return cluster
}

func (n *runningNode) stop(t *testing.T) {
	t.Helper()
	if n.stopped {
		return
	}
	n.stopped = true
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := n.node.Stop(ctx); err != nil {
		t.Errorf("stop node %s: %v", n.id, err)
	}
	if err := n.server.Shutdown(ctx); err != nil {
		t.Errorf("shutdown server %s: %v", n.id, err)
	}
}

func waitForSingleLeader(t *testing.T, cluster []*runningNode, timeout time.Duration) *runningNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leader *runningNode
		leaders := 0
		for _, member := range cluster {
			if member.stopped {
				continue
			}
			if member.node.Status().Role == raft.RoleLeader {
				leader = member
				leaders++
			}
		}
		if leaders == 1 {
			return leader
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("cluster did not converge to exactly one leader")
	return nil
}
