package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"raftdemo/internal/raft"
	"raftdemo/internal/storage"
)

type apiClient struct {
	statuses map[string]raft.Status
	errors   map[string]error
}

func (apiClient) RequestVote(context.Context, raft.Peer, raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	return raft.RequestVoteResponse{}, errors.New("not used")
}

func (apiClient) AppendEntries(context.Context, raft.Peer, raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	return raft.AppendEntriesResponse{}, errors.New("not used")
}

func (c apiClient) Status(_ context.Context, peer raft.Peer) (raft.Status, error) {
	if err := c.errors[peer.ID]; err != nil {
		return raft.Status{}, err
	}
	return c.statuses[peer.ID], nil
}

func TestHealthStatusAndFrontend(t *testing.T) {
	t.Parallel()
	node, client := testAPINode(t)
	handler, err := New(node, client, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()

	for _, path := range []string{"/health", "/status", "/"} {
		response, requestErr := http.Get(server.URL + path)
		if requestErr != nil {
			t.Fatalf("GET %s: %v", path, requestErr)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("GET %s status = %d", path, response.StatusCode)
		}
	}
}

func TestUnknownMemberRPCDoesNotChangeState(t *testing.T) {
	t.Parallel()
	node, client := testAPINode(t)
	handler, err := New(node, client, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()

	response, err := http.Post(server.URL+"/raft/request-vote", "application/json", bytes.NewBufferString(`{"term":9,"candidate_id":"intruder","last_log_index":0,"last_log_term":0}`))
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", response.StatusCode)
	}
	if status := node.Status(); status.Term != 0 || status.VotedFor != nil {
		t.Fatalf("invalid RPC changed state: %+v", status)
	}
}

func TestClusterStatusKeepsUnreachablePeer(t *testing.T) {
	t.Parallel()
	node, client := testAPINode(t)
	handler, err := New(node, client, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()

	response, err := http.Get(server.URL + "/cluster/status")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var cluster ClusterStatus
	if err := json.NewDecoder(response.Body).Decode(&cluster); err != nil {
		t.Fatal(err)
	}
	if len(cluster.Nodes) != 2 {
		t.Fatalf("nodes = %d, want 2", len(cluster.Nodes))
	}
	if !cluster.Nodes[0].Reachable || cluster.Nodes[1].Reachable || cluster.Nodes[1].Error == nil || *cluster.Nodes[1].Error != "unreachable" {
		t.Fatalf("unexpected cluster status: %+v", cluster)
	}
}

func TestRPCRejectsUnsupportedLogPayloadAndWrongMethod(t *testing.T) {
	t.Parallel()
	node, client := testAPINode(t)
	handler, err := New(node, client, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()

	response, err := http.Post(server.URL+"/raft/append-entries", "application/json", bytes.NewBufferString(`{"term":1,"leader_id":"node2","entries":[{}]}`))
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("unsupported log status = %d, want 400", response.StatusCode)
	}

	request, _ := http.NewRequest(http.MethodPost, server.URL+"/status", nil)
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("wrong method status = %d, want 405", response.StatusCode)
	}
}

func testAPINode(t *testing.T) (*raft.Node, apiClient) {
	t.Helper()
	peers := []raft.Peer{
		{ID: "node1", URL: "http://127.0.0.1:9001"},
		{ID: "node2", URL: "http://127.0.0.1:9002"},
	}
	client := apiClient{
		statuses: map[string]raft.Status{},
		errors:   map[string]error{"node2": errors.New("offline")},
	}
	node, err := raft.NewNode(raft.Config{
		ID:                 "node1",
		Address:            peers[0].URL,
		Peers:              peers,
		HeartbeatInterval:  10 * time.Millisecond,
		ElectionTimeoutMin: 100 * time.Millisecond,
		ElectionTimeoutMax: 200 * time.Millisecond,
		RPCTimeout:         20 * time.Millisecond,
	}, client, storage.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	return node, client
}
