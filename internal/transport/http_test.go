package transport

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"raftdemo/internal/raft"
)

func TestRequestVote(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/raft/request-vote" || request.Method != http.MethodPost {
			t.Fatalf("unexpected request %s %s", request.Method, request.URL.Path)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"term":3,"vote_granted":true}`))
	}))
	defer server.Close()

	client := NewClient()
	response, err := client.RequestVote(context.Background(), raft.Peer{ID: "node2", URL: server.URL}, raft.RequestVoteRequest{Term: 3, CandidateID: "node1"})
	if err != nil {
		t.Fatalf("RequestVote() error = %v", err)
	}
	if response.Term != 3 || !response.VoteGranted {
		t.Fatalf("unexpected response: %+v", response)
	}
}

func TestRequestVoteRejectsNon2xx(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		http.Error(writer, "bad peer", http.StatusBadRequest)
	}))
	defer server.Close()

	client := NewClient()
	if _, err := client.RequestVote(context.Background(), raft.Peer{ID: "node2", URL: server.URL}, raft.RequestVoteRequest{Term: 1, CandidateID: "node1"}); err == nil {
		t.Fatal("RequestVote() error = nil, want non-2xx error")
	}
}
