package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"raftdemo/internal/raft"
)

const maxResponseBody = 1 << 20

type Client struct {
	httpClient *http.Client
}

func NewClient() *Client {
	return &Client{httpClient: &http.Client{}}
}

func NewClientWithHTTPClient(httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	return &Client{httpClient: httpClient}
}

func (c *Client) RequestVote(ctx context.Context, peer raft.Peer, request raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	body, err := c.post(ctx, peer.URL+"/raft/request-vote", request)
	if err != nil {
		return raft.RequestVoteResponse{}, err
	}
	defer body.Close()
	var wire struct {
		Term        *uint64 `json:"term"`
		VoteGranted *bool   `json:"vote_granted"`
	}
	if err := decodeResponse(body, &wire); err != nil {
		return raft.RequestVoteResponse{}, err
	}
	if wire.Term == nil || wire.VoteGranted == nil {
		return raft.RequestVoteResponse{}, errors.New("request-vote response misses required fields")
	}
	return raft.RequestVoteResponse{Term: *wire.Term, VoteGranted: *wire.VoteGranted}, nil
}

func (c *Client) AppendEntries(ctx context.Context, peer raft.Peer, request raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	body, err := c.post(ctx, peer.URL+"/raft/append-entries", request)
	if err != nil {
		return raft.AppendEntriesResponse{}, err
	}
	defer body.Close()
	var wire struct {
		Term    *uint64 `json:"term"`
		Success *bool   `json:"success"`
	}
	if err := decodeResponse(body, &wire); err != nil {
		return raft.AppendEntriesResponse{}, err
	}
	if wire.Term == nil || wire.Success == nil {
		return raft.AppendEntriesResponse{}, errors.New("append-entries response misses required fields")
	}
	return raft.AppendEntriesResponse{Term: *wire.Term, Success: *wire.Success}, nil
}

func (c *Client) Status(ctx context.Context, peer raft.Peer) (raft.Status, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, peer.URL+"/status", nil)
	if err != nil {
		return raft.Status{}, err
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return raft.Status{}, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return raft.Status{}, responseError(response)
	}
	var status raft.Status
	if err := decodeResponse(io.LimitReader(response.Body, maxResponseBody), &status); err != nil {
		return raft.Status{}, err
	}
	if status.NodeID == "" || status.Address == "" || !validRole(status.Role) {
		return raft.Status{}, errors.New("status response misses required fields")
	}
	return status, nil
}

func (c *Client) post(ctx context.Context, target string, value any) (io.ReadCloser, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		defer response.Body.Close()
		return nil, responseError(response)
	}
	return &limitedReadCloser{Reader: io.LimitReader(response.Body, maxResponseBody), Closer: response.Body}, nil
}

func decodeResponse(reader io.Reader, target any) error {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("response contains more than one JSON value")
	}
	return nil
}

func responseError(response *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
	message := strings.TrimSpace(string(body))
	if message == "" {
		message = http.StatusText(response.StatusCode)
	}
	return fmt.Errorf("peer returned HTTP %d: %s", response.StatusCode, message)
}

func validRole(role raft.Role) bool {
	return role == raft.RoleFollower || role == raft.RoleCandidate || role == raft.RoleLeader
}

type limitedReadCloser struct {
	io.Reader
	io.Closer
}
