package httpapi

import (
	"embed"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"time"

	"raftdemo/internal/raft"
)

const maxRPCBody = 64 << 10

//go:embed web/*
var webFiles embed.FS

type Server struct {
	node       *raft.Node
	status     StatusClient
	rpcTimeout time.Duration
	handler    http.Handler
}

func New(node *raft.Node, status StatusClient, rpcTimeout time.Duration) (*Server, error) {
	assets, err := fs.Sub(webFiles, "web")
	if err != nil {
		return nil, err
	}
	server := &Server{node: node, status: status, rpcTimeout: rpcTimeout}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", server.handleHealth)
	mux.HandleFunc("GET /status", server.handleStatus)
	mux.HandleFunc("GET /cluster/status", server.handleClusterStatus)
	mux.HandleFunc("POST /raft/request-vote", server.handleRequestVote)
	mux.HandleFunc("POST /raft/append-entries", server.handleAppendEntries)
	mux.Handle("GET /assets/", http.StripPrefix("/assets/", http.FileServer(http.FS(assets))))
	mux.HandleFunc("GET /", func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/" {
			http.NotFound(writer, request)
			return
		}
		writer.Header().Set("Content-Type", "text/html; charset=utf-8")
		data, readErr := webFiles.ReadFile("web/index.html")
		if readErr != nil {
			http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		_, _ = writer.Write(data)
	})
	server.handler = mux
	return server, nil
}

func (s *Server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	s.handler.ServeHTTP(writer, request)
}

func (s *Server) handleHealth(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]string{"status": "ok", "node_id": s.node.Status().NodeID})
}

func (s *Server) handleStatus(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, s.node.Status())
}

func (s *Server) handleClusterStatus(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, collectClusterStatus(request.Context(), s.node, s.status, s.rpcTimeout))
}

func (s *Server) handleRequestVote(writer http.ResponseWriter, request *http.Request) {
	var payload raft.RequestVoteRequest
	if err := decodeJSON(writer, request, &payload); err != nil {
		writeAPIError(writer, http.StatusBadRequest, err.Error())
		return
	}
	if payload.Term == 0 || payload.CandidateID == "" {
		writeAPIError(writer, http.StatusBadRequest, "term и candidate_id обязательны")
		return
	}
	response, err := s.node.HandleRequestVote(payload)
	if err != nil {
		s.writeRaftError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, response)
}

func (s *Server) handleAppendEntries(writer http.ResponseWriter, request *http.Request) {
	var payload raft.AppendEntriesRequest
	if err := decodeJSON(writer, request, &payload); err != nil {
		writeAPIError(writer, http.StatusBadRequest, err.Error())
		return
	}
	if payload.Term == 0 || payload.LeaderID == "" {
		writeAPIError(writer, http.StatusBadRequest, "term и leader_id обязательны")
		return
	}
	response, err := s.node.HandleAppendEntries(payload)
	if err != nil {
		s.writeRaftError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, response)
}

func (s *Server) writeRaftError(writer http.ResponseWriter, err error) {
	if errors.Is(err, raft.ErrUnknownMember) || errors.Is(err, raft.ErrUnsupportedLog) || errors.Is(err, raft.ErrInvalidRPC) {
		writeAPIError(writer, http.StatusBadRequest, err.Error())
		return
	}
	writeAPIError(writer, http.StatusInternalServerError, "internal error")
}

func decodeJSON(writer http.ResponseWriter, request *http.Request, target any) error {
	request.Body = http.MaxBytesReader(writer, request.Body, maxRPCBody)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("body должен содержать ровно одно JSON-значение")
	}
	return nil
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func writeAPIError(writer http.ResponseWriter, status int, message string) {
	writeJSON(writer, status, map[string]string{"error": message})
}
