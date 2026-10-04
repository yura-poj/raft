package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"raftdemo/internal/config"
	"raftdemo/internal/httpapi"
	"raftdemo/internal/raft"
	"raftdemo/internal/storage"
	"raftdemo/internal/transport"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	cfg, err := config.Parse(os.Args[1:])
	if err != nil {
		return fmt.Errorf("конфигурация: %w", err)
	}

	peers := make([]raft.Peer, 0, len(cfg.Peers))
	var ownAddress string
	for _, peer := range cfg.Peers {
		peers = append(peers, raft.Peer{ID: peer.ID, URL: peer.URL})
		if peer.ID == cfg.NodeID {
			ownAddress = peer.URL
		}
	}

	client := transport.NewClient()
	store, err := storage.NewFileStore(cfg.StateFile)
	if err != nil {
		return fmt.Errorf("создание stable storage: %w", err)
	}
	node, err := raft.NewNode(raft.Config{
		ID:                 cfg.NodeID,
		Address:            ownAddress,
		Peers:              peers,
		HeartbeatInterval:  cfg.HeartbeatInterval,
		ElectionTimeoutMin: cfg.ElectionTimeoutMin,
		ElectionTimeoutMax: cfg.ElectionTimeoutMax,
		RPCTimeout:         cfg.RPCTimeout,
	}, client, store)
	if err != nil {
		return fmt.Errorf("создание Raft-узла: %w", err)
	}

	handler, err := httpapi.New(node, client, cfg.RPCTimeout)
	if err != nil {
		return fmt.Errorf("создание HTTP API: %w", err)
	}
	listener, err := net.Listen("tcp", cfg.ListenAddress)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.ListenAddress, err)
	}

	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 2 * time.Second,
	}
	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- server.Serve(listener)
	}()
	node.Start()
	log.Printf("узел %s слушает %s; интерфейс: %s/", cfg.NodeID, cfg.ListenAddress, ownAddress)

	signalCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	var serveErr error
	select {
	case <-signalCtx.Done():
	case serveErr = <-serverErrors:
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := node.Stop(shutdownCtx); err != nil {
		return fmt.Errorf("остановка Raft-узла: %w", err)
	}
	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("остановка HTTP server: %w", err)
	}
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return fmt.Errorf("HTTP server: %w", serveErr)
	}
	return nil
}
