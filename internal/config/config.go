package config

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"
)

type Peer struct {
	ID  string
	URL string
}

type Config struct {
	NodeID             string
	ListenAddress      string
	StateFile          string
	Peers              []Peer
	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	RPCTimeout         time.Duration
}

func Parse(args []string) (Config, error) {
	var cfg Config
	var peers string

	fs := flag.NewFlagSet("raft-node", flag.ContinueOnError)
	fs.StringVar(&cfg.NodeID, "id", "", "уникальный ID узла")
	fs.StringVar(&cfg.ListenAddress, "listen", "", "адрес HTTP listener")
	fs.StringVar(&cfg.StateFile, "state-file", "", "файл durable Raft-состояния")
	fs.StringVar(&peers, "peers", "", "полный список узлов id=http://host:port через запятую")
	fs.DurationVar(&cfg.HeartbeatInterval, "heartbeat", 100*time.Millisecond, "интервал heartbeat")
	fs.DurationVar(&cfg.ElectionTimeoutMin, "election-min", 300*time.Millisecond, "минимальный election timeout")
	fs.DurationVar(&cfg.ElectionTimeoutMax, "election-max", 600*time.Millisecond, "максимальный election timeout")
	fs.DurationVar(&cfg.RPCTimeout, "rpc-timeout", 150*time.Millisecond, "timeout одного peer RPC")
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	if fs.NArg() != 0 {
		return Config{}, fmt.Errorf("неожиданные аргументы: %s", strings.Join(fs.Args(), " "))
	}

	parsedPeers, err := parsePeers(peers)
	if err != nil {
		return Config{}, err
	}
	cfg.Peers = parsedPeers
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.NodeID) == "" {
		return errors.New("--id обязателен")
	}
	if strings.TrimSpace(c.ListenAddress) == "" {
		return errors.New("--listen обязателен")
	}
	if strings.TrimSpace(c.StateFile) == "" {
		return errors.New("--state-file обязателен")
	}
	if _, err := net.ResolveTCPAddr("tcp", c.ListenAddress); err != nil {
		return fmt.Errorf("некорректный --listen: %w", err)
	}
	if len(c.Peers) == 0 {
		return errors.New("--peers обязателен")
	}

	ids := make(map[string]struct{}, len(c.Peers))
	urls := make(map[string]struct{}, len(c.Peers))
	selfCount := 0
	for _, peer := range c.Peers {
		if strings.TrimSpace(peer.ID) == "" {
			return errors.New("ID участника не может быть пустым")
		}
		if _, exists := ids[peer.ID]; exists {
			return fmt.Errorf("повторяющийся ID участника %q", peer.ID)
		}
		ids[peer.ID] = struct{}{}

		parsed, err := url.Parse(peer.URL)
		if err != nil || parsed.Scheme != "http" || parsed.Host == "" || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.User != nil {
			return fmt.Errorf("некорректный URL участника %q: требуется http://host:port без path, query и fragment", peer.URL)
		}
		if _, exists := urls[peer.URL]; exists {
			return fmt.Errorf("повторяющийся URL участника %q", peer.URL)
		}
		urls[peer.URL] = struct{}{}
		if peer.ID == c.NodeID {
			selfCount++
		}
	}
	if selfCount != 1 {
		return fmt.Errorf("текущий узел %q должен присутствовать в --peers ровно один раз", c.NodeID)
	}
	if c.HeartbeatInterval <= 0 {
		return errors.New("--heartbeat должен быть больше нуля")
	}
	if c.RPCTimeout <= 0 {
		return errors.New("--rpc-timeout должен быть больше нуля")
	}
	if c.ElectionTimeoutMin <= c.HeartbeatInterval {
		return errors.New("--election-min должен быть больше --heartbeat")
	}
	if c.ElectionTimeoutMax <= c.ElectionTimeoutMin {
		return errors.New("--election-max должен быть больше --election-min")
	}
	return nil
}

func parsePeers(value string) ([]Peer, error) {
	if strings.TrimSpace(value) == "" {
		return nil, errors.New("--peers обязателен")
	}
	parts := strings.Split(value, ",")
	peers := make([]Peer, 0, len(parts))
	for _, part := range parts {
		id, rawURL, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || strings.TrimSpace(id) == "" || strings.TrimSpace(rawURL) == "" {
			return nil, fmt.Errorf("некорректный участник %q: ожидается id=http://host:port", part)
		}
		peers = append(peers, Peer{ID: strings.TrimSpace(id), URL: strings.TrimSpace(rawURL)})
	}
	return peers, nil
}
