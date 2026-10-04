package config

import (
	"strings"
	"testing"
)

func TestParseThreeNodeCluster(t *testing.T) {
	t.Parallel()
	cfg, err := Parse([]string{
		"--id", "node1",
		"--listen", "127.0.0.1:8081",
		"--state-file", "./data/node1.json",
		"--peers", "node1=http://127.0.0.1:8081,node2=http://127.0.0.1:8082,node3=http://127.0.0.1:8083",
	})
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if cfg.NodeID != "node1" || len(cfg.Peers) != 3 {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestParseRejectsInvalidConfigurations(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "missing state file",
			args: []string{"--id", "node1", "--listen", "127.0.0.1:8081", "--peers", "node1=http://127.0.0.1:8081"},
			want: "--state-file",
		},
		{
			name: "self is absent",
			args: []string{"--id", "node1", "--listen", "127.0.0.1:8081", "--state-file", "node1.json", "--peers", "node2=http://127.0.0.1:8082"},
			want: "должен присутствовать",
		},
		{
			name: "duplicate id",
			args: []string{"--id", "node1", "--listen", "127.0.0.1:8081", "--state-file", "node1.json", "--peers", "node1=http://127.0.0.1:8081,node1=http://127.0.0.1:8082"},
			want: "повторяющийся ID",
		},
		{
			name: "url with path",
			args: []string{"--id", "node1", "--listen", "127.0.0.1:8081", "--state-file", "node1.json", "--peers", "node1=http://127.0.0.1:8081/api"},
			want: "некорректный URL",
		},
		{
			name: "election below heartbeat",
			args: []string{"--id", "node1", "--listen", "127.0.0.1:8081", "--state-file", "node1.json", "--peers", "node1=http://127.0.0.1:8081", "--heartbeat", "100ms", "--election-min", "50ms"},
			want: "--election-min",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Parse(test.args)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Parse() error = %v, want substring %q", err, test.want)
			}
		})
	}
}
