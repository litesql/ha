package nats

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

type Config struct {
	Name       string
	Port       int
	StoreDir   string
	User       string
	Pass       string
	File       string
	EnableLogs bool
}

func RunEmbeddedNATSServer(cfg Config) (*nats.Conn, *server.Server, error) {
	var (
		opts *server.Options
		err  error
	)
	if cfg.File != "" {
		opts, err = server.ProcessConfigFile(cfg.File)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to process nats config file: %w", err)
		}
	} else {
		opts = &server.Options{
			ServerName: cfg.Name,
			Port:       cfg.Port,
			StoreDir:   cfg.StoreDir,
		}
		if cfg.User != "" && cfg.Pass != "" {
			appAcct := server.NewAccount("app")
			appAcct.EnableJetStream(map[string]server.JetStreamAccountLimits{
				"": {
					MaxMemory:    -1,
					MaxStore:     -1,
					MaxStreams:   -1,
					MaxConsumers: -1,
				},
			})
			opts.Accounts = []*server.Account{appAcct}
			opts.Users = []*server.User{
				{
					Username: cfg.User,
					Password: cfg.Pass,
					Account:  appAcct,
				},
			}
		}
	}
	opts.JetStream = true
	opts.DisableJetStreamBanner = true
	ns, err := server.NewServer(opts)
	if err != nil {
		return nil, nil, err
	}
	if cfg.EnableLogs {
		ns.ConfigureLogger()
	}
	slog.Info("starting NATS server", "port", opts.Port, "store_dir", opts.StoreDir)
	go ns.Start()

	if !ns.ReadyForConnections(5 * time.Second) {
		return nil, nil, err
	}

	if ns.ClusterName() != "" {
		// wait for the leader
		waitForLeader(ns, opts.Cluster.PoolSize)
	}

	slog.Info("embedded NATS server is ready", "cluster", ns.ClusterName(), "jetstream", ns.JetStreamEnabled())
	nc, err := nats.Connect("", nats.InProcessServer(ns))
	if err != nil {
		ns.Shutdown()
		return nil, nil, err
	}
	return nc, ns, nil
}

func waitForLeader(ns *server.Server, size int) {
	for {
		raftz := ns.Raftz(&server.RaftzOptions{})
		if raftz != nil {
			for _, raftGroup := range *raftz {
				for _, raft := range raftGroup {
					if raft.Leader != "" {
						return
					}
				}
			}
		}
		slog.Info("waiting for the cluster leader", "peers", len(ns.JetStreamClusterPeers()), "size", size)
		time.Sleep(500 * time.Millisecond)
	}
}
