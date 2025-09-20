package nats

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

type Config struct {
	Name     string
	Port     int
	StoreDir string
	File     string
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
			JetStream:  true,
			StoreDir:   cfg.StoreDir,
		}
	}
	ns, err := server.NewServer(opts)
	if err != nil {
		return nil, nil, err
	}
	slog.Info("starting NATS server", "store_dir", ns.StoreDir())
	go ns.Start()

	if !ns.ReadyForConnections(5 * time.Second) {
		return nil, nil, err
	}
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		ns.Shutdown()
		return nil, nil, err
	}
	return nc, ns, nil
}
