package nats

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/litesql/ha/internal/sqlite"
)

var (
	processID = time.Now().UnixNano()
	seq       uint64
)

type CDCPublisher struct {
	nc      *nats.Conn
	js      jetstream.JetStream
	stream  string
	timeout time.Duration
}

func NewCDCPublisher(nc *nats.Conn, url string, replicas int, stream string, maxAge time.Duration, timeout time.Duration) (*CDCPublisher, error) {
	var err error
	if nc == nil {
		nc, err = nats.Connect(url,
			nats.ReconnectHandler(func(c *nats.Conn) {
				slog.Info("reconnected to NATS server", "url", c.ConnectedUrl())
			}),
			nats.DisconnectHandler(func(c *nats.Conn) {
				slog.Warn("disconnected from NATS server", "url", c.ConnectedUrl())
			}),
			nats.ClosedHandler(func(c *nats.Conn) {
				slog.Info("NATS connection closed permanently")
			}))
		if err != nil {
			return nil, err
		}
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	// Create a stream to hold the CDC messages
	streamConfig := jetstream.StreamConfig{
		Name:      stream,
		Replicas:  replicas,
		Subjects:  []string{stream},
		Storage:   jetstream.FileStorage,
		MaxAge:    maxAge,
		Discard:   jetstream.DiscardOld,
		Retention: jetstream.LimitsPolicy,
	}
	_, err = js.CreateOrUpdateStream(ctx, streamConfig)
	if err != nil {
		if nc.ConnectedClusterName() == "" {
			return nil, err
		}
		slog.Warn("failed to create of update stream", "error", err)
	}
	return &CDCPublisher{
		nc:      nc,
		js:      js,
		stream:  stream,
		timeout: timeout,
	}, nil
}

func (p *CDCPublisher) Publish(cs *sqlite.ChangeSet) error {
	cs.ProcessID = processID
	data, err := json.Marshal(cs)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()
	pubAck, err := p.js.Publish(ctx, p.stream, data)
	if err != nil {
		return err
	}
	seq = pubAck.Sequence
	slog.Info("published CDC message", "stream", pubAck.Stream, "seq", pubAck.Sequence, "duplicate", pubAck.Duplicate)
	return nil
}
