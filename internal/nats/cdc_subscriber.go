package nats

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/litesql/ha/internal/sqlite"
)

type CDCSubscriber struct {
	nc     *nats.Conn
	js     jetstream.JetStream
	node   string
	stream string
	db     *sql.DB
}

func NewCDCSubscriber(node string, nc *nats.Conn, url string, stream string, db *sql.DB) (*CDCSubscriber, error) {
	var err error
	if nc == nil {
		nc, err = nats.Connect(url)
		if err != nil {
			return nil, err
		}
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	s := CDCSubscriber{
		nc:     nc,
		js:     js,
		node:   node,
		stream: stream,
		db:     db,
	}

	consumer, err := s.js.CreateOrUpdateConsumer(context.Background(), s.stream, jetstream.ConsumerConfig{
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: s.stream,
		Durable:       s.node,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		Name:          s.node,
	})
	if err != nil {
		return nil, err
	}

	_, err = consumer.Consume(s.handler)
	if err != nil {
		slog.Error("failed to start CDC consumer", "error", err, "node", s.node, "stream", s.stream)
		return nil, err
	}

	return &s, nil
}

func (s *CDCSubscriber) Close() error {
	slog.Info("Closing CDC subscriber", "node", s.node, "stream", s.stream)
	err := s.nc.Drain()
	if s.nc != nil && !s.nc.IsClosed() {
		s.nc.Close()
	}
	return err
}

func (s *CDCSubscriber) handler(msg jetstream.Msg) {
	var cs sqlite.ChangeSet
	err := json.Unmarshal(msg.Data(), &cs)
	if err != nil {
		slog.Error("failed to unmarshal CDC message", "error", err)
		s.ack(msg)
		return
	}
	if cs.Node == s.node {
		s.ack(msg)
		return
	}
	slog.Info("received CDC message", "subject", msg.Subject(), "node", cs.Node, "changes", len(cs.Changes))
	err = cs.Apply()
	if err != nil {
		slog.Error("failed to apply CDC message", "error", err)
		return
	}
	s.ack(msg)
}

func (s *CDCSubscriber) ack(msg jetstream.Msg) {
	err := msg.Ack()
	if err != nil {
		slog.Error("failed to ack message", "error", err, "subject", msg.Subject())
	}
}
