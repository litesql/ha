package nats

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

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

func NewCDCSubscriber(node string, nc *nats.Conn, url string, stream string, policy string, db *sql.DB) (*CDCSubscriber, error) {
	var (
		deliverPolicy jetstream.DeliverPolicy
		startSeq      uint64
		startTime     *time.Time
	)
	switch policy {
	case "all", "":
		deliverPolicy = jetstream.DeliverAllPolicy
	case "last":
		deliverPolicy = jetstream.DeliverLastPolicy
	case "new":
		deliverPolicy = jetstream.DeliverNewPolicy
	default:
		matched, err := regexp.MatchString(`^by_start_sequence=\d+`, policy)
		if err != nil {
			return nil, err
		}
		if matched {
			deliverPolicy = jetstream.DeliverByStartSequencePolicy
			_, err := fmt.Sscanf(policy, "by_start_sequence=%d", &startSeq)
			if err != nil {
				return nil, fmt.Errorf("invalid CDC subscriber start sequence: %w", err)
			}
			break

		}
		matched, err = regexp.MatchString(`^by_start_time=\w+`, policy)
		if err != nil {
			return nil, err
		}
		if matched {
			deliverPolicy = jetstream.DeliverByStartTimePolicy
			dateTime := strings.TrimPrefix(policy, "by_start_time=")
			t, err := time.Parse(time.DateTime, dateTime)
			if err != nil {
				return nil, fmt.Errorf("invalid CDC subscriber start time: %w", err)
			}
			startTime = &t
			break
		}
		return nil, fmt.Errorf("invalid deliver policy: %s", policy)
	}

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
		return nil, err
	}

	s := CDCSubscriber{
		nc:     nc,
		js:     js,
		node:   node,
		stream: stream,
		db:     db,
	}

	consumer, err := s.js.CreateConsumer(context.Background(), s.stream, jetstream.ConsumerConfig{
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: s.stream,
		Durable:       s.node,
		DeliverPolicy: deliverPolicy,
		OptStartSeq:   startSeq,
		OptStartTime:  startTime,
	})
	if err != nil {
		if !errors.Is(err, jetstream.ErrConsumerExists) {
			return nil, err
		}
		consumer, err = s.js.Consumer(context.Background(), stream, s.node)
		if err != nil {
			return nil, err
		}
	}

	_, err = consumer.Consume(s.handler)
	if err != nil {
		slog.Error("failed to start CDC consumer", "error", err, "node", s.node, "stream", s.stream)
		return nil, err
	}

	return &s, nil
}

func (s *CDCSubscriber) Close() {
	slog.Info("drain CDC subscriber", "node", s.node, "stream", s.stream)
	if s.nc != nil && !s.nc.IsClosed() {
		s.nc.Drain()
	}
}

func (s *CDCSubscriber) handler(msg jetstream.Msg) {
	var cs sqlite.ChangeSet
	err := json.Unmarshal(msg.Data(), &cs)
	if err != nil {
		slog.Error("failed to unmarshal CDC message", "error", err)
		s.ack(msg)
		return
	}
	if cs.Node == s.node && cs.ProcessID == processID {
		// Ignore changes originated from this process and node itself
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
