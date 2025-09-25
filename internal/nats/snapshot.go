package nats

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/litesql/ha/internal/sqlite"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const latestSnapshotName = "latest"

type Snapshotter struct {
	objectStore jetstream.ObjectStore
	mu          sync.Mutex
}

func NewSnapshotter(ctx context.Context, nc *nats.Conn, url string, replicas int, stream string, dsn string, memdb bool, interval time.Duration) (*Snapshotter, error) {
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
	bucketName := stream + "_SNAPSHOTS"
	objectStore, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket:      bucketName,
		Storage:     jetstream.FileStorage,
		Compression: true,
		Replicas:    replicas,
	})
	if err != nil {
		if !errors.Is(err, jetstream.ErrBucketExists) {
			return nil, err
		}
		objectStore, err = js.ObjectStore(ctx, bucketName)
		if err != nil {
			return nil, err
		}
	}
	s := &Snapshotter{
		objectStore: objectStore,
	}
	if interval > 0 {
		go s.start(ctx, dsn, memdb, interval)
	}
	return s, nil
}

func (s *Snapshotter) start(ctx context.Context, dsn string, memdb bool, interval time.Duration) {
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ticker.C:
			sequence, err := s.TakeSnapshot(ctx, dsn, memdb)
			if err != nil {
				slog.Error("failed to take snapshot", "error", err)
			} else {
				slog.Info("snapshot taken", "sequence", sequence)
			}
		case <-ctx.Done():
			ticker.Stop()
			return
		}
	}
}

func (s *Snapshotter) TakeSnapshot(ctx context.Context, dsn string, memdb bool) (sequence uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sequence = seq
	headers := make(nats.Header)
	headers.Set("seq", fmt.Sprint(sequence))
	bkpFile := fmt.Sprintf("bkp_%d", time.Now().Nanosecond())
	if err := s.objectStore.UpdateMeta(ctx, latestSnapshotName, jetstream.ObjectMeta{
		Name: bkpFile,
	}); err != nil && !errors.Is(err, jetstream.ErrUpdateMetaDeleted) {
		return 0, err
	}
	defer func() {
		if err == nil {
			s.objectStore.Delete(ctx, bkpFile)
		} else {
			s.objectStore.UpdateMeta(ctx, bkpFile, jetstream.ObjectMeta{
				Name: latestSnapshotName,
			})
		}
	}()

	reader, writer := io.Pipe()
	errReaderCh := make(chan error, 1)
	errWriterCh := make(chan error, 1)
	go func() {
		errWriterCh <- sqlite.Backup(ctx, dsn, memdb, writer)
	}()

	go func() {
		info, err := s.objectStore.Put(ctx, jetstream.ObjectMeta{
			Name:    latestSnapshotName,
			Headers: headers,
		}, reader)
		if err != nil {
			errReaderCh <- err
		} else {
			slog.Info("snapshot stored", "bucket", info.Bucket, "name", info.Name, "size", info.Size, "modTime", info.ModTime)
			errReaderCh <- nil
		}
	}()

	select {
	case err2 := <-errWriterCh:
		if err2 != nil {
			writer.CloseWithError(err2)
			err = errors.Join(err, err2)
		} else {
			writer.Close()
		}
		select {
		case err2 := <-errReaderCh:
			err = errors.Join(err, err2)
		case <-ctx.Done():
			err = errors.Join(err, ctx.Err())
		}
	case err2 := <-errReaderCh:
		err = errors.Join(err, err2)
		select {
		case err2 := <-errWriterCh:
			if err2 != nil {
				writer.CloseWithError(err2)
				err = errors.Join(err, err2)
			} else {
				writer.Close()
			}
		case <-ctx.Done():
			err = errors.Join(err, ctx.Err())
		}
	case <-ctx.Done():
		err = ctx.Err()
	}

	return
}

func (s *Snapshotter) GetLatestSnapshot(ctx context.Context) (uint64, io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	info, err := s.objectStore.GetInfo(ctx, latestSnapshotName)
	if err != nil {
		return 0, nil, err
	}
	sequenceStr := info.Headers.Get("seq")
	var sequence uint64
	if sequenceStr != "" {
		sequence, err = strconv.ParseUint(sequenceStr, 10, 64)
		if err != nil {
			return 0, nil, fmt.Errorf("convert sequence header: %w", err)
		}
	}

	reader, err := s.objectStore.Get(ctx, latestSnapshotName)
	return sequence, reader, err
}
