package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/peterbourgon/ff/v4"
	"github.com/peterbourgon/ff/v4/ffhelp"

	ha_nats "github.com/litesql/ha/internal/nats"
	"github.com/litesql/ha/internal/pgwire"
	"github.com/litesql/ha/internal/sqlite"
)

var (
	version string = "dev"
	commit  string = "none"
	date    string = "unknown"
)

var (
	fs   *ff.FlagSet
	name *string
	port *uint

	memDB              *bool
	snapshotInterval   *time.Duration
	fromLatestSnapshot *bool

	pgPort *int
	pgUser *string
	pgPass *string
	pgCert *string
	pgKey  *string

	concurrentQueries *int
	extensions        *string

	natsLogs     *bool
	natsPort     *int
	natsUser     *string
	natsPass     *string
	natsStoreDir *string
	natsConfig   *string

	replicationStream  *string
	replicationTimeout *time.Duration
	replicationMaxAge  *time.Duration
	replicationURL     *string
	replicationPolicy  *string
	replicas           *int
)

func main() {
	fs = ff.NewFlagSet("ha")
	name = fs.String('n', "name", "", "Node name")
	port = fs.Uint('p', "port", 8080, "Server port")
	memDB = fs.Bool('m', "memory", "Store database in memory")
	fromLatestSnapshot = fs.BoolLong("from-latest-snapshot", "Use the latest database snapshot from NATS JetStream Object Store (if available at startup)")
	snapshotInterval = fs.DurationLong("snapshot-interval", 0, "Interval to create database snapshot to NATS JetStream Object Store (0 to disable)")

	natsLogs = fs.BoolLong("nats-logs", "Enable NATS server logging")
	natsPort = fs.IntLong("nats-port", 4222, "Embedded NATS server port (0 to disable)")
	natsStoreDir = fs.StringLong("nats-store-dir", "", "Embedded NATS server store directory")
	natsUser = fs.StringLong("nats-user", "", "Embedded NATS server user")
	natsPass = fs.StringLong("nats-pass", "", "Embedded NATS server password")
	natsConfig = fs.StringLong("nats-config", "", "Embedded NATS server config file")

	pgPort = fs.IntLong("pg-port", 5432, "PostgreSQL Server port")
	pgUser = fs.StringLong("pg-user", "ha", "PostgreSQL Auth user")
	pgPass = fs.StringLong("pg-pass", "ha", "PostgreSQL Auth password")
	pgCert = fs.StringLong("pg-cert", "", "PostgreSQL TLS certificate file")
	pgKey = fs.StringLong("pg-key", "", "PostgreSQL TLS key file")

	concurrentQueries = fs.IntLong("concurrent-queries", 50, "Number of concurrent queries")
	extensions = fs.StringLong("extensions", "", "Comma-separated list of SQLite extensions to load")

	replicas = fs.IntLong("replicas", 1, "Number of replicas to keep for the stream and object store in clustered jetstream. Defaults to 1, maximum is 5")
	replicationTimeout = fs.DurationLong("replication-timeout", 15*time.Second, "Replication publisher timeout")
	replicationStream = fs.StringLong("replication-stream", "ha_replication", "Replication stream name")
	replicationMaxAge = fs.DurationLong("replication-max-age", 24*time.Hour, "Replication stream max age")
	replicationURL = fs.StringLong("replication-url", "", "Replication NATS url (defaults to embedded NATS server)")
	replicationPolicy = fs.StringLong("replication-policy", "", "Replication subscriver delivery policy (all|last|new|by_start_sequence=X|by_start_time=x)")
	printVersion := fs.BoolLong("version", "Print version information and exit")
	_ = fs.String('c', "config", "", "config file (optional)")

	if err := ff.Parse(fs, os.Args[1:],
		ff.WithEnvVarPrefix("HA"),
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
	); err != nil {
		fmt.Printf("%s\n", ffhelp.Flags(fs))
		fmt.Printf("err=%v\n", err)
		return
	}

	if *printVersion {
		fmt.Println("ha")
		fmt.Printf("Version: %s\n", version)
		fmt.Printf("Commit: %s\n", commit)
		fmt.Printf("Date: %s\n", date)
		return
	}

	if err := run(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	var sqlExtensions []string
	if *extensions != "" {
		sqlExtensions = strings.Split(*extensions, ",")
	}

	if *concurrentQueries < 1 {
		return fmt.Errorf("--concurrent-queries must be at least 1")
	}

	nodeName := *name
	if nodeName == "" {
		// generate node name
		nodeName = fmt.Sprintf("ha_%d", time.Now().UnixNano())
	}

	var (
		natsConn   *nats.Conn
		natsServer *server.Server
		err        error
	)
	if *natsPort > 0 || *natsConfig != "" {
		natsConn, natsServer, err = ha_nats.RunEmbeddedNATSServer(ha_nats.Config{
			Name:       nodeName,
			Port:       *natsPort,
			StoreDir:   *natsStoreDir,
			File:       *natsConfig,
			EnableLogs: *natsLogs,
		})
		if err != nil {
			return fmt.Errorf("failed to start embedded NATS server: %w", err)
		}
	}

	var cdcPublisher sqlite.CDCPublisher
	if natsConn != nil || *replicationURL != "" {
		slog.Info("starting replicator publisher", "stream", *replicationStream)
		cdcPublisher, err = ha_nats.NewCDCPublisher(natsConn, *replicationURL, *replicas, *replicationStream, *replicationMaxAge, *replicationTimeout)
		if err != nil {
			return fmt.Errorf("failed to start CDC NATS publisher: %w", err)
		}
	}

	sqlite.RegisterDriver(sqlExtensions, nodeName, cdcPublisher)
	var (
		db  *sql.DB
		dsn string
	)
	args := fs.GetArgs()
	if *memDB {
		slog.Info("using in-memory database")
		db, err = sql.Open("sqlite3-ha", "file:/ha.db?vfs=memdb")
		if err != nil {
			return err
		}
		defer db.Close()
		db.SetConnMaxIdleTime(0)
		db.SetConnMaxLifetime(0)
		db.SetMaxOpenConns(*concurrentQueries)
		db.SetMaxIdleConns(*concurrentQueries)

		sqlite.SetGlobalDB(db)

		if len(args) > 0 {
			filename := args[0]
			slog.Info("loading database", "file", filename)
			err := sqlite.Deserialize(context.Background(), filename)
			if err != nil {
				return fmt.Errorf("failed to load database %q: %w", filename, err)
			}
			if *replicationPolicy == "" {
				matched, _ := regexp.MatchString(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}`, filepath.Base(filename))
				if matched {
					dateTime := filepath.Base(filename)[0:len(time.DateTime)]
					_, err := time.Parse(time.DateTime, dateTime)
					if err == nil {
						policy := fmt.Sprintf("by_start_time=%s", dateTime)
						replicationPolicy = &policy
					}
				}
			}
		}
	} else {
		dsn = "file:ha.db?_journal=WAL&_busy_timeout=5000"
		if len(args) > 0 {
			dsn = args[0]
		}
		slog.Info("using data source name", "dsn", dsn)
		db, err = sql.Open("sqlite3-ha", dsn)
		if err != nil {
			return err
		}
		defer db.Close()
		db.SetConnMaxIdleTime(0)
		db.SetConnMaxLifetime(0)
		db.SetMaxOpenConns(*concurrentQueries)
		db.SetMaxIdleConns(*concurrentQueries)

		sqlite.SetGlobalDB(db)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var (
		snapshotter   *ha_nats.Snapshotter
		cdcSubscriber *ha_nats.CDCSubscriber
	)
	if natsConn != nil || *replicationURL != "" {
		slog.Info("starting snapshotter", "interval", *snapshotInterval)
		snapshotter, err = ha_nats.NewSnapshotter(ctx, natsConn, *replicationURL, *replicas, *replicationStream, "", *memDB, *snapshotInterval)
		if err != nil {
			return fmt.Errorf("failed to start snapshotter: %w", err)
		}

		if *fromLatestSnapshot {
			slog.Info("loading latest snapshot from NATS JetStream Object Store")
			sequence, reader, err := snapshotter.LatestSnapshot(context.Background())
			if err != nil && !errors.Is(err, jetstream.ErrObjectNotFound) {
				return fmt.Errorf("failed to load latest snapshot: %w", err)
			}
			if reader != nil {
				if *memDB {
					err = sqlite.DeserializeFromReader(ctx, reader)
					if err != nil {
						return fmt.Errorf("failed to load latest snapshot: %w", err)
					}
				} else {
					filename, err := sqlite.Filename(ctx)
					if err != nil {
						return fmt.Errorf("failed to get database filename: %w", err)
					}
					snapshotFilename := filepath.Join(filepath.Dir(filename), fmt.Sprintf("snapshot_%d_%s", sequence, filepath.Base(filename)))
					u, err := url.Parse(dsn)
					if err != nil {
						return fmt.Errorf("failed to parse dsn: %w", err)
					}
					f, err := os.Create(snapshotFilename)
					if err != nil {
						return fmt.Errorf("failed to create snapshot file %q: %w", snapshotFilename, err)
					}
					_, err = io.Copy(f, reader)
					if err != nil {
						f.Close()
						return fmt.Errorf("failed to write snapshot file %q: %w", snapshotFilename, err)
					}
					f.Close()
					slog.Info("loading snapshot", "filename", snapshotFilename)
					db.Close()
					db, err = sql.Open("sqlite3-ha", fmt.Sprintf("file:%s?%s", snapshotFilename, u.RawQuery))
					if err != nil {
						return fmt.Errorf("failed to open database %q: %w", dsn, err)
					}
					defer db.Close()
					db.SetConnMaxIdleTime(0)
					db.SetConnMaxLifetime(0)
					db.SetMaxOpenConns(*concurrentQueries)
					db.SetMaxIdleConns(*concurrentQueries)

					sqlite.SetGlobalDB(db)

				}
				if sequence > 0 && *replicationPolicy == "" {
					policy := fmt.Sprintf("by_start_sequence=%d", sequence)
					replicationPolicy = &policy
				}
			}
		}

		slog.Info("starting CDC subscriber", "stream", *replicationStream, "deliverPolicy", *replicationPolicy)
		cdcSubscriber, err = ha_nats.NewCDCSubscriber(nodeName, natsConn, *replicationURL, *replicationStream, *replicationPolicy, db)
		if err != nil {
			return fmt.Errorf("failed to start CDC NATS subscriber: %w", err)
		}
		defer cdcSubscriber.Close()
		snapshotter.SetSeqProvider(cdcSubscriber)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("POST /", func(w http.ResponseWriter, r *http.Request) {
		var req []sqlite.Request
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		res, err := sqlite.Transaction(r.Context(), req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string][]*sqlite.Response{
			"results": res,
		})
	})
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		filename := fmt.Sprintf("%s_ha.db", time.Now().UTC().Format(time.DateTime))
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", filename))
		w.Header().Set("Content-Type", "application/octet-stream")
		err := sqlite.Backup(r.Context(), dsn, *memDB, w)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})

	mux.HandleFunc("POST /snapshot", func(w http.ResponseWriter, r *http.Request) {
		if snapshotter == nil {
			http.Error(w, "snapshotter not enabled", http.StatusNotImplemented)
			return
		}
		sequence, err := snapshotter.TakeSnapshot(r.Context(), dsn, *memDB)
		if err != nil {
			slog.Error("take snapshot", "error", err)
			http.Error(w, fmt.Sprintf("failed to take snapshot: %v", err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("X-Sequence", fmt.Sprint(sequence))
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("GET /snapshot", func(w http.ResponseWriter, r *http.Request) {
		if snapshotter == nil {
			http.Error(w, "snapshotter not enabled", http.StatusNotImplemented)
			return
		}
		sequence, reader, err := snapshotter.LatestSnapshot(r.Context())
		if err != nil {
			slog.ErrorContext(r.Context(), "failed o get latest snapshot", "error", err)
			http.Error(w, fmt.Sprintf("failed to get latest snapshot: %v", err), http.StatusInternalServerError)
			return
		}
		defer reader.Close()
		filename := fmt.Sprintf("%s_ha_snapshot_%d.db", time.Now().UTC().Format(time.DateTime), sequence)
		w.Header().Set("X-Sequence", fmt.Sprint(sequence))
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", filename))
		w.Header().Set("Content-Type", "application/octet-stream")
		_, err = io.Copy(w, reader)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to send latest snapshot: %v", err), http.StatusInternalServerError)
			return
		}
	})

	mux.HandleFunc("GET /replications", func(w http.ResponseWriter, r *http.Request) {
		info, err := cdcSubscriber.DeliveredInfo(r.Context(), "")
		if err != nil {
			slog.Error("failed to get replication info", "error", err)
			http.Error(w, fmt.Sprintf("failed to get replication info: %v", err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string][]*jetstream.ConsumerInfo{
			"replications": info,
		})
	})

	mux.HandleFunc("GET /replications/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		info, err := cdcSubscriber.DeliveredInfo(r.Context(), name)
		if err != nil {
			slog.Error("failed to get replication info", "error", err, "name", name)
			http.Error(w, fmt.Sprintf("failed to get replication info: %v", err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string][]*jetstream.ConsumerInfo{
			"replications": info,
		})
	})

	mux.HandleFunc("DELETE /replications/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if name == "" {
			http.Error(w, "name is required", http.StatusInternalServerError)
			return
		}
		err := cdcSubscriber.RemoveConsumer(r.Context(), name)
		if err != nil {
			slog.Error("failed remove consumer", "error", err, "name", name)
			http.Error(w, fmt.Sprintf("failed to remove consumer: %v", err), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	pgServer, err := pgwire.NewServer(pgwire.Config{
		User:    *pgUser,
		Pass:    *pgPass,
		TLSCert: *pgCert,
		TLSKey:  *pgKey,
	}, db)
	if err != nil {
		return fmt.Errorf("failed to create PostgreSQL server: %w", err)
	}

	if *pgPort > 0 {
		slog.Info("starting HA postgreSQL wire Protocol server", "port", *pgPort)
		go func() {
			err = pgServer.ListenAndServe(*pgPort)
			if err != nil {
				log.Fatalf("PostgreSQL server error: %v", err)
			}
		}()
	}

	server := http.Server{
		Addr:    fmt.Sprintf(":%d", *port),
		Handler: mux,
	}

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-done
		slog.Warn("signal detected...", "signal", sig)
		if err := pgServer.Close(); err != nil {
			slog.Error("PostgreSQL server shutdown failed", "error", err)
		}
		if natsConn != nil {
			natsConn.Close()
		}
		if natsServer != nil {
			natsServer.WaitForShutdown()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			slog.Error("HTTP server shutdown failed", "error", err)
		}
	}()

	slog.Info("starting HA HTTP server", "port", *port, "version", version, "commit", commit, "date", date)
	return server.ListenAndServe()
}
