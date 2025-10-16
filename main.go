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

	ha "github.com/litesql/go-ha"
	sqlite3ha "github.com/litesql/go-sqlite3-ha"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/peterbourgon/ff/v4"
	"github.com/peterbourgon/ff/v4/ffhelp"

	"github.com/litesql/ha/internal/interceptor"
	"github.com/litesql/ha/internal/pgwire"
	"github.com/litesql/ha/internal/sqlite"
)

var (
	version string = "dev"
	commit  string = "none"
	date    string = "unknown"
)

var (
	fs       *ff.FlagSet
	name     *string
	port     *uint
	logLevel *string

	memDB              *bool
	snapshotInterval   *time.Duration
	fromLatestSnapshot *bool
	disableDDLSync     *bool

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

	interceptorPath *string
)

func main() {
	fs = ff.NewFlagSet("ha")
	name = fs.String('n', "name", "", "Node name")
	port = fs.Uint('p', "port", 8080, "Server port")
	interceptorPath = fs.String('i', "interceptor", "", "Path to a golang script to customize replication behaviour")
	logLevel = fs.StringLong("log-level", "info", "Log level (info, warn, error, debug)")

	memDB = fs.Bool('m', "memory", "Store database in memory")
	fromLatestSnapshot = fs.BoolLong("from-latest-snapshot", "Use the latest database snapshot from NATS JetStream Object Store (if available at startup)")
	snapshotInterval = fs.DurationLong("snapshot-interval", 0, "Interval to create database snapshot to NATS JetStream Object Store (0 to disable)")
	disableDDLSync = fs.BoolLong("disable-ddl-sync", "Disable DDL commands publisher")

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
	replicationPolicy = fs.StringLong("replication-policy", "", "Replication subscriber delivery policy (all|last|new|by_start_sequence=X|by_start_time=x)")

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
	switch strings.ToUpper(*logLevel) {
	case "INFO":
		slog.SetLogLoggerLevel(slog.LevelInfo)
	case "DEBUG":
		slog.SetLogLoggerLevel(slog.LevelDebug)
	case "ERROR":
		slog.SetLogLoggerLevel(slog.LevelError)
	case "WARN":
		slog.SetLogLoggerLevel(slog.LevelWarn)
	default:
		return fmt.Errorf("invalid log-level! Valid values: info, debug, error, warm")
	}

	if *concurrentQueries < 1 {
		return fmt.Errorf("--concurrent-queries must be at least 1")
	}

	nodeName := *name
	if nodeName == "" {
		var err error
		nodeName, err = os.Hostname()
		if err != nil {
			return fmt.Errorf("failed to get hostname: %w", err)
		}
	}

	var (
		db  *sql.DB
		dsn string
		err error
	)
	args := fs.GetArgs()
	if *memDB {
		dsn = "file:/ha.db?vfs=memdb"
		if len(args) > 0 {
			dsn = fmt.Sprintf("file:/%s?vfs=memdb", strings.TrimPrefix(args[0], "/"))
		}
	} else {
		dsn = "file:ha.db?_journal=WAL&_timeout=5000"
		if len(args) > 0 {
			dsn = args[0]
		}
	}

	opts := []ha.Option{
		ha.WithName(nodeName),
		ha.WithReplicas(*replicas),
		ha.WithStreamMaxAge(*replicationMaxAge),
		ha.WithReplicationURL(*replicationURL),
		ha.WithReplicationStream(*replicationStream),
		ha.WithPublisherTimeout(*replicationTimeout),
		ha.WithDeliverPolicy(*replicationPolicy),
		ha.WithSnapshotInterval(*snapshotInterval),
		ha.WithNatsOptions(
			nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
				if err != nil {
					slog.Error("Got disconnected!", "reason", err)
				}
			}),
			nats.ReconnectHandler(func(nc *nats.Conn) {
				slog.Info("Got reconnected!", "url", nc.ConnectedUrl())
			}),
			nats.ClosedHandler(func(nc *nats.Conn) {
				if err := nc.LastError(); err != nil {
					slog.Error("Connection closed.", "reason", err)
				}
			}),
		),
	}
	if *disableDDLSync {
		opts = append(opts, ha.WithDisableDDLSync())
	}
	if *extensions != "" {
		opts = append(opts, ha.WithExtensions(strings.Split(*extensions, ",")...))
	}
	if *natsPort > 0 || *natsConfig != "" {
		opts = append(opts, ha.WithEmbeddedNatsConfig(&ha.EmbeddedNatsConfig{
			Name:       nodeName,
			Port:       *natsPort,
			StoreDir:   *natsStoreDir,
			User:       *natsUser,
			Pass:       *natsPass,
			File:       *natsConfig,
			EnableLogs: *natsLogs,
		}))
	}

	if *interceptorPath != "" {
		changeSetInterceptor, err := interceptor.Load(*interceptorPath)
		if err != nil {
			return fmt.Errorf("failed to load custom interceptor: %w", err)
		}
		opts = append(opts, ha.WithChangeSetInterceptor(changeSetInterceptor))
	}

	var connector *ha.Connector

	if *fromLatestSnapshot {
		slog.Info("loading latest snapshot from NATS JetStream Object Store")
		sequence, reader, err := ha.LatestSnapshot(context.Background(), dsn, opts...)
		if err != nil && !errors.Is(err, jetstream.ErrObjectNotFound) {
			return fmt.Errorf("failed to load latest snapshot: %w", err)
		}
		if sequence > 0 && *replicationPolicy == "" {
			policy := fmt.Sprintf("by_start_sequence=%d", sequence)
			opts = append(opts, ha.WithDeliverPolicy(policy))
		}
		if reader != nil {
			if *memDB {
				connector, err = sqlite3ha.NewConnector(dsn, opts...)
				if err != nil {
					return err
				}
				defer connector.Close()
				err = sqlite.DeserializeFromReader(context.Background(), reader)
				if err != nil {
					return fmt.Errorf("failed to load latest snapshot: %w", err)
				}
			} else {
				filename := filenameFromDSN(dsn)
				f, err := os.Create(filename)
				if err != nil {
					return fmt.Errorf("failed to create snapshot file %q: %w", filename, err)
				}
				_, err = io.Copy(f, reader)
				if err != nil {
					f.Close()
					return fmt.Errorf("failed to write snapshot file %q: %w", filename, err)
				}
				f.Close()
				slog.Info("loading snapshot", "filename", filename)
				db.Close()
				connector, err = sqlite3ha.NewConnector(dsn, opts...)
				if err != nil {
					return err
				}
				defer connector.Close()
				db = sql.OpenDB(connector)
				defer db.Close()
				configDB(db)
			}
		}
	} else {
		if *memDB {
			slog.Info("using in-memory database")
			var filename string
			if len(args) > 0 {
				filename = args[0]
			}
			if filename != "" && *replicationPolicy == "" {
				matched, _ := regexp.MatchString(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}`, filepath.Base(filename))
				if matched {
					dateTime := filepath.Base(filename)[0:len(time.DateTime)]
					_, err := time.Parse(time.DateTime, dateTime)
					if err == nil {
						policy := fmt.Sprintf("by_start_time=%s", dateTime)
						opts = append(opts, ha.WithDeliverPolicy(policy))
					}
				}
			}

			connector, err = sqlite3ha.NewConnector(dsn, opts...)
			if err != nil {
				return err
			}
			defer connector.Close()
			db = sql.OpenDB(connector)
			defer db.Close()
			configDB(db)

			if filename != "" {
				slog.Info("loading database", "file", filename)
				err := sqlite.Deserialize(context.Background(), filename)
				if err != nil {
					return fmt.Errorf("failed to load database %q: %w", filename, err)
				}
			}
		} else {
			slog.Info("using data source name", "dsn", dsn)
			connector, err = sqlite3ha.NewConnector(dsn, opts...)
			if err != nil {
				return err
			}
			defer connector.Close()
			db = sql.OpenDB(connector)
			defer db.Close()
			configDB(db)
		}
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
		err := sqlite3ha.Backup(r.Context(), db, w)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})

	mux.HandleFunc("POST /snapshot", func(w http.ResponseWriter, r *http.Request) {
		sequence, err := connector.TakeSnapshot(r.Context(), db)
		if err != nil {
			slog.Error("take snapshot", "error", err)
			http.Error(w, fmt.Sprintf("failed to take snapshot: %v", err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("X-Sequence", fmt.Sprint(sequence))
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("GET /snapshot", func(w http.ResponseWriter, r *http.Request) {
		sequence, reader, err := connector.LatestSnapshot(r.Context())
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
		info, err := connector.DeliveredInfo(r.Context(), "")
		if err != nil {
			slog.Error("failed to get replication info", "error", err)
			http.Error(w, fmt.Sprintf("failed to get replication info: %v", err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"replications": info,
		})
	})

	mux.HandleFunc("GET /replications/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		info, err := connector.DeliveredInfo(r.Context(), name)
		if err != nil {
			slog.Error("failed to get replication info", "error", err, "name", name)
			http.Error(w, fmt.Sprintf("failed to get replication info: %v", err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"replications": info,
		})
	})

	mux.HandleFunc("DELETE /replications/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if name == "" {
			http.Error(w, "name is required", http.StatusInternalServerError)
			return
		}
		err := connector.RemoveConsumer(r.Context(), name)
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
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-done
		slog.Warn("signal detected...", "signal", sig)
		if err := pgServer.Close(); err != nil {
			slog.Error("PostgreSQL server shutdown failed", "error", err)
		}
		if connector != nil {
			connector.Close()
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

func configDB(db *sql.DB) {
	db.SetConnMaxIdleTime(0)
	db.SetConnMaxLifetime(0)
	db.SetMaxOpenConns(*concurrentQueries)
	db.SetMaxIdleConns(*concurrentQueries)

	sqlite.SetGlobalDB(db)
}

func filenameFromDSN(dsn string) string {
	var filename string
	u, err := url.Parse(dsn)
	if err == nil {
		filename = u.Path
	}
	if filename == "" {
		filename = strings.TrimPrefix(dsn, "file:")
		if i := strings.Index(filename, "?"); i > 0 {
			filename = filename[0:i]
		}
	}
	if filename != "" {
		return filepath.Base(filename)
	}
	return ""
}
