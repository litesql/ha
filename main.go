package main

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	ha "github.com/litesql/go-ha"
	"github.com/nats-io/nats.go"
	"github.com/peterbourgon/ff/v4"
	"github.com/peterbourgon/ff/v4/ffhelp"

	hahttp "github.com/litesql/ha/internal/http"
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
	flagSet  *ff.FlagSet
	dbParams *string
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

	asyncReplication          *bool
	asyncReplicationOutboxDir *string
	replicationStream         *string
	replicationTimeout        *time.Duration
	replicationMaxAge         *time.Duration
	replicationURL            *string
	replicationPolicy         *string
	replicas                  *int

	interceptorPath *string
)

//go:embed static
var staticFiles embed.FS

func main() {
	flagSet = ff.NewFlagSet("ha")
	dbParams = flagSet.StringLong("db-params", "_journal=WAL&_timeout=5000&_sync=NORMAL", "SQLite DSN parameters (added to each database file DSN if not defined)")
	name = flagSet.String('n', "name", "", "Node name")
	port = flagSet.Uint('p', "port", 8080, "Server port")
	interceptorPath = flagSet.String('i', "interceptor", "", "Path to a golang script to customize replication behaviour")
	logLevel = flagSet.StringLong("log-level", "info", "Log level (info, warn, error, debug)")

	memDB = flagSet.Bool('m', "memory", "Store database in memory")
	fromLatestSnapshot = flagSet.BoolLong("from-latest-snapshot", "Use the latest database snapshot from NATS JetStream Object Store (if available at startup)")
	snapshotInterval = flagSet.DurationLong("snapshot-interval", 0, "Interval to create database snapshot to NATS JetStream Object Store (0 to disable)")
	disableDDLSync = flagSet.BoolLong("disable-ddl-sync", "Disable DDL commands publisher")

	natsLogs = flagSet.BoolLong("nats-logs", "Enable NATS server logging")
	natsPort = flagSet.IntLong("nats-port", 4222, "Embedded NATS server port (0 to disable)")
	natsStoreDir = flagSet.StringLong("nats-store-dir", "", "Embedded NATS server store directory")
	natsUser = flagSet.StringLong("nats-user", "", "Embedded NATS server user")
	natsPass = flagSet.StringLong("nats-pass", "", "Embedded NATS server password")
	natsConfig = flagSet.StringLong("nats-config", "", "Embedded NATS server config file")

	pgPort = flagSet.IntLong("pg-port", 5432, "PostgreSQL Server port")
	pgUser = flagSet.StringLong("pg-user", "ha", "PostgreSQL Auth user")
	pgPass = flagSet.StringLong("pg-pass", "ha", "PostgreSQL Auth password")
	pgCert = flagSet.StringLong("pg-cert", "", "PostgreSQL TLS certificate file")
	pgKey = flagSet.StringLong("pg-key", "", "PostgreSQL TLS key file")

	concurrentQueries = flagSet.IntLong("concurrent-queries", 50, "Number of concurrent queries")
	extensions = flagSet.StringLong("extensions", "", "Comma-separated list of SQLite extensions to load")

	asyncReplication = flagSet.BoolLong("async-replication", "Enables asynchronous replication message publishing")
	asyncReplicationOutboxDir = flagSet.StringLong("async-replication-store-dir", "", "Directory path for storing outbox messages used in asynchronous replication")
	replicas = flagSet.IntLong("replicas", 1, "Number of replicas to keep for the stream and object store in clustered jetstream. Defaults to 1, maximum is 5")
	replicationTimeout = flagSet.DurationLong("replication-timeout", 15*time.Second, "Replication publisher timeout")
	replicationStream = flagSet.StringLong("replication-stream", "ha_replication", "Replication stream name")
	replicationMaxAge = flagSet.DurationLong("replication-max-age", 24*time.Hour, "Replication stream max age")
	replicationURL = flagSet.StringLong("replication-url", "", "Replication NATS url (defaults to embedded NATS server)")
	replicationPolicy = flagSet.StringLong("replication-policy", "", "Replication subscriber delivery policy (all|last|new|by_start_sequence=X|by_start_time=x)")

	printVersion := flagSet.BoolLong("version", "Print version information and exit")
	_ = flagSet.String('c', "config", "", "config file (optional)")

	if err := ff.Parse(flagSet, os.Args[1:],
		ff.WithEnvVarPrefix("HA"),
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
	); err != nil {
		fmt.Printf("%s\n", ffhelp.Flags(flagSet))
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

	dsnList := make([]string, 0)
	dsnParams := *dbParams
	dsnPrefix := "file:"
	if *memDB {
		dsnParams = "vfs=memdb"
		dsnPrefix = "file:/"
	}
	for _, pattern := range flagSet.GetArgs() {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			log.Fatal(err)
		}

		for _, file := range matches {
			dsn := fmt.Sprintf("%s%s?%s", dsnPrefix, file, dsnParams)
			dsnList = append(dsnList, dsn)
		}
		if len(matches) == 0 && !strings.Contains(pattern, "*") {
			dsn := fmt.Sprintf("%s%s", dsnPrefix, strings.TrimPrefix(pattern, "file:"))
			if !strings.Contains(dsn, "?") {
				dsn = fmt.Sprintf("%s?%s", dsn, dsnParams)
			}
			dsnList = append(dsnList, dsn)
		}
	}
	if len(dsnList) == 0 {
		dsnList = append(dsnList, fmt.Sprintf("%s%s?%s", dsnPrefix, "ha.db", dsnParams))
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

	if *asyncReplication {
		opts = append(opts, ha.WithAsyncPublisher())
		opts = append(opts, ha.WithAsyncPublisherOutboxDir(*asyncReplicationOutboxDir))
	}
	err := sqlite.Load(dsnList, *memDB, *fromLatestSnapshot, *replicationPolicy, *concurrentQueries, opts...)
	if err != nil {
		return fmt.Errorf("failed to load database: %w", err)
	}

	staticFs, err := fs.Sub(staticFiles, "static")
	if err != nil {
		return err
	}
	fileServer := http.FileServer(http.FS(staticFs))

	mux := http.NewServeMux()
	mux.Handle("GET /", fileServer)

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /databases", hahttp.DatabasesHandler)

	mux.HandleFunc("POST /databases/{id}", hahttp.QueryHandler)
	mux.HandleFunc("POST /", hahttp.QueryHandler)

	mux.HandleFunc("GET /databases/{id}", hahttp.DownloadHandler)
	mux.HandleFunc("GET /download", hahttp.DownloadHandler)

	mux.HandleFunc("POST /databases/{id}/snapshot", hahttp.TakeSnapshotHandler)
	mux.HandleFunc("POST /snapshot", hahttp.TakeSnapshotHandler)

	mux.HandleFunc("GET /databases/{id}/snapshot", hahttp.DownloadSnapshotHandler)
	mux.HandleFunc("GET /snapshot", hahttp.DownloadSnapshotHandler)

	mux.HandleFunc("GET /databases/{id}/replications", hahttp.ReplicationsHandler)
	mux.HandleFunc("GET /replications", hahttp.ReplicationsHandler)
	mux.HandleFunc("GET /databases/{id}/replications/{name}", hahttp.ReplicationsHandler)
	mux.HandleFunc("GET /replications/{name}", hahttp.ReplicationsHandler)

	mux.HandleFunc("DELETE /databases/{id}/replications/{name}", hahttp.DeleteReplicationHandler)
	mux.HandleFunc("DELETE /replications/{name}", hahttp.DeleteReplicationHandler)

	pgServer, err := pgwire.NewServer(pgwire.Config{
		User:    *pgUser,
		Pass:    *pgPass,
		TLSCert: *pgCert,
		TLSKey:  *pgKey,
	})
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
		ha.Shutdown()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			slog.Error("HTTP server shutdown failed", "error", err)
		}
	}()

	slog.Info("starting HA HTTP server", "port", *port, "version", version, "commit", commit, "date", date)
	return server.ListenAndServe()
}
