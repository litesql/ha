package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
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

	pgPort *int
	pgUser *string
	pgPass *string
	pgCert *string
	pgKey  *string

	concurrentQueries *int
	extensions        *string

	natsPort     *int
	natsStoreDir *string
	natsConfig   *string

	replicationStream  *string
	replicationTimeout *time.Duration
	replicationMaxAge  *time.Duration
	replicationURL     *string
)

func main() {
	fs = ff.NewFlagSet("ha")
	name = fs.String('n', "name", "", "Node name")
	port = fs.Uint('p', "port", 8080, "Server port")

	natsPort = fs.IntLong("nats-port", 4222, "Embedded NATS server port (0 to disable)")
	natsStoreDir = fs.StringLong("nats-store-dir", "", "Embedded NATS server store directory")
	natsConfig = fs.StringLong("nats-config", "", "Embedded NATS server config file")

	pgPort = fs.IntLong("pg-port", 5432, "PostgreSQL Server port")
	pgUser = fs.StringLong("pg-user", "ha", "PostgreSQL Auth user")
	pgPass = fs.StringLong("pg-pass", "ha", "PostgreSQL Auth password")
	pgCert = fs.StringLong("pg-cert", "", "PostgreSQL TLS certificate file")
	pgKey = fs.StringLong("pg-key", "", "PostgreSQL TLS key file")

	concurrentQueries = fs.IntLong("concurrent-queries", 50, "Number of concurrent queries")
	extensions = fs.StringLong("extensions", "", "Comma-separated list of SQLite extensions to load")

	replicationTimeout = fs.DurationLong("replication-timeout", 5*time.Second, "Replication publisher timeout")
	replicationStream = fs.StringLong("replication-stream", "ha_replication", "Replication stream name")
	replicationMaxAge = fs.DurationLong("replication-max-age", 24*time.Hour, "Replication stream max age")
	replicationURL = fs.StringLong("replication-url", "", "Replication NATS url (defaults to embedded NATS server)")
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

	var natsConn *nats.Conn
	if *natsPort > 0 || *natsConfig != "" {
		slog.Info("starting embedded NATS server")
		nc, ns, err := ha_nats.RunEmbeddedNATSServer(ha_nats.Config{
			Name:     nodeName,
			Port:     *natsPort,
			StoreDir: *natsStoreDir,
			File:     *natsConfig,
		})
		if err != nil {
			return fmt.Errorf("failed to start embedded NATS server: %w", err)
		}
		natsConn = nc
		defer ns.Shutdown()
	}

	var cdcPublisher sqlite.CDCPublisher
	var err error
	if natsConn != nil || *replicationURL != "" {
		slog.Info("starting replicator publisher", "stream", *replicationStream)
		cdcPublisher, err = ha_nats.NewCDCPublisher(natsConn, *replicationURL, *replicationStream, *replicationMaxAge, *replicationTimeout)
		if err != nil {
			return fmt.Errorf("failed to start CDC NATS publisher: %w", err)
		}
	}

	sqlite.RegisterDriver(sqlExtensions, nodeName, cdcPublisher)
	db, err := sql.Open("sqlite3-ha", "file:ha.db?vfs=memdb")
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetConnMaxIdleTime(-1)
	db.SetConnMaxLifetime(-1)
	db.SetMaxOpenConns(*concurrentQueries)

	sqlite.SetGlobalDB(db)

	args := fs.GetArgs()
	if len(args) > 0 {
		filename := args[0]
		slog.Info("loading database", "file", filename)
		err := sqlite.Deserialize(context.Background(), filename)
		if err != nil {
			return fmt.Errorf("failed to load database %q: %w", filename, err)
		}
	}

	if natsConn != nil || *replicationURL != "" {
		slog.Info("starting CDC subscriber", "stream", *replicationStream)
		cdcSubscriber, err := ha_nats.NewCDCSubscriber(nodeName, natsConn, *replicationURL, *replicationStream, db)
		if err != nil {
			return fmt.Errorf("failed to start CDC NATS subscriber: %w", err)
		}
		defer cdcSubscriber.Close()
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
		data, err := sqlite.Serialize(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Disposition", "attachment; filename=ha.db")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(data)
	})

	server := http.Server{
		Addr:    fmt.Sprintf(":%d", *port),
		Handler: mux,
	}

	pgServer, err := pgwire.NewServer(pgwire.Config{
		User:    *pgUser,
		Pass:    *pgPass,
		TLSCert: *pgCert,
		TLSKey:  *pgKey,
	}, db)
	if err != nil {
		return fmt.Errorf("failed to create PostgreSQL server: %w", err)
	}

	err = pgServer.ListenAndServe(*pgPort)
	if err != nil {
		log.Fatalf("PostgreSQL server error: %v", err)
	}

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-done
		slog.Warn("signal detected...", "signal", sig)
		if err := pgServer.Close(); err != nil {
			slog.Error("PostgreSQL server shutdown failed", "error", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			slog.Error("HTTP server shutdown failed", "error", err)
		}
	}()

	slog.Info("Starting HA server", "port", *port, "version", version, "commit", commit, "date", date)
	return server.ListenAndServe()
}
