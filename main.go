package main

import (
	"context"
	"crypto/subtle"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
	ha "github.com/litesql/go-ha"
	haconnect "github.com/litesql/go-ha/connect"
	"github.com/peterbourgon/ff/v4"
	"github.com/peterbourgon/ff/v4/ffhelp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/litesql/ha/internal/cli"
	"github.com/litesql/ha/internal/interceptor"
	"github.com/litesql/ha/internal/mcp"
	"github.com/litesql/ha/internal/o11y"
	halog "github.com/litesql/ha/internal/o11y/log"
	"github.com/litesql/ha/internal/o11y/metric"
	"github.com/litesql/ha/internal/o11y/trace"
	"github.com/litesql/ha/internal/sqlite"
	hahttp "github.com/litesql/ha/internal/wire/http"
	"github.com/litesql/ha/internal/wire/mysql"
	"github.com/litesql/ha/internal/wire/postgresql"
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
	token    *string
	logLevel *string

	createDatabaseDir *string

	memDB              *bool
	snapshotInterval   *time.Duration
	fromLatestSnapshot *bool
	disableDDLSync     *bool

	staticRemoteLeaderAddr *string
	dynamicLocalLeaderAddr *string
	grpcInsecure           *bool

	pgPort            *int
	pgUser            *string
	pgPass            *string
	pgCert            *string
	pgKey             *string
	pgProxied         *string
	pgPublicationName *string
	pgSlotName        *string

	proxyLocalDB         *string
	proxyUseSchema       *bool
	proxyDisableRedirect *bool
	proxyReadYourWrites  *bool

	mysqlPort              *int
	mysqlUser              *string
	mysqlPass              *string
	mysqlProxied           *string
	mysqlProxiedInclude    *string
	mysqlProxiedExclude    *string
	mysqlProxiedDumpBin    *string
	mysqlProxiedDumpDB     *string
	mysqlProxiedDumpTables *string
	mysqlProxyID           *string

	debeziumBrokers   *string
	debeziumGroup     *string
	debeziumTopics    *[]string
	debeziumSourceDSN *string

	concurrentQueries *int
	extensions        *string

	natsLogs     *bool
	natsPort     *int
	natsUser     *string
	natsPass     *string
	natsStoreDir *string
	natsConfig   *string

	asyncReplication   *bool
	replicationStream  *string
	replicationTimeout *time.Duration
	replicationMaxAge  *time.Duration
	replicationURL     *string
	replicationPolicy  *string
	replicas           *int
	rowIdentify        *string

	interceptorPath *string

	observability *bool

	remote *string
)

//go:embed openapi.yaml
var openAPI []byte

//go:embed docs.html
var docsHTML []byte

func main() {
	flagSet = ff.NewFlagSet("ha")
	dbParams = flagSet.StringLong("db-params", defaultDBOptions, "SQLite DSN parameters appended to each database file DSN unless already present")
	name = flagSet.String('n', "name", "", "Node name")
	port = flagSet.Uint('p', "port", 8080, "Server port for HTTP and gRPC endpoints")
	token = flagSet.StringLong("token", "", "API auth token for HTTP and gRPC requests")
	interceptorPath = flagSet.String('i', "interceptor", "", "Path to a Go script that customizes replication behavior")
	logLevel = flagSet.StringLong("log-level", "info", "Log verbosity level: info, warn, error, or debug")

	createDatabaseDir = flagSet.StringLong("create-db-dir", "", "Directory where new database files are created")

	memDB = flagSet.Bool('m', "memory", "Store the database in memory instead of on disk")
	fromLatestSnapshot = flagSet.BoolLong("from-latest-snapshot", "Load the latest database snapshot from NATS JetStream Object Store at startup if available")
	snapshotInterval = flagSet.DurationLong("snapshot-interval", 0, "Interval for automatic snapshots to NATS JetStream Object Store (0 disables)")
	disableDDLSync = flagSet.BoolLong("disable-ddl-sync", "Disable publishing DDL commands")

	natsLogs = flagSet.BoolLong("nats-logs", "Enable logging for the embedded NATS server")
	natsPort = flagSet.IntLong("nats-port", 4222, "Embedded NATS server port (0 disables embedded NATS)")
	natsStoreDir = flagSet.StringLong("nats-store-dir", "", "Embedded NATS server storage directory")
	natsUser = flagSet.StringLong("nats-user", "", "Embedded NATS server username")
	natsPass = flagSet.StringLong("nats-pass", "", "Embedded NATS server password")
	natsConfig = flagSet.StringLong("nats-config", "", "Embedded NATS server configuration file")

	dynamicLocalLeaderAddr = flagSet.StringLong("leader-addr", "", "Address used when this node becomes leader; enables leader election")
	staticRemoteLeaderAddr = flagSet.StringLong("leader-static", "", "Static leader address; disables leader election")
	grpcInsecure = flagSet.BoolLong("grpc-insecure", "Use plaintext gRPC for leader messages; only on trusted networks")

	mysqlPort = flagSet.IntLong("mysql-port", 0, "Port for MySQL wire protocol server")
	mysqlUser = flagSet.StringLong("mysql-user", "ha", "MySQL authentication user")
	mysqlPass = flagSet.StringLong("mysql-pass", "", "MySQL authentication password")
	mysqlProxied = flagSet.StringLong("mysql-proxied", "", "Source MySQL DSN to replicate into the local HA instance and redirect writes")
	mysqlProxiedInclude = flagSet.StringLong("mysql-include", "^db.*", "Regexp matching tables to include from the proxied MySQL source; empty includes all")
	mysqlProxiedExclude = flagSet.StringLong("mysql-exclude", "", "Regexp matching tables to exclude from the proxied MySQL source")
	mysqlProxiedDumpBin = flagSet.StringLong("mysql-dump-bin", "", "Path to the mysqldump executable used for proxied MySQL import")
	mysqlProxiedDumpDB = flagSet.StringLong("mysql-dump-db", "", "Database name used with mysqldump when importing from proxied MySQL")
	mysqlProxiedDumpTables = flagSet.StringLong("mysql-dump-include", "", "Table filter used by mysqldump during proxied MySQL import")
	mysqlProxyID = flagSet.StringLong("mysql-proxy-id", "sqlite-ha", "Identifier for this proxied MySQL connection in replication metadata")

	pgPort = flagSet.IntLong("pg-port", 0, "Port for PostgreSQL wire protocol server")
	pgUser = flagSet.StringLong("pg-user", "ha", "PostgreSQL authentication user")
	pgPass = flagSet.StringLong("pg-pass", "ha", "PostgreSQL authentication password")
	pgCert = flagSet.StringLong("pg-cert", "", "TLS certificate file for PostgreSQL server")
	pgKey = flagSet.StringLong("pg-key", "", "TLS key file for PostgreSQL server")
	pgProxied = flagSet.StringLong("pg-proxied", "", "Source PostgreSQL DSN to replicate from and proxy to")
	pgPublicationName = flagSet.StringLong("pg-publication", "ha_publication", "Publication name in the source PostgreSQL database for logical replication")
	pgSlotName = flagSet.StringLong("pg-slot", "ha_slot", "Replication slot name to create in the source PostgreSQL database")

	proxyLocalDB = flagSet.StringLong("proxy-local", "ha.db", "Local SQLite file path used as a proxy for the source database")
	proxyUseSchema = flagSet.BoolLong("proxy-use-schema", "Create local tables using the source database schema")
	proxyDisableRedirect = flagSet.BoolLong("proxy-disable-redirect", "Disable query redirect; execute all queries on the local HA SQLite database")
	proxyReadYourWrites = flagSet.BoolLong("proxy-read-your-writes", "Enable read-your-writes behavior for proxied queries")

	debeziumBrokers = flagSet.StringLong("debezium-brokers", "", "Comma-separated Kafka brokers for Debezium sink mode")
	debeziumGroup = flagSet.StringLong("debezium-group", "", "Kafka consumer group for Debezium sink")
	debeziumTopics = flagSet.StringListLong("debezium-topics", "Kafka topics to consume")
	debeziumSourceDSN = flagSet.StringLong("debezium-source-dsn", "", "Source DSN for Debezium write redirection")

	concurrentQueries = flagSet.IntLong("concurrent-queries", 50, "Maximum number of concurrent queries")

	asyncReplication = flagSet.BoolLong("async-replication", "Enable asynchronous replication message publishing")
	replicas = flagSet.IntLong("replicas", 1, "Number of JetStream replicas for stream and object store, from 1 to 5")
	replicationTimeout = flagSet.DurationLong("replication-timeout", 15*time.Second, "Timeout for replication publisher operations")
	replicationStream = flagSet.StringLong("replication-stream", "ha_replication", "Replication stream name")
	replicationMaxAge = flagSet.DurationLong("replication-max-age", 24*time.Hour, "Maximum age for messages in the replication stream")
	replicationURL = flagSet.StringLong("replication-url", "", "NATS URL for replication; defaults to embedded NATS when empty")
	replicationPolicy = flagSet.StringLong("replication-policy", "", "Replication subscriber delivery policy: all, last, new, by_start_sequence=X, or by_start_time=x")
	rowIdentify = flagSet.StringLong("row-identify", "pk", "Row identification strategy for replication: pk, rowid, or full")

	remote = flagSet.String('r', "remote", "", "Remote HA server address for client mode instead of starting a local server")

	observability = flagSet.BoolLong("observability", "Enable OpenTelemetry observability for metrics, logs and traces")

	initDynamicFlags()

	printVersion := flagSet.BoolLong("version", "Print version information and exit")
	_ = flagSet.String('c', "config", "", "config file (optional)")

	if err := ff.Parse(flagSet, os.Args[1:],
		ff.WithEnvVarPrefix("HA"),
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
	); err != nil {
		fmt.Printf("%s\n", ffhelp.Flags(flagSet))
		fmt.Printf("err=%v\n", err)
		os.Exit(1)
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
	var level slog.Level
	switch strings.ToUpper(*logLevel) {
	case "INFO":
		level = slog.LevelInfo
	case "DEBUG":
		level = slog.LevelDebug
	case "ERROR":
		level = slog.LevelError
	case "WARN":
		level = slog.LevelWarn
	default:
		return fmt.Errorf("invalid log-level! Valid values: info, debug, error, warn")
	}

	nodeName := *name
	if nodeName == "" {
		var err error
		nodeName, err = os.Hostname()
		if err != nil {
			return fmt.Errorf("failed to get hostname: %w\n set a static name with --name", err)
		}
	}

	if *remote != "" {
		ctx := context.Background()
		var flushObservability func(context.Context) error
		if *observability {
			var err error
			flushObservability, err = initObservability(ctx, nodeName, level, true)
			if err != nil {
				return err
			}
		}
		cli.Start(*remote, *token, *observability)
		if flushObservability != nil {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if err := flushObservability(shutdownCtx); err != nil {
				return fmt.Errorf("observability shutdown failed: %w", err)
			}
		}
		return nil
	}

	if *concurrentQueries < 1 {
		return fmt.Errorf("--concurrent-queries must be at least 1")
	}

	dsnList := make([]string, 0)
	seenDBIDs := make(map[string]struct{})
	addDSN := func(dsn string) {
		id := sqlite.IdFromDSN(dsn)
		if id != "" {
			if _, exists := seenDBIDs[id]; exists {
				return
			}
			seenDBIDs[id] = struct{}{}
		}
		dsnList = append(dsnList, dsn)
	}
	dsnParams := *dbParams
	dsnPrefix := "file:"
	if *memDB {
		dsnParams = "vfs=memdb"
		dsnPrefix = "file:/"
	}
	patterns := flagSet.GetArgs()
	if *createDatabaseDir != "" {
		err := os.MkdirAll(*createDatabaseDir, os.ModePerm)
		if err != nil {
			return fmt.Errorf("failed to create database directory: %w", err)
		}
		err = filepath.Walk(*createDatabaseDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() && isSQLiteFile(path) {
				dsn := fmt.Sprintf("%s%s?%s", dsnPrefix, path, dsnParams)
				addDSN(dsn)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to scan database directory: %w", err)
		}
	}
	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return fmt.Errorf("invalid pattern %q: %w", pattern, err)
		}

		for _, file := range matches {
			dsn := fmt.Sprintf("%s%s?%s", dsnPrefix, file, dsnParams)
			addDSN(dsn)
		}
		if len(matches) == 0 && !strings.Contains(pattern, "*") {
			dsn := fmt.Sprintf("%s%s", dsnPrefix, strings.TrimPrefix(pattern, "file:"))
			if !strings.Contains(dsn, "?") {
				dsn = fmt.Sprintf("%s?%s", dsn, dsnParams)
			}
			addDSN(dsn)
		}
	}
	if len(dsnList) == 0 {
		addDSN(fmt.Sprintf("%s%s?%s", dsnPrefix, filepath.Join(*createDatabaseDir, "ha.db"), dsnParams))
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
		ha.WithGrpcInsecure(*grpcInsecure),
	}
	if *disableDDLSync {
		opts = append(opts, ha.WithDisableDDLSync())
	}
	if extensions != nil && *extensions != "" {
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

	if *staticRemoteLeaderAddr != "" {
		opts = append(opts, ha.WithLeaderProvider(&ha.StaticLeader{
			Target: *staticRemoteLeaderAddr,
		}))
	} else if *dynamicLocalLeaderAddr != "" {
		opts = append(opts, ha.WithLeaderElectionLocalTarget(*dynamicLocalLeaderAddr))
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
	}

	if *rowIdentify != "" {
		switch *rowIdentify {
		case string(ha.PK):
			opts = append(opts, ha.WithRowIdentify(ha.PK))
		case string(ha.Rowid):
			opts = append(opts, ha.WithRowIdentify(ha.Rowid))
		case string(ha.Full):
			opts = append(opts, ha.WithRowIdentify(ha.Full))
		default:
			return fmt.Errorf("invalid --row-identify. Use pk, rowid or full")
		}
	}

	var dumpTables []string
	if *mysqlProxiedDumpTables != "" {
		dumpTables = strings.Split(*mysqlProxiedDumpTables, ",")
	}
	proxyCfg := sqlite.ProxiedDBConfig{
		PgDSN:             *pgProxied,
		PgPublicationName: *pgPublicationName,
		PgSlotName:        *pgSlotName,
		MysqlDSN:          *mysqlProxied,
		MysqlInclude:      *mysqlProxiedInclude,
		MysqlExclude:      *mysqlProxiedExclude,
		MysqlID:           *mysqlProxyID,
		MysqlDumpBin:      *mysqlProxiedDumpBin,
		MysqlDumpDB:       *mysqlProxiedDumpDB,
		MysqlDumpTables:   dumpTables,
		DebeziumBroker:    *debeziumBrokers,
		DebeziumGroup:     *debeziumGroup,
		DebeziumTopics:    *debeziumTopics,
		DebeziumSourceDSN: *debeziumSourceDSN,
		LocalDB:           *proxyLocalDB,
		UseSchema:         *proxyUseSchema,
		DisableRedirect:   *proxyDisableRedirect,
		ReadYourWrites:    *proxyReadYourWrites,
	}

	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	flushObservability, err := initObservability(ctx, nodeName, level, *observability)
	if err != nil {
		return err
	}
	var otelInterceptor *otelconnect.Interceptor
	if *observability {
		opts = append(opts, ha.WithOpenTelemetry(nil, nil))
		otelInterceptor, err = otelconnect.NewInterceptor()
		if err != nil {
			return fmt.Errorf("failed to create OpenTelemetry Connect interceptor: %w", err)
		}
	}

	for _, dsn := range dsnList {
		err := sqlite.Load(ctx, dsn, sqlite.LoadConfig{
			MemDB:              *memDB,
			FromLatestSnapshot: *fromLatestSnapshot,
			DeliverPolicy:      *replicationPolicy,
			MaxConns:           *concurrentQueries,
			ProxiedDBConfig:    proxyCfg,
			Options:            opts,
		})
		if err != nil {
			return fmt.Errorf("failed to load database %q: %w", dsn, err)
		}
	}

	mux := http.NewServeMux()
	mux.Handle("GET /openapi.yaml", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		w.Write(openAPI)
	}))
	mux.Handle("GET /docs", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write(docsHTML)
	}))

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /readyz", hahttp.ReadyHandler)
	mux.HandleFunc("GET /databases", hahttp.DatabasesHandler)
	mux.HandleFunc("POST /databases", hahttp.CreateDatabaseHandler(*createDatabaseDir,
		*memDB, dsnParams, *fromLatestSnapshot, *replicationPolicy, *concurrentQueries, proxyCfg, opts...))
	mux.HandleFunc("DELETE /databases/{id}", hahttp.DropDatabaseHandler())

	mux.HandleFunc("POST /databases/{id}", hahttp.QueryHandler)
	mux.HandleFunc("POST /databases/{id}/undo/{param}", hahttp.UndoHandler(haconnect.UndoFilterNone))
	mux.HandleFunc("POST /databases/{id}/undoe/{param}", hahttp.UndoHandler(haconnect.UndoFilterEntity))
	mux.HandleFunc("POST /databases/{id}/undot/{param}", hahttp.UndoHandler(haconnect.UndoFilterTransaction))
	mux.HandleFunc("GET /databases/{id}/history/{param}", hahttp.HistoryHandler)
	mux.HandleFunc("POST /query", hahttp.QueryHandler)
	mux.HandleFunc("POST /undo/{param}", hahttp.UndoHandler(haconnect.UndoFilterNone))
	mux.HandleFunc("POST /undoe/{param}", hahttp.UndoHandler(haconnect.UndoFilterEntity))
	mux.HandleFunc("POST /undot/{param}", hahttp.UndoHandler(haconnect.UndoFilterTransaction))
	mux.HandleFunc("GET /history/{param}", hahttp.HistoryHandler)

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

	mux.Handle("/mcp", mcp.NewHTTPHandler(version))

	mysqlServer, err := mysql.NewServer(mysql.Config{
		Port: *mysqlPort,
		User: *mysqlUser,
		Pass: *mysqlPass,
		ConnectorProvider: func(dbName string) (*ha.Connector, bool) {
			connector, err := sqlite.Connector(dbName)
			if err != nil {
				return nil, false
			}
			return connector, true
		},
		DBProvider: func(dbName string) (*sql.DB, bool) {
			db, err := sqlite.DB(dbName)
			if err != nil {
				return nil, false
			}
			return db, true
		},
		CreateDatabaseOptions: sqlite.LoadConfig{
			Dir:                *createDatabaseDir,
			MemDB:              *memDB,
			DefaultDSNParams:   dsnParams,
			FromLatestSnapshot: *fromLatestSnapshot,
			DeliverPolicy:      *replicationPolicy,
			MaxConns:           *concurrentQueries,
			ProxiedDBConfig:    proxyCfg,
			Options:            opts,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create MySQL server: %w", err)
	}

	if *mysqlPort > 0 {
		if err := mysqlServer.ListenAndServe(); err != nil {
			return fmt.Errorf("failed to start MySQL server: %w", err)
		}
	}

	pgServer, err := postgresql.NewServer(postgresql.Config{
		User:    *pgUser,
		Pass:    *pgPass,
		TLSCert: *pgCert,
		TLSKey:  *pgKey,
		CreateOpts: sqlite.LoadConfig{
			Dir:                *createDatabaseDir,
			MemDB:              *memDB,
			DefaultDSNParams:   dsnParams,
			FromLatestSnapshot: *fromLatestSnapshot,
			DeliverPolicy:      *replicationPolicy,
			MaxConns:           *concurrentQueries,
			ProxiedDBConfig:    proxyCfg,
			Options:            opts,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create PostgreSQL server: %w", err)
	}

	var shuttingDown atomic.Bool
	if *pgPort > 0 {
		slog.Info("starting HA PostgreSQL wire Protocol server", "port", *pgPort)
		go func() {
			if err := pgServer.ListenAndServe(*pgPort); err != nil && !errors.Is(err, net.ErrClosed) {
				if !shuttingDown.Load() {
					slog.Error("PostgreSQL server error", "error", err)
				}
			}
		}()
	}

	connectOpts := make([]connect.HandlerOption, 0)
	if *token != "" {
		authInterceptor := haconnect.NewAuthInterceptor(*token)
		connectOpts = append(connectOpts, connect.WithInterceptors(authInterceptor))
	}
	if otelInterceptor != nil {
		connectOpts = append(connectOpts, connect.WithInterceptors(otelInterceptor))
	}
	mux.Handle(ha.ConnectHandler(connectOpts...))
	p := new(http.Protocols)
	p.SetHTTP1(true)
	p.SetUnencryptedHTTP2(true)
	server := http.Server{
		Addr:              fmt.Sprintf(":%d", *port),
		Handler:           mux,
		Protocols:         p,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	if *token != "" {
		expectedToken := []byte(*token)
		server.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			if len(authHeader) >= 7 && strings.EqualFold(authHeader[:7], "Bearer ") {
				authHeader = authHeader[7:]
			}
			if subtle.ConstantTimeCompare([]byte(authHeader), expectedToken) != 1 &&
				r.URL.Path != "/healthz" && r.URL.Path != "/readyz" && r.URL.Path != "/openapi.yaml" && r.URL.Path != "/docs" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			mux.ServeHTTP(w, r)
		})
	}
	if *observability {
		server.Handler = otelhttp.NewHandler(server.Handler, "ha.http", otelhttp.WithFilter(func(r *http.Request) bool {
			return r.URL.Path != "/healthz" && r.URL.Path != "/readyz" && r.URL.Path != "/openapi.yaml" && r.URL.Path != "/docs"
		}))
	}

	done := make(chan os.Signal, 1)
	shutdownDone := make(chan struct{})
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		defer close(shutdownDone)
		sig := <-done
		shuttingDown.Store(true)
		slog.Warn("signal detected...", "signal", sig)
		if err := mysqlServer.Close(); err != nil {
			slog.Error("MySQL server shutdown failed", "error", err)
		}
		if err := pgServer.Close(); err != nil {
			slog.Error("PostgreSQL server shutdown failed", "error", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			slog.Error("HTTP server shutdown failed", "error", err)
		}
		sqlite.Shutdown()
		ha.Shutdown()
		if err := flushObservability(ctx); err != nil {
			slog.Error("observability shutdown failed", "error", err)
		}
	}()

	slog.Info("starting HA HTTP server", "port", *port, "version", version, "commit", commit, "date", date)
	err = server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		<-shutdownDone
	}
	return err
}

func initObservability(ctx context.Context, nodeName string, level slog.Level, enabled bool) (func(context.Context) error, error) {
	o11y.SetVersion(version)
	logsFlusher, err := halog.Init(ctx, nodeName, level, enabled)
	if err != nil {
		return nil, err
	}
	metricsFlusher, err := metric.Init(ctx, nodeName, enabled)
	if err != nil {
		return nil, errors.Join(err, logsFlusher(ctx))
	}
	tracesFlusher, err := trace.Init(ctx, nodeName, enabled)
	if err != nil {
		return nil, errors.Join(err, metricsFlusher(ctx), logsFlusher(ctx))
	}
	return func(ctx context.Context) error {
		return errors.Join(logsFlusher(ctx), metricsFlusher(ctx), tracesFlusher(ctx))
	}, nil
}

func isSQLiteFile(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()

	header := make([]byte, 16)
	_, err = f.Read(header)
	if err != nil {
		return false
	}

	return string(header) == "SQLite format 3\x00"
}
