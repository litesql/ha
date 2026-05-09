package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	_ "github.com/go-mysql-org/go-mysql/driver"
	_ "github.com/jackc/pgx/v5/stdlib"
	debeziumsink "github.com/litesql/debezium-sink/consumer"
	"github.com/litesql/go-ha"
	mysqlreplication "github.com/litesql/mysql/replication"
	pgreplication "github.com/litesql/postgresql/replication"
	_ "github.com/microsoft/go-mssqldb"
	"github.com/nats-io/nats.go/jetstream"
	_ "github.com/sijms/go-ora/v2"
	"github.com/twmb/franz-go/pkg/kgo"
)

type connectorDB struct {
	db        *sql.DB
	connector *ha.Connector
}

type stoppableSubscription interface {
	Stop()
}

type baseProxiedPositionTracker interface {
	ha.ProxiedPositionProvider
	SetReplicaDB(*sql.DB)
}

var (
	dbs                 = make(map[string]*connectorDB)
	proxiedSubscription = make(map[string]stoppableSubscription)
	muDBs               sync.Mutex
)

var reDateTime = regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}`)

type LoadConfig struct {
	Dir                string
	MemDB              bool
	FromLatestSnapshot bool
	DeliverPolicy      string
	MaxConns           int
	ProxiedDBConfig    ProxiedDBConfig
	Options            []ha.Option
}

type ProxiedDBConfig struct {
	PgDSN             string
	PgPublicationName string
	PgSlotName        string

	MysqlDSN        string
	MysqlInclude    string
	MysqlExclude    string
	MysqlID         string
	MysqlDumpBin    string
	MysqlDumpDB     string
	MysqlDumpTables []string

	DebeziumBroker    string
	DebeziumGroup     string
	DebeziumTopics    []string
	DebeziumSourceDSN string

	LocalDB         string
	UseSchema       bool
	DisableRedirect bool
	ReadYourWrites  bool
}

func Load(ctx context.Context, dsn string, cfg LoadConfig) error {
	muDBs.Lock()
	defer muDBs.Unlock()
	defaultDB := false
	if len(dbs) == 0 {
		defaultDB = true
	}
	id := IdFromDSN(dsn)
	if _, exists := dbs[id]; exists {
		return fmt.Errorf("database with id %q already added", id)
	}
	options := slices.Clone(cfg.Options)

	var proxiedPositionProvider baseProxiedPositionTracker
	if cfg.ProxiedDBConfig.LocalDB == id && !cfg.ProxiedDBConfig.DisableRedirect {
		switch {
		case cfg.ProxiedDBConfig.PgDSN != "":
			proxiedDB, err := sql.Open("pgx", cfg.ProxiedDBConfig.PgDSN)
			if err != nil {
				return fmt.Errorf("failed to open proxied db: %w", err)
			}
			options = append(options, ha.WithProxiedDB(proxiedDB))

			proxiedPositionProvider = &pgPositionTracker{
				sourceDB: proxiedDB,
			}
		case cfg.ProxiedDBConfig.MysqlDSN != "":
			proxiedDB, err := sql.Open("mysql", cfg.ProxiedDBConfig.MysqlDSN)
			if err != nil {
				return fmt.Errorf("failed to open proxied db: %w", err)
			}
			options = append(options, ha.WithProxiedDB(proxiedDB))

			proxiedPositionProvider = &mysqlPositionTracker{
				sourceDB: proxiedDB,
			}
		case cfg.ProxiedDBConfig.DebeziumSourceDSN != "":
			var driver string
			switch {
			case strings.HasPrefix(cfg.ProxiedDBConfig.DebeziumSourceDSN, "mysql"), strings.HasPrefix(cfg.ProxiedDBConfig.DebeziumSourceDSN, "mariadb"):
				driver = "mysql"
			case strings.HasPrefix(cfg.ProxiedDBConfig.DebeziumSourceDSN, "oracle"):
				driver = "oracle"
			case strings.HasPrefix(cfg.ProxiedDBConfig.DebeziumSourceDSN, "postgres"):
				driver = "pgx"
			case strings.HasPrefix(cfg.ProxiedDBConfig.DebeziumSourceDSN, "sqlserver"):
				driver = "sqlserver"
			default:
				return fmt.Errorf("unsupported debezium source DSN")
			}

			proxiedDB, err := sql.Open(driver, cfg.ProxiedDBConfig.DebeziumSourceDSN)
			if err != nil {
				return fmt.Errorf("failed to open proxied db: %w", err)
			}
			options = append(options, ha.WithProxiedDB(proxiedDB))
		}
		if cfg.ProxiedDBConfig.ReadYourWrites {
			options = append(options, ha.WithProxiedPositionProvider(proxiedPositionProvider))
		}
	}

	waitFor := make(chan struct{})
	options = append(options, ha.WithWaitFor(waitFor))
	var connector *ha.Connector
	if cfg.FromLatestSnapshot {
		slog.Info("loading latest snapshot from NATS JetStream Object Store", "dsn", dsn)
		sequence, reader, err := ha.LatestSnapshot(ctx, dsn, cfg.Options...)
		if err != nil && !errors.Is(err, jetstream.ErrObjectNotFound) {
			return fmt.Errorf("failed to load latest snapshot: %w", err)
		}

		if sequence > 0 && cfg.DeliverPolicy == "" {
			policy := fmt.Sprintf("by_start_sequence=%d", sequence)
			options = append(options, ha.WithDeliverPolicy(policy))
		}
		if reader != nil {
			if cfg.MemDB {
				connector, err = newConnector(dsn, options...)
				if err != nil {
					return err
				}
				err = deserializeFromReader(ctx, connector, reader)
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
				connector, err = newConnector(dsn, options...)
				if err != nil {
					return err
				}
			}
		}
	} else {
		if cfg.MemDB {
			slog.Info("using in-memory database", "dsn", dsn)
			filename := filenameFromDSN(dsn)
			options := slices.Clone(options)
			if filename != "" && cfg.DeliverPolicy == "" {
				matched := reDateTime.MatchString(filepath.Base(filename))
				if matched {
					dateTime := filepath.Base(filename)[0:len(time.DateTime)]
					_, err := time.Parse(time.DateTime, dateTime)
					if err == nil {
						policy := fmt.Sprintf("by_start_time=%s", dateTime)
						options = append(options, ha.WithDeliverPolicy(policy))
					}
				}
			}
			var err error
			connector, err = newConnector(dsn, options...)
			if err != nil {
				return err
			}

			if filename != "" {
				fi, err := os.Stat(filename)
				if err == nil && !fi.IsDir() {
					slog.Info("loading database", "file", filename)
					err := deserialize(ctx, connector, filename)
					if err != nil {
						return fmt.Errorf("failed to load database %q: %w", filename, err)
					}
				}
			}
		} else {
			slog.Info("using data source name", "dsn", dsn)
			var err error
			connector, err = newConnector(dsn, options...)
			if err != nil {
				return err
			}
		}
	}

	if len(dbs) == 0 {
		slog.Debug("waiting for the leader...")
		<-connector.LeaderProvider().Ready()
	}

	db := sql.OpenDB(connector)
	db.SetConnMaxIdleTime(0)
	db.SetConnMaxLifetime(0)
	db.SetMaxOpenConns(cfg.MaxConns)
	db.SetMaxIdleConns(cfg.MaxConns)

	if proxiedPositionProvider != nil {
		proxiedPositionProvider.SetReplicaDB(db)
	}

	connector.Subscriber().SetDB(db)

	if connector.Snapshotter() != nil {
		connector.Snapshotter().SetDB(db)
	}

	if cfg.ProxiedDBConfig.LocalDB == id {
		_, err := db.ExecContext(ha.ContextLocalDB(ctx, true),
			`CREATE TABLE IF NOT EXISTS ha_proxied_tracker(
				position TEXT,
				server_time,
				CHECK (rowid = 1)
			)`)
		if err != nil {
			return fmt.Errorf("create proxied tracker table: %w", err)
		}
		switch {
		case cfg.ProxiedDBConfig.PgDSN != "":
			slog.Info("setting up replication proxy", "source_dsn", cfg.ProxiedDBConfig.PgDSN, "publication", cfg.ProxiedDBConfig.PgPublicationName, "slot", cfg.ProxiedDBConfig.PgSlotName)
			info, err := pgreplication.CreateSlot(cfg.ProxiedDBConfig.PgDSN, cfg.ProxiedDBConfig.PgSlotName)
			if err != nil {
				slog.Warn("failed to create replication slot", "error", err)
			} else {
				slog.Info("replication slot created", "lsn", info.RestartLSN, "snapshot_name", info.SnapshotName, "plugin", info.Plugin)
			}
			subscription, err := pgreplication.Subscribe(pgreplication.Config{
				DSN:             cfg.ProxiedDBConfig.PgDSN,
				PublicationName: cfg.ProxiedDBConfig.PgPublicationName,
				SlotName:        cfg.ProxiedDBConfig.PgSlotName,
				Timeout:         15 * time.Second,
			}, handlePgProxiedChanges(db), cfg.ProxiedDBConfig.UseSchema)
			if err != nil {
				return fmt.Errorf("subscribe to proxied db: %w", err)
			} else {
				proxiedSubscription[id] = subscription
			}
			go subscription.Start(slog.Default(), pgCheckpointLoader(db), true)
		case cfg.ProxiedDBConfig.MysqlDSN != "":
			slog.Info("setting up replication proxy", "source_dsn", cfg.ProxiedDBConfig.MysqlDSN, "id", cfg.ProxiedDBConfig.MysqlID, "includes", cfg.ProxiedDBConfig.MysqlInclude, "excludes", cfg.ProxiedDBConfig.MysqlExclude, "dump", cfg.ProxiedDBConfig.MysqlDumpBin, "dump_db", cfg.ProxiedDBConfig.MysqlDumpDB, "dump_tables", cfg.ProxiedDBConfig.MysqlDumpTables)

			subscription, err := mysqlreplication.Subscribe(mysqlreplication.Config{
				DSN:                cfg.ProxiedDBConfig.MysqlDSN,
				IncludeTablesRegex: cfg.ProxiedDBConfig.MysqlInclude,
				ExcludeTablesRegex: cfg.ProxiedDBConfig.MysqlExclude,
				Localhost:          cfg.ProxiedDBConfig.MysqlID,
				DumpExecutionPath:  cfg.ProxiedDBConfig.MysqlDumpBin,
				DumpDB:             cfg.ProxiedDBConfig.MysqlDumpDB,
				DumpTables:         cfg.ProxiedDBConfig.MysqlDumpTables,
			}, handleMysqlProxiedChanges(db), cfg.ProxiedDBConfig.UseSchema)
			if err != nil {
				return fmt.Errorf("subscribe to proxied db: %w", err)
			} else {
				proxiedSubscription[id] = subscription
			}
			go subscription.Start(slog.Default(), mysqlCheckpointLoader(db), true)
		case cfg.ProxiedDBConfig.DebeziumBroker != "":
			var opts []kgo.Opt
			opts = append(opts, kgo.SeedBrokers(strings.Split(cfg.ProxiedDBConfig.DebeziumBroker, ",")...))
			opts = append(opts, kgo.ConsumerGroup(cfg.ProxiedDBConfig.DebeziumGroup))
			opts = append(opts, kgo.ConsumeTopics(cfg.ProxiedDBConfig.DebeziumTopics...))
			consumer, err := debeziumsink.New(opts, false)
			if err != nil {
				return fmt.Errorf("subscribe to proxied debezium: %w", err)
			}
			go consumer.Start(slog.Default(), handleDebeziumProxiedChanges(db))
		}
	}
	close(waitFor)

	connDB := &connectorDB{
		db:        db,
		connector: connector,
	}
	dbs[id] = connDB
	if defaultDB {
		dbs[""] = connDB
	}

	return nil
}

type Request struct {
	Sql    string         `json:"sql"`
	Params map[string]any `json:"params"`
}

type Response struct {
	Columns      []string `json:"columns"`
	Rows         [][]any  `json:"rows"`
	RowsAffected int64    `json:"-"`
	NoReturning  bool     `json:"-"`
}

type execerQuerier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func Exec(ctx context.Context, eq execerQuerier, sql string, params map[string]any) (*Response, error) {
	slog.Debug("Executing statement", "sql", sql, "params", params)
	upper := strings.ToUpper(strings.TrimSpace(sql))
	if strings.HasPrefix(upper, "SELECT") || strings.HasPrefix(upper, "EXPLAIN") {
		return doQuery(ctx, eq, sql, params)
	}

	return doExec(ctx, eq, sql, params)
}

func Databases() []string {
	var list []string
	for id := range dbs {
		if id == "" {
			continue
		}
		list = append(list, id)
	}
	return list
}

func DB(id string) (*sql.DB, error) {
	dbConnector, ok := dbs[id]
	if !ok {
		return nil, fmt.Errorf("database with id %q not found", id)
	}
	return dbConnector.db, nil
}

func Connector(id string) (*ha.Connector, error) {
	dbConnector, ok := dbs[id]
	if !ok {
		return nil, fmt.Errorf("database with id %q not found", id)
	}
	return dbConnector.connector, nil
}

func Transaction(ctx context.Context, db *sql.DB, queries []Request) ([]*Response, error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
		ReadOnly:  false,
	})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var list []*Response
	for _, query := range queries {
		res, err := Exec(ctx, tx, query.Sql, query.Params)
		if err != nil {
			return nil, err
		}
		list = append(list, res)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return list, nil
}

func Drop(ctx context.Context, id string) (string, error) {
	muDBs.Lock()
	defer muDBs.Unlock()
	dbConnector, ok := dbs[id]
	if !ok {
		return "", fmt.Errorf("database with id %q not found", id)
	}
	var filename string
	err := dbConnector.db.QueryRowContext(ctx, "SELECT file FROM pragma_database_list WHERE name = ?", "main").Scan(&filename)
	if err != nil {
		return "", fmt.Errorf("failed to get db filename: %w", err)
	}
	if proxiedSubscription[id] != nil {
		proxiedSubscription[id].Stop()
		delete(proxiedSubscription, id)
	}
	dbConnector.connector.Close()
	delete(dbs, id)
	return filename, nil
}

func deserializeFromReader(ctx context.Context, connector driver.Connector, r io.Reader) error {
	dest, err := os.CreateTemp("", "ha-*.db")
	if err != nil {
		return err
	}
	defer os.Remove(dest.Name())

	_, err = io.Copy(dest, r)
	if err != nil {
		return err
	}
	dest.Close()

	return deserialize(ctx, connector, dest.Name())
}

func deserialize(ctx context.Context, connector driver.Connector, file string) error {
	data, err := os.ReadFile(file)
	if err != nil {
		return err
	}

	conn, err := connector.Connect(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	deserializerConn, err := deserializerConn(conn)
	if err != nil {
		return err
	}
	buf := make([]byte, len(data))
	data = append(data, buf...)
	return deserializerConn.Deserialize(data, "")
}

type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func doQuery(ctx context.Context, querier querier, query string, args map[string]any) (*Response, error) {
	slog.Warn("Query", "q", querier, "query", query, "ctx", ctx)
	rows, err := querier.QueryContext(ctx, query, getArgs(args)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	columnsCount := len(columns)
	if columnsCount == 0 {
		return nil, fmt.Errorf("zero columns")
	}

	dataRows := make([][]any, 0)
	for rows.Next() {
		values := make([]any, columnsCount)
		for i := range values {
			values[i] = &values[i]
		}
		if err := rows.Scan(values...); err != nil {
			return nil, err
		}
		dataRows = append(dataRows, values)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &Response{
		Columns: columns,
		Rows:    dataRows,
	}, nil
}

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func doExec(ctx context.Context, execer execer, query string, params map[string]any) (*Response, error) {
	args := getArgs(params)
	res, err := execer.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	rowsAffected, _ := res.RowsAffected()
	lastInsertID, _ := res.LastInsertId()

	return &Response{
		Columns:      []string{"rows_affected", "last_insert_id"},
		Rows:         [][]any{{rowsAffected, lastInsertID}},
		RowsAffected: rowsAffected,
		NoReturning:  true}, nil
}

func getArgs(params map[string]any) []any {
	if len(params) == 0 {
		return nil
	}
	for k := range params {
		if isPositional(rune(k[0])) {
			return getPositionalArgs(params)
		}
		break
	}
	var args []any = make([]any, 0, len(params))
	for k, v := range params {
		if len(k) > 1 && (strings.HasPrefix(k, "$") || (strings.HasPrefix(k, "?") ||
			strings.HasPrefix(k, ":") || strings.HasPrefix(k, "@"))) {
			k = k[1:]
		}
		args = append(args, sql.Named(k, v))
	}
	return args
}

func isPositional(r rune) bool {
	return r == '$'
}

func getPositionalArgs(params map[string]any) []any {
	total := len(params)
	args := make([]any, total)
	for i := range total {
		args[i] = params[fmt.Sprintf("$%d", i+1)]
	}
	return args
}

type deserializer interface {
	Deserialize(b []byte, schema string) error
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
	return filename
}

func IdFromDSN(dsn string) string {
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
