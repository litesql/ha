package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/litesql/go-ha"
	haconnect "github.com/litesql/go-ha/connect"

	"github.com/litesql/ha/internal/sqlite"
)

const maxRequestBodyBytes = 10 << 20 // 10 MiB

func writeDBError(w http.ResponseWriter, msg string, err error) {
	status := http.StatusInternalServerError
	if errors.Is(err, sqlite.ErrDatabaseNotFound) {
		status = http.StatusNotFound
	}
	if msg != "" {
		http.Error(w, fmt.Sprintf("%s: %v", msg, err), status)
		return
	}
	http.Error(w, err.Error(), status)
}

func ReadyHandler(w http.ResponseWriter, r *http.Request) {
	db, err := sqlite.DB("")
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := db.QueryRowContext(ctx, "SELECT 1").Scan(new(int)); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func DatabasesHandler(w http.ResponseWriter, r *http.Request) {
	dbs := sqlite.Databases()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string][]string{
		"databases": dbs,
	})
}

func CreateDatabaseHandler(createDatabaseDir string, memDB bool, defaultDSNOpts string, fromLatestSnapshot bool, deliverPolicy string, maxConns int, proxiedConfig sqlite.ProxiedDBConfig, opts ...ha.Option) http.HandlerFunc {
	type request struct {
		DSN string `json:"dsn"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
		var req request
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.DSN == "" {
			http.Error(w, "DSN is required", http.StatusBadRequest)
			return
		}
		if !memDB && createDatabaseDir == "" {
			http.Error(w, "create database is disabled, inform flag --create-db-dir at startup", http.StatusInternalServerError)
			return
		}

		cfg := sqlite.LoadConfig{
			Dir:                createDatabaseDir,
			MemDB:              memDB,
			DefaultDSNParams:   defaultDSNOpts,
			FromLatestSnapshot: fromLatestSnapshot,
			DeliverPolicy:      deliverPolicy,
			MaxConns:           maxConns,
			ProxiedDBConfig:    proxiedConfig,
			Options:            opts,
		}
		dsn, err := sqlite.BuildCreateDSN(req.DSN, cfg)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		err = sqlite.Load(r.Context(), dsn, cfg)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
	}
}

func DropDatabaseHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		dbfile, err := sqlite.Drop(r.Context(), id)
		if err != nil {
			writeDBError(w, "", err)
			return
		}
		if dbfile != "" {
			err = os.Remove(dbfile)
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				http.Error(w, fmt.Sprintf("failed to remove database file: %v", err), http.StatusInternalServerError)
				return
			}
			os.Remove(dbfile + "-shm")
			os.Remove(dbfile + "-wal")
		}
	}
}

type QueriesRequest struct {
	Queries []sqlite.Request
	slice   bool
}

func (r *QueriesRequest) UnmarshalJSON(b []byte) error {
	if len(b) == 0 {
		return fmt.Errorf("empty")
	}
	switch b[0] {
	case '{':
		var query sqlite.Request
		err := json.Unmarshal(b, &query)
		if err != nil {
			return err
		}
		r.Queries = []sqlite.Request{
			query,
		}
	case '[':
		r.slice = true
		return json.Unmarshal(b, &r.Queries)
	}
	return nil
}

func QueryHandler(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	var req QueriesRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if len(req.Queries) == 0 {
		http.Error(w, "no queries found", http.StatusBadRequest)
		return
	}
	dbID := r.PathValue("id")
	db, err := sqlite.DB(dbID)
	if err != nil {
		writeDBError(w, "", err)
		return
	}

	ctx := r.Context()
	if r.URL.Query().Get("local") == "true" {
		ctx = ha.ContextLocalDB(ctx, true)
	}

	if len(req.Queries) == 1 {
		res, err := sqlite.Exec(ctx, db, req.Queries[0].Sql, req.Queries[0].Params)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if !req.slice {
			json.NewEncoder(w).Encode(res)
			return
		}
		json.NewEncoder(w).Encode(map[string][]*sqlite.Response{
			"results": {res},
		})
		return
	}

	res, err := sqlite.Transaction(ctx, db, req.Queries)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string][]*sqlite.Response{
		"results": res,
	})
}

func UndoHandler(undoType haconnect.UndoFilter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		dbID := r.PathValue("id")
		c, err := sqlite.Connector(dbID)
		if err != nil {
			writeDBError(w, "", err)
			return
		}

		param := strings.TrimSpace(r.PathValue("param"))
		param = strings.TrimRight(param, "; \t\r\n")
		param = strings.Trim(param, "`'\"")
		seq, err := strconv.Atoi(param)
		if err != nil {
			duration, err := time.ParseDuration(param)
			if err != nil {
				http.Error(w, fmt.Sprintf("invalid param: %v", err), http.StatusBadRequest)
				return
			}
			if duration <= 0 {
				http.Error(w, "duration must be greater than 0", http.StatusBadRequest)
				return
			}
			err = c.UndoByTime(r.Context(), duration, undoType, nil)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			return
		}
		if seq < 0 {
			http.Error(w, "sequence must be a non-negative integer", http.StatusBadRequest)
			return
		}
		err = c.UndoBySeq(r.Context(), uint64(seq), undoType, nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

func HistoryHandler(w http.ResponseWriter, r *http.Request) {
	dbID := r.PathValue("id")
	c, err := sqlite.Connector(dbID)
	if err != nil {
		writeDBError(w, "", err)
		return
	}

	param := strings.TrimSpace(r.PathValue("param"))
	param = strings.TrimRight(param, "; \t\r\n")
	param = strings.Trim(param, "`'\"")
	var items []haconnect.HistoryItem
	seq, err := strconv.Atoi(param)
	if err != nil {
		duration, err := time.ParseDuration(param)
		if err != nil {
			http.Error(w, fmt.Sprintf("invalid param: %v", err), http.StatusBadRequest)
			return
		}
		if duration <= 0 {
			http.Error(w, "duration must be greater than 0", http.StatusBadRequest)
			return
		}
		items, err = c.HistoryByTime(r.Context(), duration)
		if err != nil {
			http.Error(w, fmt.Sprintf("history retrieval failed: %v", err), http.StatusInternalServerError)
			return
		}

	} else {
		if seq < 0 {
			http.Error(w, "sequence must be a non-negative integer", http.StatusBadRequest)
			return
		}
		items, err = c.HistoryBySeq(r.Context(), uint64(seq))
		if err != nil {
			http.Error(w, fmt.Sprintf("history retrieval failed: %v", err), http.StatusInternalServerError)
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"history": items,
	})
}

func DownloadHandler(w http.ResponseWriter, r *http.Request) {
	dbID := r.PathValue("id")
	db, err := sqlite.DB(dbID)
	if err != nil {
		writeDBError(w, "", err)
		return
	}
	filename := fmt.Sprintf("%s_ha.db", time.Now().UTC().Format(time.DateTime))
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", filename))
	w.Header().Set("Content-Type", "application/octet-stream")
	err = sqlite.Backup(r.Context(), db, w)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func TakeSnapshotHandler(w http.ResponseWriter, r *http.Request) {
	dbID := r.PathValue("id")
	connector, err := sqlite.Connector(dbID)
	if err != nil {
		slog.Error("get connector", "error", err)
		writeDBError(w, "failed to get connector", err)
		return
	}
	sequence, err := connector.TakeSnapshot(r.Context())
	if err != nil {
		slog.Error("take snapshot", "error", err)
		http.Error(w, fmt.Sprintf("failed to take snapshot: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("X-Sequence", fmt.Sprint(sequence))
	w.WriteHeader(http.StatusOK)
}

func DownloadSnapshotHandler(w http.ResponseWriter, r *http.Request) {
	dbID := r.PathValue("id")
	connector, err := sqlite.Connector(dbID)
	if err != nil {
		slog.Error("get connector", "error", err)
		writeDBError(w, "failed to get connector", err)
		return
	}
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
}

func ReplicationsHandler(w http.ResponseWriter, r *http.Request) {
	dbID := r.PathValue("id")
	connector, err := sqlite.Connector(dbID)
	if err != nil {
		slog.Error("get connector", "error", err)
		writeDBError(w, "failed to get connector", err)
		return
	}
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
}

func DeleteReplicationHandler(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}
	dbID := r.PathValue("id")
	connector, err := sqlite.Connector(dbID)
	if err != nil {
		slog.Error("get connector", "error", err)
		writeDBError(w, "failed to get connector", err)
		return
	}
	err = connector.RemoveConsumer(r.Context(), name)
	if err != nil {
		slog.Error("failed remove consumer", "error", err, "name", name)
		http.Error(w, fmt.Sprintf("failed to remove consumer: %v", err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
