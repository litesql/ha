package http

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/litesql/go-ha"
	sqlite3ha "github.com/litesql/go-sqlite3-ha"
	"github.com/litesql/ha/internal/sqlite"
)

func DatabasesHandler(w http.ResponseWriter, r *http.Request) {
	dbs := sqlite.Databases()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string][]string{
		"databases": dbs,
	})
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if len(req.Queries) == 1 {
		stmt, err := ha.ParseStatement(r.Context(), req.Queries[0].Sql)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		res, err := sqlite.Exec(r.Context(), db, stmt, req.Queries[0].Params)
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

	res, err := sqlite.Transaction(r.Context(), db, req.Queries)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string][]*sqlite.Response{
		"results": res,
	})
}

func DownloadHandler(w http.ResponseWriter, r *http.Request) {
	dbID := r.PathValue("id")
	db, err := sqlite.DB(dbID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	filename := fmt.Sprintf("%s_ha.db", time.Now().UTC().Format(time.DateTime))
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", filename))
	w.Header().Set("Content-Type", "application/octet-stream")
	err = sqlite3ha.Backup(r.Context(), db, w)
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
		http.Error(w, fmt.Sprintf("failed to get connector: %v", err), http.StatusInternalServerError)
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
		http.Error(w, fmt.Sprintf("failed to get connector: %v", err), http.StatusInternalServerError)
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
		http.Error(w, fmt.Sprintf("failed to get connector: %v", err), http.StatusBadRequest)
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
		http.Error(w, "name is required", http.StatusInternalServerError)
		return
	}
	dbID := r.PathValue("id")
	connector, err := sqlite.Connector(dbID)
	if err != nil {
		slog.Error("get connector", "error", err)
		http.Error(w, fmt.Sprintf("failed to get connector: %v", err), http.StatusInternalServerError)
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
