package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	haconnect "github.com/litesql/go-ha/connect"
	"github.com/litesql/ha/internal/sqlite"
)

func TestHandlersNotFoundAndReady(t *testing.T) {
	ctx := context.Background()
	err := sqlite.Load(ctx, "file:/http_test.db?vfs=memdb", sqlite.LoadConfig{
		MemDB:    true,
		MaxConns: 5,
	})
	if err != nil {
		t.Fatalf("failed to load memdb: %v", err)
	}
	defer sqlite.Shutdown()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /readyz", ReadyHandler)
	mux.HandleFunc("POST /databases/{id}", QueryHandler)
	mux.HandleFunc("GET /databases/{id}", DownloadHandler)
	mux.HandleFunc("GET /databases/{id}/history/{param}", HistoryHandler)
	mux.HandleFunc("POST /databases/{id}/undo/{param}", UndoHandler(haconnect.UndoFilterNone))
	mux.HandleFunc("DELETE /databases/{id}", DropDatabaseHandler())

	t.Run("readyz returns 200 OK when default db is loaded", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("missing database returns 404 Not Found", func(t *testing.T) {
		cases := []struct {
			method string
			path   string
			body   string
		}{
			{http.MethodPost, "/databases/unknown.db", `{"sql":"SELECT 1"}`},
			{http.MethodGet, "/databases/unknown.db", ""},
			{http.MethodGet, "/databases/unknown.db/history/1", ""},
			{http.MethodPost, "/databases/unknown.db/undo/1", ""},
			{http.MethodDelete, "/databases/unknown.db", ""},
		}
		for _, tc := range cases {
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			if rec.Code != http.StatusNotFound {
				t.Fatalf("%s %s: expected 404 Not Found, got %d (%s)", tc.method, tc.path, rec.Code, rec.Body.String())
			}
		}
	})

	t.Run("invalid JSON query payload returns 400 Bad Request", func(t *testing.T) {
		for _, body := range []string{`"just-a-string"`, `123`, `true`} {
			req := httptest.NewRequest(http.MethodPost, "/databases/http_test.db", strings.NewReader(body))
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("body %s: expected 400 Bad Request, got %d (%s)", body, rec.Code, rec.Body.String())
			}
		}
	})
}
