package cli

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/lipgloss/table"
	"github.com/knz/bubbline"
	"github.com/knz/bubbline/history"
	haconnect "github.com/litesql/go-ha/connect"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	sqlv1 "github.com/litesql/go-ha/api/sql/v1"
	"github.com/litesql/ha/internal/sqlite"
)

var (
	reSetDatabase    = regexp.MustCompile(`(?i)^SET\s+DATABASE\s*(=|TO)\s*([^;\s]+)`)
	reCreateDatabase = regexp.MustCompile(`(?i)^CREATE\s+DATABASE\s+([^;\s]+)`)
	reDropDatabase   = regexp.MustCompile(`(?i)^DROP\s+DATABASE\s+([^;\s]+)`)
)

var (
	white     = lipgloss.Color("255")
	gray      = lipgloss.Color("245")
	lightGray = lipgloss.Color("251")

	headerStyle  = lipgloss.NewStyle().Foreground(white).Bold(true).Align(lipgloss.Center)
	cellStyle    = lipgloss.NewStyle().Padding(0, 1)
	oddRowStyle  = cellStyle.Foreground(gray)
	evenRowStyle = cellStyle.Foreground(lightGray)
)

func Start(remote string, token string, enableOtel bool) {
	httpClient := http.DefaultClient
	if enableOtel {
		httpClient = &http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}
	}
	remote = strings.TrimSpace(remote)
	if !strings.Contains(remote, "://") {
		remote = "http://" + remote
	}
	remote = strings.TrimRight(remote, "/")
	u, err := url.Parse(remote)
	if err != nil {
		slog.Error("parse url", "error", err)
		return
	}

	var dialOpts []grpc.DialOption

	if strings.HasPrefix(remote, "http://") {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
	}
	if token != "" {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(grpcCredentials{token: token}))
	}
	if enableOtel {
		dialOpts = append(dialOpts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	}

	cc, err := grpc.NewClient(u.Host, dialOpts...)
	if err != nil {
		slog.Error("grpc connect", "error", err)
		return
	}
	defer cc.Close()

	client := sqlv1.NewDatabaseServiceClient(cc)
	fmt.Println("Connecting to", u.String(), "(Ctrl+D to exit)")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reqChan := make(chan *cliRequest, 1)
	go maintainQueryStream(ctx, client, reqChan)

	m := bubbline.New()
	defer m.Close()

	historyPath := cliHistoryPath()
	h, _ := history.LoadHistory(historyPath)
	m.SetHistory(h)

	var (
		command       string
		replicationID string
	)
	for {
		if command == "" {
			m.Prompt = fmt.Sprintf("%s> ", replicationID)
		} else {
			m.Prompt = ""
		}

		line, err := m.GetLine()
		if err != nil {
			if err == io.EOF {
				return
			}
			if errors.Is(err, bubbline.ErrInterrupted) {
				// Entered Ctrl+C to cancel input.
				fmt.Println("^C")
				command = ""
				continue
			} else if errors.Is(err, bubbline.ErrTerminated) {
				fmt.Println("terminated")
				return
			} else {
				fmt.Println("error:", err)
			}
			continue
		}
		command += line
		command = strings.TrimSpace(command)
		if !strings.HasSuffix(command, ";") {
			command += "\n"
			continue
		}

		m.AddHistoryEntry(command)
		history.SaveHistory(m.GetHistory(), historyPath)

		if strings.HasPrefix(strings.ToUpper(command), "EXIT") {
			return
		}

		if strings.HasPrefix(strings.ToUpper(command), "SHOW DATABASES") {
			command = ""
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			resp, err := client.ReplicationIDs(ctx, &sqlv1.ReplicationIDsRequest{})
			cancel()
			if err != nil {
				slog.Error("databases", "error", err)
				continue
			}
			t := table.New().
				Border(lipgloss.NormalBorder()).
				BorderStyle(lipgloss.NewStyle().Foreground(white)).
				StyleFunc(func(row, col int) lipgloss.Style {
					switch {
					case row == table.HeaderRow:
						return headerStyle
					case row%2 == 0:
						return evenRowStyle
					default:
						return oddRowStyle
					}
				}).Headers("Databases")
			for _, row := range resp.ReplicationId {
				t.Row(row)
			}
			fmt.Println(t.Render())
			continue
		}

		if match := reCreateDatabase.FindStringSubmatch(command); len(match) == 2 {
			dsn := strings.Trim(strings.TrimRight(match[1], "; \t\r\n"), "`'\"")
			command = ""
			err := createDatabase(httpClient, remote, token, dsn)
			if err != nil {
				fmt.Println("create database error:", err)
			} else {
				fmt.Println("database created")
			}
			continue
		}

		if match := reDropDatabase.FindStringSubmatch(command); len(match) == 2 {
			id := strings.Trim(strings.TrimRight(match[1], "; \t\r\n"), "`'\"")
			command = ""
			err := dropDatabase(httpClient, remote, token, id)
			if err != nil {
				fmt.Println("drop database error:", err)
			} else {
				fmt.Println("database dropped")
			}
			continue
		}

		if match := reSetDatabase.FindStringSubmatch(command); len(match) == 3 {
			replicationID = strings.Trim(strings.TrimRight(match[2], "; \t\r\n"), "`'\"")
			command = ""
			continue
		}

		if strings.HasPrefix(strings.ToUpper(command), "UNSET DATABASE") {
			replicationID = ""
			command = ""
			continue
		}

		start := time.Now()
		response := make(chan queryResult, 1)
		reqChan <- &cliRequest{
			request: &sqlv1.QueryRequest{
				Sql:           strings.TrimSuffix(command, ";"),
				ReplicationId: replicationID,
				Type:          queryTypeFromSQL(command),
			},
			response: response,
		}
		command = ""
		result := <-response
		if result.err != nil {
			fmt.Println("query failed; reconnecting:", result.err)
			continue
		}
		resp := result.response
		if resp.Error != "" {
			fmt.Println(resp.Error)
			continue
		}
		if resp.ResultSet != nil {
			if len(resp.ResultSet.Columns) == 2 && resp.ResultSet.Columns[0] == "rows_affected" && len(resp.ResultSet.Rows) == 1 {
				fmt.Printf("Query OK, %d rows affected (%s)\n", resp.RowsAffected, time.Since(start))
				continue
			}
			t := table.New().
				Border(lipgloss.NormalBorder()).
				BorderStyle(lipgloss.NewStyle().Foreground(white)).
				StyleFunc(func(row, col int) lipgloss.Style {
					switch {
					case row == table.HeaderRow:
						return headerStyle
					case row%2 == 0:
						return evenRowStyle
					default:
						return oddRowStyle
					}
				}).Headers(resp.ResultSet.Columns...)
			for _, row := range resp.ResultSet.Rows {
				var cells []string
				for _, val := range row.Values {
					v := haconnect.FromAnypb(val)
					if v == nil {
						cells = append(cells, "")
						continue
					}
					cells = append(cells, fmt.Sprint(v))
				}
				t.Row(cells...)
			}
			fmt.Println(t.Render())
		}
		if resp.RowsAffected == 0 {
			fmt.Printf("Query OK (%s)\n", time.Since(start))
			continue
		}
		fmt.Printf("Query OK, %d rows affected (%s)\n", resp.RowsAffected, time.Since(start))
	}
}

type cliRequest struct {
	request  *sqlv1.QueryRequest
	response chan queryResult
}

type queryResult struct {
	response *sqlv1.QueryResponse
	err      error
}

type queryStream interface {
	Send(*sqlv1.QueryRequest) error
	Recv() (*sqlv1.QueryResponse, error)
}

type receivedQueryResponse struct {
	response *sqlv1.QueryResponse
	err      error
}

func maintainQueryStream(ctx context.Context, client sqlv1.DatabaseServiceClient, requests <-chan *cliRequest) {
	retryDelay := time.Second
	for {
		streamCtx, cancel := context.WithCancel(ctx)
		stream, err := client.Query(streamCtx)
		if err == nil {
			slog.Info("connected to remote server")
			retryDelay = time.Second
			err = serveQueryStream(streamCtx, stream, requests)
		}
		cancel()

		if ctx.Err() != nil {
			return
		}
		slog.Warn("query stream disconnected; reconnecting", "error", err, "retry_in", retryDelay)
		timer := time.NewTimer(retryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if retryDelay < 30*time.Second {
			retryDelay *= 2
			if retryDelay > 30*time.Second {
				retryDelay = 30 * time.Second
			}
		}
	}
}

func serveQueryStream(ctx context.Context, stream queryStream, requests <-chan *cliRequest) error {
	received := make(chan receivedQueryResponse, 1)
	go func() {
		for {
			response, err := stream.Recv()
			select {
			case received <- receivedQueryResponse{response: response, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	heartbeat := time.NewTimer(25 * time.Second)
	defer heartbeat.Stop()

	var pending *cliRequest
	pingPending := false
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case request := <-requests:
			if pending != nil {
				request.response <- queryResult{err: errors.New("another query is still in progress")}
				continue
			}
			pending = request
			if err := stream.Send(request.request); err != nil {
				pending.response <- queryResult{err: err}
				return err
			}
		case response := <-received:
			if response.err != nil {
				if pending != nil {
					pending.response <- queryResult{err: response.err}
				}
				return response.err
			}
			if pending != nil {
				pending.response <- queryResult{response: response.response}
				pending = nil
				heartbeat.Reset(25 * time.Second)
			} else if pingPending {
				pingPending = false
				heartbeat.Reset(25 * time.Second)
			} else {
				slog.Warn("received an unexpected response from remote server")
			}
		case <-heartbeat.C:
			if pingPending {
				return errors.New("query stream heartbeat timed out")
			}
			if pending == nil {
				if err := stream.Send(&sqlv1.QueryRequest{Type: sqlv1.QueryType_QUERY_TYPE_PING}); err != nil {
					return err
				}
				pingPending = true
			}
			heartbeat.Reset(25 * time.Second)
		}
	}
}

type grpcCredentials struct {
	token string
}

func (c grpcCredentials) GetRequestMetadata(ctx context.Context, in ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": c.token,
	}, nil
}

func (c grpcCredentials) RequireTransportSecurity() bool {
	return false
}

func createDatabase(httpClient *http.Client, baseURL string, token string, dsn string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var payload struct {
		DSN string `json:"dsn"`
	}
	payload.DSN = dsn
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/databases", strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		data, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to create database: %s, %s", resp.Status, string(data))
	}
	return nil
}

func dropDatabase(httpClient *http.Client, baseURL string, token string, id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, strings.TrimRight(baseURL, "/")+"/databases/"+url.PathEscape(id), nil)
	if err != nil {
		return err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		data, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to drop database: %s, %s", resp.Status, string(data))
	}
	return nil
}

func queryTypeFromSQL(sql string) sqlv1.QueryType {
	if sqlite.IsQuery(context.Background(), sql) {
		return sqlv1.QueryType_QUERY_TYPE_EXEC_QUERY
	}

	return sqlv1.QueryType_QUERY_TYPE_EXEC_UPDATE
}

func cliHistoryPath() string {
	if configDir, err := os.UserConfigDir(); err == nil && configDir != "" {
		haDir := filepath.Join(configDir, "ha")
		if err := os.MkdirAll(haDir, 0o700); err == nil {
			return filepath.Join(haDir, "cli.history")
		}
	}
	if homeDir, err := os.UserHomeDir(); err == nil && homeDir != "" {
		return filepath.Join(homeDir, ".ha_cli_history")
	}
	return filepath.Join(os.TempDir(), "ha_cli.history")
}
