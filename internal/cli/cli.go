package cli

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/lipgloss/table"
	"github.com/knz/bubbline"
	"github.com/knz/bubbline/history"
	haconnect "github.com/litesql/go-ha/connect"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	sqlv1 "github.com/litesql/go-ha/api/sql/v1"
)

func Start(remote string) {
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
	if u.User != nil {
		token, _ := u.User.Password()
		if token != "" {
			dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(grpcCredentials{token: token}))
		}
	}

	cc, err := grpc.NewClient(u.Host, dialOpts...)
	if err != nil {
		slog.Error("grpc connect", "error", err)
		return
	}
	client := sqlv1.NewDatabaseServiceClient(cc)
	stream, err := client.Query(context.Background())
	if err != nil {
		slog.Error("stream", "error", err)
		return
	}
	defer stream.CloseSend()

	fmt.Println("Connected to", u.String(), "(Ctrl+D to exit)")

	respChan := make(chan *sqlv1.QueryResponse)

	go func() {
		for {
			res, err := stream.Recv()
			if err != nil {
				slog.Error(err.Error())
				os.Exit(-1)
			}
			respChan <- res
		}

	}()

	var (
		white     = lipgloss.Color("255")
		gray      = lipgloss.Color("245")
		lightGray = lipgloss.Color("251")

		headerStyle  = lipgloss.NewStyle().Foreground(white).Bold(true).Align(lipgloss.Center)
		cellStyle    = lipgloss.NewStyle().Padding(0, 1)
		oddRowStyle  = cellStyle.Foreground(gray)
		evenRowStyle = cellStyle.Foreground(lightGray)
	)

	m := bubbline.New()

	historyPath := filepath.Join(os.TempDir(), "ha_cli.history")
	h, _ := history.LoadHistory(historyPath)
	m.SetHistory(h)

	var command string
	for {
		fmt.Print("> ")

		line, err := m.GetLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			if errors.Is(err, bubbline.ErrInterrupted) {
				// Entered Ctrl+C to cancel input.
				fmt.Println("^C")
			} else if errors.Is(err, bubbline.ErrTerminated) {
				fmt.Println("terminated")
				break
			} else {
				fmt.Println("error:", err)
			}
			continue
		}

		command += line
		if !strings.HasSuffix(strings.TrimSpace(command), ";") {
			command += "\n"
			continue
		}
		m.AddHistoryEntry(strings.TrimSpace(command))
		history.SaveHistory(m.GetHistory(), historyPath)
		err = stream.Send(&sqlv1.QueryRequest{
			Sql: command,
		})
		if err != nil {
			slog.Error("send", "error", err)
			return
		}
		command = ""

		resp := <-respChan
		if resp.Error != "" {
			fmt.Println(resp.Error)
			continue
		}
		if resp.ResultSet != nil {
			if len(resp.ResultSet.Columns) == 2 && resp.ResultSet.Columns[0] == "rows_affected" && len(resp.ResultSet.Rows) == 1 {
				fmt.Printf("%d rows affected\n", resp.RowsAffected)
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
					cells = append(cells, fmt.Sprint(haconnect.FromAnypb(val)))
				}
				t.Row(cells...)
			}
			fmt.Println(t.Render())
		}
		if resp.RowsAffected == 0 {
			continue
		}
		fmt.Printf("%d rows affected\n", resp.RowsAffected)
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
