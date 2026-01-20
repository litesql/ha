package mysql

import (
	"fmt"
	"log/slog"
	"net"

	"github.com/go-mysql-org/go-mysql/server"
	"github.com/litesql/go-ha"
)

type Config struct {
	Port                  int
	User                  string
	Pass                  string
	DBProvider            DBProvider
	CreateDatabaseOptions CreateDatabaseOptions
}

type CreateDatabaseOptions struct {
	Dir                string
	MemDB              bool
	FromLatestSnapshot bool
	DeliverPolicy      string
	MaxConns           int
	Opts               []ha.Option
}

type Server struct {
	DBProvider DBProvider
	Port       int
	User       string
	Pass       string

	createDatabaseOptions CreateDatabaseOptions
	listener              net.Listener
	closed                bool
}

func NewServer(cfg Config) (*Server, error) {
	return &Server{
		DBProvider:            cfg.DBProvider,
		Port:                  cfg.Port,
		User:                  cfg.User,
		Pass:                  cfg.Pass,
		createDatabaseOptions: cfg.CreateDatabaseOptions,
	}, nil
}

func (s *Server) ListenAndServe() error {
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
	if err != nil {
		return err
	}
	s.listener = l

	go func() {
		slog.Info("MySQL server listening", "port", s.Port)
		mysqlServer := server.NewDefaultServer()
		for {
			c, err := l.Accept()
			if err != nil {
				if s.closed {
					return
				}
				slog.Error("Accept conn", "error", err)
				continue
			}

			go func(c net.Conn) {
				defer c.Close()

				slog.Debug("New mysql connection", "remote", c.RemoteAddr().String())
				slog.Info("MySQL user/pass", "user", s.User, "pass", s.Pass)
				conn, err := mysqlServer.NewConn(c, s.User, s.Pass, &Handler{
					provider:              s.DBProvider,
					createDatabaseOptions: s.createDatabaseOptions,
				})
				if err != nil {
					slog.Error("New conn", "error", err)
					return
				}
				for {
					if err := conn.HandleCommand(); err != nil {
						slog.Error("HandleCommand", "error", err)
						return
					}
				}
			}(c)
		}
	}()
	return nil
}

func (s *Server) Close() error {
	s.closed = true
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}
