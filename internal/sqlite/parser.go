package sqlite

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/antlr4-go/antlr/v4"
	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/litesql/ha/internal/sqlite/parser"
)

var (
	ErrInvalidSQL = fmt.Errorf("invalid SQL")
	cache, _      = lru.New[string, *Statement](256)
)

const (
	TypeExplain            = "EXPLAIN"
	TypeSelect             = "SELECT"
	TypeInsert             = "INSERT"
	TypeUpdate             = "UPDATE"
	TypeDelete             = "DELETE"
	TypeCreateTable        = "CREATE TABLE"
	TypeCreateIndex        = "CREATE INDEX"
	TypeCreateView         = "CREATE VIEW"
	TypeCreateTrigger      = "CREATE TRIGGER"
	TypeCreateVirtualTable = "CREATE VIRTUAL TABLE"
	TypeAlterTable         = "ALTER TABLE"
	TypeVacuum             = "VACUUM"
	TypeDrop               = "DROP"
	TypeAnalyze            = "ANALYZE"
	TypeBegin              = "BEGIN"
	TypeCommit             = "COMMIT"
	TypeRollback           = "ROLLBACK"
	TypeSavepoint          = "SAVEPOINT"
	TypeRelease            = "RELEASE"
	TypeOther              = "OTHER"
)

type Statement struct {
	source         string
	hasDistinct    bool
	hasReturning   bool
	typ            string
	parameters     []string
	columns        []string
	ddl            bool
	statementCount int
}

func NewStatement(ctx context.Context, sql string) (*Statement, error) {
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sql)), "BEGIN") {
		return &Statement{
			source: sql,
			typ:    TypeBegin,
		}, nil
	}
	stmt := new(Statement)
	stmt.source = sql
	err := stmt.parse(ctx)
	if err != nil {
		return nil, err
	}
	if stmt.statementCount > 1 {
		return nil, fmt.Errorf("multiple SQL statements are not allowed: %w", ErrInvalidSQL)
	}
	cache.Add(sql, stmt)
	return stmt, nil
}

func (s *Statement) Source() string {
	return s.source
}

func (s *Statement) Type() string {
	return s.typ
}

func (s *Statement) HasDistinct() bool {
	return s.hasDistinct
}

func (s *Statement) Parameters() []string {
	return s.parameters
}

func (s *Statement) Columns() []string {
	return s.columns
}

func (s *Statement) IsExplain() bool {
	return s.typ == TypeExplain
}

func (s *Statement) HasReturning() bool {
	return s.hasReturning
}

func (s *Statement) IsSelect() bool {
	return s.typ == TypeSelect
}

func (s *Statement) IsInsert() bool {
	return s.typ == TypeInsert
}

func (s *Statement) IsUpdate() bool {
	return s.typ == TypeUpdate
}

func (s *Statement) IsDelete() bool {
	return s.typ == TypeDelete
}

func (s *Statement) IsCreateTable() bool {
	return s.typ == TypeCreateTable
}

func (s *Statement) DDL() bool {
	return s.ddl
}

func (s *Statement) Begin() bool {
	return s.typ == TypeBegin
}

func (s *Statement) Commit() bool {
	return s.typ == TypeCommit
}

func (s *Statement) Rollback() bool {
	return s.typ == TypeRollback
}

func (s *Statement) parse(ctx context.Context) error {
	slog.InfoContext(ctx, "Parse", "sql", s.source)
	input := antlr.NewInputStream(s.source)
	lexer := parser.NewSQLiteLexer(input)
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	p := parser.NewSQLiteParser(stream)
	p.RemoveErrorListeners()
	p.AddParseListener(&sqlListener{
		statement: s,
	})
	var errorListener errorListener
	p.AddErrorListener(&errorListener)
	p.Parse()
	if errorListener.err != nil {
		slog.ErrorContext(ctx, "Parse error", "error", errorListener.err, "sql", s.source)
		return errorListener.err
	}

	return nil
}

type errorListener struct {
	antlr.DefaultErrorListener
	err error
}

func (d *errorListener) SyntaxError(_ antlr.Recognizer, _ any, line, column int, msg string, _ antlr.RecognitionException) {
	if msg != "" {
		d.err = fmt.Errorf("%d:%d: %s: %w", line, column, msg, ErrInvalidSQL)
	}
}

type sqlListener struct {
	parser.BaseSQLiteParserListener
	statement *Statement
}

func (s *sqlListener) ExitExpr(ctx *parser.ExprContext) {
	if ctx.BIND_PARAMETER() != nil {
		if !slices.Contains(s.statement.parameters, ctx.GetText()) || ctx.GetText() == "?" {
			s.statement.parameters = append(s.statement.parameters, ctx.GetText())
		}
	}
}

func (s *sqlListener) ExitSelect_stmt(c *parser.Select_stmtContext) {
	if c.AllSelect_core() != nil && len(c.AllSelect_core()) > 0 {
		mainSelect := c.AllSelect_core()[0]
		if mainSelect.DISTINCT_() != nil {
			s.statement.hasDistinct = true
		} else {
			s.statement.hasDistinct = false
		}

		s.statement.columns = make([]string, 0)
		for _, col := range mainSelect.AllResult_column() {
			if col.Column_alias() != nil {
				s.statement.columns = append(s.statement.columns, col.Column_alias().GetText())
				continue
			}
			s.statement.columns = append(s.statement.columns, col.GetText())
		}
	}
}

func (s *sqlListener) ExitSql_stmt(c *parser.Sql_stmtContext) {
	s.statement.statementCount++
	if c.EXPLAIN_() != nil {
		s.statement.typ = TypeExplain
		s.statement.columns = []string{"id", "parent", "notused", "detail"}
		return
	}
	switch {
	case c.Select_stmt() != nil:
		s.statement.typ = TypeSelect
	case c.Insert_stmt() != nil:
		s.statement.typ = TypeInsert
		s.statement.hasReturning = c.Insert_stmt().Returning_clause() != nil
	case c.Update_stmt() != nil:
		s.statement.typ = TypeUpdate
		s.statement.hasReturning = c.Update_stmt().Returning_clause() != nil
	case c.Delete_stmt() != nil:
		s.statement.typ = TypeDelete
		s.statement.hasReturning = c.Delete_stmt().Returning_clause() != nil
	case c.Begin_stmt() != nil:
		s.statement.typ = TypeBegin
	case c.Commit_stmt() != nil:
		s.statement.typ = TypeCommit
	case c.Rollback_stmt() != nil:
		s.statement.typ = TypeRollback
	case c.Create_table_stmt() != nil:
		s.statement.typ = TypeCreateTable
	case c.Create_index_stmt() != nil:
		s.statement.typ = TypeCreateIndex
	case c.Create_trigger_stmt() != nil:
		s.statement.typ = TypeCreateTrigger
	case c.Create_view_stmt() != nil:
		s.statement.typ = TypeCreateView
	case c.Create_virtual_table_stmt() != nil:
		s.statement.typ = TypeCreateVirtualTable
	case c.Alter_table_stmt() != nil:
		s.statement.typ = TypeAlterTable
	case c.Vacuum_stmt() != nil:
		s.statement.typ = TypeVacuum
	case c.Drop_stmt() != nil:
		s.statement.typ = TypeDrop
	case c.Analyze_stmt() != nil:
		s.statement.typ = TypeAnalyze
	case c.Savepoint_stmt() != nil:
		s.statement.typ = TypeSavepoint
	case c.Release_stmt() != nil:
		s.statement.typ = TypeRelease
	default:
		s.statement.typ = TypeOther
	}
}

func (s *sqlListener) ExitReturning_clause(ctx *parser.Returning_clauseContext) {
	if len(ctx.AllResult_column()) > 0 {
		s.statement.columns = make([]string, 0)
	}
	for _, col := range ctx.AllResult_column() {
		if col.Column_alias() != nil {
			s.statement.columns = append(s.statement.columns, col.Column_alias().GetText())
			continue
		}
		s.statement.columns = append(s.statement.columns, col.GetText())
	}
}

func (s *sqlListener) ExitCreate_table_stmt(ctx *parser.Create_table_stmtContext) {
	s.statement.ddl = true
}

func (s *sqlListener) ExitCreate_index_stmt(ctx *parser.Create_index_stmtContext) {
	s.statement.ddl = true
}

func (s *sqlListener) ExitCreate_trigger_stmt(ctx *parser.Create_trigger_stmtContext) {
	s.statement.ddl = true
}

func (s *sqlListener) ExitCreate_view_stmt(ctx *parser.Create_view_stmtContext) {
	s.statement.ddl = true
}

func (s *sqlListener) ExitCreate_virtual_table_stmt(ctx *parser.Create_virtual_table_stmtContext) {
	s.statement.ddl = true
}

func (s *sqlListener) ExitAlter_table_stmt(ctx *parser.Alter_table_stmtContext) {
	s.statement.ddl = true
}

func (s *sqlListener) ExitDrop_stmt(ctx *parser.Drop_stmtContext) {
	s.statement.ddl = true
}
