package executor

import (
	"github.com/yydzero/mnt/parser"
	"golang.org/x/net/context"
)

// An Executor executes SQL statements.
// Executor should be thread-safe
type Executor interface {
	Prepare(ctx context.Context, query string, args parser.MapArgs) ([]ResultColumn, parser.MapArgs, error)
	ExecuteStatements(ctx context.Context, stmts string, params []parser.Datum) StatementResults
}
