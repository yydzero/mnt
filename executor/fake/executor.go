package fake

import (
	"github.com/yydzero/mnt/parser"
	"golang.org/x/net/context"
	"github.com/yydzero/mnt/executor"
)

type FakeExecutor struct {

}

func (e *FakeExecutor) Prepare(ctx context.Context, query string, args parser.MapArgs) (
	[]executor.ResultColumn, parser.MapArgs, error) {
	cols := makeFakeColumns()
	args = makeFakeArgs()
	return cols, args, nil
}

func (e *FakeExecutor) ExecuteStatements(ctx context.Context, stmts string, params []parser.Datum) (
	executor.StatementResults) {
	r := makeFakeStatementResults()
	return r
}

func makeResultColumn(name string, typ parser.Datum) executor.ResultColumn {
	return executor.ResultColumn{
		Name: name,
		Typ: typ,
	}
}

func makeFakeArgs() parser.MapArgs {
	args := make(parser.MapArgs)
	args["1"] = parser.DummyInt
	return args
}

func makeFakeColumns() []executor.ResultColumn {
	cols := make([]executor.ResultColumn, 3)
	cols[0] = makeResultColumn("name", parser.DummyString)
	cols[1] = makeResultColumn("age", parser.DummyInt)
	cols[2] = makeResultColumn("description", parser.DummyString)
	return cols
}

func makeFakeRows() []executor.ResultRow {
	rows := make([]executor.ResultRow, 3)
	rows[0] = executor.ResultRow{
		Values: []parser.Datum{parser.DString("xiaowang"), parser.DInt(32), parser.DString("SMTS")},
	}
	rows[1] = executor.ResultRow{
		Values: []parser.Datum{parser.DString("xiaozhang"), parser.DInt(26), parser.DString("MTS 2")},
	}
	rows[2] = executor.ResultRow{
		Values: []parser.Datum{parser.DString("xiaohuang"), parser.DInt(30), parser.DString("MTS 3")},
	}
	return rows
}

func makeFakeStatementResults() executor.StatementResults {
	cols := makeFakeColumns()
	rows := makeFakeRows()

	r := executor.Result{
		Type:  executor.Rows,
		PGTag: "SELECT",
		Columns: cols,
		Rows: rows,
	}

	results := executor.StatementResults{
		ResultList: executor.ResultList{r},
		Empty: false,
	}

	return results
}
