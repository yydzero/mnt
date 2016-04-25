package executor

import "github.com/yydzero/mnt/parser"

type StatementType int

const (
	// Ack indicates that the statement does not have a meaningful return.
	// Eg: SET, BEGIN, COMMIT.
	Ack StatementType = iota

	// DDL indicates that the statement mutates the database schema.
	DDL

	// RowsAffected indicates that the statement returns the count of
	// affected rows.
	RowsAffected

	// Rows indicates that the statement returns the affected rows after the
	// statement was applied.
	Rows
)

// ResultList represents a list of results for a list of SQL statements.
// There is one result object per SQL statement in the request.
type ResultList []Result

// StatementResults represents a list of results from running a batch of
// SQL statements, plus some meta info about the batch
type StatementResults struct {
	ResultList

	// Indicates that after parsing, the request contained 0 non-empty statements.
	Empty bool
}

// Result corresponds to the execution of a single SQL statement.
type Result struct {
	Err          error
	Type         StatementType 		  // The type of statement that the result is for
	PGTag        string               // The tag of the statement that the result is for
	RowsAffected int                  // RowsAffected will be populated if the statement type is RowsAffected.

									  // Columns will be populated if the statement type is "Rows". It will contain
									  // the names and types of the columns returned in the result set in the order
									  // specified in the SQL statement. The number for columns will equal the number
									  // of values in each Row.
	Columns []ResultColumn

									  // Rows will be populated if the statement type is Rows. It will contain
									  // the result set of the result.
									  // TODO: streaming?
	Rows []ResultRow
}

// ResultColumn contains the name and type of a SQL column
type ResultColumn struct {
	Name string
	Typ  parser.Datum

	hidden bool // If set, this is an implicit column; used internally
}

// ResultRow is a collection of values representing one row in a result
type ResultRow struct {
	Values []parser.Datum
}

