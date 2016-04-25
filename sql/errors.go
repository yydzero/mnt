package sql

import "errors"

const (
	// PG error codes from:
	// http://www.postgresql.org/docs/9.5/static/errcodes-appendix.html

	// CodeUniquenessConstraintViolationError represents violations of uniqueness
	// constraints.
	CodeUniquenessConstraintViolationError string = "23505"
	// CodeTransactionAbortedError signals that the user tried to execute a
	// statement in the context of a SQL txn that's already aborted.
	CodeTransactionAbortedError string = "25P02"
	// CodeInternalError represents all internal cockroach errors, plus acts
	// as a catch-all for random errors for which we haven't implemented the
	// appropriate error code.
	CodeInternalError string = "XX000"

	// extensions:

	// CodeRetriableError signals to the user that the SQL txn entered the
	// RESTART_WAIT state and that a RESTART statement should be issued.
	CodeRetriableError string = "CR000"
	// CodeTransactionCommittedError signals that the SQL txn is in the
	// COMMIT_WAIT state and a COMMIT statement should be issued.
	CodeTransactionCommittedError string = "CR001"
)

var errNoTransactionInProgress = errors.New("there is no transaction in progress")
var errStaleMetadata = errors.New("metadata is still stale")
var errTransactionInProgress = errors.New("there is already a transaction in progress")
var errNotRetriable = errors.New("the transaction is not in a retriable state")
