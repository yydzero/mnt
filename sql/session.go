package sql

import (
	"github.com/yydzero/mnt/executor"
	"log"
	"net"
)

// connstr for libpq connection
type ConnectionArgs struct {
	Database       string
	User           string
	ClientEncoding string
	DateStyle      string
}

// Session contains the state of a SQL client connection.
type Session struct {
	Database string
	User     string

	TxnState txnState
}

type TxnStateEnum int

const (
	Idle TxnStateEnum = iota
	Open
	Aborted
)

// txnState contains state associated with an ongoing SQL txn.
type txnState struct {
	State TxnStateEnum
}

// NewSession creates and initializes new Session object. remote can be nil
func NewSession(args ConnectionArgs, e executor.Executor, remote net.Addr) *Session {
	s := Session{}
	s.Database = args.Database
	s.User = args.User

	remoteStr := ""
	if remote != nil {
		remoteStr = remote.String()
	}
	log.Printf("remote address: %q\n", remoteStr)
	return &s
}
