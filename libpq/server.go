package libpq

import (
	"fmt"
	"github.com/yydzero/mnt/executor"
	"io"
	"net"
	"github.com/yydzero/mnt/executor/fake"
	"log"
)

// ErrSSLRequired is returned when a client attemps to connect to a
// secure server in clear text.
const ErrSSLRequired = "cleartext connections are not permitted"

const (
	version30  = 0x30000
	versionSSL = 0x4D2162F
	versionQE  = 0x70030000
)

var (
	sslSupported   = []byte{'S'}
	sslUnsupported = []byte{'N'}
)

// Server implements the server side of the PostgreSQL wire protocol.
type Server struct {
	executor executor.Executor
}

func NewServer() Server {
	s := Server{
		executor: &fake.FakeExecutor{},
	}
	return s
}

// IsPQConnection returns true if rd appears to be a Postgres connection.
func IsPQConnection(rd io.Reader) bool {
	var buf readBuffer
	_, err := buf.readUntypedMsg(rd)
	if err != nil {
		return false
	}

	version, err := buf.getInt32()
	if err != nil {
		return false
	}
	return version == version30 || version == versionSSL
}

// Serve serves a single connection, driving the handshake process
// and delegating to the appropriate connection type.
func (s *Server) Serve(conn net.Conn) error {
	var buf readBuffer
	_, err := buf.readUntypedMsg(conn)
	if err != nil {
		return err
	}

	log.Println("Processed startup message.")

	version, err := buf.getInt32()
	if err != nil {
		return err
	}

	log.Printf("libpq version = %d\n", version)


	if version == version30 || version == versionQE {
		sessionArgs, argsErr := parseOptions(buf.msg)

		// Make a connection regardless of argsErr. If there was an error parsing
		// the args, the connection will only be used to send a report of that error.
		pqConn := newPQConn(conn, s.executor, sessionArgs)
		defer pqConn.close()

		if argsErr != nil {
			return pqConn.sendInternalError(err.Error())
		}

		return pqConn.serve(nil)
	}

	return fmt.Errorf("unknow protocol version %d", version)
}
