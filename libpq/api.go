package libpq

import (
	"bufio"
	"fmt"
	"github.com/lib/pq/oid"
	"github.com/yydzero/mnt/executor"
	"github.com/yydzero/mnt/parser"
	"github.com/yydzero/mnt/sql"
	_ "github.com/yydzero/mnt/util/reflect"
	"golang.org/x/net/context"
	"log"
	"net"
	"reflect"
	"strconv"
)

type ClientMessageType byte
type ServerMessageType byte

// http://www.postgresql.org/docs/9.5/static/protocol-message-formats.html
const (
	// clientMsgStartup
	// clientMsgCancel
	// clientMsgSSLRequest

	ClientMsgBind        ClientMessageType = 'B'
	ClientMsgClose       ClientMessageType = 'C'
	ClientMsgDescribe    ClientMessageType = 'D'
	ClientMsgExecute     ClientMessageType = 'E'
	ClientMsgFuncCall    ClientMessageType = 'F'
	ClientMsgFlush       ClientMessageType = 'H'
	ClientMsgMPPQuery    ClientMessageType = 'M'
	ClientMsgParse       ClientMessageType = 'P'
	ClientMsgPassword    ClientMessageType = 'p'
	ClientMsgSimpleQuery ClientMessageType = 'Q'
	ClientMsgTerminate   ClientMessageType = 'X'
	ClientMsgSync        ClientMessageType = 'S'

	ServerMsgAuth                 ServerMessageType = 'R'
	ServerMsgBindComplete         ServerMessageType = '2'
	ServerMsgCommandComplete      ServerMessageType = 'C'
	ServerMsgCloseComplete        ServerMessageType = '3'
	ServerMsgDataRow              ServerMessageType = 'D'
	ServerMsgEmptyQuery           ServerMessageType = 'I'
	ServerMsgErrorResponse        ServerMessageType = 'E'
	ServerMsgFuncCallResponse     ServerMessageType = 'V'
	ServerMsgKeyData              ServerMessageType = 'K'
	ServerMsgNoData               ServerMessageType = 'n'
	ServerMsgNoticeResponse       ServerMessageType = 'N'
	ServerMsgNotificationResponse ServerMessageType = 'A'
	ServerMsgParameterDescription ServerMessageType = 't'
	ServerMsgParameterStatus      ServerMessageType = 'S'
	ServerMsgParseComplete        ServerMessageType = '1'
	ServerMsgPortalSuspended      ServerMessageType = 's'
	ServerMsgReady                ServerMessageType = 'Z'
	ServerMsgRowDescription       ServerMessageType = 'T'

	// TODO: Copy
)

type PrepareType byte

const (
	PrepareStatement PrepareType = 'S'
	PreparePortal    PrepareType = 'P'
)

const (
	AuthOK int32 = 0
)

// preparedStatement is a SQL statement which has been parsed, analyzed and rewritten.
// Types of its arguments and results have been determined.
// Actual arguments are provided by BindMessage and executed by ExecuteMessage.
type preparedStatement struct {
	query    string
	argTypes []oid.Oid
	columns  []executor.ResultColumn
}

// portal represents SQL execution. preparedStatement could bind to portal.
// cursor will also create portal.
//
// Start with: portal is a preparedStatement that has been bound with parameters
type portal struct {
	name   string
	stmt   preparedStatement
	params []parser.Datum
	format []formatCode // output format
}

// TODO: session and executor
type pqConn struct {
	conn net.Conn

	r        *bufio.Reader
	w        *bufio.Writer
	readBuf  readBuffer
	writeBuf writeBuffer
	tagBuf   [64]byte

	session  *sql.Session
	executor executor.Executor

	preparedStatements map[string]preparedStatement
	portals            map[string]portal

	extendedQueryMessage, ignoreTillSync bool
}

func newPQConn(conn net.Conn, executor executor.Executor, sessionArgs sql.ConnectionArgs) pqConn {
	return pqConn{
		conn: conn,
		r:    bufio.NewReader(conn),
		w:    bufio.NewWriter(conn),

		executor: executor,

		preparedStatements: make(map[string]preparedStatement),
		portals:            make(map[string]portal),

		session: sql.NewSession(sessionArgs, executor, conn.RemoteAddr()),
	}
}

func (c *pqConn) close() {
	if err := c.w.Flush(); err != nil {
		log.Println(err.Error())
	}

	_ = c.conn.Close()
}

// parseOptions parse options from client
func parseOptions(data []byte) (sql.ConnectionArgs, error) {
	args := sql.ConnectionArgs{}
	buf := readBuffer{msg: data}

	for {
		key, err := buf.getString()
		if err != nil {
			return args, fmt.Errorf("error when reading option key: %s", err)
		}
		if len(key) == 0 {
			break
		}
		value, err := buf.getString()
		if err != nil {
			return args, fmt.Errorf("error when reading option value: %s", err)
		}

		switch key {
		case "database":
			args.Database = value
		case "user":
			args.User = value
		case "client_encoding":
			args.ClientEncoding = value
		case "datestyle":
			args.DateStyle = value
		default:
			log.Printf("unrecognized connection parameter %q", key)
		}
	}

	//r.PrintVarInJson(args)

	return args, nil
}

// serve serves a session/connection.
// main loop
func (c *pqConn) serve(authenticationHook func(string, bool) error) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if authenticationHook != nil {
		// TODO: c.session.User
		if err := authenticationHook("", true); err != nil {
			return c.sendInternalError(err.Error())
		}
	}

	// Server response with AuthMessage
	c.writeBuf.initMsg(ServerMsgAuth)
	c.writeBuf.putInt32(AuthOK)
	if err := c.writeBuf.finishMsg(c.w); err != nil {
		return err
	}

	// Server response with client_encoding/DateStyle/server_version parameters
	for key, value := range map[string]string{
		"client_encoding": "UTF8",
		"datestyle":       "ISO",
		"server_version":  "9.5.0",
	} {
		c.writeBuf.initMsg(ServerMsgParameterStatus)
		for _, str := range [...]string{key, value} {
			if err := c.writeBuf.writeString(str); err != nil {
				return err
			}
		}
		if err := c.writeBuf.finishMsg(c.w); err != nil {
			return err
		}
	}
	if err := c.w.Flush(); err != nil {
		return err
	}

	log.Printf("Now ready to goto main loop\n")

	// Main loop to handle client requests
	for {
		if !c.extendedQueryMessage {
			// Non extended query protocol
			c.writeBuf.initMsg(ServerMsgReady)
			var txnStatus byte
			switch c.session.TxnState.State {
			case sql.Aborted:
				txnStatus = 'E'
			case sql.Open:
				txnStatus = 'T'
			case sql.Idle:
				txnStatus = 'I'
			default:
				return fmt.Errorf("Wrong txn status: %v", c.session.TxnState.State)
			}

			c.writeBuf.WriteByte(txnStatus)
			if err := c.writeBuf.finishMsg(c.w); err != nil {
				return err
			}

			// We only flush on every message if not doing an extended query.
			// If we are, wait for an explicit Flush message. See:
			// http://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY.
			if err := c.w.Flush(); err != nil {
				return err
			}
		}

		typ, len, err := c.readBuf.readTypedMsg(c.r)
		if err != nil {
			return err
		}

		log.Printf("Message: Type=%c, Len=%d, Content=%q\n", typ, len, string(c.readBuf.msg))

		// When an error occurs handling an extended query message, we have to ignore
		// any messages until get a sync.
		if c.ignoreTillSync && typ != ClientMsgSync {
			continue
		}

		// TODO: how to make this scalable and extensable
		switch typ {
		case ClientMsgSync:
			c.extendedQueryMessage = false
			c.ignoreTillSync = false

		case ClientMsgSimpleQuery:
			c.extendedQueryMessage = false
			err = c.handleSimpleQuery(ctx, &c.readBuf)

		case ClientMsgMPPQuery:
			c.extendedQueryMessage = false
			err = c.handleMPPQuery(ctx, &c.readBuf)

		case ClientMsgTerminate:
			return nil

		case ClientMsgParse:
			c.extendedQueryMessage = true
			err = c.handleParse(ctx, &c.readBuf)

		case ClientMsgDescribe:
			c.extendedQueryMessage = true
			err = c.handleDescribe(&c.readBuf)

		case ClientMsgClose:
			c.extendedQueryMessage = true
			err = c.handleClose(&c.readBuf)

		case ClientMsgBind:
			c.extendedQueryMessage = true
			err = c.handleBind(&c.readBuf)

		case ClientMsgExecute:
			c.extendedQueryMessage = true
			err = c.handleExecute(ctx, &c.readBuf)

		case ClientMsgFlush:
			c.extendedQueryMessage = true
			err = c.w.Flush()

		default:
			err = c.sendInternalError(fmt.Sprintf("unknown client message type: %s", typ))
		}

		if err != nil {
			return err
		}
	}
}

func (c *pqConn) handleSimpleQuery(ctx context.Context, buf *readBuffer) error {
	query, err := buf.getString()
	if err != nil {
		return err
	}

	return c.executeStatements(ctx, query, nil, nil, true, 0)
}

// handleMPPQuery act as GPDB QE and process request received from QD.
// message format:
//	'M'
//	len
//	siceIndex
//
func (c *pqConn) handleMPPQuery(ctx context.Context, buf *readBuffer) error {
	query, err := buf.getString()
	if err != nil {
		return err
	}

	//c.sendInternalError(fmt.Sprintf("fake error"))
	//return nil

	return c.executeStatements(ctx, query, nil, nil, true, 0)
}

// handleParse parses prepared statement, eg:
//	SELECT * FROM tbl WHERE id = $1 AND name like $2
//	parameters are 1-indexed.
func (c *pqConn) handleParse(ctx context.Context, buf *readBuffer) error {
	name, err := buf.getString()
	if err != nil {
		return err
	}

	// Unnamed prepared statement can be overritten.
	if name != "" {
		if _, ok := c.preparedStatements[name]; ok {
			return c.sendInternalError(fmt.Sprintf("prepared statement %q already exists", name))
		}
	}

	// Query for prepared statement
	query, err := buf.getString()
	if err != nil {
		return err
	}

	// Number of parameters for prepared statement
	numParamTypes, err := buf.getInt16()
	if err != nil {
		return err
	}

	// Type hints for each parameter, this is not an indication of the number of
	// parameters that appear in the query string, only the number that the
	// frontend wants to prespecify types for.
	inTypeHints := make([]oid.Oid, numParamTypes)
	for i := range inTypeHints {
		typ, err := buf.getInt32()
		if err != nil {
			return err
		}
		inTypeHints[i] = oid.Oid(typ)
	}

	args := make(parser.MapArgs)
	for i, t := range inTypeHints {
		if t == 0 {
			continue
		}
		v, ok := oidToDatum[t]
		if !ok {
			return c.sendInternalError(fmt.Sprintf("unknown oid type: %v", t))
		}
		args[fmt.Sprint(i+1)] = v
	}

	// In PostgreSQL, inTypeHints is only for hints and not used internally.
	// http://www.postgresql.org/docs/9.4/static/protocol-message-formats.html
	//
	// parse_analyze_varparams(raw_parse_tree,  query_string, &paramTypes, &numParams)
	// is used to get numParams and paramTypes in query.

	cols, args, err := c.executor.Prepare(ctx, query, args)
	if err != nil {
		return c.sendInternalError(err.Error())
	}

	pq := preparedStatement{
		query:    query,
		argTypes: make([]oid.Oid, 0, len(args)),
		columns:  cols,
	}

	for k, v := range args {
		i, err := strconv.Atoi(k)
		if err != nil {
			return c.sendInternalError(fmt.Sprintf("non-integer parameter: %s", k))
		}

		// ValArgs are 1-indexed, pq.inTypes are 0-index.
		i--
		if i < 0 {
			return c.sendInternalError(fmt.Sprintf("there is no paramter $%s", k))
		}

		// Grow pq.inTypes to be at least as large as i
		for j := len(pq.argTypes); j <= i; j++ {
			pq.argTypes = append(pq.argTypes, 0)
			if j < len(inTypeHints) {
				pq.argTypes[j] = inTypeHints[j]
			}
		}

		// OID to Datum is not a 1-1 mapping (eg: int4 and int8 both map to DummInt),
		// so we need to maintain the types sent by the client.
		if pq.argTypes[i] != 0 {
			continue
		}
		id, ok := datumToOid[reflect.TypeOf(v)]
		if !ok {
			return c.sendInternalError(fmt.Sprintf("unknown datum type: %s", v.Type()))
		}
		pq.argTypes[i] = id
	}

	for i := range pq.argTypes {
		if pq.argTypes[i] == 0 {
			return c.sendInternalError(fmt.Sprintf("could not determine data type of parameter $%d", i+1))
		}
	}

	c.preparedStatements[name] = pq
	c.writeBuf.initMsg(ServerMsgParseComplete)
	return c.writeBuf.finishMsg(c.w)
}

func (c *pqConn) handleDescribe(buf *readBuffer) error {
	typ, err := buf.getPrepareType()
	if err != nil {
		return c.sendInternalError(err.Error())
	}

	name, err := buf.getString()
	if err != nil {
		return err
	}

	switch typ {
	case PrepareStatement:
		stmt, ok := c.preparedStatements[name]
		if !ok {
			return c.sendInternalError(fmt.Sprintf("unknown prepared statement %q", name))
		}

		c.writeBuf.initMsg(ServerMsgParameterDescription)
		c.writeBuf.putInt16(int16(len(stmt.argTypes)))

		for _, t := range stmt.argTypes {
			c.writeBuf.putInt32(int32(t))
		}
		if err := c.writeBuf.finishMsg(c.w); err != nil {
			return err
		}

		return c.sendRowDescription(stmt.columns, nil)
	case PreparePortal:
		p, ok := c.portals[name]
		if !ok {
			return c.sendInternalError(fmt.Sprintf("unknow portal %q", name))
		}

		stmt, ok := c.preparedStatements[p.name]
		if !ok {
			return c.sendInternalError(fmt.Sprintf("unknown prepared statement %q", name))
		}

		return c.sendRowDescription(stmt.columns, p.format)
	default:
		return fmt.Errorf("unknown describe type: %s", typ)
	}
}

// handleClose close statement or portal
func (c *pqConn) handleClose(buf *readBuffer) error {
	typ, err := buf.getPrepareType()
	if err != nil {
		return c.sendInternalError(err.Error())
	}

	name, err := buf.getString()
	if err != nil {
		return err
	}

	switch typ {
	case PrepareStatement:
		if _, ok := c.preparedStatements[name]; ok {
			// delete corresponding portals???
		}
		delete(c.preparedStatements, name)
	case PreparePortal:
		if _, ok := c.portals[name]; ok {
			// delete corresponding port from stmt.portNames?
			// what is this?
		}
		delete(c.portals, name)
	default:
		return fmt.Errorf("unknown close type: %s", typ)
	}
	return nil
}

func (c *pqConn) handleBind(buf *readBuffer) error {
	portalName, err := buf.getString()
	if err != nil {
		return err
	}

	// Unnamed portal can be freely overwritten.
	if portalName != "" {
		if _, ok := c.portals[portalName]; ok {
			return c.sendInternalError(fmt.Sprintf("portal %q already exists", portalName))
		}
	}

	statementName, err := buf.getString()
	if err != nil {
		return err
	}

	stmt, ok := c.preparedStatements[statementName]
	if !ok {
		return c.sendInternalError(fmt.Sprintf("unknown prepared statement %q", statementName))
	}

	numParams := int16(len(stmt.argTypes))
	paramFormatCodes := make([]formatCode, numParams)

	// From the docs on number of parameter format codes to bind:
	// This can be zero to indicate that there are no parameters or that the
	// parameters all use the default format (text); or one, in which case the
	// specified format code is applied to all parameters; or it can equal the
	// actual number of parameters.
	// http://www.postgresql.org/docs/current/static/protocol-message-formats.html
	numParamFormatCodes, err := buf.getInt16()
	if err != nil {
		return err
	}

	switch numParamFormatCodes {
	case 0:
	case 1:
		// '1' means read one code and apply it to every param
		c, err := buf.getInt16()
		if err != nil {
			return err
		}
		fmtCode := formatCode(c)
		for i := range paramFormatCodes {
			paramFormatCodes[i] = fmtCode
		}
	case numParams:
		// Read one format code for each param and apply it to that param
		for i := range paramFormatCodes {
			c, err := buf.getInt16()
			if err != nil {
				return err
			}
			paramFormatCodes[i] = formatCode(c)
		}
	default:
		return c.sendInternalError(fmt.Sprintf("wrong number of format codes specified: %d for %d parameters", numParamFormatCodes, numParams))
	}

	numValues, err := buf.getInt16()
	if err != nil {
		return err
	}
	if numParams != numValues {
		return c.sendInternalError(fmt.Sprintf("expected %d parameters, got %d", numParams, numValues))
	}

	params := make([]parser.Datum, numParams)
	for i, t := range stmt.argTypes {
		plen, err := buf.getInt32()
		if err != nil {
			return err
		}
		if plen == -1 {
			continue
		}
		b, err := buf.getBytes(int(plen))
		if err != nil {
			return err
		}
		d, err := decodeOidDatum(t, paramFormatCodes[i], b)
		if err != nil {
			return c.sendInternalError(fmt.Sprintf("param $%d: %s", i+1, err))
		}
		params[i] = d
	}

	numColumns := int16(len(stmt.columns))
	columnFormatCodes := make([]formatCode, numColumns)

	// From the docs on number of result-column format codes to bind:
	// This can be zero to indicate that there are no result columns or that
	// the result columns should all use the default format (text); or one, in
	// which case the specified format code is applied to all result columns
	// (if any); or it can equal the actual number of result columns of the
	// query.
	// http://www.postgresql.org/docs/current/static/protocol-message-formats.html
	numColumnFormatCodes, err := buf.getInt16()
	if err != nil {
		return err
	}
	switch numColumnFormatCodes {
	case 0:
	case 1:
		// Read one code and apply it to every column
		c, err := buf.getInt16()
		if err != nil {
			return err
		}
		fmtCode := formatCode(c)
		for i := range columnFormatCodes {
			columnFormatCodes[i] = formatCode(fmtCode)
		}
	case numColumns:
		// Read one format code for each column and apply it to that column.
		for i := range columnFormatCodes {
			c, err := buf.getInt16()
			if err != nil {
				return err
			}
			columnFormatCodes[i] = formatCode(c)
		}
	default:
		return c.sendInternalError(fmt.Sprintf("expected, 0, 1, or %d for number of format codes, got %d", numColumns, numColumnFormatCodes))
	}

	// BindMessage bind portal with prepared statement???
	// statement contains portal names map.  Usage???
	// portals map contains name -> portal map.
	c.portals[portalName] = portal{
		name:   statementName,
		stmt:   stmt,
		params: params,
		format: columnFormatCodes,
	}

	c.writeBuf.initMsg(ServerMsgBindComplete)
	return c.writeBuf.finishMsg(c.w)
}

func (c *pqConn) handleExecute(ctx context.Context, buf *readBuffer) error {
	portalName, err := buf.getString()
	if err != nil {
		return err
	}

	portal, ok := c.portals[portalName]
	if !ok {
		return c.sendInternalError(fmt.Sprintf("unknown portal %q", portalName))
	}
	limit, err := buf.getInt32()
	if err != nil {
		return err
	}

	return c.executeStatements(ctx, portal.stmt.query, portal.params, portal.format, false, limit)
}

func (c *pqConn) executeStatements(
	ctx context.Context,
	stmts string,
	params []parser.Datum,
	formatCodes []formatCode,
	sendDescription bool,
	limit int32,
) error {
	results := c.executor.ExecuteStatements(ctx, stmts, params)
	if results.Empty {
		// Skip executor and just send EmptyQueryResponse
		c.writeBuf.initMsg(ServerMsgEmptyQuery)
		return c.writeBuf.finishMsg(c.w)
	}
	return c.sendResponse(results.ResultList, formatCodes, sendDescription, limit)
}

func (c *pqConn) sendCommandComplete(tag []byte) error {
	c.writeBuf.initMsg(ServerMsgCommandComplete)
	c.writeBuf.Write(tag)
	c.writeBuf.WriteByte(0)
	return c.writeBuf.finishMsg(c.w)
}

func (c *pqConn) sendResponse(results executor.ResultList, formatCodes []formatCode, sendDescription bool, limit int32) error {
	if len(results) == 0 {
		return c.sendCommandComplete(nil)
	}

	for _, result := range results {
		// Handle result error?
		if limit != 0 && len(result.Rows) > int(limit) {
			if err := c.sendInternalError(fmt.Sprintf("execute row count limits not supported: %d of %d", limit, len(result.Rows))); err != nil {
				return err
			}
			break
		}

		if result.PGTag == "INSERT" {
			// From the postgres docs (49.5. Message Formats):
			// `INSERT oid rows`... oid is the object ID of the inserted row if
			//	rows is 1 and the target table has OIDs; otherwise oid is 0.
			result.PGTag = "INSERT 0"
		}
		tag := append(c.tagBuf[:0], result.PGTag...)

		switch result.Type {
		case executor.RowsAffected:
			// Send CommandComplete
			tag = append(tag, ' ')
			tag = strconv.AppendInt(tag, int64(result.RowsAffected), 10)
			if err := c.sendCommandComplete(tag); err != nil {
				return err
			}
		case executor.Rows:
			if sendDescription {
				if err := c.sendRowDescription(result.Columns, formatCodes); err != nil {
					return err
				}
			}

			// Send DataRows
			for _, row := range result.Rows {
				c.writeBuf.initMsg(ServerMsgDataRow)
				c.writeBuf.putInt16(int16(len(row.Values)))

				for i, col := range row.Values {
					fmtCode := formatText
					if formatCodes != nil {
						fmtCode = formatCodes[i]
					}

					switch fmtCode {
					case formatText:
						if err := c.writeBuf.writeTextDatum(col); err != nil {
							return err
						}
					case formatBinary:
						if err := c.writeBuf.writeBinaryDatum(col); err != nil {
							return err
						}
					default:
						return fmt.Errorf("unsupported format cdoe %s", fmtCode)
					}
				}

				if err := c.writeBuf.finishMsg(c.w); err != nil {
					return err
				}
			}

			// Send CommandComplete
			tag = append(tag, ' ')
			tag = append(tag, []byte(strconv.Itoa(len(result.Rows)))...)

			if err := c.sendCommandComplete(tag); err != nil {
				return err
			}

		// Ack messages do not have a corresponding protobuf field, so handle those with default
		// This also includes DDLs which want CommandComplete as well
		default:
			if err := c.sendCommandComplete(tag); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *pqConn) sendRowDescription(columns []executor.ResultColumn, formatCodes []formatCode) error {
	if len(columns) == 0 {
		c.writeBuf.initMsg(ServerMsgNoData)
		return c.writeBuf.finishMsg(c.w)
	}

	c.writeBuf.initMsg(ServerMsgRowDescription)
	c.writeBuf.putInt16(int16(len(columns)))

	for i, column := range columns {
		if err := c.writeBuf.writeString(column.Name); err != nil {
			return err
		}

		typ := typeForDatum(column.Typ)
		c.writeBuf.putInt32(0) // Table OID (optional).
		c.writeBuf.putInt16(0) // Column attribute ID (optional)
		c.writeBuf.putInt32(int32(typ.oid))
		c.writeBuf.putInt16(int16(typ.size))
		c.writeBuf.putInt32(0)

		if formatCodes == nil {
			c.writeBuf.putInt16(int16(formatText))
		} else {
			c.writeBuf.putInt16(int16(formatCodes[i]))
		}
	}

	return c.writeBuf.finishMsg(c.w)
}

func (c *pqConn) sendInternalError(errToSend string) error {
	return c.sendError(sql.CodeInternalError, errToSend)
}

func (c *pqConn) sendError(errCode, errToSend string) error {
	if c.extendedQueryMessage {
		c.ignoreTillSync = true
	}

	c.writeBuf.initMsg(ServerMsgErrorResponse)
	if err := c.writeBuf.WriteByte('S'); err != nil {
		return err
	}
	if err := c.writeBuf.writeString("ERROR"); err != nil {
		return err
	}
	if err := c.writeBuf.WriteByte('C'); err != nil {
		return err
	}
	if err := c.writeBuf.writeString(errCode); err != nil {
		return err
	}
	if err := c.writeBuf.WriteByte('M'); err != nil {
		return err
	}
	if err := c.writeBuf.writeString(errToSend); err != nil {
		return err
	}
	if err := c.writeBuf.WriteByte(0); err != nil {
		return err
	}
	if err := c.writeBuf.finishMsg(c.w); err != nil {
		return err
	}

	return c.w.Flush()
}
