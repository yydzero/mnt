package libpq

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/lib/pq"
	"github.com/lib/pq/oid"
	"github.com/yydzero/mnt/parser"
	"reflect"
	"strconv"
	"time"
)

const pgTimeStampFormat = "2016-01-02 15:04:05.999999999-07:00"
const secondsInDay = 24 * 60 * 60

// http://www.postgresql.org/docs/9.5/static/protocol-overview.html#PROTOCOL-FORMAT-CODES
type formatCode int16

// Clients can specify a format code for each transmitted parameter value and
// for each column of a query result. Text has format code zero, binary has
// format code one, and all other format codes are reserved for future definition.
const (
	formatText   formatCode = 0
	formatBinary formatCode = 1
)

// pgType contains type metadata used in RowDescription messages.
type pgType struct {
	oid oid.Oid

	// Variable-size types have size=-1.
	// Note that the protocol has both int16 and int32 size fields,
	// so this attribute is an unsized int and should be cast as needed.
	// This field does *not* correspond to the encoded length of a
	// data type, so it's unclear what, if anything, it is used for.
	// To get the right value, "SELECT oid, typlen FROM pg_type" on a postgres server.
	size int
}

// typeForDatum return type info (pg_type, include oid and size of type) for a datum
func typeForDatum(d parser.Datum) pgType {
	if d == parser.DNull {
		return pgType{}
	}

	switch d.(type) {
	case parser.DBool:
		return pgType{oid.T_bool, 1}

	case parser.DBytes:
		return pgType{oid.T_bytea, -1}

	case parser.DInt:
		return pgType{oid.T_int8, 8}

	case parser.DFloat:
		return pgType{oid.T_float8, 8}

	case *parser.DDecimal:
		return pgType{oid.T_numeric, -1}

	case parser.DString:
		return pgType{oid.T_text, -1}

	case parser.DDate:
		return pgType{oid.T_date, 8}

	case parser.DTimestamp:
		return pgType{oid.T_timestamptz, 8}

	case parser.DInterval:
		return pgType{oid.T_interval, 8}

	default:
		panic(fmt.Sprintf("unsupported type %T", d))
	}
}

var (
	oidToDatum = map[oid.Oid]parser.Datum{
		oid.T_bool:        parser.DummyBool,
		oid.T_bytea:       parser.DummyBytes,
		oid.T_date:        parser.DummyDate,
		oid.T_float4:      parser.DummyFloat,
		oid.T_float8:      parser.DummyFloat,
		oid.T_int2:        parser.DummyInt,
		oid.T_int4:        parser.DummyInt,
		oid.T_int8:        parser.DummyInt,
		oid.T_interval:    parser.DummyInterval,
		oid.T_numeric:     parser.DummyDecimal,
		oid.T_text:        parser.DummyString,
		oid.T_timestamp:   parser.DummyTimestamp,
		oid.T_timestamptz: parser.DummyTimestamp,
		oid.T_varchar:     parser.DummyString,
	}

	// Using reflection to support unhashable types.
	datumToOid = map[reflect.Type]oid.Oid{
		reflect.TypeOf(parser.DummyBool):      oid.T_bool,
		reflect.TypeOf(parser.DummyBytes):     oid.T_bytea,
		reflect.TypeOf(parser.DummyDate):      oid.T_date,
		reflect.TypeOf(parser.DummyFloat):     oid.T_float8,
		reflect.TypeOf(parser.DummyInt):       oid.T_int8,
		reflect.TypeOf(parser.DummyInterval):  oid.T_interval,
		reflect.TypeOf(parser.DummyDecimal):   oid.T_numeric,
		reflect.TypeOf(parser.DummyString):    oid.T_text,
		reflect.TypeOf(parser.DummyTimestamp): oid.T_timestamptz,
	}
)

// decodeOidDatum decodes bytes according to specified Oid and format code into a datum
func decodeOidDatum(id oid.Oid, code formatCode, b []byte) (parser.Datum, error) {
	var d parser.Datum

	switch id {
	case oid.T_bool:
		switch code {
		case formatText:
			v, err := strconv.ParseBool(string(b))
			if err != nil {
				return d, err
			}
			d = parser.DBool(v)
		default:
			return d, fmt.Errorf("unsupported bool format code: %d", code)
		}

	case oid.T_int2:
		switch code {
		case formatText:
			i, err := strconv.ParseInt(string(b), 10, 64)
			if err != nil {
				return d, err
			}
			d = parser.DInt(i)
		case formatBinary:
			var i int16
			err := binary.Read(bytes.NewReader(b), binary.BigEndian, &i)
			if err != nil {
				return d, err
			}
			d = parser.DInt(i)
		default:
			return d, fmt.Errorf("unsupported int2 format code: %d", code)
		}

	case oid.T_int4:
		switch code {
		case formatText:
			i, err := strconv.ParseInt(string(b), 10, 64)
			if err != nil {
				return d, err
			}
			d = parser.DInt(i)
		case formatBinary:
			var i int32
			err := binary.Read(bytes.NewReader(b), binary.BigEndian, &i)
			if err != nil {
				return d, err
			}
			d = parser.DInt(i)
		default:
			return d, fmt.Errorf("unsupported int4 format code: %d", code)
		}

	case oid.T_int8:
		switch code {
		case formatText:
			i, err := strconv.ParseInt(string(b), 10, 64)
			if err != nil {
				return d, err
			}
			d = parser.DInt(i)
		case formatBinary:
			var i int64
			err := binary.Read(bytes.NewReader(b), binary.BigEndian, &i)
			if err != nil {
				return d, err
			}
			d = parser.DInt(i)
		default:
			return d, fmt.Errorf("unsupported int8 format code: %d", code)
		}

	case oid.T_float4:
		switch code {
		case formatText:
			f, err := strconv.ParseFloat(string(b), 64)
			if err != nil {
				return d, err
			}
			d = parser.DFloat(f)
		case formatBinary:
			var f float32
			err := binary.Read(bytes.NewReader(b), binary.BigEndian, &f)
			if err != nil {
				return d, err
			}
			d = parser.DFloat(f)
		default:
			return d, fmt.Errorf("unsupported float4 format code: %d", code)
		}

	case oid.T_float8:
		switch code {
		case formatText:
			f, err := strconv.ParseFloat(string(b), 64)
			if err != nil {
				return d, err
			}
			d = parser.DFloat(f)
		case formatBinary:
			var f float64
			err := binary.Read(bytes.NewReader(b), binary.BigEndian, &f)
			if err != nil {
				return d, err
			}
			d = parser.DFloat(f)
		default:
			return d, fmt.Errorf("unsupported float8 format code: %d", code)
		}

	case oid.T_numeric:
		switch code {
		case formatText:
			dd := &parser.DDecimal{}
			if _, ok := dd.SetString(string(b)); !ok {
				return nil, fmt.Errorf("could not parse string %q as decimal", b)
			}
			d = dd
		default:
			return d, fmt.Errorf("unsupported numberic format code: %d", code)
		}

	case oid.T_text, oid.T_varchar:
		switch code {
		case formatText:
			d = parser.DString(b)
		default:
			return d, fmt.Errorf("unsupported text format code: %d", code)
		}

	case oid.T_bytea:
		switch code {
		case formatText:
			// http://www.postgresql.org/docs/current/static/datatype-binary.html#AEN5667
			// Code cribbed from github.com/lib/pq.

			// Only support hex encoding
			if len(b) >= 2 && bytes.Equal(b[:2], []byte("\\x")) {
				b = b[2:] // trim off leading "\\x"
				result := make([]byte, hex.DecodedLen(len(b)))
				_, err := hex.Decode(result, b)
				if err != nil {
					return d, err
				}
				d = parser.DBytes(result)
			} else {
				return d, fmt.Errorf("unsupported bytea encoding: %q", b)
			}
		case formatBinary:
			d = parser.DBytes(b)
		default:
			return d, fmt.Errorf("unsupported bytea format code: %d", code)
		}

	case oid.T_timestamp, oid.T_timestamptz:
		switch code {
		case formatText:
			ts, err := parseTs(string(b))
			if err != nil {
				return d, fmt.Errorf("could not parse string %q as timestamp", b)
			}
			d = parser.DTimestamp{Time: ts}
		default:
			return d, fmt.Errorf("unsupported timestamp format code: %d", code)
		}

	case oid.T_date:
		switch code {
		case formatText:
			ts, err := parseTs(string(b))
			if err != nil {
				return d, fmt.Errorf("could not parse string %q as date", b)
			}
			daysSinceEpoch := ts.Unix() / secondsInDay
			d = parser.DDate(daysSinceEpoch)
		default:
			return d, fmt.Errorf("unsupported date format code: %d", code)
		}

	default:
		return d, fmt.Errorf("unsupported OID: %v", id)
	}

	return d, nil
}

// parseTs parses timestamps in any of the formats that PostgreSQL accepts over the wire protocol.
//
// PostgreSQL is lenient in what it accepts as a timestamp, so we must also be lenient.
func parseTs(s string) (time.Time, error) {
	// RFC3339Nano is sent by github.com/lib/pq (go).
	if ts, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return ts, nil
	}

	// pq.ParseTimestamp parses the timestamp format.
	return pq.ParseTimestamp(nil, s)
}

// formatTs formats t into a format pq understands.
func formatTs(t time.Time) (b []byte) {
	// Need to send dates before 0001 A.D. with " BC" suffix, instead of the
	// minus sign preferred by Go.
	// Beware "0000" in ISO is "1 BC", "-0001" is "2 BC" and so on
	bc := false
	if t.Year() <= 0 {
		// flip the year sign, and add 1, eg: "0" will be "1" and "-10" will be "11"
		t = t.AddDate((-t.Year())*2+1, 0, 0)
		bc = true
	}

	b = []byte(t.Format(pgTimeStampFormat))

	_, offset := t.Zone()
	offset = offset % 60
	if offset != 0 {
		// RFC3339Nano already printed the minus sign
		if offset < 0 {
			offset = -offset
		}
		b = append(b, ':')
		if offset < 10 {
			b = append(b, '0')
		}
		b = strconv.AppendInt(b, int64(offset), 10)
	}

	if bc {
		b = append(b, " BC"...)
	}

	return b
}
