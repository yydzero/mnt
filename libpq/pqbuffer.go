package libpq

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/yydzero/mnt/parser"
	"io"
	"strconv"
	"time"
	"unsafe"
)

const maxMessageSize = 1 << 24

// readBuffer is a buffer for write data to or read data from.
//
// readUntypedMsg, readTypedMsg read data from reader and put into buffer.
// getString, getPrepareType, getBytes, getInt16 etc read  typed data from buffer.

type readBuffer struct {
	msg []byte
	tmp [4]byte
}

// reset sets b.msg to exactly size, attempting to use spare capacity
// at the end of the existing slice when possible and allocating a new
// slice when necessary.
func (b *readBuffer) reset(size int) {
	if b.msg != nil {
		b.msg = b.msg[len(b.msg):]
	}

	if cap(b.msg) >= size {
		b.msg = b.msg[:size]
		return
	}

	allocSize := size
	if allocSize < 4096 {
		allocSize = 4096
	}
	b.msg = make([]byte, size, allocSize)
}

// readUntypedMsg reads a length-prefixed message. It is only used directly
// during the authentication phase of the protocol; readTypedMsg is
// used at all other times. This returns the number of bytes read and an error,
// if there was one. The number of bytes returned can be non-zero even with an
// error (e.g. if data was read but didn't validate) so that we can more
// accurately measure network traffic.
func (b *readBuffer) readUntypedMsg(rd io.Reader) (int, error) {
	nread, err := io.ReadFull(rd, b.tmp[:])
	if err != nil {
		return nread, err
	}
	size := int(binary.BigEndian.Uint32(b.tmp[:]))
	size -= 4 // size includes itself.
	if size > maxMessageSize || size < 0 {
		return nread, fmt.Errorf("message size %d out of bounds (0..%d)", size, maxMessageSize)
	}

	b.reset(size)
	n, err := io.ReadFull(rd, b.msg)

	return nread + n, err
}

// readTypedMsg reads a message from the provided reader, returning its type code and body.
// It returns the message type, number of bytes read, and an error if there was one.
func (b *readBuffer) readTypedMsg(rd *bufio.Reader) (ClientMessageType, int, error) {
	typ, err := rd.ReadByte()
	if err != nil {
		return 0, 0, err
	}
	n, err := b.readUntypedMsg(rd)
	return ClientMessageType(typ), n, err
}

// getString reads a null-terminated string.
func (b *readBuffer) getString() (string, error) {
	pos := bytes.IndexByte(b.msg, 0)
	if pos == -1 {
		return "", fmt.Errorf("NUL terminator not found")
	}
	// Note: this is a conversion from a byte slice to a string which avoids
	// allocation and copying. It is safe because we never reuse the bytes in our
	// read buffer. It is effectively the same as: "s := string(b.msg[:pos])"
	s := b.msg[:pos]
	b.msg = b.msg[pos + 1:]
	return *((*string)(unsafe.Pointer(&s))), nil
}

func (b *readBuffer) getPrepareType() (PrepareType, error) {
	v, err := b.getBytes(1)
	return PrepareType(v[0]), err
}

func (b *readBuffer) getBytes(n int) ([]byte, error) {
	if len(b.msg) < n {
		return nil, fmt.Errorf("insufficient data: %d", len(b.msg))
	}
	v := b.msg[:n]
	b.msg = b.msg[n:]
	return v, nil
}

func (b *readBuffer) getInt16() (int16, error) {
	if len(b.msg) < 2 {
		return 0, fmt.Errorf("insufficient data: %d", len(b.msg))
	}
	v := int16(binary.BigEndian.Uint16(b.msg[:2]))
	b.msg = b.msg[2:]
	return v, nil
}

func (b *readBuffer) getInt32() (int32, error) {
	if len(b.msg) < 4 {
		return 0, fmt.Errorf("insufficient data: %d", len(b.msg))
	}
	v := int32(binary.BigEndian.Uint32(b.msg[:4]))
	b.msg = b.msg[4:]
	return v, nil
}

type writeBuffer struct {
	bytes.Buffer
	putbuf [64]byte
}

// writeString writes a null-terminated string.
func (b *writeBuffer) writeString(s string) error {
	if _, err := b.WriteString(s); err != nil {
		return err
	}
	return b.WriteByte(0)
}

func (b *writeBuffer) putInt16(v int16) {
	binary.BigEndian.PutUint16(b.putbuf[:], uint16(v))
	b.Write(b.putbuf[:2])
}

func (b *writeBuffer) putInt32(v int32) {
	binary.BigEndian.PutUint32(b.putbuf[:], uint32(v))
	b.Write(b.putbuf[:4])
}

func (b *writeBuffer) putInt64(v int64) {
	binary.BigEndian.PutUint64(b.putbuf[:], uint64(v))
	b.Write(b.putbuf[:8])
}

func (b *writeBuffer) initMsg(typ ServerMessageType) {
	b.Reset()
	b.putbuf[0] = byte(typ)
	b.Write(b.putbuf[:5]) // message type + message length
}

func (b *writeBuffer) finishMsg(w io.Writer) error {
	bytes := b.Bytes()
	binary.BigEndian.PutUint32(bytes[1:5], uint32(b.Len() - 1))
	_, err := w.Write(bytes) // err is not nil for partial write.
	b.Reset()
	return err
}

// writeTextDatum writes given datum in text format to buffer.
func (b *writeBuffer) writeTextDatum(d parser.Datum) error {
	if d == parser.DNull {
		// NULL is encoded as -1; all other values have a length prefix.
		b.putInt32(-1)
		return nil
	}

	switch v := d.(type) {
	case parser.DBool:
		b.putInt32(1)
		if v {
			return b.WriteByte('t')
		}
		return b.WriteByte('f')

	case parser.DInt:
		// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
		s := strconv.AppendInt(b.putbuf[4:4], int64(v), 10)
		b.putInt32(int32(len(s)))
		_, err := b.Write(s)
		return err

	case parser.DFloat:
		// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
		s := strconv.AppendFloat(b.putbuf[4:4], float64(v), 'f', -1, 64)
		b.putInt32(int32(len(s)))
		_, err := b.Write(s)
		return err

	case *parser.DDecimal:
		vs := v.Dec.String()
		b.putInt32(int32(len(vs)))
		_, err := b.WriteString(vs)
		return err

	case parser.DBytes:
		// http://www.postgresql.org/docs/current/static/datatype-binary.html#AEN5667
		// Code cribbed from github.com/lib/pq.
		result := make([]byte, 2 + hex.EncodedLen(len(v)))
		result[0] = '\\'
		result[1] = 'x'
		hex.Encode(result[2:], []byte(v))

		b.putInt32(int32(len(result)))
		_, err := b.Write(result)
		return err

	case parser.DString:
		b.putInt32(int32(len(v)))
		_, err := b.WriteString(string(v))
		return err

	case parser.DDate:
		t := time.Unix(int64(v) * secondsInDay, 0).UTC()
		s := formatTs(t)
		b.putInt32(int32(len(s)))
		_, err := b.Write(s)
		return err

	case parser.DTimestamp:
		t := v.UTC()
		s := formatTs(t)
		b.putInt32(int32(len(s)))
		_, err := b.Write(s)
		return err

	case parser.DInterval:
		s := v.String()
		b.putInt32(int32(len(s)))
		_, err := b.WriteString(s)
		return err

	default:
		return fmt.Errorf("unsupported type %T", d)
	}
}

func (b *writeBuffer) writeBinaryDatum(d parser.Datum) error {
	if d == parser.DNull {
		b.putInt32(-1)
		return nil
	}

	switch v := d.(type) {
	case parser.DInt:
		b.putInt32(8)
		b.putInt64(int64(v))
		return nil

	case parser.DBytes:
		b.putInt32(int32(len(v)))
		_, err := b.Write([]byte(v))
		return err

	default:
		return fmt.Errorf("unsupported type %T", d)
	}
}
