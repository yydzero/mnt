package parser

import (
	"gopkg.in/inf.v0"
	"reflect"
	"time"
	"github.com/yydzero/mnt/util/duration"
)

var (
	DummyBool      Datum = DBool(false)
	DummyInt       Datum = DInt(0)
	DummyFloat     Datum = DFloat(0)
	DummyDecimal   Datum = &DDecimal{}
	DummyString    Datum = DString("")
	DummyBytes     Datum = DBytes("")
	DummyDate      Datum = DDate(0)
	DummyTimestamp Datum = DTimestamp{}
	DummyInterval  Datum = DInterval{}
	dummyTuple     Datum = DTuple{}
	DNull          Datum = dNull{}

	boolType      = reflect.TypeOf(DummyBool)
	intType       = reflect.TypeOf(DummyInt)
	floatType     = reflect.TypeOf(DummyFloat)
	decimalType   = reflect.TypeOf(DummyDecimal)
	stringType    = reflect.TypeOf(DummyString)
	bytesType     = reflect.TypeOf(DummyBytes)
	dateType      = reflect.TypeOf(DummyDate)
	timestampType = reflect.TypeOf(DummyTimestamp)
	intervalType  = reflect.TypeOf(DummyInterval)
	tupleType     = reflect.TypeOf(dummyTuple)
	nullType      = reflect.TypeOf(DNull)
	valargType    = reflect.TypeOf(DValArg{})
)

type Datum interface {
	Type() string
}

type DBool bool
func (d DBool) Type() string {
	return "bool"
}

type DInt int64
func (d DInt) Type() string {
	return "int"
}

type DFloat float64
func (d DFloat) Type() string {
	return "float"
}

type DDecimal struct {
	inf.Dec
}
func (d *DDecimal) Type() string {
	return "decimal"
}

type DString string
func (d DString) Type() string {
	return "string"
}

type DBytes string
func (d DBytes) Type() string {
	return "bytes"
}

type DDate int64
func (d DDate) Type() string {
	return "date"
}

type DTimestamp struct {
	time.Time
}
func (d DTimestamp) Type() string {
	return "timestamp"
}

type DInterval struct {
	duration.Duration
}
func (d DInterval) Type() string {
	return "interval"
}

type DTuple []Datum
func (d DTuple) Type() string {
	return "tuple"
}

type dNull struct{}
func (d dNull) Type() string {
	return "NULL"
}

// DValArg is the named bind var argument Datum.
type DValArg struct {
	name string
}

func (DValArg) Type() string {
	return "parameter"
}
