package parser

// MapArgs is an Args implementation which is used for the type
// inference necessary to support the postgres wire protocol.
// See various TypeCheck() implementations for details.
//
// key is 1 index.
type MapArgs map[string]Datum
