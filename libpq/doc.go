package libpq


// libpq server is similar as RPC server: read a request from socket, process it and write response.
//
// 1. Many message type, different type has different processing logic.
//
// 2. Use Executor type to handle the underlying logic. Different executor implementation
//    have different logic. Eg: some return real data, some return fake data.
//
// 3. What is common logic we should use?
//		- statistics
//		- protocol handling: we encapsulate all other details.
//
// 4. Protocol message type
//		- SimpleQuery
//		- Parse/Bind/Execute
//
// 5. data types: oid, datum
//			oid: generated from pg_types
//			Datum: parser/datum.go.  Datum is an interface
//			pgType:  pg_type information
//
// 6. Result encapsulation: columns
//
