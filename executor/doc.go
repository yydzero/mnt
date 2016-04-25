package executor

// package executor provide interface to execute SQL in stateful mode.
//
// exeuctor is stateful, so it must belong to specific session, include
// database, username and transaction status.
// How about planner?  Does it belong to session?
