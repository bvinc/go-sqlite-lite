// Copyright 2013 The Go-SQLite Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite3

/*
// SQLite compilation options.
// [http://www.sqlite.org/compile.html]
// [http://www.sqlite.org/footprint.html]
#cgo CFLAGS: -std=gnu99
#cgo CFLAGS: -Os
#cgo CFLAGS: -DNDEBUG=1
#cgo CFLAGS: -DSQLITE_CORE=1
#cgo CFLAGS: -DSQLITE_THREADSAFE=2
#cgo CFLAGS: -DSQLITE_TEMP_STORE=2
#cgo CFLAGS: -DSQLITE_USE_URI=1
#cgo CFLAGS: -DSQLITE_ENABLE_FTS3_PARENTHESIS=1
#cgo CFLAGS: -DSQLITE_ENABLE_FTS4=1
#cgo CFLAGS: -DSQLITE_ENABLE_RTREE=1
#cgo CFLAGS: -DSQLITE_ENABLE_STAT3=1
#cgo CFLAGS: -DSQLITE_SOUNDEX=1
#cgo CFLAGS: -DSQLITE_OMIT_AUTHORIZATION=1
#cgo CFLAGS: -DSQLITE_OMIT_AUTOINIT=1
#cgo CFLAGS: -DSQLITE_OMIT_LOAD_EXTENSION=1
#cgo CFLAGS: -DSQLITE_OMIT_TRACE=1
#cgo CFLAGS: -DSQLITE_OMIT_UTF16=1

// Fix for BusyTimeout on *nix systems.
#cgo !windows CFLAGS: -DHAVE_USLEEP=1

// Fix "_localtime32(0): not defined" linker error.
#cgo windows,386 CFLAGS: -D_localtime32=localtime

#include "sqlite3.h"

// cgo doesn't handle variadic functions.
static void set_temp_dir(const char *path) {
	sqlite3_temp_directory = sqlite3_mprintf("%s", path);
}

// cgo doesn't handle SQLITE_{STATIC,TRANSIENT} pointer constants.
static int bind_text(sqlite3_stmt *s, int i, const char *p, int n, int copy) {
	if (n > 0) {
		return sqlite3_bind_text(s, i, p, n,
			(copy ? SQLITE_TRANSIENT : SQLITE_STATIC));
	}
	return sqlite3_bind_text(s, i, "", 0, SQLITE_STATIC);
}
static int bind_blob(sqlite3_stmt *s, int i, const void *p, int n, int copy) {
	if (n > 0) {
		return sqlite3_bind_blob(s, i, p, n,
			(copy ? SQLITE_TRANSIENT : SQLITE_STATIC));
	}
	return sqlite3_bind_zeroblob(s, i, 0);
}

// Faster retrieval of column data types (1 cgo call instead of n).
static void column_types(sqlite3_stmt *s, unsigned char p[], int n) {
	int i = 0;
	for (; i < n; ++i, ++p) {
		*p = sqlite3_column_type(s, i);
	}
}

// Macro for creating callback setter functions.
#define SET(x) \
static void set_##x(sqlite3 *db, void *conn, int enable) { \
	(enable ? sqlite3_##x(db, go_##x, conn) : sqlite3_##x(db, 0, 0)); \
}

*/
import "C"

import (
	"io"
	"os"
	"runtime"
	"time"
	"unsafe"
)

// initErr indicates a SQLite initialization error, which disables this package.
var initErr error

func init() {
	// Initialize SQLite (required with SQLITE_OMIT_AUTOINIT).
	// [http://www.sqlite.org/c3ref/initialize.html]
	if rc := C.sqlite3_initialize(); rc != OK {
		initErr = errStr(rc)
		return
	}

	// Use the same temporary directory as Go.
	// [http://www.sqlite.org/c3ref/temp_directory.html]
	tmp := os.TempDir() + "\x00"
	C.set_temp_dir(cStr(tmp))
}

// Conn is a connection handle, which may have multiple databases attached to it
// by using the ATTACH SQL statement.
// [http://www.sqlite.org/c3ref/sqlite3.html]
type Conn struct {
	db *C.sqlite3
}

// Open creates a new connection to a SQLite database. The name can be 1) a path
// to a file, which is created if it does not exist, 2) a URI using the syntax
// described at http://www.sqlite.org/uri.html, 3) the string ":memory:", which
// creates a temporary in-memory database, or 4) an empty string, which creates
// a temporary on-disk database (deleted when closed) in the directory returned
// by os.TempDir().
// [http://www.sqlite.org/c3ref/open.html]
func Open(name string) (*Conn, error) {
	if initErr != nil {
		return nil, initErr
	}
	name += "\x00"

	var db *C.sqlite3
	rc := C.sqlite3_open_v2(cStr(name), &db,
		C.SQLITE_OPEN_READWRITE|C.SQLITE_OPEN_CREATE, nil)
	if rc != OK {
		err := errMsg(rc, db)
		C.sqlite3_close(db)
		return nil, err
	}
	c := &Conn{db: db}
	C.sqlite3_extended_result_codes(db, 1)
	runtime.SetFinalizer(c, (*Conn).Close)
	return c, nil
}

// Close releases all resources associated with the connection. If any prepared
// statements, incremental I/O operations, or backup operations are still
// active, the connection becomes an unusable "zombie" and is closed after all
// remaining statements and operations are destroyed. A BUSY error code is
// returned if the connection is left in this "zombie" status, which may
// indicate a programming error where some previously allocated resource is not
// properly released.
// [http://www.sqlite.org/c3ref/close.html]
func (c *Conn) Close() error {
	if db := c.db; db != nil {
		c.db = nil
		runtime.SetFinalizer(c, nil)
		if rc := C.sqlite3_close(db); rc != OK {
			err := errMsg(rc, db)
			if rc == BUSY {
				C.sqlite3_close_v2(db)
			}
			return err
		}
		*c = Conn{} // Clear callback handlers only if db was closed
	}
	return nil
}

// Prepare compiles the first statement in sql. Any remaining text after the
// first statement is saved in tail.  This function may return a nil stmt and a
// nil error, if the sql string is empty.
// [http://www.sqlite.org/c3ref/prepare.html]
func (c *Conn) Prepare(sql string) (s *Stmt, tail string, err error) {
	zSQL := sql + "\x00"

	var stmt *C.sqlite3_stmt
	var cTail *C.char
	rc := C.sqlite3_prepare_v2(c.db, cStr(zSQL), -1, &stmt, &cTail)
	if rc != OK {
		return nil, "", errStr(rc)
	}

	if stmt == nil {
		return nil, "", nil
	}

	s = &Stmt{stmt: stmt}
	if cTail != nil {
		if n := cStrOffset(zSQL, cTail); n < len(sql) {
			tail = sql[n:]
		}
	}
	return s, tail, nil
}

// [http://www.sqlite.org/c3ref/exec.html]
func (c *Conn) Exec(sql string, args ...interface{}) error {
	if c.db == nil {
		return ErrBadConn
	}

	// Fast path via sqlite3_exec, which doesn't support parameter binding
	if len(args) == 0 {
		sql += "\x00"
		return c.exec(cStr(sql))
	}

	return nil
}

// Query is a convenience method for executing the first query in sql. It
// returns a prepared statement ready for stepping.
func (c *Conn) Query(sql string, args ...interface{}) (*Stmt, error) {
	if c.db == nil {
		return nil, ErrBadConn
	}
	s, _, err := c.Prepare(sql)
	if err == nil && s != nil {
		if err = s.Query(args...); err == nil {
			return s, nil
		}
		s.Close()
	}
	return nil, err
}

// Begin starts a new deferred transaction. Use c.Exec("BEGIN...") to start an
// immediate or an exclusive transaction.
// [http://www.sqlite.org/lang_transaction.html]
func (c *Conn) Begin() error {
	if c.db == nil {
		return ErrBadConn
	}
	return c.exec(cStr("BEGIN\x00"))
}

// Commit saves all changes made within a transaction to the database.
func (c *Conn) Commit() error {
	if c.db == nil {
		return ErrBadConn
	}
	return c.exec(cStr("COMMIT\x00"))
}

// Rollback aborts the current transaction without saving any changes.
func (c *Conn) Rollback() error {
	if c.db == nil {
		return ErrBadConn
	}
	return c.exec(cStr("ROLLBACK\x00"))
}

// Interrupt causes any pending database operation to abort and return at its
// earliest opportunity. It is safe to call this method from a goroutine
// different from the one that is currently running the database operation, but
// it is not safe to call this method on a connection that might close before
// the call returns.
// [http://www.sqlite.org/c3ref/interrupt.html]
func (c *Conn) Interrupt() {
	if db := c.db; db != nil {
		C.sqlite3_interrupt(db)
	}
}

// AutoCommit returns true if the database connection is in auto-commit mode
// (i.e. outside of an explicit transaction started by BEGIN).
// [http://www.sqlite.org/c3ref/get_autocommit.html]
func (c *Conn) AutoCommit() bool {
	if c.db == nil {
		return false
	}
	return C.sqlite3_get_autocommit(c.db) != 0
}

// LastInsertRowID returns the ROWID of the most recent successful INSERT
// statement.
// [http://www.sqlite.org/c3ref/last_insert_rowid.html]
func (c *Conn) LastInsertRowID() int64 {
	if c.db == nil {
		return 0
	}
	return int64(C.sqlite3_last_insert_rowid(c.db))
}

// Changes returns the number of rows that were changed, inserted, or deleted
// by the most recent statement. Auxiliary changes caused by triggers or
// foreign key actions are not counted.
// [http://www.sqlite.org/c3ref/changes.html]
func (c *Conn) Changes() int {
	if c.db == nil {
		return 0
	}
	return int(C.sqlite3_changes(c.db))
}

// TotalChanges returns the number of rows that were changed, inserted, or
// deleted since the database connection was opened, including changes caused by
// trigger and foreign key actions.
// [http://www.sqlite.org/c3ref/total_changes.html]
func (c *Conn) TotalChanges() int {
	if c.db == nil {
		return 0
	}
	return int(C.sqlite3_total_changes(c.db))
}

// Backup starts an online database backup of c.srcName into dst.dstName.
// Connections c and dst must be distinct. All existing contents of the
// destination database are overwritten.
//
// A read lock is acquired on the source database only while it is being read
// during a call to Backup.Step. The source connection may be used for other
// purposes between these calls. The destination connection must not be used for
// anything until the backup is closed.
// [http://www.sqlite.org/backup.html]
func (c *Conn) Backup(srcName string, dst *Conn, dstName string) (*Backup, error) {
	if c.db == nil || c == dst || dst == nil || dst.db == nil {
		return nil, ErrBadConn
	}
	return newBackup(c, srcName, dst, dstName)
}

// BlobIO opens a BLOB or TEXT value for incremental I/O, allowing the value to
// be treated as a file for reading and/or writing. The value is located as if
// by the following query:
//
// 	SELECT col FROM db.tbl WHERE rowid=row
//
// If rw is true, the value is opened with read-write access, otherwise it is
// read-only. It is not possible to open a column that is part of an index or
// primary key for writing. If foreign key constraints are enabled, it is not
// possible to open a column that is part of a child key for writing.
// [http://www.sqlite.org/c3ref/blob_open.html]
func (c *Conn) BlobIO(db, tbl, col string, row int64, rw bool) (*BlobIO, error) {
	if c.db == nil {
		return nil, ErrBadConn
	}
	return newBlobIO(c, db, tbl, col, row, rw)
}

// BusyTimeout enables the built-in busy handler, which retries the table
// locking operation for the specified duration before aborting. It returns the
// callback function that was previously registered with Conn.BusyFunc, if any.
// The busy handler is disabled if d is negative or zero.
// [http://www.sqlite.org/c3ref/busy_timeout.html]
func (c *Conn) BusyTimeout(d time.Duration) {
	if c.db != nil {
		C.sqlite3_busy_timeout(c.db, C.int(d/time.Millisecond))
	}
	return
}

// Path returns the full file path of an attached database. An empty string is
// returned for temporary databases.
// [http://www.sqlite.org/c3ref/db_filename.html]
func (c *Conn) Path(db string) string {
	if c.db != nil {
		db += "\x00"
		if path := C.sqlite3_db_filename(c.db, cStr(db)); path != nil {
			return C.GoString(path)
		}
	}
	return ""
}

// Status returns the current and peak values of a connection performance
// counter, specified by one of the DBSTATUS constants. If reset is true, the
// peak value is reset back down to the current value after retrieval.
// [http://www.sqlite.org/c3ref/db_status.html]
func (c *Conn) Status(op int, reset bool) (cur, peak int, err error) {
	if c.db == nil {
		return 0, 0, ErrBadConn
	}
	var cCur, cPeak C.int
	rc := C.sqlite3_db_status(c.db, C.int(op), &cCur, &cPeak, cBool(reset))
	if rc != OK {
		return 0, 0, pkgErr(MISUSE, "invalid connection status op (%d)", op)
	}
	return int(cCur), int(cPeak), nil
}

// Limit changes a per-connection resource usage or performance limit, specified
// by one of the LIMIT constants, returning its previous value. If the new value
// is negative, the limit is left unchanged and its current value is returned.
// [http://www.sqlite.org/c3ref/limit.html]
func (c *Conn) Limit(id, value int) (prev int) {
	if c.db != nil {
		prev = int(C.sqlite3_limit(c.db, C.int(id), C.int(value)))
	}
	return
}

// exec calls sqlite3_exec on sql, which must be a null-terminated C string.
func (c *Conn) exec(sql *C.char) error {
	if rc := C.sqlite3_exec(c.db, sql, nil, nil, nil); rc != OK {
		return errMsg(rc, c.db)
	}
	return nil
}

// Stmt is a prepared statement handle.
//
// SQLite automatically recompiles prepared statements when the database schema
// changes. A query like "SELECT * FROM x" may return a different number of
// columns from one execution to the next if the table is altered. The
// recompilation, if required, only happens at the start of execution (during a
// call to Exec or Query). Statements that are already running (Busy() == true)
// are not recompiled. Thus, the safest way of obtaining column information is
// to call Exec or Query first, followed by NumColumns, Columns, DeclTypes, etc.
// [http://www.sqlite.org/c3ref/stmt.html]
type Stmt struct {
	stmt *C.sqlite3_stmt
}

// Close releases all resources associated with the prepared statement. This
// method can be called at any point in the statement's life cycle.
// [http://www.sqlite.org/c3ref/finalize.html]
func (s *Stmt) Close() error {
	if rc := C.sqlite3_finalize(s.stmt); rc != OK {
		return errStr(rc)
	}
	return nil
}

// Busy returns true if the prepared statement is in the middle of execution
// with a row available for scanning. It is not necessary to reset a busy
// statement before making another call to Exec or Query.
func (s *Stmt) Busy() bool {
	return C.sqlite3_stmt_busy(s.stmt) != 0
}

// ReadOnly returns true if the prepared statement makes no direct changes to
// the content of the database file.
// [http://www.sqlite.org/c3ref/stmt_readonly.html]
func (s *Stmt) ReadOnly() bool {
	return C.sqlite3_stmt_readonly(s.stmt) != 0
}

// BindParameterCount returns the number of bound parameters in the prepared
// statement. This is also the number of arguments required for calling Exec or
// Query without a NamedArgs map.
// [http://www.sqlite.org/c3ref/bind_parameter_count.html]
func (s *Stmt) BindParameterCount() int {
	return int(C.sqlite3_bind_parameter_count(s.stmt))
}

// ColumnCount returns the number of columns produced by the prepared
// statement.
// [http://www.sqlite.org/c3ref/column_count.html]
func (s *Stmt) ColumnCount() int {
	return int(C.sqlite3_column_count(s.stmt))
}

// unnamedVars is assigned to Stmt.varNames if the prepared statement does not
// use named parameters. It just causes s.varNames == nil to evaluate to false.
var unnamedVars = make([]string, 0, 1)

// // Params returns the names of bound parameters in the prepared statement. Nil
// // is returned if the statement does not use named parameters.
// // [http://www.sqlite.org/c3ref/bind_parameter_name.html]
// func (s *Stmt) Params() []string {
// 	if s.varNames == nil {
// 		var names []string
// 		for i := 0; i < s.nVars; i++ {
// 			name := C.sqlite3_bind_parameter_name(s.stmt, C.int(i+1))
// 			if name == nil {
// 				names = unnamedVars
// 				break
// 			}
// 			if names == nil {
// 				names = make([]string, s.nVars)
// 			}
// 			names[i] = C.GoString(name)
// 		}
// 		s.varNames = names
// 	}
// 	if len(s.varNames) == 0 {
// 		return nil // unnamedVars -> nil
// 	}
// 	return s.varNames
// }

// ColumnName returns the name of column produced by the prepared statement.
// The leftmost column is number 0.
// [http://www.sqlite.org/c3ref/column_name.html]
func (s *Stmt) ColumnName(i int) string {
	return C.GoString(C.sqlite3_column_name(s.stmt, C.int(i)))
}

// Columns returns the names of columns produced by the prepared statement.
// [http://www.sqlite.org/c3ref/column_name.html]
func (s *Stmt) ColumnNames() []string {
	nCols := s.ColumnCount()
	names := make([]string, nCols)
	for i := range names {
		names[i] = s.ColumnName(i)
	}
	return names
}

// DeclType returns the type declaration of columns produced by the prepared
// statement. The leftmost column is number 0.
// [http://www.sqlite.org/c3ref/column_decltype.html]
func (s *Stmt) DeclType(i int) string {
	return C.GoString(C.sqlite3_column_decltype(s.stmt, C.int(i)))
}

// DeclTypes returns the type declarations of columns produced by the prepared
// statement.
// [http://www.sqlite.org/c3ref/column_decltype.html]
func (s *Stmt) DeclTypes() []string {
	nCols := s.ColumnCount()
	declTypes := make([]string, nCols)
	for i := range declTypes {
		declTypes[i] = s.DeclType(i)
	}
	return declTypes
}

// DataTypes returns the data type codes of columns in the current row. Possible
// data types are INTEGER, FLOAT, TEXT, BLOB, and NULL. These represent the
// actual storage classes used by SQLite to store each value before any
// conversion. The returned slice should not be modified.
// [http://www.sqlite.org/c3ref/column_blob.html]
func (s *Stmt) DataTypes() []uint8 {
	return s.colTypes()
}

// Exec executes and resets the prepared statement. No rows are returned.
// [http://www.sqlite.org/c3ref/step.html]
func (s *Stmt) Exec(args ...interface{}) error {
	if s.stmt == nil {
		return ErrBadStmt
	}
	err := s.bindArgs(args)

	for {
		hasRow, err := s.Step()
		if err != nil || !hasRow {
			break
		}
	}

	s.Reset()
	return err
}

// Query executes the prepared statement and makes the first returned row
// available for stepping.
func (s *Stmt) Query(args ...interface{}) error {
	err := s.bindArgs(args)
	return err
}

// Scan retrieves data from the current row, storing successive column values
// into successive arguments. If the last argument is an instance of RowMap,
// then all remaining column/value pairs are assigned into the map. The same row
// may be scanned multiple times. Nil arguments are silently skipped.
// [http://www.sqlite.org/c3ref/column_blob.html]
func (s *Stmt) Scan(dst ...interface{}) error {
	n := len(dst)
	if n == 0 {
		return nil
	}
	for i, v := range dst[:n] {
		if v != nil {
			if err := s.scan(C.int(i), v); err != nil {
				return err
			}
		}
	}
	return nil
}

// Reset returns the prepared statement to its initial state, ready to be
// re-executed. This should be done when the remaining rows returned by a query
// are not needed, which releases some resources that would otherwise persist
// until the next call to Exec or Query.
// [http://www.sqlite.org/c3ref/reset.html]
func (s *Stmt) Reset() error {
	if rc := C.sqlite3_reset(s.stmt); rc != OK {
		return errStr(rc)
	}
	return nil
}

// ClearBindings clears the bindings on a prepared statement.  Reset does not
// clear bindings.
// [https://www.sqlite.org/c3ref/clear_bindings.html]
func (s *Stmt) ClearBindings() error {
	if rc := C.sqlite3_clear_bindings(s.stmt); rc != OK {
		return errStr(rc)
	}
	return nil
}

// Status returns the current value of a statement performance counter,
// specified by one of the STMTSTATUS constants. If reset is true, the value is
// reset back down to 0 after retrieval.
// [http://www.sqlite.org/c3ref/stmt_status.html]
func (s *Stmt) Status(op int, reset bool) int {
	return int(C.sqlite3_stmt_status(s.stmt, C.int(op), cBool(reset)))
}

// bindArgs binds either the named arguments or unnamed arguments depending on
// the type of arguments passed
func (s *Stmt) bindArgs(args []interface{}) (err error) {
	if named := namedArgs(args); named != nil {
		err = s.bindNamed(named)
	} else {
		err = s.bindUnnamed(args)
	}
	return
}

// bindNamed binds statement parameters using the name/value pairs in args.
func (s *Stmt) bindNamed(args NamedArgs) error {
	for name, value := range args {
		zName := name + "\x00"
		i := C.sqlite3_bind_parameter_index(s.stmt, cStr(zName))
		if err := s.bind(C.int(i), value, name); err != nil {
			return err
		}
	}
	return nil
}

// bindUnnamed binds statement parameters using successive values in args.
func (s *Stmt) bindUnnamed(args []interface{}) error {
	for i, v := range args {
		if err := s.bind(C.int(i+1), v, ""); err != nil {
			return err
		}
	}
	return nil
}

// bind binds statement parameter i (starting at 1) to the value v. The
// parameter name is only used for error reporting.
func (s *Stmt) bind(i C.int, v interface{}, name string) error {
	if v == nil {
		return nil // Unbound parameters are NULL by default
	}
	var rc C.int
	switch v := v.(type) {
	case int:
		rc = C.sqlite3_bind_int64(s.stmt, i, C.sqlite3_int64(v))
	case int64:
		rc = C.sqlite3_bind_int64(s.stmt, i, C.sqlite3_int64(v))
	case float64:
		rc = C.sqlite3_bind_double(s.stmt, i, C.double(v))
	case bool:
		rc = C.sqlite3_bind_int64(s.stmt, i, C.sqlite3_int64(cBool(v)))
	case string:
		rc = C.bind_text(s.stmt, i, cStr(v), C.int(len(v)), 1)
	case []byte:
		rc = C.bind_blob(s.stmt, i, cBytes(v), C.int(len(v)), 1)
	case time.Time:
		rc = C.sqlite3_bind_int64(s.stmt, i, C.sqlite3_int64(v.Unix()))
	case RawString:
		rc = C.bind_text(s.stmt, i, cStr(string(v)), C.int(len(v)), 0)
	case RawBytes:
		rc = C.bind_blob(s.stmt, i, cBytes(v), C.int(len(v)), 0)
	case ZeroBlob:
		rc = C.sqlite3_bind_zeroblob(s.stmt, i, C.int(v))
	default:
		if name != "" {
			return pkgErr(MISUSE, "unsupported type for %s (%T)", name, v)
		}
		return pkgErr(MISUSE, "unsupported type at index %d (%T)", int(i-1), v)
	}
	if rc != OK {
		return errStr(rc)
	}
	return nil
}

// Step evaluates the next step in the statement's program.  It returns true if
// if a new row of data is ready for processing.
func (s *Stmt) Step() (bool, error) {
	rc := C.sqlite3_step(s.stmt)
	if rc == DONE {
		return false, nil
	} else if rc == ROW {
		return true, nil
	}
	return false, errStr(rc)
}

// // colType returns the data type code of column i in the current row (one of
// // INTEGER, FLOAT, TEXT, BLOB, or NULL). The value becomes undefined after a
// // type conversion, so this method must be called for column i to cache the
// // original value before using any other sqlite3_column_* functions.
// func (s *Stmt) colType(i C.int) byte {
// 	if len(s.colTypes) == 0 {
// 		n := s.nCols
// 		if cap(s.colTypes) < n {
// 			s.colTypes = make([]uint8, n)
// 		} else {
// 			s.colTypes = s.colTypes[:n]
// 		}
// 		C.column_types(s.stmt, (*C.uchar)(cBytes(s.colTypes)), C.int(n))
// 	}
// 	return s.colTypes[i]
// }

func (s *Stmt) colTypes() []byte {
	n := s.ColumnCount()
	colTypes := make([]uint8, n)
	if n != 0 {
		C.column_types(s.stmt, (*C.uchar)(cBytes(colTypes)), C.int(n))
	}
	return colTypes
}

// scan scans the value of column i (starting at 0) into v.
func (s *Stmt) scan(i C.int, v interface{}) error {
	switch v := v.(type) {
	case *int:
		*v = int(C.sqlite3_column_int64(s.stmt, i))
	case *int64:
		*v = int64(C.sqlite3_column_int64(s.stmt, i))
	case *float64:
		*v = float64(C.sqlite3_column_double(s.stmt, i))
	case *bool:
		*v = C.sqlite3_column_int64(s.stmt, i) != 0
	case *string:
		*v = text(s.stmt, i, true)
	case *[]byte:
		*v = blob(s.stmt, i, true)
	case *time.Time:
		*v = time.Unix(int64(C.sqlite3_column_int64(s.stmt, i)), 0)
	case *RawString:
		*v = RawString(text(s.stmt, i, false))
	case *RawBytes:
		*v = RawBytes(blob(s.stmt, i, false))
	case io.Writer:
		if _, err := v.Write(blob(s.stmt, i, false)); err != nil {
			return err
		}
	default:
		return pkgErr(MISUSE, "unscannable type for column %d (%T)", int(i), v)
	}
	// BUG(mxk): If a SQLite memory allocation fails while scanning column
	// values, the error is not reported until the next call to Stmt.Next or
	// Stmt.Close. This behavior may change in the future to check for and
	// return the error immediately from Stmt.Scan.
	return nil
}

// namedArgs checks if args contains named parameter values, and if so, returns
// the NamedArgs map.
func namedArgs(args []interface{}) (named NamedArgs) {
	if len(args) == 1 {
		named, _ = args[0].(NamedArgs)
	}
	return
}

// resize changes len(s) to n, reallocating s if needed.
func resize(s []string, n int) []string {
	if n <= cap(s) {
		return s[:n]
	}
	tmp := make([]string, n)
	copy(tmp, s[:cap(s)])
	return tmp
}

// text returns the value of column i as a UTF-8 string. If copy is false, the
// string will point to memory allocated by SQLite.
func text(stmt *C.sqlite3_stmt, i C.int, copy bool) string {
	p := (*C.char)(unsafe.Pointer(C.sqlite3_column_text(stmt, i)))
	if n := C.sqlite3_column_bytes(stmt, i); n > 0 {
		if copy {
			return C.GoStringN(p, n)
		}
		return goStrN(p, n)
	}
	return ""
}

// blob returns the value of column i as a []byte. If copy is false, the []byte
// will point to memory allocated by SQLite.
func blob(stmt *C.sqlite3_stmt, i C.int, copy bool) []byte {
	if p := C.sqlite3_column_blob(stmt, i); p != nil {
		n := C.sqlite3_column_bytes(stmt, i)
		if copy {
			return C.GoBytes(p, n)
		}
		return goBytes(p, n)
	}
	return nil
}
