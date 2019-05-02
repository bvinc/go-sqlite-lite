// Copyright 2018 The go-sqlite-lite Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// go-sqlite-lite is a SQLite driver for the Go programming language. It is
// designed to be simple, lightweight, performant, understandable,
// unsurprising, debuggable, ergonomic, and fully featured.  This driver does
// not provide a database/sql interface.
package sqlite3

/*
// SQLite compilation options.
// https://www.sqlite.org/compile.html
// https://www.sqlite.org/footprint.html
#cgo CFLAGS: -std=gnu99
#cgo CFLAGS: -Os
#cgo CFLAGS: -DNDEBUG=1
#cgo CFLAGS: -DSQLITE_CORE=1
#cgo CFLAGS: -DSQLITE_ENABLE_API_ARMOR=1
#cgo CFLAGS: -DSQLITE_ENABLE_FTS3=1
#cgo CFLAGS: -DSQLITE_ENABLE_FTS3_PARENTHESIS=1
#cgo CFLAGS: -DSQLITE_ENABLE_FTS4=1
#cgo CFLAGS: -DSQLITE_ENABLE_FTS5=1
#cgo CFLAGS: -DSQLITE_ENABLE_GEOPOLY=1
#cgo CFLAGS: -DSQLITE_ENABLE_JSON1=1
#cgo CFLAGS: -DSQLITE_ENABLE_PREUPDATE_HOOK
#cgo CFLAGS: -DSQLITE_ENABLE_RTREE=1
#cgo CFLAGS: -DSQLITE_ENABLE_SESSION
#cgo CFLAGS: -DSQLITE_ENABLE_STAT4=1
#cgo CFLAGS: -DSQLITE_ENABLE_UNLOCK_NOTIFY
#cgo CFLAGS: -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT=1
#cgo CFLAGS: -DSQLITE_OMIT_AUTOINIT=1
#cgo CFLAGS: -DSQLITE_OMIT_DEPRECATED=1
#cgo CFLAGS: -DSQLITE_OMIT_PROGRESS_CALLBACK=1
#cgo CFLAGS: -DSQLITE_OMIT_LOAD_EXTENSION=1
#cgo CFLAGS: -DSQLITE_OMIT_TRACE=1
#cgo CFLAGS: -DSQLITE_OMIT_UTF16=1
#cgo CFLAGS: -DSQLITE_SOUNDEX=1
#cgo CFLAGS: -DSQLITE_TEMP_STORE=2
#cgo CFLAGS: -DSQLITE_THREADSAFE=2
#cgo CFLAGS: -DSQLITE_USE_ALLOCA=1
#cgo CFLAGS: -DSQLITE_USE_URI=1
#cgo linux LDFLAGS: -lm
#cgo openbsd LDFLAGS: -lm

#cgo linux,!android CFLAGS: -DHAVE_FDATASYNC=1
#cgo linux,!android CFLAGS: -DHAVE_PREAD=1 -DHAVE_PWRITE=1
#cgo darwin CFLAGS: -DHAVE_FDATASYNC=1
#cgo darwin CFLAGS: -DHAVE_PREAD=1 -DHAVE_PWRITE=1

#cgo windows LDFLAGS: -Wl,-Bstatic -lwinpthread -Wl,-Bdynamic

// Fix for BusyTimeout on *nix systems.
#cgo !windows CFLAGS: -DHAVE_USLEEP=1


// Fix "_localtime32(0): not defined" linker error.
#cgo windows,386 CFLAGS: -D_localtime32=localtime

#include <assert.h>
#include <pthread.h>
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
static void set_##x(sqlite3 *db, void *data, int enable) { \
	(enable ? sqlite3_##x(db, go_##x, data) : sqlite3_##x(db, 0, 0)); \
}

// util.go exports.
int go_busy_handler(void*,int);
int go_commit_hook(void*);
void go_rollback_hook(void*);
void go_update_hook(void* data, int op,const char *db, const char *tbl, sqlite3_int64 row);
int go_set_authorizer(void* data, int op, const char *arg1, const char *arg2, const char *db, const char *entity);

SET(busy_handler)
SET(commit_hook)
SET(rollback_hook)
SET(update_hook)
SET(set_authorizer)

// A pointer to an instance of this structure is passed as the user-context
// pointer when registering for an unlock-notify callback.
typedef struct UnlockNotification UnlockNotification;
struct UnlockNotification {
    int fired;              // True after unlock event has occurred
    pthread_cond_t cond;    // Condition variable to wait on
    pthread_mutex_t mutex;  // Mutex to protect structure
};

// This function is an unlock-notify callback registered with SQLite.
static void unlock_notify_cb(void **apArg, int nArg){
    int i;
    for(i=0; i<nArg; i++){
        UnlockNotification *p = (UnlockNotification *)apArg[i];
        pthread_mutex_lock(&p->mutex);
        p->fired = 1;
        pthread_cond_signal(&p->cond);
        pthread_mutex_unlock(&p->mutex);
    }
}

// This function assumes that an SQLite API call (either sqlite3_prepare_v2()
// or sqlite3_step()) has just returned SQLITE_LOCKED. The argument is the
// associated database connection.
//
// This function calls sqlite3_unlock_notify() to register for an
// unlock-notify callback, then blocks until that callback is delivered
// and returns SQLITE_OK. The caller should then retry the failed operation.
//
// Or, if sqlite3_unlock_notify() indicates that to block would deadlock
// the system, then this function returns SQLITE_LOCKED immediately. In
// this case the caller should not retry the operation and should roll
// back the current transaction (if any).
static int wait_for_unlock_notify(sqlite3 *db){
    int rc;
    UnlockNotification un;

    // Initialize the UnlockNotification structure.
    un.fired = 0;
    pthread_mutex_init(&un.mutex, 0);
    pthread_cond_init(&un.cond, 0);

    // Register for an unlock-notify callback.
    rc = sqlite3_unlock_notify(db, unlock_notify_cb, (void *)&un);
    assert( rc==SQLITE_LOCKED || rc==SQLITE_OK );

    // The call to sqlite3_unlock_notify() always returns either SQLITE_LOCKED
    // or SQLITE_OK.
    //
    // If SQLITE_LOCKED was returned, then the system is deadlocked. In this
    // case this function needs to return SQLITE_LOCKED to the caller so
    // that the current transaction can be rolled back. Otherwise, block
    // until the unlock-notify callback is invoked, then return SQLITE_OK.
    if( rc==SQLITE_OK ){
        pthread_mutex_lock(&un.mutex);
        if( !un.fired ){
            pthread_cond_wait(&un.cond, &un.mutex);
        }
        pthread_mutex_unlock(&un.mutex);
    }

    // Destroy the mutex and condition variables.
    pthread_cond_destroy(&un.cond);
    pthread_mutex_destroy(&un.mutex);

    return rc;
}

// This function is a wrapper around the SQLite function sqlite3_step().
// It functions in the same way as step(), except that if a required
// shared-cache lock cannot be obtained, this function may block waiting for
// the lock to become available. In this scenario the normal API step()
// function always returns SQLITE_LOCKED.
//
// If this function returns SQLITE_LOCKED, the caller should rollback
// the current transaction (if any) and try again later. Otherwise, the
// system may become deadlocked.
int sqlite3_blocking_step(sqlite3 *db, sqlite3_stmt *pStmt){
    int rc;
    for (;;) {
		rc = sqlite3_step(pStmt);
        if( rc != SQLITE_LOCKED ) {
            break;
        }
        if( sqlite3_extended_errcode(db) != SQLITE_LOCKED_SHAREDCACHE ) {
            break;
        }
        rc = wait_for_unlock_notify(sqlite3_db_handle(pStmt));
        if( rc!=SQLITE_OK ) {
			break;
		}
		sqlite3_reset(pStmt);
	}
	return rc;
}

// This function is a wrapper around the SQLite function sqlite3_prepare_v2().
// It functions in the same way as prepare_v2(), except that if a required
// shared-cache lock cannot be obtained, this function may block waiting for
// the lock to become available. In this scenario the normal API prepare_v2()
// function always returns SQLITE_LOCKED.
//
// If this function returns SQLITE_LOCKED, the caller should rollback
// the current transaction (if any) and try again later. Otherwise, the
// system may become deadlocked.
int sqlite3_blocking_prepare_v2(
  sqlite3 *db,              // Database handle.
  const char *zSql,         // UTF-8 encoded SQL statement.
  int nSql,                 // Length of zSql in bytes.
  sqlite3_stmt **ppStmt,    // OUT: A pointer to the prepared statement
  const char **pz           // OUT: End of parsed string
){
	int rc;
	for (;;) {
		rc = sqlite3_prepare_v2(db, zSql, nSql, ppStmt, pz);
		if( rc != SQLITE_LOCKED ){
			break;
		}
        if( sqlite3_extended_errcode(db) != SQLITE_LOCKED_SHAREDCACHE ) {
            break;
        }
        rc = wait_for_unlock_notify(db);
        if( rc!=SQLITE_OK ) {
			break;
		}
	}
    return rc;
}
*/
import "C"

import (
	"fmt"
	"io"
	"os"
	"time"
	"unsafe"
)

// initErr indicates a SQLite initialization error, which disables this package.
var initErr error

// emptyByteSlice is what we might return on ColumnRawBytes to avoid
// allocation.  Since they are not allowed to modify the slice, this is safe.
var emptyByteSlice = []byte{}

var busyRegistry = newRegistry()
var commitRegistry = newRegistry()
var rollbackRegistry = newRegistry()
var updateRegistry = newRegistry()
var authorizerRegistry = newRegistry()

func init() {
	// Initialize SQLite (required with SQLITE_OMIT_AUTOINIT).
	// https://www.sqlite.org/c3ref/initialize.html
	if rc := C.sqlite3_initialize(); rc != OK {
		initErr = errStr(rc)
		return
	}

	// Use the same temporary directory as Go.
	// https://www.sqlite.org/c3ref/temp_directory.html
	tmp := os.TempDir() + "\x00"
	C.set_temp_dir(cStr(tmp))
}

// Conn is a connection handle, which may have multiple databases attached to it
// by using the ATTACH SQL statement.
// https://www.sqlite.org/c3ref/sqlite3.html
type Conn struct {
	db *C.sqlite3

	busyIdx       int
	commitIdx     int
	rollbackIdx   int
	updateIdx     int
	authorizerIdx int
}

// Open creates a new connection to a SQLite database. The name can be 1) a path
// to a file, which is created if it does not exist, 2) a URI using the syntax
// described at https://www.sqlite.org/uri.html, 3) the string ":memory:",
// which creates a temporary in-memory database, or 4) an empty string, which
// creates a temporary on-disk database (deleted when closed) in the directory
// returned by os.TempDir(). Flags to Open can optionally be provided.  If no
// flags are provided, the default flags of OPEN_READWRITE|OPEN_CREATE are
// used.
// https://www.sqlite.org/c3ref/open.html
func Open(name string, flagArgs ...int) (*Conn, error) {
	if len(flagArgs) > 1 {
		return nil, pkgErr(MISUSE, "too many arguments provided to Open")
	}

	if initErr != nil {
		return nil, initErr
	}
	name += "\x00"

	var db *C.sqlite3
	flags := C.SQLITE_OPEN_READWRITE | C.SQLITE_OPEN_CREATE
	if len(flagArgs) == 1 {
		flags = flagArgs[0]
	}
	rc := C.sqlite3_open_v2(cStr(name), &db, C.int(flags), nil)
	if rc != OK {
		err := libErr(rc, db)
		C.sqlite3_close(db)
		return nil, err
	}
	c := &Conn{db: db}
	C.sqlite3_extended_result_codes(db, 1)
	return c, nil
}

// Close releases all resources associated with the connection. If any prepared
// statements, incremental I/O operations, or backup operations are still
// active, the connection becomes an unusable "zombie" and is closed after all
// remaining statements and operations are destroyed. A BUSY error code is
// returned if the connection is left in this "zombie" status, which may
// indicate a programming error where some previously allocated resource is not
// properly released.
// https://www.sqlite.org/c3ref/close.html
func (c *Conn) Close() error {
	if db := c.db; db != nil {
		c.db = nil

		// Unregister all of the globally registered callbacks
		if c.busyIdx != 0 {
			busyRegistry.unregister(c.busyIdx)
		}
		if c.commitIdx != 0 {
			commitRegistry.unregister(c.commitIdx)
		}
		if c.rollbackIdx != 0 {
			rollbackRegistry.unregister(c.rollbackIdx)
		}
		if c.updateIdx != 0 {
			updateRegistry.unregister(c.updateIdx)
		}
		if c.authorizerIdx != 0 {
			authorizerRegistry.unregister(c.authorizerIdx)
		}

		if rc := C.sqlite3_close(db); rc != OK {
			err := libErr(rc, db)
			if rc == BUSY {
				C.sqlite3_close_v2(db)
			}
			return err
		}
	}
	return nil
}

// Prepare compiles the first statement in sql. Any remaining text after the
// first statement is saved in s.Tail.  This function may return a nil stmt and
// a nil error, if the sql string contains nothing to do.  For convenience,
// this function can also bind arguments to the returned statement.  If an
// error occurs during binding, the statement is closed/finalized and the error
// is returned.
// https://www.sqlite.org/c3ref/prepare.html
func (c *Conn) Prepare(sql string, args ...interface{}) (s *Stmt, err error) {
	zSQL := sql + "\x00"

	var stmt *C.sqlite3_stmt
	var cTail *C.char
	rc := C.sqlite3_blocking_prepare_v2(c.db, cStr(zSQL), -1, &stmt, &cTail)
	if rc != OK {
		return nil, libErr(rc, c.db)
	}

	if stmt == nil {
		return nil, nil
	}

	var tail string
	if cTail != nil {
		n := cStrOffset(zSQL, cTail)
		if n >= 0 && n < len(sql) {
			tail = sql[n:]
		}
	}

	s = &Stmt{stmt: stmt, db: c.db, Tail: tail}

	if len(args) > 0 {
		if err = s.Bind(args...); err != nil {
			s.Close()
			return nil, err
		}
	}

	return s, nil
}

// Exec is a convenience function that will call sqlite3_exec if no argument
// are given.  If arguments are given, it's simply a convenient way to
// Prepare a statement, Bind arguments, Step the statement to completion,
// and Close/finalize the statement.
// https://www.sqlite.org/c3ref/exec.html
func (c *Conn) Exec(sql string, args ...interface{}) error {
	// Fast path via sqlite3_exec, which doesn't support parameter binding
	if len(args) == 0 {
		sql += "\x00"
		return c.exec(cStr(sql))
	}

	s, err := c.Prepare(sql)
	if err != nil {
		return err
	}
	if s == nil {
		return nil
	}
	defer s.Close()

	if err = s.Bind(args...); err != nil {
		return err
	}

	if err = s.StepToCompletion(); err != nil {
		return err
	}

	return nil
}

// Begin starts a new deferred transaction. This is equivalent to
// c.Exec("BEGIN")
// https://www.sqlite.org/lang_transaction.html
func (c *Conn) Begin() error {
	return c.exec(cStr("BEGIN\x00"))
}

// BeginImmediate starts a new immediate transaction. This is equivalent to
// c.Exec("BEGIN IMMEDIATE")
// https://www.sqlite.org/lang_transaction.html
func (c *Conn) BeginImmediate() error {
	return c.exec(cStr("BEGIN IMMEDIATE\x00"))
}

// BeginExclusive starts a new exclusive transaction. This is equivalent to
// c.Exec("BEGIN EXCLUSIVE")
// https://www.sqlite.org/lang_transaction.html
func (c *Conn) BeginExclusive() error {
	return c.exec(cStr("BEGIN EXCLUSIVE\x00"))
}

// Commit saves all changes made within a transaction to the database.
func (c *Conn) Commit() error {
	return c.exec(cStr("COMMIT\x00"))
}

// Rollback aborts the current transaction without saving any changes.
func (c *Conn) Rollback() error {
	return c.exec(cStr("ROLLBACK\x00"))
}

// WithTx is a convenience method that begins a deferred transaction, calls the
// function f, and will commit the transaction if f does not return an error,
// and will roll back the transaction if f does return an error.
func (c *Conn) WithTx(f func() error) error {
	if err := c.Begin(); err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}

	// Perform work inside the transaction
	err := f()
	if err != nil {
		err2 := c.Rollback()
		if err2 == nil {
			return err
		}
		return fmt.Errorf("%v, additionally rolling back transaction failed: %v", err, err2)
	}

	if err = c.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}
	return nil
}

// WithTxImmediate is a convenience method that begins an immediate
// transaction, calls the function f, and will commit the transaction if f does
// not return an error, and will roll back the transaction if f does return an
// error.
func (c *Conn) WithTxImmediate(f func() error) error {
	if err := c.BeginImmediate(); err != nil {
		return fmt.Errorf("failed to begin immediate transaction: %v", err)
	}

	// Perform work inside the transaction
	err := f()
	if err != nil {
		err2 := c.Rollback()
		if err2 == nil {
			return err
		}
		return fmt.Errorf("%v, additionally rolling back transaction failed: %v", err, err2)
	}

	if err = c.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}
	return nil
}

// WithTxExclusive is a convenience method that begins a exclusive transaction,
// calls the function f, and will commit the transaction if f does not return
// an error, and will roll back the transaction if f does return an error.
func (c *Conn) WithTxExclusive(f func() error) error {
	if err := c.BeginExclusive(); err != nil {
		return fmt.Errorf("failed to begin exclusive transaction: %v", err)
	}

	// Perform work inside the transaction
	err := f()
	if err != nil {
		err2 := c.Rollback()
		if err2 == nil {
			return err
		}
		return fmt.Errorf("%v, additionally rolling back transaction failed: %v", err, err2)
	}

	if err = c.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}
	return nil
}

// Interrupt causes any pending database operation to abort and return at its
// earliest opportunity. It is safe to call this method from a goroutine
// different from the one that is currently running the database operation, but
// it is not safe to call this method on a connection that might close before
// the call returns.
// https://www.sqlite.org/c3ref/interrupt.html
func (c *Conn) Interrupt() {
	if db := c.db; db != nil {
		C.sqlite3_interrupt(db)
	}
}

// AutoCommit returns true if the database connection is in auto-commit mode
// (i.e. outside of an explicit transaction started by BEGIN).
// https://www.sqlite.org/c3ref/get_autocommit.html
func (c *Conn) AutoCommit() bool {
	return C.sqlite3_get_autocommit(c.db) != 0
}

// LastInsertRowID returns the ROWID of the most recent successful INSERT
// statement.
// https://www.sqlite.org/c3ref/last_insert_rowid.html
func (c *Conn) LastInsertRowID() int64 {
	return int64(C.sqlite3_last_insert_rowid(c.db))
}

// Changes returns the number of rows that were changed, inserted, or deleted
// by the most recent statement. Auxiliary changes caused by triggers or
// foreign key actions are not counted.
// https://www.sqlite.org/c3ref/changes.html
func (c *Conn) Changes() int {
	return int(C.sqlite3_changes(c.db))
}

// TotalChanges returns the number of rows that were changed, inserted, or
// deleted since the database connection was opened, including changes caused by
// trigger and foreign key actions.
// https://www.sqlite.org/c3ref/total_changes.html
func (c *Conn) TotalChanges() int {
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
// https://www.sqlite.org/backup.html
func (c *Conn) Backup(srcName string, dst *Conn, dstName string) (*Backup, error) {
	if c == dst || dst == nil {
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
// https://www.sqlite.org/c3ref/blob_open.html
func (c *Conn) BlobIO(db, tbl, col string, row int64, rw bool) (*BlobIO, error) {
	return newBlobIO(c, db, tbl, col, row, rw)
}

// BusyTimeout enables the built-in busy handler, which retries the table
// locking operation for the specified duration before aborting. The busy
// handler is disabled if d is negative or zero.
// https://www.sqlite.org/c3ref/busy_timeout.html
func (c *Conn) BusyTimeout(d time.Duration) {
	C.sqlite3_busy_timeout(c.db, C.int(d/time.Millisecond))
}

// BusyFunc registers a function that is invoked by SQLite when it is unable to
// acquire a lock on a table. The function f should return true to make another
// lock acquisition attempt, or false to let the operation fail with BUSY or
// IOERR_BLOCKED error code.
// https://www.sqlite.org/c3ref/busy_handler.html
func (c *Conn) BusyFunc(f BusyFunc) {
	idx := busyRegistry.register(f)
	c.busyIdx = idx
	C.set_busy_handler(c.db, unsafe.Pointer(&c.busyIdx), cBool(f != nil))
}

// FileName returns the full file path of an attached database. An empty string
// is returned for temporary databases.
// https://www.sqlite.org/c3ref/db_filename.html
func (c *Conn) FileName(db string) string {
	db += "\x00"
	if path := C.sqlite3_db_filename(c.db, cStr(db)); path != nil {
		return C.GoString(path)
	}
	return ""
}

// Status returns the current and peak values of a connection performance
// counter, specified by one of the DBSTATUS constants. If reset is true, the
// peak value is reset back down to the current value after retrieval.
// https://www.sqlite.org/c3ref/db_status.html
func (c *Conn) Status(op int, reset bool) (cur, peak int, err error) {
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
// https://www.sqlite.org/c3ref/limit.html
func (c *Conn) Limit(id, value int) (prev int) {
	prev = int(C.sqlite3_limit(c.db, C.int(id), C.int(value)))
	return
}

// exec calls sqlite3_exec on sql, which must be a null-terminated C string.
func (c *Conn) exec(sql *C.char) error {
	if rc := C.sqlite3_exec(c.db, sql, nil, nil, nil); rc != OK {
		return libErr(rc, c.db)
	}
	return nil
}

// Stmt is a prepared statement handle.
// https://www.sqlite.org/c3ref/stmt.html
type Stmt struct {
	Tail string

	stmt *C.sqlite3_stmt
	db   *C.sqlite3
	// Data type codes for all columns in the current row.  This is
	// unfortunately absolutely necessary to keep around.  Column types are
	// required in order to differentiate NULL from error conditions, and
	// sqlite3_column_type is undefined after a type conversion happens.
	colTypes     []uint8
	haveColTypes bool
}

// Close releases all resources associated with the prepared statement. This
// method can be called at any point in the statement's life cycle.
// https://www.sqlite.org/c3ref/finalize.html
func (s *Stmt) Close() error {
	rc := C.sqlite3_finalize(s.stmt)
	s.stmt = nil
	if rc != OK {
		return libErr(rc, s.db)
	}
	return nil
}

// Busy returns true if the prepared statement is in the middle of execution
// with a row available for scanning.
func (s *Stmt) Busy() bool {
	return C.sqlite3_stmt_busy(s.stmt) != 0
}

// ReadOnly returns true if the prepared statement makes no direct changes to
// the content of the database file.
// https://www.sqlite.org/c3ref/stmt_readonly.html
func (s *Stmt) ReadOnly() bool {
	return C.sqlite3_stmt_readonly(s.stmt) != 0
}

// BindParameterCount returns the number of SQL parameters in the prepared
// statement.
// https://www.sqlite.org/c3ref/bind_parameter_count.html
func (s *Stmt) BindParameterCount() int {
	return int(C.sqlite3_bind_parameter_count(s.stmt))
}

// ColumnCount returns the number of columns produced by the prepared
// statement.
// https://www.sqlite.org/c3ref/column_count.html
func (s *Stmt) ColumnCount() int {
	return int(C.sqlite3_column_count(s.stmt))
}

// // Params returns the names of bound parameters in the prepared statement. Nil
// // is returned if the statement does not use named parameters.
// // https://www.sqlite.org/c3ref/bind_parameter_name.html
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
// https://www.sqlite.org/c3ref/column_name.html
func (s *Stmt) ColumnName(i int) string {
	return C.GoString(C.sqlite3_column_name(s.stmt, C.int(i)))
}

// ColumnNames returns the names of columns produced by the prepared
// statement.
// https://www.sqlite.org/c3ref/column_name.html
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
// https://www.sqlite.org/c3ref/column_decltype.html
func (s *Stmt) DeclType(i int) string {
	return C.GoString(C.sqlite3_column_decltype(s.stmt, C.int(i)))
}

// DeclTypes returns the type declarations of columns produced by the prepared
// statement.
// https://www.sqlite.org/c3ref/column_decltype.html
func (s *Stmt) DeclTypes() []string {
	nCols := s.ColumnCount()
	declTypes := make([]string, nCols)
	for i := range declTypes {
		declTypes[i] = s.DeclType(i)
	}
	return declTypes
}

// Exec is a convenience method that binds the given arguments to the statement
// then steps the statement to completion and resets the prepared statement. No
// rows are returned.  Reset is always called, even in error cases. Note that
// bindings are not cleared.
func (s *Stmt) Exec(args ...interface{}) error {
	err := s.Bind(args...)
	if err != nil {
		s.Reset()
		return err
	}

	if err = s.StepToCompletion(); err != nil {
		s.Reset()
		return err
	}

	if err = s.Reset(); err != nil {
		return err
	}

	return err
}

// Bind binds either the named arguments or unnamed arguments depending on the
// type of arguments passed
func (s *Stmt) Bind(args ...interface{}) error {
	for i, v := range args {
		var rc C.int
		if v == nil {
			rc = C.sqlite3_bind_null(s.stmt, C.int(i+1))
			if rc != OK {
				return errStr(rc)
			}
			continue
		}
		switch v := v.(type) {
		case int:
			rc = C.sqlite3_bind_int64(s.stmt, C.int(i+1), C.sqlite3_int64(v))
		case int64:
			rc = C.sqlite3_bind_int64(s.stmt, C.int(i+1), C.sqlite3_int64(v))
		case float64:
			rc = C.sqlite3_bind_double(s.stmt, C.int(i+1), C.double(v))
		case bool:
			rc = C.sqlite3_bind_int64(s.stmt, C.int(i+1), C.sqlite3_int64(cBool(v)))
		case string:
			rc = C.bind_text(s.stmt, C.int(i+1), cStr(v), C.int(len(v)), 1)
		case []byte:
			// This is a strange case.  nil byte arrays should be treated as inserting NULL
			if []byte(v) == nil {
				rc = C.sqlite3_bind_null(s.stmt, C.int(i+1))
			} else {
				rc = C.bind_blob(s.stmt, C.int(i+1), cBytes(v), C.int(len(v)), 1)
			}
		case RawString:
			rc = C.bind_text(s.stmt, C.int(i+1), cStr(string(v)), C.int(len(v)), 0)
		case RawBytes:
			rc = C.bind_blob(s.stmt, C.int(i+1), cBytes(v), C.int(len(v)), 0)
		case ZeroBlob:
			rc = C.sqlite3_bind_zeroblob(s.stmt, C.int(i+1), C.int(v))
		case NamedArgs:
			if i != 0 || len(args) != 1 {
				return pkgErr(MISUSE, "NamedArgs must be used as the only argument to Bind()")
			}
			return s.bindNamed(v)
		default:
			return pkgErr(MISUSE, "unsupported type at index %d (%T)", i, v)
		}
		if rc != OK {
			return errStr(rc)
		}
	}
	return nil
}

// Scan retrieves data from the current row, storing successive column values
// into successive arguments. The same row may be scanned multiple times. Nil
// arguments are silently skipped.
// https://www.sqlite.org/c3ref/column_blob.html
func (s *Stmt) Scan(dst ...interface{}) error {
	n := len(dst)
	if n == 0 {
		return nil
	}

	for i, v := range dst[:n] {
		if v != nil {
			if err := s.scan(i, v); err != nil {
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
// https://www.sqlite.org/c3ref/reset.html
func (s *Stmt) Reset() error {
	s.colTypes = s.colTypes[:0]
	s.haveColTypes = false
	if rc := C.sqlite3_reset(s.stmt); rc != OK {
		return errStr(rc)
	}
	return nil
}

// ClearBindings clears the bindings on a prepared statement.  Reset does not
// clear bindings.
// https://www.sqlite.org/c3ref/clear_bindings.html
func (s *Stmt) ClearBindings() error {
	if rc := C.sqlite3_clear_bindings(s.stmt); rc != OK {
		return errStr(rc)
	}
	return nil
}

// Status returns the current value of a statement performance counter,
// specified by one of the STMTSTATUS constants. If reset is true, the value is
// reset back down to 0 after retrieval.
// https://www.sqlite.org/c3ref/stmt_status.html
func (s *Stmt) Status(op int, reset bool) int {
	return int(C.sqlite3_stmt_status(s.stmt, C.int(op), cBool(reset)))
}

// bindNamed binds statement parameters using the name/value pairs in args.
func (s *Stmt) bindNamed(args NamedArgs) error {
	for name, v := range args {
		zName := name + "\x00"
		i := C.sqlite3_bind_parameter_index(s.stmt, cStr(zName))
		if i == 0 {
			// The name wasn't found in the prepared statement
			continue
		}

		var rc C.int
		if v == nil {
			rc = C.sqlite3_bind_null(s.stmt, i)
			if rc != OK {
				return errStr(rc)
			}
			continue
		}
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
			// This is a strange case.  nil byte arrays should be treated as inserting NULL
			if []byte(v) == nil {
				rc = C.sqlite3_bind_null(s.stmt, i)
			} else {
				rc = C.bind_blob(s.stmt, i, cBytes(v), C.int(len(v)), 1)
			}
		case RawString:
			rc = C.bind_text(s.stmt, i, cStr(string(v)), C.int(len(v)), 0)
		case RawBytes:
			rc = C.bind_blob(s.stmt, i, cBytes(v), C.int(len(v)), 0)
		case ZeroBlob:
			rc = C.sqlite3_bind_zeroblob(s.stmt, i, C.int(v))
		default:
			return pkgErr(MISUSE, "unsupported type for %s (%T)", name, v)
		}
		if rc != OK {
			return errStr(rc)
		}
	}
	return nil
}

// Step evaluates the next step in the statement's program.  It returns true if
// if a new row of data is ready for processing.
// https://www.sqlite.org/c3ref/step.html
func (s *Stmt) Step() (bool, error) {
	s.colTypes = s.colTypes[:0]
	s.haveColTypes = false
	rc := C.sqlite3_blocking_step(s.db, s.stmt)
	if rc == ROW {
		return true, nil
	}
	if rc == DONE {
		return false, nil
	}
	return false, errStr(rc)
}

// StepToCompletion is a convenience method that repeatedly calls Step until no
// more rows are returned or an error occurs.
// https://www.sqlite.org/c3ref/step.html
func (s *Stmt) StepToCompletion() error {
	s.colTypes = s.colTypes[:0]
	s.haveColTypes = false
	for {
		rc := C.sqlite3_blocking_step(s.db, s.stmt)
		if rc == ROW {
			continue
		} else if rc == DONE {
			break
		} else {
			return errStr(rc)
		}
	}
	return nil
}

// assureColTypes asks SQLite for column types for the current row if we don't
// currently have them.  We must cache the column types since they're important
// for error detection and their values are undefined after type conversions.
func (s *Stmt) assureColTypes() {
	if !s.haveColTypes {
		n := s.ColumnCount()
		if cap(s.colTypes) < n {
			s.colTypes = make([]uint8, n)
		} else {
			s.colTypes = s.colTypes[:n]
		}
		C.column_types(s.stmt, (*C.uchar)(cBytes(s.colTypes)), C.int(n))
		s.haveColTypes = true
	}
}

// ColumnType returns the data type code of column i in the current row (one of
// INTEGER, FLOAT, TEXT, BLOB, or NULL). Unlike SQLite, these values are still
// defined even after type conversion.
// https://www.sqlite.org/c3ref/column_blob.html
func (s *Stmt) ColumnType(i int) byte {
	s.assureColTypes()
	return s.colTypes[i]
}

// ColumnTypes returns the data type codes of columns in the current row.
// Possible data types are INTEGER, FLOAT, TEXT, BLOB, and NULL. These
// represent the actual storage classes used by SQLite to store each value.
// Unlike SQLite, these values are still defined after type conversion.
// https://www.sqlite.org/c3ref/column_blob.html
func (s *Stmt) ColumnTypes() []byte {
	s.assureColTypes()
	return s.colTypes
}

// scan scans the value of column i (starting at 0) into v.
func (s *Stmt) scan(i int, v interface{}) error {
	var err error
	switch v := v.(type) {
	case *interface{}:
		return s.scanDynamic(i, v)
	case *int:
		*v, _, err = s.ColumnInt(i)
	case *int64:
		*v, _, err = s.ColumnInt64(i)
	case *float64:
		*v, _, err = s.ColumnDouble(i)
	case *bool:
		var b int64
		b, _, err = s.ColumnInt64(i)
		*v = b != 0
	case *string:
		*v, _, err = s.ColumnText(i)
	case *[]byte:
		*v, err = s.ColumnBlob(i)
	case *RawString:
		*v, _, err = s.ColumnRawString(i)
	case *RawBytes:
		*v, err = s.ColumnRawBytes(i)
	case io.Writer:
		_, err = v.Write(blob(s.stmt, C.int(i), false))
	default:
		return pkgErr(MISUSE, "unscannable type for column %d (%T)", int(i), v)
	}
	if err != nil {
		return err
	}
	return nil
}

// scanDynamic scans the value of column i (starting at 0) into v, using the
// column's data type and declaration to select an appropriate representation.
func (s *Stmt) scanDynamic(i int, v *interface{}) error {
	var err error
	switch typ := s.ColumnType(int(i)); typ {
	case INTEGER:
		*v, _, err = s.ColumnInt64(i)
	case FLOAT:
		*v, _, err = s.ColumnDouble(i)
	case TEXT:
		*v, _, err = s.ColumnText(i)
	case BLOB:
		*v, err = s.ColumnBlob(i)
	case NULL:
		*v = nil
	default:
		*v = nil
		return pkgErr(ERROR, "unknown column type (%d)", typ)
	}
	if err != nil {
		return err
	}
	return nil
}

// ColumnBlob gets the blob value of column i (starting at 0).  If the blob
// is NULL, then nil is returned.
// https://www.sqlite.org/c3ref/column_blob.html
func (s *Stmt) ColumnBlob(i int) (val []byte, err error) {
	s.assureColTypes()
	if i >= len(s.colTypes) {
		return nil, errStr(RANGE)
	}
	if s.colTypes[i] == NULL {
		return nil, nil
	}

	n := C.sqlite3_column_bytes(s.stmt, C.int(i))
	if n == 0 {
		return []byte{}, nil
	}

	p := C.sqlite3_column_blob(s.stmt, C.int(i))
	if p == nil {
		rc := C.sqlite3_errcode(s.db)
		return nil, libErr(rc, s.db)
	}

	// Copy the blob
	return C.GoBytes(p, n), nil
}

// ColumnDouble gets the double value of column i (starting at 0).  If the
// value is NULL, then val is set to 0.0 and ok is set to false.
// https://www.sqlite.org/c3ref/column_blob.html
func (s *Stmt) ColumnDouble(i int) (val float64, ok bool, err error) {
	s.assureColTypes()
	if i >= len(s.colTypes) {
		return 0.0, false, errStr(RANGE)
	}
	if s.colTypes[i] == NULL {
		return 0.0, false, nil
	}

	val = float64(C.sqlite3_column_double(s.stmt, C.int(i)))
	return val, true, nil
}

// ColumnInt gets the int value of column i (starting at 0).  If the value is
// NULL, then val is set to 0 and ok is set to false.
// https://www.sqlite.org/c3ref/column_blob.html
func (s *Stmt) ColumnInt(i int) (val int, ok bool, err error) {
	s.assureColTypes()
	if i >= len(s.colTypes) {
		return 0, false, errStr(RANGE)
	}
	if s.colTypes[i] == NULL {
		return 0, false, nil
	}

	val = int(C.sqlite3_column_int64(s.stmt, C.int(i)))
	return val, true, nil
}

// ColumnInt64 gets the int64 value of column i (starting at 0).  If the
// value is NULL, then val is set to 0 and ok is set to false.
// https://www.sqlite.org/c3ref/column_blob.html
func (s *Stmt) ColumnInt64(i int) (val int64, ok bool, err error) {
	s.assureColTypes()
	if i >= len(s.colTypes) {
		return 0, false, errStr(RANGE)
	}
	if s.colTypes[i] == NULL {
		return 0, false, nil
	}

	val = int64(C.sqlite3_column_int64(s.stmt, C.int(i)))
	return val, true, nil
}

// ColumnText gets the text value of column i (starting at 0).  If the value is
// NULL, then val is set to "" and ok is set to false.
// https://www.sqlite.org/c3ref/column_blob.html
func (s *Stmt) ColumnText(i int) (val string, ok bool, err error) {
	s.assureColTypes()
	if i >= len(s.colTypes) {
		return "", false, errStr(RANGE)
	}
	if s.colTypes[i] == NULL {
		return "", false, nil
	}

	n := C.sqlite3_column_bytes(s.stmt, C.int(i))
	if n == 0 {
		return "", true, nil
	}

	p := (*C.char)(unsafe.Pointer(C.sqlite3_column_text(s.stmt, C.int(i))))
	if p == nil {
		rc := C.sqlite3_errcode(s.db)
		return "", false, libErr(rc, s.db)
	}

	// Copy the string
	return C.GoStringN(p, n), true, nil
}

// ColumnBytes gets the size of a blob or UTF-8 text in column i (starting at
// 0).
// https://www.sqlite.org/c3ref/column_blob.html
func (s *Stmt) ColumnBytes(i int) (int, error) {
	s.assureColTypes()
	if i >= len(s.colTypes) {
		return 0, errStr(RANGE)
	}

	return int(C.sqlite3_column_bytes(s.stmt, C.int(i))), nil
}

// ColumnRawBytes gets the blob value of column i (starting at 0).  CAUTION:
// The internal []byte pointer is set to reference memory belonging to SQLite.
// The memory remains valid until another method is called on the Stmt object
// and should not be modified.  This is similar to ColumnBlob, except faster
// and less safe.  Consider using ColumnBlob unless performance is critical.
// https://www.sqlite.org/c3ref/column_blob.html
func (s *Stmt) ColumnRawBytes(i int) (val RawBytes, err error) {
	s.assureColTypes()
	if i >= len(s.colTypes) {
		return nil, errStr(RANGE)
	}
	if s.colTypes[i] == NULL {
		return nil, nil
	}

	n := C.sqlite3_column_bytes(s.stmt, C.int(i))
	if n == 0 {
		return emptyByteSlice, nil
	}

	p := C.sqlite3_column_blob(s.stmt, C.int(i))
	if p == nil {
		rc := C.sqlite3_errcode(s.db)
		return nil, libErr(rc, s.db)
	}

	// Don't copy the blob
	return goBytes(p, n), nil
}

// ColumnRawString gets the text value of column i (starting at 0).  CAUTION:
// The internal string pointer is set to reference memory belonging to SQLite.
// The memory remains valid until another method is called on the Stmt object
// and should not be modified.  This is similar to ColumnText, except faster
// and less safe.  Consider using ColumnText unless performance is critical.
// https://www.sqlite.org/c3ref/column_blob.html
func (s *Stmt) ColumnRawString(i int) (val RawString, ok bool, err error) {
	s.assureColTypes()
	if i >= len(s.colTypes) {
		return "", false, errStr(RANGE)
	}
	if s.colTypes[i] == NULL {
		return "", false, nil
	}

	n := C.sqlite3_column_bytes(s.stmt, C.int(i))
	if n == 0 {
		return "", true, nil
	}

	p := (*C.char)(unsafe.Pointer(C.sqlite3_column_text(s.stmt, C.int(i))))
	if p == nil {
		rc := C.sqlite3_errcode(s.db)
		return "", false, libErr(rc, s.db)
	}

	// Don't copy the string
	return RawString(goStrN(p, n)), true, nil
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

// CommitFunc registers a function that is invoked by SQLite before a
// transaction is committed. It returns the previous commit handler, if any. If
// the function f returns true, the transaction is rolled back instead, causing
// the rollback handler to be invoked, if one is registered.
// https://www.sqlite.org/c3ref/commit_hook.html
func (c *Conn) CommitFunc(f CommitFunc) (prev CommitFunc) {
	idx := commitRegistry.register(f)
	prevIdx := c.commitIdx
	c.commitIdx = idx
	C.set_commit_hook(c.db, unsafe.Pointer(&c.commitIdx), cBool(f != nil))
	prev, _ = commitRegistry.unregister(prevIdx).(CommitFunc)
	return
}

// RollbackFunc registers a function that is invoked by SQLite when a
// transaction is rolled back. It returns the previous rollback handler, if any.
// https://www.sqlite.org/c3ref/commit_hook.html
func (c *Conn) RollbackFunc(f RollbackFunc) (prev RollbackFunc) {
	idx := rollbackRegistry.register(f)
	prevIdx := c.rollbackIdx
	c.rollbackIdx = idx
	C.set_rollback_hook(c.db, unsafe.Pointer(&c.rollbackIdx), cBool(f != nil))
	prev, _ = rollbackRegistry.unregister(prevIdx).(RollbackFunc)
	return
}

// UpdateFunc registers a function that is invoked by SQLite when a row is
// updated, inserted, or deleted. It returns the previous update handler, if
// any.
// https://www.sqlite.org/c3ref/update_hook.html
func (c *Conn) UpdateFunc(f UpdateFunc) (prev UpdateFunc) {
	idx := updateRegistry.register(f)
	prevIdx := c.updateIdx
	c.updateIdx = idx
	C.set_update_hook(c.db, unsafe.Pointer(&c.updateIdx), cBool(f != nil))
	prev, _ = updateRegistry.unregister(prevIdx).(UpdateFunc)
	return
}

// AuthorizerFunc registers a function that is invoked by SQLite During sql
// statement compilation. Function can return sqlite3.OK to accept statement,
// sqlite3.IGNORE to disallow specyfic action, but allow further statement
// processing, or sqlite3.DENY to deny action completly and stop processing.
// https://www.sqlite.org/c3ref/set_authorizer.html
func (c *Conn) AuthorizerFunc(f AuthorizerFunc) (prev AuthorizerFunc) {
	idx := authorizerRegistry.register(f)
	prevIdx := c.authorizerIdx
	c.authorizerIdx = idx
	C.set_set_authorizer(c.db, unsafe.Pointer(&c.authorizerIdx), cBool(f != nil))
	prev, _ = authorizerRegistry.unregister(prevIdx).(AuthorizerFunc)
	return
}
