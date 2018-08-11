// Copyright 2013 The Go-SQLite Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite3

/*
#include "sqlite3.h"
*/
import "C"

import (
	"fmt"
	"reflect"
	"unsafe"
)

// NamedArgs is a name/value map of arguments passed to a prepared statement
// that uses ?NNN, :AAA, @AAA, and/or $AAA parameter formats. Name matching is
// case-sensitive and the prefix character (one of [?:@$]) must be included in
// the name. Names that are missing from the map are treated as NULL. Names that
// are not used in the prepared statement are ignored.
//
// It is not possible to mix named and anonymous ("?") parameters in the same
// statement.
// [http://www.sqlite.org/lang_expr.html#varparam]
type NamedArgs map[string]interface{}

// RawString and RawBytes are special string and []byte types that may be used
// for database input and output without the cost of an extra copy operation.
//
// When used as an argument to a statement, the contents are bound using
// SQLITE_STATIC instead of SQLITE_TRANSIENT flag. This requires the contents to
// remain valid and unmodified until the end of statement execution. In
// particular, the caller must keep a reference to the value to prevent it from
// being garbage collected.
//
// When used for retrieving query output, the internal string/[]byte pointer is
// set to reference memory belonging to SQLite. The memory remains valid until
// another method is called on the Stmt object and should not be modified.
type (
	RawString string
	RawBytes  []byte
)

// Copy returns a Go-managed copy of s.
func (s RawString) Copy() string {
	if s == "" {
		return ""
	}
	h := (*reflect.StringHeader)(unsafe.Pointer(&s))
	return C.GoStringN((*C.char)(unsafe.Pointer(h.Data)), C.int(h.Len))
}

// Copy returns a Go-managed copy of b.
func (b RawBytes) Copy() []byte {
	if len(b) == 0 {
		if b == nil {
			return nil
		}
		return []byte("")
	}
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	return C.GoBytes(unsafe.Pointer(h.Data), C.int(h.Len))
}

// ZeroBlob is a special argument type used to allocate a zero-filled BLOB of
// the specified length. The BLOB can then be opened for incremental I/O to
// efficiently transfer a large amount of data. The maximum BLOB size can be
// queried with Conn.Limit(LIMIT_LENGTH, -1).
type ZeroBlob int

// Error is returned for all SQLite API result codes other than OK, ROW, and
// DONE.
type Error struct {
	rc  int
	msg string
}

// NewError creates a new Error instance using the specified SQLite result code
// and error message.
func NewError(rc int, msg string) *Error {
	return &Error{rc, msg}
}

func errStr(rc C.int) error {
	return &Error{int(rc), C.GoString(C.sqlite3_errstr(rc))}
}

func errMsg(rc C.int, db *C.sqlite3) error {
	if db == nil {
		return errStr(rc)
	}
	return &Error{int(rc), C.GoString(C.sqlite3_errmsg(db))}
}

// pkgErr reports an error originating in this package.
func pkgErr(rc int, msg string, v ...interface{}) error {
	if len(v) == 0 {
		return &Error{rc, msg}
	}
	return &Error{rc, fmt.Sprintf(msg, v...)}
}

// Code returns the SQLite extended result code.
func (err *Error) Code() int {
	return err.rc
}

// Error implements the error interface.
func (err *Error) Error() string {
	return fmt.Sprintf("sqlite3: %s [%d]", err.msg, err.rc)
}

// Errors returned for access attempts to closed or invalid objects.
var (
	ErrBadConn   = &Error{MISUSE, "closed or invalid connection"}
	ErrBadIO     = &Error{MISUSE, "closed or invalid incremental I/O operation"}
	ErrBadBackup = &Error{MISUSE, "closed or invalid backup operation"}
)

// Complete returns true if sql appears to contain a complete statement that is
// ready to be parsed. This does not validate the statement syntax.
// [http://www.sqlite.org/c3ref/complete.html]
func Complete(sql string) bool {
	if initErr != nil {
		return false
	}
	sql += "\x00"
	return C.sqlite3_complete(cStr(sql)) == 1
}

// ReleaseMemory attempts to free n bytes of heap memory by deallocating
// non-essential memory held by the SQLite library. It returns the number of
// bytes actually freed.
//
// This function is currently a no-op because SQLite is not compiled with the
// SQLITE_ENABLE_MEMORY_MANAGEMENT option.
// [http://www.sqlite.org/c3ref/release_memory.html]
func ReleaseMemory(n int) int {
	if initErr != nil {
		return 0
	}
	return int(C.sqlite3_release_memory(C.int(n)))
}

// SoftHeapLimit sets and/or queries the soft limit on the amount of heap memory
// that may be allocated by SQLite. A negative value for n keeps the current
// limit, while 0 removes the limit. The previous limit value is returned, with
// negative values indicating an error.
// [http://www.sqlite.org/c3ref/soft_heap_limit64.html]
func SoftHeapLimit(n int64) int64 {
	if initErr != nil {
		return -1
	}
	return int64(C.sqlite3_soft_heap_limit64(C.sqlite3_int64(n)))
}

// SourceID returns the check-in identifier of SQLite within its configuration
// management system.
// [http://www.sqlite.org/c3ref/c_source_id.html]
func SourceID() string {
	if initErr != nil {
		return ""
	}
	return C.GoString(C.sqlite3_sourceid())
}

// Status returns the current and peak values of a core performance
// counter, specified by one of the STATUS constants. If reset is true, the peak
// value is reset back down to the current value after retrieval.
// [http://www.sqlite.org/c3ref/status.html]
func Status(op int, reset bool) (cur, peak int, err error) {
	if initErr != nil {
		return 0, 0, initErr
	}
	var cCur, cPeak C.int
	rc := C.sqlite3_status(C.int(op), &cCur, &cPeak, cBool(reset))
	if rc != OK {
		return 0, 0, pkgErr(MISUSE, "invalid status op (%d)", op)
	}
	return int(cCur), int(cPeak), nil
}

// Version returns the SQLite version as a string in the format "X.Y.Z[.N]".
// [http://www.sqlite.org/c3ref/libversion.html]
func Version() string {
	if initErr != nil {
		return ""
	}
	return goStr(C.sqlite3_libversion())
}

// VersionNum returns the SQLite version as an integer in the format X*1000000 +
// Y*1000 + Z, where X is the major version, Y is the minor version, and Z is
// the release number.
func VersionNum() int {
	if initErr != nil {
		return 0
	}
	return int(C.sqlite3_libversion_number())
}

// raw casts s to a RawString.
func raw(s string) RawString {
	return RawString(s)
}

// cStr returns a pointer to the first byte in s.
func cStr(s string) *C.char {
	h := (*reflect.StringHeader)(unsafe.Pointer(&s))
	return (*C.char)(unsafe.Pointer(h.Data))
}

// cStrOffset returns the offset of p in s or -1 if p doesn't point into s.
func cStrOffset(s string, p *C.char) int {
	h := (*reflect.StringHeader)(unsafe.Pointer(&s))
	if off := uintptr(unsafe.Pointer(p)) - h.Data; off < uintptr(h.Len) {
		return int(off)
	}
	return -1
}

// cBytes returns a pointer to the first byte in b.
func cBytes(b []byte) unsafe.Pointer {
	return unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&b)).Data)
}

// cBool returns a C representation of a Go bool (false = 0, true = 1).
func cBool(b bool) C.int {
	if b {
		return 1
	}
	return 0
}

// goStr returns a Go representation of a null-terminated C string.
func goStr(p *C.char) (s string) {
	if p != nil && *p != 0 {
		h := (*reflect.StringHeader)(unsafe.Pointer(&s))
		h.Data = uintptr(unsafe.Pointer(p))
		for *p != 0 {
			p = (*C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(p)) + 1)) // p++
		}
		h.Len = int(uintptr(unsafe.Pointer(p)) - h.Data)
	}
	return
}

// goStrN returns a Go representation of an n-byte C string.
func goStrN(p *C.char, n C.int) (s string) {
	if n > 0 {
		h := (*reflect.StringHeader)(unsafe.Pointer(&s))
		h.Data = uintptr(unsafe.Pointer(p))
		h.Len = int(n)
	}
	return
}

// goBytes returns a Go representation of an n-byte C array.
func goBytes(p unsafe.Pointer, n C.int) (b []byte) {
	if n > 0 {
		h := (*reflect.SliceHeader)(unsafe.Pointer(&b))
		h.Data = uintptr(p)
		h.Len = int(n)
		h.Cap = int(n)
	}
	return
}

// bstr returns a string pointing into the byte slice b.
func bstr(b []byte) (s string) {
	if len(b) > 0 {
		h := (*reflect.StringHeader)(unsafe.Pointer(&s))
		h.Data = uintptr(unsafe.Pointer(&b[0]))
		h.Len = len(b)
	}
	return
}
