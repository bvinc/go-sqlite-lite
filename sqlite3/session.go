// Copyright 2018 The go-sqlite-lite Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
//
// This code was adapted from David Crawshaw's sqlite driver.
// https://github.com/crawshaw/sqlite
// The license to the original code is as follows:
//
// Copyright (c) 2018 David Crawshaw <david@zentus.com>
//
// Permission to use, copy, modify, and distribute this software for any
// purpose with or without fee is hereby granted, provided that the above
// copyright notice and this permission notice appear in all copies.
//
// THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
// WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
// ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
// WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
// ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
// OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

package sqlite3

/*
#include "sqlite3.h"

extern int strm_r_tramp(void*, char*, int*);
extern int strm_w_tramp(void*, char*, int);
extern int xapply_conflict_tramp(void*, int, sqlite3_changeset_iter*);
extern int xapply_filter_tramp(void*, char*);
*/
import "C"

import (
	"io"
	"unsafe"
)

var strmWriterReg = newRegistry()
var strmReaderReg = newRegistry()
var xapplyReg = newRegistry()

type Session struct {
	sess *C.sqlite3_session
}

// CreateSession creates a new session object.
// https://www.sqlite.org/session/sqlite3session_create.html
func (conn *Conn) CreateSession(db string) (*Session, error) {
	db += "\x00"
	s := &Session{}
	rc := C.sqlite3session_create(conn.db, cStr(db), &s.sess)
	if rc != OK {
		return nil, errStr(rc)
	}

	return s, nil
}

// Close closes a session object previously created with CreateSession.
// https://www.sqlite.org/session/sqlite3session_delete.html
func (s *Session) Close() {
	C.sqlite3session_delete(s.sess)
	s.sess = nil
}

// Enable enables recording of changes by a Session.
// New Sessions start enabled.
// https://www.sqlite.org/session/sqlite3session_enable.html
func (s *Session) Enable() {
	C.sqlite3session_enable(s.sess, 1)
}

// Disable disables recording of changes by a Session.
// https://www.sqlite.org/session/sqlite3session_enable.html
func (s *Session) Disable() {
	C.sqlite3session_enable(s.sess, 0)
}

// IsEnabled queries if the session is currently enabled.
// https://www.sqlite.org/session/sqlite3session_enable.html
func (s *Session) IsEnabled() bool {
	return C.sqlite3session_enable(s.sess, -1) != 0
}

// https://sqlite.org/session/sqlite3session_indirect.html
func (s *Session) IsIndirect() bool {
	return C.sqlite3session_indirect(s.sess, -1) != 0
}

// https://sqlite.org/session/sqlite3session_indirect.html
func (s *Session) SetIndirect(indirect bool) {
	C.sqlite3session_indirect(s.sess, cBool(indirect))
}

// https://sqlite.org/session/sqlite3session_isempty.html
func (s *Session) IsEmpty() bool {
	return C.sqlite3session_isempty(s.sess) != 0
}

// Attach attaches a table to a session object.  If the argument tab is equal
// to "", then changes are recorded for all tables in the database.
// https://www.sqlite.org/session/sqlite3session_attach.html
func (s *Session) Attach(tab string) error {
	var zTab *C.char
	if tab != "" {
		zTab = cStr(tab + "\x00")
	}
	rc := C.sqlite3session_attach(s.sess, zTab)
	if rc != OK {
		return errStr(rc)
	}
	return nil
}

func (s *Session) Diff(fromDB, tbl string) error {
	fromDB += "\x00"
	tbl += "\x00"
	// TODO:  provide a pointer to get a more accurate error message
	rc := C.sqlite3session_diff(s.sess, cStr(fromDB), cStr(tbl), nil)
	if rc != OK {
		return errStr(rc)
	}
	return nil
}

func (s *Session) Changeset(w io.Writer) error {
	idx := strmWriterReg.register(w)
	defer strmWriterReg.unregister(idx)

	rc := C.sqlite3session_changeset_strm(s.sess, (*[0]byte)(C.strm_w_tramp), unsafe.Pointer(&idx))
	if rc != OK {
		return errStr(rc)
	}
	return nil
}

// https://www.sqlite.org/session/sqlite3session_patchset.html
func (s *Session) Patchset(w io.Writer) error {
	idx := strmWriterReg.register(w)
	defer strmWriterReg.unregister(idx)

	rc := C.sqlite3session_patchset_strm(s.sess, (*[0]byte)(C.strm_w_tramp), unsafe.Pointer(&idx))
	if rc != OK {
		return errStr(rc)
	}
	return nil
}

// https://www.sqlite.org/session/sqlite3changeset_apply.html
func (conn *Conn) ChangesetApply(r io.Reader, filterFn func(tableName string) bool, conflictFn func(ConflictType, ChangesetIter) ConflictAction) error {
	readerIdx := strmReaderReg.register(r)
	defer strmReaderReg.unregister(readerIdx)

	x := &xapply{
		conn:       conn,
		filterFn:   filterFn,
		conflictFn: conflictFn,
	}

	xapplyIdx := xapplyReg.register(x)
	defer xapplyReg.unregister(xapplyIdx)

	var filterTramp, conflictTramp *[0]byte
	if x.filterFn != nil {
		filterTramp = (*[0]byte)(C.xapply_filter_tramp)
	}
	if x.conflictFn != nil {
		conflictTramp = (*[0]byte)(C.xapply_conflict_tramp)
	}

	rc := C.sqlite3changeset_apply_strm(conn.db, (*[0]byte)(C.strm_r_tramp), unsafe.Pointer(&readerIdx), filterTramp, conflictTramp, unsafe.Pointer(&xapplyIdx))
	if rc != OK {
		return errStr(rc)
	}

	return nil
}

// https://www.sqlite.org/session/sqlite3changeset_invert.html
func ChangesetInvert(w io.Writer, r io.Reader) error {
	readerIdx := strmReaderReg.register(r)
	defer strmReaderReg.unregister(readerIdx)

	writerIdx := strmWriterReg.register(w)
	defer strmWriterReg.unregister(writerIdx)
	rc := C.sqlite3changeset_invert_strm(
		(*[0]byte)(C.strm_r_tramp), unsafe.Pointer(&readerIdx),
		(*[0]byte)(C.strm_w_tramp), unsafe.Pointer(&writerIdx),
	)
	if rc != OK {
		return errStr(rc)
	}
	return nil
}

// https://www.sqlite.org/session/sqlite3changeset_concat.html
func ChangesetConcat(w io.Writer, r1, r2 io.Reader) error {
	readerIdx1 := strmReaderReg.register(r1)
	defer strmReaderReg.unregister(readerIdx1)
	readerIdx2 := strmReaderReg.register(r2)
	defer strmReaderReg.unregister(readerIdx2)
	writerIdx := strmWriterReg.register(w)
	defer strmWriterReg.unregister(writerIdx)

	rc := C.sqlite3changeset_concat_strm(
		(*[0]byte)(C.strm_r_tramp), unsafe.Pointer(&readerIdx1),
		(*[0]byte)(C.strm_r_tramp), unsafe.Pointer(&readerIdx2),
		(*[0]byte)(C.strm_w_tramp), unsafe.Pointer(&writerIdx),
	)
	if rc != OK {
		return errStr(rc)
	}
	return nil
}

type ChangesetIter struct {
	ptr       *C.sqlite3_changeset_iter
	readerIdx *int
	reader    io.Reader
}

// https://www.sqlite.org/session/sqlite3changeset_start.html
func ChangesetIterStart(r io.Reader) (ChangesetIter, error) {
	idx := strmReaderReg.register(r)
	iter := ChangesetIter{
		reader:    r,
		readerIdx: new(int),
	}
	*iter.readerIdx = idx
	// rc := C.sqlite3changeset_start_strm(&iter.ptr, (*[0]byte)(C.strm_r_tramp), unsafe.Pointer(&iter.readerIdx))
	rc := C.sqlite3changeset_start_strm(&iter.ptr, (*[0]byte)(C.strm_r_tramp), unsafe.Pointer(iter.readerIdx))
	if rc != OK {
		return ChangesetIter{}, errStr(rc)
	}
	return iter, nil
}

// https://www.sqlite.org/session/sqlite3changeset_finalize.html
func (iter ChangesetIter) Close() error {
	rc := C.sqlite3changeset_finalize(iter.ptr)
	iter.ptr = nil
	strmReaderReg.unregister(*iter.readerIdx)
	*iter.readerIdx = 0
	if rc != OK {
		return errStr(rc)
	}
	return nil
}

// https://www.sqlite.org/session/sqlite3changeset_old.html
func (iter ChangesetIter) Old(col int) (v Value, err error) {
	rc := C.sqlite3changeset_old(iter.ptr, C.int(col), &v.ptr)
	if rc != OK {
		return Value{}, errStr(rc)
	}
	return v, nil
}

// https://www.sqlite.org/session/sqlite3changeset_new.html
func (iter ChangesetIter) New(col int) (v Value, err error) {
	rc := C.sqlite3changeset_new(iter.ptr, C.int(col), &v.ptr)
	if rc != OK {
		return Value{}, errStr(rc)
	}
	return v, nil
}

// https://www.sqlite.org/session/sqlite3changeset_conflict.html
func (iter ChangesetIter) Conflict(col int) (v Value, err error) {
	rc := C.sqlite3changeset_conflict(iter.ptr, C.int(col), &v.ptr)
	if rc != OK {
		return Value{}, errStr(rc)
	}
	return v, nil
}

func (iter ChangesetIter) Next() (rowReturned bool, err error) {
	rc := C.sqlite3changeset_next(iter.ptr)
	switch rc {
	case ROW:
		return true, nil
	case DONE:
		return false, nil
	default:
		return false, errStr(rc)
	}
}

// https://www.sqlite.org/session/sqlite3changeset_op.html
func (iter ChangesetIter) Op() (table string, numCols int, opType OpType, indirect bool, err error) {
	var zTab *C.char
	var nCol, op, bIndirect C.int
	rc := C.sqlite3changeset_op(iter.ptr, &zTab, &nCol, &op, &bIndirect)
	if rc != OK {
		return "", 0, 0, false, errStr(rc)
	}
	table = C.GoString(zTab)
	numCols = int(nCol)
	opType = OpType(op)
	indirect = bIndirect != 0
	return table, numCols, opType, indirect, nil
}

// https://www.sqlite.org/session/sqlite3changeset_fk_conflicts.html
func (iter ChangesetIter) FKConflicts() (int, error) {
	var pnOut C.int
	rc := C.sqlite3changeset_fk_conflicts(iter.ptr, &pnOut)
	if rc != OK {
		return 0, errStr(rc)
	}
	return int(pnOut), nil
}

// https://www.sqlite.org/session/sqlite3changeset_pk.html
func (iter ChangesetIter) PK() ([]bool, error) {
	var pabPK *C.uchar
	var pnCol C.int
	rc := C.sqlite3changeset_pk(iter.ptr, &pabPK, &pnCol)
	if rc != OK {
		return nil, errStr(rc)
	}
	vals := (*[127]byte)(unsafe.Pointer(pabPK))[:pnCol:pnCol]
	cols := make([]bool, pnCol)
	for i, val := range vals {
		if val != 0 {
			cols[i] = true
		}
	}
	return cols, nil
}

type OpType int

type ConflictType int

const (
	CHANGESET_DATA        = C.SQLITE_CHANGESET_DATA
	CHANGESET_NOTFOUND    = C.SQLITE_CHANGESET_NOTFOUND
	CHANGESET_CONFLICT    = C.SQLITE_CHANGESET_CONFLICT
	CHANGESET_CONSTRAINT  = C.SQLITE_CHANGESET_CONSTRAINT
	CHANGESET_FOREIGN_KEY = C.SQLITE_CHANGESET_FOREIGN_KEY
)

type ConflictAction int

const (
	CHANGESET_OMIT    = C.SQLITE_CHANGESET_OMIT
	CHANGESET_ABORT   = C.SQLITE_CHANGESET_ABORT
	CHANGESET_REPLACE = C.SQLITE_CHANGESET_REPLACE
)

type Changegroup struct {
	ptr *C.sqlite3_changegroup
}

// https://www.sqlite.org/session/sqlite3changegroup_new.html
func NewChangegroup() (*Changegroup, error) {
	c := &Changegroup{}
	rc := C.sqlite3changegroup_new(&c.ptr)
	if rc != OK {
		return nil, errStr(rc)
	}
	return c, nil
}

// https://www.sqlite.org/session/sqlite3changegroup_add.html
func (cg Changegroup) Add(r io.Reader) error {
	idx := strmReaderReg.register(r)
	defer strmReaderReg.unregister(idx)
	rc := C.sqlite3changegroup_add_strm(cg.ptr, (*[0]byte)(C.strm_r_tramp), unsafe.Pointer(&idx))
	if rc != OK {
		return errStr(rc)
	}
	return nil
}

// Delete deletes a Changegroup.
//
// https://www.sqlite.org/session/sqlite3changegroup_delete.html
func (cg Changegroup) Delete() {
	C.sqlite3changegroup_delete(cg.ptr)
}

// https://www.sqlite.org/session/sqlite3changegroup_output.html
func (cg Changegroup) Output(w io.Writer) (err error) {
	idx := strmWriterReg.register(w)
	defer strmWriterReg.unregister(idx)

	rc := C.sqlite3changegroup_output_strm(cg.ptr, (*[0]byte)(C.strm_w_tramp), unsafe.Pointer(&idx))
	if rc != OK {
		return errStr(rc)
	}
	return nil
}

//export strm_w_tramp
func strm_w_tramp(pOut unsafe.Pointer, pData *C.char, n C.int) C.int {
	w := strmWriterReg.lookup(*(*int)(pOut)).(io.Writer)
	b := (*[1 << 30]byte)(unsafe.Pointer(pData))[:n:n]
	for len(b) > 0 {
		nw, err := w.Write(b)
		b = b[nw:]

		if err != nil {
			return C.SQLITE_IOERR
		}
	}
	return C.SQLITE_OK
}

//export strm_r_tramp
func strm_r_tramp(pIn unsafe.Pointer, pData *C.char, pnData *C.int) C.int {
	r := strmReaderReg.lookup(*(*int)(pIn)).(io.Reader)
	b := (*[1 << 30]byte)(unsafe.Pointer(pData))[:*pnData:*pnData]

	var n int
	var err error
	for n == 0 && err == nil {
		// Technically an io.Reader is allowed to return (0, nil)
		// and it is not treated as the end of the stream.
		//
		// So we spin here until the io.Reader is gracious enough
		// to get off its butt and actually do something.
		n, err = r.Read(b)
	}

	*pnData = C.int(n)
	if err != nil && err != io.EOF {
		return C.SQLITE_IOERR
	}
	return C.SQLITE_OK
}

type xapply struct {
	id         int
	conn       *Conn
	filterFn   func(string) bool
	conflictFn func(ConflictType, ChangesetIter) ConflictAction
}

//export xapply_filter_tramp
func xapply_filter_tramp(pCtx unsafe.Pointer, zTab *C.char) C.int {
	x := xapplyReg.lookup(*(*int)(pCtx)).(*xapply)

	tableName := C.GoString(zTab)
	if x.filterFn(tableName) {
		return 1
	}
	return 0
}

//export xapply_conflict_tramp
func xapply_conflict_tramp(pCtx unsafe.Pointer, eConflict C.int, p *C.sqlite3_changeset_iter) C.int {
	x := xapplyReg.lookup(*(*int)(pCtx)).(*xapply)

	action := x.conflictFn(ConflictType(eConflict), ChangesetIter{ptr: p})
	return C.int(action)
}
