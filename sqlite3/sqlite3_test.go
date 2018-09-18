// Copyright 2018 The go-sqlite-lite Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite3

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
	"unsafe"
)

// skip, when set to true, causes all remaining tests to be skipped.
var skip = false

type T struct{ *testing.T }

func begin(t *testing.T) T {
	if skip {
		t.SkipNow()
	}
	return T{t}
}

func (t T) skipRestIfFailed() {
	// skip = skip || t.Failed()
}

func (t T) open(name string) *Conn {
	c, err := Open(name)
	if c == nil || err != nil {
		t.Fatalf(cl("Open(%q) unexpected error: %v"), name, err)
	}
	return c
}

func (t T) close(c io.Closer) {
	if c != nil {
		if db, _ := c.(*Conn); db != nil {
			if path := db.FileName("main"); path != "" {
				defer os.Remove(path)
			}
		}
		if err := c.Close(); err != nil {
			if !t.Failed() {
				t.Fatalf(cl("(%T).Close() unexpected error: %v"), c, err)
			}
			t.FailNow()
		}
	}
}

func (t T) prepare(c *Conn, args ...interface{}) (s *Stmt) {
	var sql string
	var err error
	sql = args[0].(string)
	s, err = c.Prepare(sql, args[1:]...)
	if s == nil || err != nil {
		t.Fatalf(cl("(%T).Prepare(%q) unexpected error: %v"), c, sql, err)
	}
	return
}

func (t T) bind(s *Stmt, args ...interface{}) {
	err := s.Bind(args...)
	if s == nil || err != nil {
		t.Fatalf(cl("(%T).Bind(%q) unexpected error: %v"), s, args, err)
	}
}

func (t T) exec(cs interface{}, args ...interface{}) {
	var sql string
	var err error
	if c, ok := cs.(*Conn); ok {
		sql = args[0].(string)
		err = c.Exec(sql, args[1:]...)
	} else {
		s := cs.(*Stmt)
		err = s.Exec(args...)
	}
	if err != nil {
		t.Fatalf(cl("(%T).Exec(%q) unexpected error: %v"), cs, sql, err)
	}
}

func (t T) reset(s *Stmt) {
	if err := s.Reset(); err != nil {
		t.Fatalf(cl("s.Reset() unexpected error: %v"), err)
	}
}

func (t T) scan(s *Stmt, dst ...interface{}) {
	if err := s.Scan(dst...); err != nil {
		t.Fatalf(cl("s.Scan() unexpected error: %v"), err)
	}
}

func (t T) step(s *Stmt, wantRow bool) {
	haveRow, haveErr := s.Step()
	if haveErr != nil {
		t.Fatalf(cl("s.Step() expected success; got %v"), haveErr)
	}
	if haveRow != wantRow {
		t.Fatalf(cl("s.Next() expected row %v; got row %v"), wantRow, haveRow)
	}
}

func (t T) stepErr(s *Stmt) {
	haveRow, haveErr := s.Step()
	if haveErr == nil {
		t.Fatalf(cl("s.Step() expected an error; got success"))
	}
	if haveRow {
		t.Fatalf(cl("s.Step() expected an error; which it got, but it also got a row"))
	}
}

func (t T) tmpFile() string {
	f, err := ioutil.TempFile("", "go-sqlite.db.")
	if err != nil {
		t.Fatalf(cl("tmpFile() unexpected error: %v"), err)
	}
	defer f.Close()
	return f.Name()
}

func (t T) errCode(have error, want int) {
	if e, ok := have.(*Error); !ok || e.Code() != want {
		t.Fatalf(cl("errCode() expected error code [%d]; got %v"), want, have)
	}
}

func cl(s string) string {
	_, thisFile, _, _ := runtime.Caller(1)
	_, testFile, line, ok := runtime.Caller(2)
	if ok && thisFile == testFile {
		return fmt.Sprintf("%d: %s", line, s)
	}
	return s
}

func sHdr(s string) *reflect.StringHeader {
	return (*reflect.StringHeader)(unsafe.Pointer(&s))
}

func bHdr(b []byte) *reflect.SliceHeader {
	return (*reflect.SliceHeader)(unsafe.Pointer(&b))
}

func TestLib(T *testing.T) {
	t := begin(T)
	defer t.skipRestIfFailed()

	if v, min := VersionNum(), 3024000; v < min {
		t.Errorf("VersionNum() expected >= %d; got %d", min, v)
	}

	sql := "CREATE TABLE x(a)"
	if Complete(sql) {
		t.Errorf("Complete(%q) expected false", sql)
	}
	if sql += ";"; !Complete(sql) {
		t.Errorf("Complete(%q) expected true", sql)
	}
}

func TestCreate(T *testing.T) {
	t := begin(T)
	defer t.skipRestIfFailed()

	checkPath := func(c *Conn, name, want string) {
		if have := c.FileName(name); have != want {
			t.Fatalf(cl("c.FileName() expected %q; got %q"), want, have)
		}
	}
	sql := "CREATE TABLE x(a); INSERT INTO x VALUES(1);"
	tmp := t.tmpFile()

	// File
	os.Remove(tmp)
	c := t.open(tmp)
	defer t.close(c)
	checkPath(c, "main", tmp)
	t.exec(c, sql)
	if err := c.Close(); err != nil {
		t.Fatalf("c.Close() unexpected error: %v", err)
	}
	if err := c.Exec(sql); err == nil {
		t.Fatalf("c.Exec() expected an error")
	}

	// URI (existing)
	uri := strings.NewReplacer("?", "%3f", "#", "%23").Replace(tmp)
	if runtime.GOOS == "windows" {
		uri = "/" + strings.Replace(uri, "\\", "/", -1)
	}
	c = t.open("file:" + uri)
	defer t.close(c)
	checkPath(c, "main", tmp)
	t.exec(c, "INSERT INTO x VALUES(2)")

	// Temporary (in-memory)
	c = t.open(":memory:")
	defer t.close(c)
	checkPath(c, "main", "")
	t.exec(c, sql)

	// Temporary (file)
	c = t.open("")
	defer t.close(c)
	checkPath(c, "main", "")
	t.exec(c, sql)
}

func TestPrepare(T *testing.T) {
	t := begin(T)
	defer t.skipRestIfFailed()

	sql := `
		CREATE TABLE x(a, b, c, d, e);
		INSERT INTO x VALUES(NULL, 123, 1.23, 'TEXT', x'424C4F42');
	`
	type row struct {
		a interface{}
		b int
		c float64
		d string
		e []byte
	}
	want := &row{nil, 123, 1.23, "TEXT", []byte("BLOB")}
	have := &row{}

	c := t.open(":memory:")
	defer t.close(c)
	t.exec(c, sql)

	s := t.prepare(c, "SELECT * FROM x")
	defer t.close(s)
	t.step(s, true)
	t.scan(s, &have.a, &have.b, &have.c, &have.d, &have.e)
	if !reflect.DeepEqual(have, want) {
		t.Errorf("s.Scan() expected %v; got %v", want, have)
	}

	t.step(s, false)
	t.close(s)
	s.Bind()
	t.stepErr(s)
}

func TestScan(T *testing.T) {
	t := begin(T)
	defer t.skipRestIfFailed()

	type types struct {
		v         interface{}
		int       int
		int64     int64
		float64   float64
		bool      bool
		string    string
		bytes     []byte
		RawString RawString
		RawBytes  RawBytes
		Writer    io.Writer
	}
	scan := func(s *Stmt, dst ...interface{}) {
		// Re-query to avoid interference from type conversion
		t.reset(s)
		s.ClearBindings()
		t.step(s, true)

		t.scan(s, dst...)
	}
	skipCols := make([]interface{}, 0, 16)
	scanNext := func(s *Stmt, have, want *types) {
		scan(s, append(skipCols, &have.v)...)
		scan(s, append(skipCols, &have.int)...)
		scan(s, append(skipCols, &have.int64)...)
		scan(s, append(skipCols, &have.float64)...)
		scan(s, append(skipCols, &have.bool)...)
		scan(s, append(skipCols, &have.string)...)
		scan(s, append(skipCols, &have.bytes)...)
		scan(s, append(skipCols, have.Writer)...)

		// RawString must be copied, RawBytes (last access) can be used directly
		scan(s, append(skipCols, &have.RawString)...)
		have.RawString = RawString(have.RawString.Copy())
		scan(s, append(skipCols, &have.RawBytes)...)

		if !reflect.DeepEqual(have, want) {
			t.Errorf(cl("scanNext() expected\n%#v; got\n%#v"), want, have)
			if have.Writer != want.Writer {
				t.Errorf(cl("not equal:  expected\n%#v; got\n%#v"), want.Writer, have.Writer)
			}
			if !reflect.DeepEqual(have.Writer, want.Writer) {
				t.Errorf(cl("not deep equal:  expected\n%#v; got\n%#v"), want.Writer, have.Writer)
			}
			t.Fatalf(cl("Failed"))
		}
		skipCols = append(skipCols, nil)
	}

	c := t.open(":memory:")
	defer t.close(c)
	t.exec(c, `
		CREATE TABLE x(a, b, c, d, e, f, g, h, i);
		INSERT INTO x VALUES(NULL, '', x'', 0, 0.0, 4.2, 42, '42', x'3432');
	`)
	s := t.prepare(c, "SELECT * FROM x")
	defer t.close(s)

	t.step(s, true)

	// Verify data types
	wantT := []uint8{NULL, TEXT, BLOB, INTEGER, FLOAT, FLOAT, INTEGER, TEXT, BLOB}
	if haveT := s.ColumnTypes(); !reflect.DeepEqual(haveT, wantT) {
		t.Fatalf(cl("s.ColumnTypes() expected %v; got %v"), wantT, haveT)
	}

	// NULL
	have := &types{Writer: new(bytes.Buffer)}
	want := &types{Writer: new(bytes.Buffer)}
	scanNext(s, have, want)

	// ''
	want.v = ""
	want.bytes = []byte{}
	want.RawBytes = []byte{}
	scanNext(s, have, want)

	// x''
	want.v = []byte{}
	scanNext(s, have, want)

	// 0
	want.v = int64(0)
	want.string = "0"
	want.bytes = []byte("0")
	want.RawString = RawString("0")
	want.RawBytes = RawBytes("0")
	want.Writer.Write([]byte("0"))
	scanNext(s, have, want)

	// 0.0
	want.v = 0.0
	want.string = "0.0"
	want.bytes = []byte("0.0")
	want.RawString = RawString("0.0")
	want.RawBytes = RawBytes("0.0")
	want.Writer.Write([]byte("0.0"))
	scanNext(s, have, want)

	// 4.2
	want.v = 4.2
	want.int = 4
	want.int64 = 4
	want.float64 = 4.2
	want.bool = true
	want.string = "4.2"
	want.bytes = []byte("4.2")
	want.RawString = RawString("4.2")
	want.RawBytes = RawBytes("4.2")
	want.Writer.Write([]byte("4.2"))
	scanNext(s, have, want)

	// 42
	want.v = int64(42)
	want.int = 42
	want.int64 = 42
	want.float64 = 42
	want.string = "42"
	want.bytes = []byte("42")
	want.RawString = RawString("42")
	want.RawBytes = RawBytes("42")
	want.Writer.Write([]byte("42"))
	scanNext(s, have, want)

	// '42'
	want.v = "42"
	want.Writer.Write([]byte("42"))
	scanNext(s, have, want)

	// x'3432'
	want.v = []byte("42")
	want.Writer.Write([]byte("42"))
	scanNext(s, have, want)

	// Zero destinations
	t.scan(s)

	// Unsupported type
	var f32 float32
	t.errCode(s.Scan(&f32), MISUSE)

	// EOF
	t.step(s, false)
}

func TestScanDynamic(T *testing.T) {
	t := begin(T)
	defer t.skipRestIfFailed()

	type row struct {
		a, b, c, d interface{}
	}
	scanNext := func(s *Stmt, have, want *row) {
		t.scan(s, &have.a, &have.b, &have.c, &have.d)
		if !reflect.DeepEqual(have, want) {
			t.Errorf("%s %s\n", reflect.TypeOf(want.c), reflect.TypeOf(have.c))
			t.Errorf("%s %s\n", reflect.TypeOf(want.d), reflect.TypeOf(have.d))
			t.Fatalf(cl("scanNext() expected\n%#v; got\n%#v"), want, have)
		}
		if haveRow, err := s.Step(); !haveRow {
			t.reset(s)
			t.step(s, true)
		} else if err != nil {
			t.Fatalf(cl("s.Next() unexpected error: %v"), err)
		}
	}

	c := t.open(":memory:")
	defer t.close(c)
	t.exec(c, `
		-- Affinity: NONE, INTEGER, REAL, TEXT
		CREATE TABLE x(a, b INTEGER, c FLOAT, d TEXT);
		INSERT INTO x VALUES(NULL, NULL, NULL, NULL);
		INSERT INTO x VALUES('', '', '', '');
		INSERT INTO x VALUES(x'', x'', x'', x'');
		INSERT INTO x VALUES(0, 0, 0, 0);
		INSERT INTO x VALUES(0.0, 0.0, 0.0, 0.0);
		INSERT INTO x VALUES(4.2, 4.2, 4.2, 4.2);
		INSERT INTO x VALUES(42, 42, 42, 42);
		INSERT INTO x VALUES('42', '42', '42', '42');
		INSERT INTO x VALUES(x'3432', x'3432', x'3432', x'3432');
	`)
	s := t.prepare(c, "SELECT * FROM x ORDER BY rowid")
	defer t.close(s)
	t.step(s, true)

	// NULL
	have, want := &row{}, &row{}
	scanNext(s, have, want)

	// ''
	want = &row{"", "", "", ""}
	scanNext(s, have, want)

	// x''
	want = &row{[]byte{}, []byte{}, []byte{}, []byte{}}
	scanNext(s, have, want)

	// 0
	want = &row{int64(0), int64(0), 0.0, "0"}
	scanNext(s, have, want)

	// 0.0
	want = &row{0.0, int64(0), 0.0, "0.0"}
	scanNext(s, have, want)

	// 4.2
	want = &row{4.2, 4.2, 4.2, "4.2"}
	scanNext(s, have, want)

	// 42
	want = &row{int64(42), int64(42), 42.0, "42"}
	scanNext(s, have, want)

	// '42'
	want = &row{"42", int64(42), 42.0, "42"}
	scanNext(s, have, want)

	// x'3432'
	want = &row{[]byte("42"), []byte("42"), []byte("42"), []byte("42")}
	scanNext(s, have, want)
}

func TestTail(T *testing.T) {
	t := begin(T)
	defer t.skipRestIfFailed()

	c := t.open(":memory:")
	defer t.close(c)

	check := func(sql, tail string) {
		s, _ := c.Prepare(sql)
		if s != nil {
			defer t.close(s)
		}
		if s == nil {
			return
		}

		if s.Tail != tail {
			t.Errorf(cl("tail expected %q; got %q"), tail, s.Tail)
		}
	}
	head := "CREATE TABLE x(a);"
	tail := " -- comment"

	check("", "")
	check(head, "")
	check(head+tail, tail)
	check(tail, "")
}

func TestParams(T *testing.T) {
	t := begin(T)
	defer t.skipRestIfFailed()

	c := t.open(":memory:")
	defer t.close(c)
	t.exec(c, "CREATE TABLE x(a, b, c, d)")

	dt := func(v interface{}) uint8 {
		switch v.(type) {
		case int64:
			return INTEGER
		case float64:
			return FLOAT
		case string:
			return TEXT
		case []byte:
			return BLOB
		}
		return NULL
	}
	verify := func(_a, _b, _c, _d interface{}) {
		s := t.prepare(c, "SELECT * FROM x ORDER BY rowid LIMIT 1")
		defer t.close(s)

		t.step(s, true)

		wantT := []uint8{dt(_a), dt(_b), dt(_c), dt(_d)}
		if haveT := s.ColumnTypes(); !reflect.DeepEqual(haveT, wantT) {
			t.Fatalf(cl("s.ColumnTypes() expected %v; got %v"), wantT, haveT)
		}

		type row struct{ a, b, c, d interface{} }
		want, have := &row{_a, _b, _c, _d}, &row{}
		t.scan(s, &have.a, &have.b, &have.c, &have.d)
		if !reflect.DeepEqual(have, want) {
			t.Fatalf(cl("verify() expected\n%#v; got\n%#v"), want, have)
		}
		t.exec(c, "DELETE FROM x WHERE rowid=(SELECT min(rowid) FROM x)")
	}

	// Unnamed
	sql := "INSERT INTO x VALUES(?, ?, ?, ?)"
	s := t.prepare(c, sql)
	defer t.close(s)

	t.exec(s, nil, nil, nil, nil)
	verify(nil, nil, nil, nil)

	t.exec(s, int(0), int(1), int64(math.MinInt64), int64(math.MaxInt64))
	verify(int64(0), int64(1), int64(math.MinInt64), int64(math.MaxInt64))

	t.exec(s, 0.0, 1.0, math.SmallestNonzeroFloat64, math.MaxFloat64)
	verify(0.0, 1.0, math.SmallestNonzeroFloat64, math.MaxFloat64)

	t.exec(s, false, true, "", "x\x00y")
	verify(int64(0), int64(1), "", "x\x00y")

	t.exec(s, []byte(nil), []byte{}, []byte{0}, []byte("1"))
	verify(nil, []byte{}, []byte{0}, []byte("1"))

	t.exec(s, int64(0), int64(1), RawString(""), RawString("x"))
	verify(int64(0), int64(1), "", "x")

	t.exec(s, RawBytes(""), RawBytes("x"), ZeroBlob(0), ZeroBlob(2))
	verify([]byte{}, []byte("x"), []byte{}, []byte{0, 0})

	// Named
	s = t.prepare(c, "INSERT INTO x VALUES(:a, @B, :a, $d)")
	defer t.close(s)

	t.exec(s, NamedArgs{})
	verify(nil, nil, nil, nil)

	t.exec(s, 0, 1, 2)
	verify(int64(0), int64(1), int64(0), int64(2))

	t.exec(s, NamedArgs{":a": "a", "@B": "b", "$d": "d", "$c": nil})
	verify("a", "b", "a", "d")
	s.ClearBindings()

	t.exec(s, NamedArgs{"@B": RawString("hello"), "$d": RawBytes("world")})
	verify(nil, "hello", nil, []byte("world"))

	// Conn.Prepare
	s, err := c.Prepare(sql, 1, 2, 3, 4)
	if err != nil {
		t.Fatal("failed to insert 1,2,3,4")
	}
	defer s.Close()
	t.step(s, false)
	verify(int64(1), int64(2), int64(3), int64(4))

	// Conn.Exec
	t.exec(c, "INSERT INTO x VALUES(?, ?, NULL, NULL)", 1, 2)
	t.exec(c, "INSERT INTO x VALUES(NULL, NULL, ?, ?);", 3, 4)
	verify(int64(1), int64(2), nil, nil)
	verify(nil, nil, int64(3), int64(4))

	t.exec(c, "INSERT INTO x VALUES($a, $b, NULL, NULL);", NamedArgs{"$a": "a", "$b": "b"})
	t.exec(c, "INSERT INTO x VALUES($a, $a, $c, $d);", NamedArgs{"$a": "a", "$b": "b", "$c": "c", "$d": "d"})
	verify("a", "b", nil, nil)
	verify("a", "a", "c", "d")
}

func TestTx(T *testing.T) {
	t := begin(T)
	defer t.skipRestIfFailed()

	c := t.open(":memory:")
	defer t.close(c)
	t.exec(c, "CREATE TABLE x(a)")

	// Begin/Commit
	if err := c.Begin(); err != nil {
		t.Fatalf("c.Begin() unexpected error: %v", err)
	}
	t.exec(c, "INSERT INTO x VALUES(1)")
	t.exec(c, "INSERT INTO x VALUES(2)")
	if err := c.Commit(); err != nil {
		t.Fatalf("c.Commit() unexpected error: %v", err)
	}

	// Begin/Rollback
	if err := c.Begin(); err != nil {
		t.Fatalf("c.Begin() unexpected error: %v", err)
	}
	t.exec(c, "INSERT INTO x VALUES(3)")
	t.exec(c, "INSERT INTO x VALUES(4)")
	if err := c.Rollback(); err != nil {
		t.Fatalf("c.Rollback() unexpected error: %v", err)
	}

	// Verify
	s := t.prepare(c, "SELECT * FROM x ORDER BY rowid")
	defer t.close(s)
	t.step(s, true)

	var i int
	if t.scan(s, &i); i != 1 {
		t.Fatalf("s.Scan() expected 1; got %d", i)
	}
	t.step(s, true)
	if t.scan(s, &i); i != 2 {
		t.Fatalf("s.Scan() expected 2; got %d", i)
	}
	t.step(s, false)
}

func TestIO(T *testing.T) {
	t := begin(T)

	c := t.open(":memory:")
	defer t.close(c)
	t.exec(c, "CREATE TABLE x(a)")
	t.exec(c, "INSERT INTO x VALUES(?)", ZeroBlob(8))
	t.exec(c, "INSERT INTO x VALUES(?)", "hello, world")

	// Open
	b, err := c.BlobIO("main", "x", "a", 1, true)
	if b == nil || err != nil {
		t.Fatalf("c.BlobIO() unexpected error: %v", err)
	}
	defer t.close(b)

	// State
	if b.Conn() != c {
		t.Fatalf("b.Conn() expected %p; got %p", c, b.Conn())
	}
	if b.Row() != 1 {
		t.Fatalf("b.Row() expected 1; got %d", b.Row())
	}
	if b.Len() != 8 {
		t.Fatalf("b.Len() expected 8; got %d", b.Len())
	}

	// Write
	in := []byte("1234567")
	if n, err := b.Write(in); n != 7 || err != nil {
		t.Fatalf("b.Write(%q) expected 7, <nil>; got %d, %v", in, n, err)
	}
	in = []byte("89")
	if n, err := b.Write(in); n != 0 || err != ErrBlobFull {
		t.Fatalf("b.Write(%q) expected 0, ErrBlobFull; got %d, %v", in, n, err)
	}

	// Reopen
	if err := b.Reopen(2); err != nil {
		t.Fatalf("b.Reopen(2) unexpected error: %v", err)
	}
	if b.Row() != 2 {
		t.Fatalf("b.Row() expected 2; got %d", b.Row())
	}
	if b.Len() != 12 {
		t.Fatalf("b.Len() expected 12; got %d", b.Len())
	}

	// Read
	for i := 0; i < 2; i++ {
		out := make([]byte, 13)
		if n, err := b.Read(out); n != 12 || err != nil {
			t.Fatalf("b.Read() #%d expected 12, <nil>; got %d, %v", i, n, err)
		}
		have := string(out)
		if want := "hello, world\x00"; have != want {
			t.Fatalf("b.Read() #%d expected %q; got %q", i, have, want)
		}
		if p, err := b.Seek(0, 0); p != 0 || err != nil {
			t.Fatalf("b.Seek() #%d expected 0, <nil>; got %d, %v", i, p, err)
		}
	}

	// Close
	t.close(b)
	if err := b.Reopen(1); err != ErrBadIO {
		t.Fatalf("b.Reopen(1) expected %v; got %v", ErrBadIO, err)
	}

	// Verify
	s := t.prepare(c, "SELECT * FROM x ORDER BY rowid")
	defer t.close(s)
	var have string
	t.step(s, true)
	t.scan(s, &have)
	if want := "1234567\x00"; have != want {
		t.Fatalf("s.Scan() expected %q; got %q", want, have)
	}
	t.step(s, true)
	t.scan(s, &have)
	if want := "hello, world"; have != want {
		t.Fatalf("s.Scan() expected %q; got %q", want, have)
	}
	t.step(s, false)
}

func TestBackup(T *testing.T) {
	t := begin(T)

	c1, c2 := t.open(":memory:"), t.open(":memory:")
	defer t.close(c1)
	defer t.close(c2)
	t.exec(c1, "CREATE TABLE x(a)")
	t.exec(c1, "INSERT INTO x VALUES(?)", "1234567\x00")
	t.exec(c1, "INSERT INTO x VALUES(?)", "hello, world")

	// Backup
	b, err := c1.Backup("main", c2, "main")
	if b == nil || err != nil {
		t.Fatalf("b.Backup() unexpected error: %v", err)
	}
	defer t.close(b)
	if pr, pt := b.Progress(); pr != 0 || pt != 0 {
		t.Fatalf("b.Progress() expected 0, 0; got %d, %d", pr, pt)
	}
	if err = b.Step(1); err != nil {
		t.Fatalf("b.Step(1) expected <nil>; got %v", err)
	}
	if pr, pt := b.Progress(); pr != 1 || pt != 2 {
		t.Fatalf("b.Progress() expected 1, 2; got %d, %d", pr, pt)
	}
	if err = b.Step(-1); err != io.EOF {
		t.Fatalf("b.Step(-1) expected EOF; got %v", err)
	}

	// Close
	t.close(b)
	if err = b.Step(-1); err != ErrBadBackup {
		t.Fatalf("b.Step(-1) expected %v; got %v", ErrBadBackup, err)
	}

	// Verify
	s := t.prepare(c2, "SELECT * FROM x ORDER BY rowid")
	defer t.close(s)
	t.step(s, true)

	var have string
	t.scan(s, &have)
	if want := "1234567\x00"; have != want {
		t.Fatalf("s.Scan() expected %q; got %q", want, have)
	}
	t.step(s, true)
	t.scan(s, &have)
	if want := "hello, world"; have != want {
		t.Fatalf("s.Scan() expected %q; got %q", want, have)
	}
	t.step(s, false)
}

func TestSchema(T *testing.T) {
	t := begin(T)

	c := t.open(":memory:")
	defer t.close(c)
	t.exec(c, "CREATE TABLE x(a int)")
	t.exec(c, "INSERT INTO x VALUES(1)")
	t.exec(c, "INSERT INTO x VALUES(2)")

	checkCols := func(s *Stmt, want ...string) {
		if have := s.ColumnCount(); have != len(want) {
			t.Fatalf(cl("s.ColumnCount() expected %d; got %d"), len(want), have)
		}
		if have := s.ColumnNames(); !reflect.DeepEqual(have, want) {
			t.Fatalf(cl("s.ColumnNames() expected %v; got %v"), want, have)
		}
	}
	checkDecls := func(s *Stmt, want ...string) {
		if have := s.DeclTypes(); !reflect.DeepEqual(have, want) {
			t.Fatalf(cl("s.DeclTypes() expected %v; got %v"), want, have)
		}
	}
	s := t.prepare(c, "SELECT * FROM x ORDER BY rowid")
	defer t.close(s)

	t.step(s, true)

	checkCols(s, "a")
	checkDecls(s, "int")
	var a interface{}
	t.scan(s, &a)
	if a != int64(1) {
		t.Fatal("Expected 1, got", a)
	}

	// Schema changes do not affect running statements
	t.exec(c, "ALTER TABLE x ADD b text")
	t.step(s, true)

	checkCols(s, "a")
	checkDecls(s, "int")
	t.scan(s, &a)
	if a != int64(2) {
		t.Fatal("Expected 2")
	}
	t.step(s, false)

	checkCols(s, "a")
	checkDecls(s, "int")
	t.reset(s)
	t.step(s, true)

	checkCols(s, "a", "b")
	checkDecls(s, "int", "text")
	var b interface{}
	t.scan(s, &a, &b)
	if a != int64(1) {
		t.Fatal("Expected 1")
	}
	if b != nil {
		t.Fatal("Expected nil")
	}
	t.step(s, true)

	checkCols(s, "a", "b")
	checkDecls(s, "int", "text")
	t.scan(s, &a, &b)
	if a != int64(2) {
		t.Fatal("Expected 2")
	}
	if b != nil {
		t.Fatal("Expected nil")
	}
	t.step(s, false)
}

func TestUpdateDeleteLimit(T *testing.T) {
	t := begin(T)

	c := t.open(":memory:")
	defer t.close(c)
	t.exec(c, "CREATE TABLE x(a INTEGER PRIMARY KEY)")
	t.exec(c, "INSERT INTO x VALUES(?)", 1)
	t.exec(c, "INSERT INTO x VALUES(?)", 2)
	t.exec(c, "INSERT INTO x VALUES(?)", 3)
	t.exec(c, "INSERT INTO x VALUES(?)", 4)
	t.exec(c, "INSERT INTO x VALUES(?)", 5)
	t.exec(c, "UPDATE x SET a = a + 10 WHERE a >= 1 ORDER BY a LIMIT 2 OFFSET 1")
	t.exec(c, "DELETE FROM x WHERE a >= 10 ORDER BY a LIMIT 1 OFFSET 1")
}

func TestStringNull(T *testing.T) {
	t := begin(T)

	c := t.open(":memory:")
	defer t.close(c)
	t.exec(c, "CREATE TABLE x(a TEXT)")
	t.exec(c, "INSERT INTO x VALUES(?)", nil)
	s := t.prepare(c, "SELECT * from x")
	defer s.Close()

	t.step(s, true)
	x := "overwriteme"
	s.Scan(&x)
	if x != "" {
		t.Fatal("Expected empty string")
	}
	x2, ok, err := s.ColumnText(0)
	if x2 != "" {
		t.Fatal("Expected empty string")
	}
	if ok != false {
		t.Fatal("Expected ok==false")
	}
	if err != nil {
		t.Fatal("Expected err==nil")
	}
}

func TestStringNullByteSlice(T *testing.T) {
	t := begin(T)

	c := t.open(":memory:")
	defer t.close(c)
	t.exec(c, "CREATE TABLE x(a TEXT)")
	t.exec(c, "INSERT INTO x VALUES(?)", nil)
	s := t.prepare(c, "SELECT * from x")
	defer s.Close()

	t.step(s, true)
	var x []byte
	s.Scan(&x)
	if x != nil {
		t.Fatal("Expected nil byte slice")
	}
	x2, err := s.ColumnBlob(0)
	if x2 != nil {
		t.Fatal("Expected nil byte slice")
	}
	if err != nil {
		t.Fatal("Expected err==nil")
	}
}

func TestRawStringNull(T *testing.T) {
	t := begin(T)

	c := t.open(":memory:")
	defer t.close(c)
	t.exec(c, "CREATE TABLE x(a TEXT)")
	t.exec(c, "INSERT INTO x VALUES(?)", nil)
}

func TestTxHandler(T *testing.T) {
	t := begin(T)
	defer t.skipRestIfFailed()

	c := t.open(":memory:")
	defer t.close(c)
	t.exec(c, "CREATE TABLE x(a)")

	commit := 0
	rollback := 0
	c.CommitFunc(func() (abort bool) { commit++; return commit >= 2 })
	c.RollbackFunc(func() { rollback++ })

	// Allow
	c.Begin()
	t.exec(c, "INSERT INTO x VALUES(1)")
	t.exec(c, "INSERT INTO x VALUES(2)")
	if err := c.Commit(); err != nil {
		t.Fatalf("c.Commit() unexpected error: %v", err)
	}

	// Deny
	c.Begin()
	t.exec(c, "INSERT INTO x VALUES(3)")
	t.exec(c, "INSERT INTO x VALUES(4)")
	t.errCode(c.Commit(), CONSTRAINT_COMMITHOOK)

	// Verify
	if commit != 2 || rollback != 1 {
		t.Fatalf("commit/rollback expected 2/1; got %d/%d", commit, rollback)
	}
	s := t.prepare(c, "SELECT * FROM x ORDER BY rowid")
	defer t.close(s)
	var i int
	t.step(s, true)
	if t.scan(s, &i); i != 1 {
		t.Fatalf("s.Scan() expected 1; got %d", i)
	}
	t.step(s, true)
	if t.scan(s, &i); i != 2 {
		t.Fatalf("s.Scan() expected 2; got %d", i)
	}
	t.step(s, false)
}

func TestUpdateHandler(T *testing.T) {
	t := begin(T)
	defer t.skipRestIfFailed()

	c := t.open(":memory:")
	defer t.close(c)
	t.exec(c, "CREATE TABLE x(a)")

	type update struct {
		op      int
		db, tbl string
		row     int64
	}
	var have *update
	verify := func(want *update) {
		if !reflect.DeepEqual(have, want) {
			t.Fatalf(cl("verify() expected %v; got %v"), want, have)
		}
	}
	c.UpdateFunc(func(op int, db, tbl RawString, row int64) {
		have = &update{op, db.Copy(), tbl.Copy(), row}
	})

	t.exec(c, "INSERT INTO x VALUES(1)")
	verify(&update{INSERT, "main", "x", 1})

	t.exec(c, "INSERT INTO x VALUES(2)")
	verify(&update{INSERT, "main", "x", 2})

	t.exec(c, "UPDATE x SET a=3 WHERE rowid=1")
	verify(&update{UPDATE, "main", "x", 1})

	t.exec(c, "DELETE FROM x WHERE rowid=2")
	verify(&update{DELETE, "main", "x", 2})
}

func TestBusyHandler(T *testing.T) {
	t := begin(T)

	tmp := t.tmpFile()
	c1 := t.open(tmp)
	defer t.close(c1)
	c2 := t.open(tmp)
	defer t.close(c2)
	t.exec(c1, "CREATE TABLE x(a); BEGIN; INSERT INTO x VALUES(1);")

	try := func(sql string) {
		err := c2.Exec(sql)
		t.errCode(err, BUSY)
	}

	// Default
	try("INSERT INTO x VALUES(2)")

	// Built-in
	c2.BusyTimeout(100 * time.Millisecond)
	try("INSERT INTO x VALUES(3)")

	// Custom
	calls := 0
	handler := func(count int) (retry bool) {
		calls++
		time.Sleep(10 * time.Millisecond)
		return calls == count+1 && calls < 10
	}
	c2.BusyFunc(handler)
	try("INSERT INTO x VALUES(4)")
	if calls != 10 {
		t.Fatalf("calls expected 10; got %d", calls)
	}

	// Disable
	c2.BusyTimeout(0)
	try("INSERT INTO x VALUES(5)")
}

func TestLocked(T *testing.T) {
	t := begin(T)
	defer t.skipRestIfFailed()

	c := t.open(":memory:")
	defer t.close(c)
	t.exec(c, "CREATE TABLE x(a)")

	// Allow
	c.Begin()
	t.exec(c, "INSERT INTO x VALUES(1)")
	t.exec(c, "INSERT INTO x VALUES(2)")
	if err := c.Commit(); err != nil {
		t.Fatalf("c.Commit() unexpected error: %v", err)
	}

	s := t.prepare(c, "SELECT * FROM x ORDER BY rowid")
	defer t.close(s)
	t.step(s, true)

	s2 := t.prepare(c, "DROP TABLE x")
	defer s2.Close()
	_, err := s2.Step()
	if err == nil {
		t.Fatalf("expected SQLITE_LOCKED")
	}
}
