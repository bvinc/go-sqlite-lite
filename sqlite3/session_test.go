package sqlite3_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
)

func initT(t *testing.T, conn *sqlite3.Conn) {
	err := conn.Exec(`INSERT INTO t (c1, c2, c3) VALUES ("1", "2", "3");`)
	if err != nil {
		t.Fatal(err)
	}

	err = conn.Exec(`INSERT INTO t (c1, c2, c3) VALUES ("4", "5", "6");`)
	if err != nil {
		t.Fatal(err)
	}
}

func fillSession(t *testing.T) (*sqlite3.Conn, *sqlite3.Session) {
	conn, err := sqlite3.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	err = conn.Exec("CREATE TABLE t (c1 PRIMARY KEY, c2, c3);")
	if err != nil {
		t.Fatal(err)
	}
	initT(t, conn) // two rows that predate the session

	s, err := conn.CreateSession("main")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Attach(""); err != nil {
		t.Fatal(err)
	}

	stmts := []string{
		`UPDATE t SET c1="one" WHERE c1="1";`,
		`UPDATE t SET c2="two", c3="three" WHERE c1="one";`,
		`UPDATE t SET c1="noop" WHERE c2="2";`,
		`DELETE FROM t WHERE c1="4";`,
		`INSERT INTO t (c1, c2, c3) VALUES ("four", "five", "six");`,
	}

	for _, stmt := range stmts {
		err := conn.Exec(stmt)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = conn.Exec("BEGIN;")
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := conn.Prepare("INSERT INTO t (c1, c2, c3) VALUES (?,?,?);")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	for i := int64(2); i < 100; i++ {
		stmt.Bind(i, "column2", "column3")
		if _, err := stmt.Step(); err != nil {
			t.Fatal(err)
		}
		stmt.Reset()
	}
	if err := conn.Exec("COMMIT;"); err != nil {
		t.Fatal(err)
	}

	return conn, s
}

func TestFillSession(t *testing.T) {
	conn, s := fillSession(t)
	s.Close()
	conn.Close()
}

func TestChangeset(t *testing.T) {
	conn, s := fillSession(t)
	defer func() {
		s.Close()
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	}()

	buf := new(bytes.Buffer)
	if err := s.Changeset(buf); err != nil {
		t.Fatal(err)
	}
	b := buf.Bytes()
	if len(b) == 0 {
		t.Errorf("changeset has no length")
	}

	iter, err := sqlite3.ChangesetIterStart(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	numChanges := 0
	num3Cols := 0
	opTypes := make(map[sqlite3.OpType]int)
	for {
		hasRow, err := iter.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !hasRow {
			break
		}
		table, numCols, opType, _, err := iter.Op()
		if err != nil {
			t.Fatalf("numChanges=%d, Op err: %v", numChanges, err)
		}
		if table != "t" {
			t.Errorf("table=%q, want t", table)
		}
		opTypes[opType]++
		if numCols == 3 {
			num3Cols++
		}
		numChanges++
	}
	if numChanges != 102 {
		t.Errorf("numChanges=%d, want 102", numChanges)
	}
	if num3Cols != 102 {
		t.Errorf("num3Cols=%d, want 102", num3Cols)
	}
	if got := opTypes[sqlite3.INSERT]; got != 100 {
		t.Errorf("num inserts=%d, want 100", got)
	}
	if err := iter.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestChangesetInvert(t *testing.T) {
	conn, s := fillSession(t)
	defer func() {
		s.Close()
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	}()

	buf := new(bytes.Buffer)
	if err := s.Changeset(buf); err != nil {
		t.Fatal(err)
	}
	b := buf.Bytes()

	buf = new(bytes.Buffer)
	if err := sqlite3.ChangesetInvert(buf, bytes.NewReader(b)); err != nil {
		t.Fatal(err)
	}
	invB := buf.Bytes()
	if len(invB) == 0 {
		t.Error("no inverted changeset")
	}
	if bytes.Equal(b, invB) {
		t.Error("inverted changeset is unchanged")
	}

	buf = new(bytes.Buffer)
	if err := sqlite3.ChangesetInvert(buf, bytes.NewReader(invB)); err != nil {
		t.Fatal(err)
	}
	invinvB := buf.Bytes()
	if !bytes.Equal(b, invinvB) {
		t.Error("inv(inv(b)) != b")
	}
}

func TestChangesetApply(t *testing.T) {
	conn, s := fillSession(t)
	defer func() {
		s.Close()
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	}()

	buf := new(bytes.Buffer)
	if err := s.Changeset(buf); err != nil {
		t.Fatal(err)
	}
	b := buf.Bytes()

	invBuf := new(bytes.Buffer)
	if err := sqlite3.ChangesetInvert(invBuf, bytes.NewReader(b)); err != nil {
		t.Fatal(err)
	}

	// Undo the entire session.
	if err := conn.ChangesetApply(invBuf, nil, nil); err != nil {
		t.Fatal(err)
	}

	// Table t should now be equivalent to the first two statements:
	//	INSERT INTO t (c1, c2, c3) VALUES ("1", "2", "3");
	//	INSERT INTO t (c1, c2, c3) VALUES ("4", "5", "6");
	want := []string{"1,2,3", "4,5,6"}
	var got []string
	stmt, err := conn.Prepare("SELECT c1, c2, c3 FROM t ORDER BY c1;")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := stmt.Close(); err != nil {
			t.Error(err)
		}
	}()

	for {
		hasRow, err := stmt.Step()
		if err != nil {
			t.Fatal(err)
		}
		if !hasRow {
			break
		}

		t0, _, _ := stmt.ColumnText(0)
		t1, _, _ := stmt.ColumnText(1)
		t2, _, _ := stmt.ColumnText(2)
		got = append(got, t0+","+t1+","+t2)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got=%v, want=%v", got, want)
	}
}

func TestPatchsetApply(t *testing.T) {
	conn, s := fillSession(t)
	defer func() {
		if s != nil {
			s.Close()
		}
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	}()

	var rowCountBefore int

	stmt1, err := conn.Prepare("SELECT COUNT(*) FROM t;")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := stmt1.Close(); err != nil {
			t.Error(err)
		}
	}()

	_, err = stmt1.Step()
	if err != nil {
		t.Fatal(err)
	}
	rowCountBefore, _, _ = stmt1.ColumnInt(0)

	buf := new(bytes.Buffer)
	if err := s.Patchset(buf); err != nil {
		t.Fatal(err)
	}
	b := buf.Bytes()

	s.Close()
	s = nil

	if err := conn.Exec("DELETE FROM t;"); err != nil {
		t.Fatal(err)
	}
	initT(t, conn)

	filterFnCalled := false
	filterFn := func(tableName string) bool {
		if tableName == "t" {
			filterFnCalled = true
			return true
		} else {
			t.Errorf("unexpected table in filter fn: %q", tableName)
			return false
		}
	}
	conflictFn := func(sqlite3.ConflictType, sqlite3.ChangesetIter) sqlite3.ConflictAction {
		t.Error("conflict applying patchset")
		return sqlite3.CHANGESET_ABORT
	}
	if err := conn.ChangesetApply(bytes.NewReader(b), filterFn, conflictFn); err != nil {
		t.Fatal(err)
	}
	if !filterFnCalled {
		t.Error("filter function not called")
	}

	var rowCountAfter int
	stmt2, err := conn.Prepare("SELECT COUNT(*) FROM t;")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := stmt2.Close(); err != nil {
			t.Error(err)
		}
	}()

	_, err = stmt2.Step()
	if err != nil {
		t.Fatal(err)
	}
	rowCountAfter, _, _ = stmt2.ColumnInt(0)

	if rowCountBefore != rowCountAfter {
		t.Errorf("row count is %d, want %d", rowCountAfter, rowCountBefore)
	}

	// Second application of patchset should fail.
	haveConflict := false
	conflictFn = func(ct sqlite3.ConflictType, iter sqlite3.ChangesetIter) sqlite3.ConflictAction {
		if ct == sqlite3.CHANGESET_CONFLICT {
			haveConflict = true
		} else {
			t.Errorf("unexpected conflict type: %v", ct)
		}
		_, _, opType, _, err := iter.Op()
		if err != nil {
			t.Errorf("conflict iter.Op() error: %v", err)
		}
		if opType != sqlite3.INSERT {
			t.Errorf("unexpected conflict op type: %v", opType)
		}
		return sqlite3.CHANGESET_ABORT
	}
	err = conn.ChangesetApply(bytes.NewReader(b), nil, conflictFn)
	sqlite_err, _ := err.(*sqlite3.Error)
	if code := sqlite_err.Code(); code != sqlite3.ABORT {
		t.Errorf("conflicting changeset Apply error is %v, want SQLITE_ABORT", err)
	}
	if !haveConflict {
		t.Error("no conflict found")
	}
}
