[![GoDoc](https://godoc.org/github.com/bvinc/go-sqlite-lite/sqlite3?status.svg)](https://godoc.org/github.com/bvinc/go-sqlite-lite/sqlite3)
[![Build Status](https://travis-ci.com/bvinc/go-sqlite-lite.svg?branch=master)](https://travis-ci.com/bvinc/go-sqlite-lite)
[![Build status](https://ci.appveyor.com/api/projects/status/xk6fpk23wb5ppdhx?svg=true)](https://ci.appveyor.com/project/bvinc/go-sqlite-lite)


# go-sqlite-lite

go-sqlite-lite is a SQLite driver for the Go programming language.  It is designed with the following goals in mind.

* **Lightweight** - Most methods should be little more than a small wrapper around SQLite C functions.
* **Performance** - Where possible, methods should be available to allow for the highest performance possible.
* **Understandable** - You should always know what SQLite functions are being called and in what order.
* **Unsurprising** - Connections, PRAGMAs, transactions, bindings, and stepping should work out of the box exactly as you would expect with SQLite.
* **Debuggable** - When you encounter a SQLite error, the SQLite documentation should be relevant and relatable to the Go code.
* **Ergonomic** - Where it makes sense, convenient compound methods should exist to make tasks easy and to conform to Go standard interfaces.

Most database drivers include a layer to work nicely with the Go `database/sql` interface, which introduces connection pooling and behavior differences from pure SQLite.  This driver does not include a `database/sql` interface.

## Releases

* 2018-09-16 **v0.3.1** - Forgot to update sqlite3.h
* 2018-09-16 **v0.3.0** - SQLite version 3.25.0
* 2018-09-14 **v0.2.0** - Proper error and NULL handling on Column* methods.  Empty blobs and empty strings are now distinct from NULL in all cases.  A nil byte slice is interpreted as NULL for binding purposes as well as Column* methods.
* 2018-09-01 **v0.1.2** - Added Column methods to Stmt, and WithTx methods to Conn
* 2018-08-25 **v0.1.1** - Fixed linking on some Linux systems
* 2018-08-21 **v0.1.0** - SQLite version 3.24.0

## Getting started

```go
import "github.com/bvinc/go-sqlite-lite/sqlite3"
```

### Acquiring a connection
```go
conn, err := sqlite3.Open("mydatabase.db")
if err != nil {
	...
}
defer conn.Close()

// It's always a good idea to set a busy timeout
conn.BusyTimeout(5 * time.Second)
```

### Executing SQL
```go
err = conn.Exec(`CREATE TABLE student(name STRING, age INTEGER)`)
if err != nil {
	...
}
// Exec can optionally bind parameters
err = conn.Exec(`INSERT INTO student VALUES (?, ?)`, "Bob", 18)
if err != nil {
	...
}
```

### Using Prepared Statements
```go
stmt, err := conn.Prepare(`INSERT INTO student VALUES (?, ?)`)
if err != nil {
	...
}
defer stmt.Close()

// Bind the arguments
err = stmt.Bind("Bill", 18); err != nil {
	...
}
// Step the statement
hasRow, err := stmt.Step()
if err != nil {
	...
}
// Reset the statement
err = stmt.Reset()
if err != nil {
	...
}
```

### Using Prepared Statements Conveniently
```go
stmt, err := conn.Prepare(`INSERT INTO student VALUES (?, ?)`)
if err != nil {
	...
}
defer stmt.Close()

// Exec binds arguments, steps the statement to completion, and always resets the statement
err = stmt.Exec("John", 19)
if err != nil {
	...
}
```

### Using Queries Conveniently
```go
// Prepare can prepare a statement and optionally also bind arguments
stmt, err := conn.Prepare(`SELECT name, age FROM student WHERE age = ?`, 18)
if err != nil {
	...
}
defer stmt.Close()

for {
	hasRow, err := stmt.Step()
	if err != nil {
		...
	}
	if !hasRow {
		// The query is finished
		break
	}

	// Use Scan to access column data from a row
	var name string
	var age int
	err = stmt.Scan(&name, &age)
	if err != nil {
		...
	}
	fmt.Println("name:", name, "age:", age)
}
// Remember to Reset the statement if you would like to Bind new arguments and reuse the prepared staement
```

### Using Transactions
```go
err := conn.Begin()
if err != nil {
	...
}

// Do some work
...

err := conn.Commit()
if err != nil {
	...
}
```

### Using Transactions Conveniently

With error handling in Go, it can be pretty inconvenient to ensure that a transaction is rolled back in the case of an error.  The `WithTx` method is provided, which accepts a function of work to do inside of a transaction.  `WithTx` will begin the transaction and call the function.  If the function returns an error, the transaction will be rolled back.  If the function succeeds, the transaction will be committed.  `WithTx` can be a little awkward to use, but it's necessary.  For example:

```go
err := conn.WithTx(func() error {
	return insertStudents(conn)
})
if err != nil {
	...
}

func insertStudents(conn *sqlite3.Conn) error {
	...
}
```

## Advanced Features
* Binding parameters to statements using SQLite named parameters.
* SQLite Blob Incremental IO API.
* SQLite Online Backup API.
* Supports setting a custom busy handler
* Supports callback hooks on commit, rollback, and update.
* If shared cache mode is enabled and one statement receives a `SQLITE_LOCKED` error, the SQLITE [unlock_notify](https://sqlite.org/unlock_notify.html) extension is used to transparently block and try again when the conflicting statement finishes.
* Compiled with SQLite support for JSON1, RTREE, FTS5, GEOPOLY, STAT4, and SOUNDEX.
* Compiled with SQLite support for OFFSET/LIMIT on UPDATE and DELETE statements.
* RawString and RawBytes can be used to reduce copying between Go and SQLite.  Please use with caution.

## Credit
This project begain as a fork of https://github.com/mxk/go-sqlite/

## FAQ

* **Why is there no `database/sql` interface?**

If a `database/sql` interface is required, please use https://github.com/mattn/go-sqlite3 .  In my experience, using a `database/sql` interface with SQLite is painful.  Connection pooling causes unnecessary overhead and weirdness.  Transactions using `Exec("BEGIN")` don't work as expected.  Your connection does not correspond to SQLite's concept of a connection.  PRAGMA commands do not work as expected.  When you hit SQLite errors, such as locking or busy errors, it's difficult to discover why since you don't know which connection received which SQL and in what order.

* **What are the differences betwen this driver and the mxk/go-sqlite driver?**

This driver was forked from `mxk/go-sqlite-driver`.  It hadn't been maintained in years and used an ancient version of SQLite.  A large number of features were removed, reworked, and renamed.  A lot of smartness and state was removed.  It is now much easier to upgrade to newer versions of SQLite since the `codec` feature was removed.  The behavior of methods now lines up closely with the behavior of SQLite's C API.

* **What are the differences betwen this driver and the crawshaw/sqlite driver?**

The crawshaw driver is pretty well thought out and solves a lot of the same problems as this
driver.  There are a few places where our philosophies differ.  The crawshaw driver defaults (when flags of 0 are given) to SQLite shared cache mode and WAL mode.  The default WAL synchronous mode is changed.  Prepared statements are transparently cached.  Connection pools are provided.  I would be opposed to making most of these changes to this driver.  I would like this driver to provide a default, light, and unsurprising SQLite experience.

The crawshaw driver also supports the SQLite session extension, which this driver currently does not.

* **Are finalizers provided to automatically close connections and statements?**

No finalizers are used in this driver.  You are responsible for closing connections and statements.  While I mostly agree with finalizers for cleaning up most accidental resource leaks, in this case, finalizers may fix errors such as locking errors while debugging only to find that the code works unreliably in production.  Removing finalizers makes the behavior consistent.

* **Is it thread safe?**

go-sqlite-lite is as thread safe as SQLite.  SQLite with this driver is compiled with `-DSQLITE_THREADSAFE=2` which is **Multi-thread** mode.  In this mode, SQLite can be safely used by multiple threads provided that no single database connection is used simultaneously in two or more threads.  This applies to goroutines.  A single database conneciton should not be used simultaneously between two goroutines.

It is safe to use separate connection instances concurrently, even if they are accessing the same database file. For example:
```go
// ERROR (without any extra synchronization)
c, _ := sqlite3.Open("sqlite.db")
go use(c)
go use(c)
```
```go
// OK
c1, _ := sqlite3.Open("sqlite.db")
c2, _ := sqlite3.Open("sqlite.db")
go use(c1)
go use(c2)
```

Consult the SQLite documentation for more information.

https://www.sqlite.org/threadsafe.html

* **How do I pool connections for handling HTTP requests?**

Opening new connections is cheap and connection pooling is generally unnecessary for SQLite.  I would recommend that you open a new connection for each request that you're handling.  This ensures that each request is handled separately and the normal rules of SQLite database/table locking apply.

If you've decided that pooling connections provides you with an advantage, it would be outside the scope of this package and something that you would need to implement and ensure works as needed.

## License
This project is licensed under the BSD license.

