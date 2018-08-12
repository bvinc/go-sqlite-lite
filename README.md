# go-sqlite-lite

go-sqlite-lite is a SQLite driver for the Go programming language.  It is designed with the following goals in mind.

* **Lightweight** - Most methods should be little more than a small wrapper around SQLite C functions.
* **Performance** - Where possible, methods should be available to allow for the highest peformance possible.
* **Understandable** - You should always know what SQLite functions are being called and in what order.
* **Unsurprising** - Connections, PRAGMAs, transactions, bindings, and stepping should work out of the box exactly as you would expect with SQLite.
* **Debugable** - When you encounter a SQLite error, the SQLite documentation should be relevant and relatable to the Go code.
* **Ergonomic** - Where it makes sense, convenient compound methods should exist to make tasks easy and to conform to Go standard interfaces.

Most database drivers include a layer to work nicely with the Go `database/sql` interface, which introduces connection pooling and behavior differences from pure SQLite.  This driver does not include a `database/sql` interface.

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
```

### Executing SQL
```go
err = conn.Exec(`CREATE TABLE student(name STRING, age INTEGER)`)
if err != nil {
	...
}
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

## Advanced Features
* Binding parameters to statements using SQLite named parameters.
* SQLite Blob Incremental IO API.
* SQLite Online Backup API.
* RawString and RawBytes can be used to reduce copying between Go and SQLite.

## Credit
This project begain as a fork of https://github.com/mxk/go-sqlite/

## FAQ

* **Why is there no `database/sql` interface?**

If a `database/sql` interface is required, please use https://github.com/mattn/go-sqlite3 .  In my experience, using a `database/sql` interface with SQLite is painful.  Connection pooling causes unnecessary overhead and wierdness.  Transactions using `Exec("BEGIN")` don't work as expected.  Your connection does not correspond to SQLite's concept of a connection.  PRAGMA commands do not work as expected.  When you hit SQLite errors, such as locking or busy errors, it's difficult to discover why since you don't know which connection received which SQL and in what order.

* **What are the differences betwen this driver and the mxk/go-sqlite driver?**

This driver was forked from `mxk/go-sqlite-driver`.  It hasn't been maintained in years and used an ancient version of SQLite.  A large number of features were removed, reworked, and renamed.  A lot of smartness and state was removed.  It is now much easier to upgrade to newer versions of SQLite since the `codec` feature was removed.  The behavior of methods now lines up closely with the behavior of SQLite's C API.

* **Is it thread safe?**

go-sqlite-lite is as thread safe as SQLite.  SQLite with this driver is compiled with `-DSQLITE_THREADSAFE=2` which is **Multi-thread** mode.  In this mode, SQLite can be safely used by multiple threads provided that no single database connection is used simultaneously in two or more threads.  Consult the SQLite documentation for more information.

https://www.sqlite.org/threadsafe.html
https://www.sqlite.org/sharedcache.html
https://www.sqlite.org/wal.html

## License
This project is licensed under the BSD license.
