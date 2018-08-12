# go-sqlite-lite

go-sqlite-lite is a SQLite driver for the Go programming language.  It is designed with the following goals in mind.

* **Lightweight** - Methods should be little more than a small wrapper around SQLite C functions.
* **Performance** - Where possible, methods should be available to allow for the highest peformance possible.
* **Understandable** - You should always know what SQLite functions are being called and in what order.
* **Unsurprising** - Connections, PRAGMAs, transactions, bindings, and stepping should work out of the box exactly as you would expect with SQLite.
* **Debugable** - When you encounter a SQLite error, the SQLite documentation should be relevant and relatable to the Go code.
* **Ergonomic** - Where it makes sense, convenient compound methods should exist to make tasks easy.

Most database drivers include a layer to work nicely with the Go `database/sql` layer.

## Getting started

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
```
