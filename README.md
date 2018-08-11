# go-sqlite-lite

Don't use this yet.


# Getting started
```go
 conn := sqlite3.Open("mydatabase.db")
 conn.Exec(`CREATE TABLE sums(a INTEGER, b INTEGER, c INTEGER)`)
 
 stmt := conn.Prepare(`INSERT INTO TABLE sums VALUES(?, ?, ?)`)
 stmt.Bind(2, 2, 4)
 stmt.Step()
 stmt.Close()
```
