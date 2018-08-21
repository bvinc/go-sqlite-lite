# Upgrading sqlite

We need to make our own amalgamation, since we want to enable `SQLITE_ENABLE_UPDATE_DELETE_LIMIT` during the parser generator phase.

`-DSQLITE_ENABLE_UPDATE_DELETE_LIMIT=1` seems to be the only important option when creating the amalgamation.

```sh
wget https://www.sqlite.org/2018/sqlite-autoconf-XXXXXXX.tar.gz
unzip sqlite-src-XXXXXXX.zip 
cd sqlite-src-XXXXXXX
CFLAGS='-DSQLITE_ENABLE_UPDATE_DELETE_LIMIT=1' ./configure
make sqlite3.c
cp sqlite3.c ~/go/src/github.com/bvinc/go-sqlite-lite/sqlite3/lib/sqlite3.c
```