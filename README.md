# sqlx-odbc

Experimental ODBC-driver for [sqlx](https://github.com/launchbadge/sqlx)

TODOs (not exausting):

- Fix lifetime mess (currently I use lots of calls to `mem::transmute`)
- Fetch rows in batches (currenly is fetched one value at a time)
- Fix behaviour of rows (currently they can live longer than they are valid)
- Maybe switch to using `odbc_sys` direcly (this introduced a lot of the lifetime issues)
