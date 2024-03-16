# A database/sql driver for FrontBase

This package is a [database/sql driver](https://pkg.go.dev/database/sql)
  for the [FrontBase database](http://frontbase.com/).

## Todo

- Proper test suite for the main driver.
    - select all supported types, including NULL.
    - insert all supported types, including NULL.
    - esp. handle NULL values when iterating Rows.
- Support context cancellation where possible.
- Pass the [compatibility test suite](https://github.com/bradfitz/go-sql-test).
- Add support for BLOBs.
- Support comments in the prepared statements SQL parser.
- Doc: build and use with macOS.
- Doc: build and use with Docker (and therefore linux).
